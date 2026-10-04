//! Exact prepared Chat dispatch with a receipt-bound native return codec.
use super::*;
use std::collections::VecDeque;
use switchyard_protocol::{PinnedResponseCodec, PreparedChat};
use switchyard_translation::{StreamTranslationState, TranslationEngine};

const LIMIT: usize = 16 * 1024 * 1024;
fn invalid(message: &str) -> LlmClientError {
    LlmClientError::RequestEncoding(message.into())
}
fn conversion(message: &str) -> LlmClientError {
    LlmClientError::ResponseTranslation(message.into())
}

impl TranslatingLlmClient {
    pub(super) async fn call_prepared_chat(
        &self,
        llm_request: LlmRequest,
        metadata: Option<Metadata>,
        model: &ModelId,
    ) -> Result<Response> {
        let prepared = metadata
            .as_ref()
            .and_then(|m| m.prepared_chat.as_ref())
            .ok_or_else(|| invalid("prepared Chat missing"))?
            .clone();
        let backend = self
            .backend_for(model, WireFormat::OpenAiChat)
            .ok_or_else(|| invalid("prepared Chat backend missing"))?;
        if metadata.as_ref().and_then(|m| m.wire_format) != Some(WireFormat::AnthropicMessages)
            || prepared.normalized != llm_request
            || prepared.body["model"] != model.as_str()
            || !matches!(backend, Backend::OpenAiChat(_))
            || !backend.extra_body().is_empty()
            || !backend.omit_body_fields().is_empty()
            || backend.reasoning_effort().is_some()
        {
            return Err(invalid(
                "prepared Chat destination, body or worker controls changed",
            ));
        }
        let codec = Codec::new(&prepared)?;
        let streaming = prepared.body["stream"].as_bool().unwrap_or(false);
        record_gen_ai_request(&backend.url(), model, streaming);
        let mut builder =
            self.authenticated_request(&backend.url(), backend, &prepared.body, metadata.as_ref());
        if let Some(timeout) = backend.timeout() {
            builder = builder.timeout(timeout);
        }
        // A staged receipt authorizes one dispatch. Backend retry settings never
        // authorize repeating a possibly accepted prepared generation.
        let response = builder.send().await.map_err(convert_reqwest_error)?;
        let status = response.status();
        metrics::record_upstream_attempt(Some(status.as_u16()));
        if !status.is_success() {
            return Err(LlmClientError::UpstreamHttp {
                status,
                body: "prepared Chat provider refused request".into(),
            });
        }
        let upstream_headers = response.headers().clone();
        let llm_response = if streaming {
            let start = codec.call("stream_start", json!({})).await?;
            let mut state = StreamState {
                response,
                codec,
                buffer: Vec::new(),
                ready: VecDeque::new(),
                held: Vec::new(),
                sequence: 0,
                finish: false,
                done: false,
                received: 0,
                native_received: 0,
                translation: StreamTranslationState::default(),
            };
            if start.get("frames").is_some() {
                state.native(start, false)?;
            }
            let stream = stream::try_unfold(state, |mut state| async move {
                loop {
                    if let Some(raw) = state.ready.pop_front() {
                        let event = TranslationEngine::default()
                            .decode_stream_event(&mut state.translation, "anthropic_messages", raw)
                            .map_err(|_| conversion("native response observation failed"))?;
                        return Ok::<_, LlmClientError>(Some((event, state)));
                    }
                    if state.done {
                        return Ok(None);
                    }
                    state.advance().await?;
                }
            })
            .boxed();
            LlmResponse::Stream(stream)
        } else {
            let provider = read_json(response).await?;
            if provider.get("error").is_some()
                || !provider["choices"].as_array().is_some_and(|v| v.len() == 1)
                || provider["choices"][0]["finish_reason"].as_str().is_none()
            {
                return Err(conversion("prepared Chat response incomplete"));
            }
            let output = codec.call("response", json!({"body":provider})).await?;
            if !output["body"].is_object() {
                return Err(conversion("native response body missing"));
            }
            LlmResponse::Agg(
                decode_aggregated_response(&output["body"], WireFormat::AnthropicMessages)
                    .map_err(|_| conversion("native response observation failed"))?,
            )
        };
        Ok(Response {
            llm_response,
            metadata,
            upstream_headers,
        })
    }
}
async fn read_json(mut response: reqwest::Response) -> Result<Value> {
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(convert_reqwest_error)? {
        if bytes.len().saturating_add(chunk.len()) > LIMIT {
            return Err(conversion("prepared response exceeds bound"));
        }
        bytes.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&bytes).map_err(|_| conversion("prepared response JSON invalid"))
}
struct Codec {
    capability: PinnedResponseCodec,
    client: reqwest::Client,
    url: reqwest::Url,
}
impl Codec {
    fn new(prepared: &PreparedChat) -> Result<Self> {
        let c = &prepared.codec;
        let base = reqwest::Url::parse(&c.endpoint).map_err(|_| invalid("codec URL invalid"))?;
        if base.scheme() != "http"
            || !matches!(base.host_str(), Some("127.0.0.1" | "[::1]"))
            || !base.username().is_empty()
            || base.password().is_some()
            || base.query().is_some()
            || base.fragment().is_some()
            || c.owner_id.is_empty()
            || c.session_token.is_empty()
            || c.implementation_sha256.len() != 64
            || !c
                .implementation_sha256
                .bytes()
                .all(|b| b.is_ascii_hexdigit())
        {
            return Err(invalid("codec capability invalid"));
        }
        let url = reqwest::Url::parse(&format!("{}/codec", c.endpoint.trim_end_matches('/')))
            .map_err(|_| invalid("codec URL invalid"))?;
        let client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|_| invalid("codec client unavailable"))?;
        Ok(Self {
            capability: c.clone(),
            client,
            url,
        })
    }
    async fn call(&self, operation: &str, extra: Value) -> Result<Value> {
        let c = &self.capability;
        let mut body = json!({"owner_id":c.owner_id,"session_token":c.session_token,"implementation_sha256":c.implementation_sha256,"operation":operation});
        if let (Some(target), Some(extra)) = (body.as_object_mut(), extra.as_object()) {
            target.extend(extra.clone());
        }
        let response = self
            .client
            .post(self.url.clone())
            .json(&body)
            .send()
            .await
            .map_err(|_| conversion("codec service unavailable"))?;
        if !response.status().is_success() {
            return Err(conversion("codec operation refused"));
        }
        let result = read_json(response).await?;
        if result["implementation_sha256"] != c.implementation_sha256 {
            return Err(conversion("codec reply pin mismatch"));
        }
        Ok(result)
    }
}
struct StreamState {
    response: reqwest::Response,
    codec: Codec,
    buffer: Vec<u8>,
    ready: VecDeque<Value>,
    held: Vec<Value>,
    sequence: u64,
    finish: bool,
    done: bool,
    received: usize,
    native_received: usize,
    translation: StreamTranslationState,
}
fn data(frame: &str) -> String {
    frame
        .lines()
        .filter_map(|line| line.strip_prefix("data:").map(str::trim_start))
        .collect::<Vec<_>>()
        .join("\n")
}
impl StreamState {
    fn native(&mut self, reply: Value, release: bool) -> Result<()> {
        for frame in reply["frames"]
            .as_array()
            .ok_or_else(|| conversion("codec frames missing"))?
        {
            let frame = frame
                .as_str()
                .ok_or_else(|| conversion("codec frame invalid"))?;
            self.native_received = self.native_received.saturating_add(frame.len());
            if self.native_received > LIMIT
                || !(frame.ends_with("\n\n") || frame.ends_with("\r\n\r\n"))
            {
                return Err(conversion("codec frame exceeds bound or is incomplete"));
            }
            let value: Value = serde_json::from_str(&data(frame))
                .map_err(|_| conversion("codec event invalid"))?;
            if value["type"] == "error" {
                return Err(conversion("codec emitted failure"));
            }
            if value["type"] == "message_stop" || !self.held.is_empty() {
                self.held.push(value);
            } else {
                self.ready.push_back(value);
            }
        }
        if release {
            if !self.held.iter().any(|v| v["type"] == "message_stop") {
                return Err(conversion("codec terminal missing"));
            }
            self.ready.extend(self.held.drain(..));
        }
        Ok(())
    }
    async fn advance(&mut self) -> Result<()> {
        loop {
            let lf = self
                .buffer
                .windows(2)
                .position(|w| w == b"\n\n")
                .map(|n| (n, 2));
            let crlf = self
                .buffer
                .windows(4)
                .position(|w| w == b"\r\n\r\n")
                .map(|n| (n, 4));
            if let Some((end, delimiter)) =
                lf.into_iter().chain(crlf).min_by_key(|(offset, _)| *offset)
            {
                let frame = String::from_utf8(self.buffer.drain(..end + delimiter).collect())
                    .map_err(|_| conversion("provider SSE invalid"))?;
                let data = data(&frame);
                if data.is_empty() {
                    continue;
                }
                if data == "[DONE]" {
                    if !self.finish || !self.buffer.iter().all(u8::is_ascii_whitespace) {
                        return Err(conversion(
                            "provider terminal before finish or trailing data",
                        ));
                    }
                } else {
                    let value: Value = serde_json::from_str(&data)
                        .map_err(|_| conversion("provider SSE JSON invalid"))?;
                    if value.get("error").is_some() {
                        return Err(conversion("provider stream failed"));
                    }
                    if let Some(choices) = value["choices"].as_array() {
                        if choices.len() > 1 {
                            return Err(conversion("multiple choices unsupported"));
                        }
                        if let Some(choice) = choices.first() {
                            self.finish |= choice["finish_reason"].as_str().is_some();
                        }
                    }
                }
                let reply = self
                    .codec
                    .call(
                        "stream_push",
                        json!({"sequence":self.sequence,"frame":frame}),
                    )
                    .await?;
                self.sequence += 1;
                self.native(reply, false)?;
                if data == "[DONE]" {
                    let reply = self
                        .codec
                        .call("stream_finish", json!({"sequence":self.sequence}))
                        .await?;
                    self.native(reply, true)?;
                    self.done = true;
                }
                return Ok(());
            }
            let chunk = self
                .response
                .chunk()
                .await
                .map_err(convert_reqwest_error)?
                .ok_or_else(|| conversion("provider terminal missing"))?;
            self.received = self.received.saturating_add(chunk.len());
            if self.received > LIMIT {
                return Err(conversion("provider stream exceeds bound"));
            }
            self.buffer.extend_from_slice(&chunk);
        }
    }
}
