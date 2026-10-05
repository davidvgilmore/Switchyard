//! Native Messages sessions prepared by an operator-managed local ARC service.
use super::*;
use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};
use switchyard_protocol::{PinnedResponseCodec, PreparedChat, PreparedMessages, WireFormat};

static REQUESTS: AtomicU64 = AtomicU64::new(0);

/// Local service owning episode state and private steering transactions.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionConfig {
    /// Loopback HTTP base URL exposing prepare, commit, and abort.
    pub endpoint: String,
    /// Stable host installation identity, separate from each conversation.
    pub owner_id: String,
    /// Optional pinned Messages-return codec for prepared Chat providers.
    #[serde(default)]
    pub codec_sha256: Option<String>,
}
impl SessionConfig {
    pub(crate) fn validate(&self) -> switchyard_libsy::Result<()> {
        let url =
            reqwest::Url::parse(&self.endpoint).map_err(|_| error("invalid ARC session URL"))?;
        if url.scheme() != "http"
            || !matches!(url.host_str(), Some("127.0.0.1" | "[::1]"))
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
            || self.owner_id.trim().is_empty()
        {
            return Err(error(
                "ARC session service requires a literal loopback HTTP URL and owner",
            ));
        }
        if self.codec_sha256.as_ref().is_some_and(|pin| !sha256(pin)) {
            return Err(error("ARC codec SHA256 invalid"));
        }
        Ok(())
    }
}

/// Preparation failed and its cleanup was not acknowledged. The session must
/// remain fenced; the original preparation error is retained as the source.
#[derive(Debug)]
pub struct SessionRecoveryRequired {
    preparation: LibsyError,
    settlement: String,
}
impl std::fmt::Display for SessionRecoveryRequired {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}; abort acknowledgement failed: {}; session requires recovery",
            self.preparation, self.settlement
        )
    }
}
impl std::error::Error for SessionRecoveryRequired {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.preparation)
    }
}

/// Process-owned receipt retained until the HTTP response reaches its terminal frame.
#[derive(Clone, Deserialize, Serialize)]
pub struct SessionReceipt {
    /// Validated local session endpoint.
    pub endpoint: String,
    /// Host installation identity.
    pub owner_id: String,
    /// Opaque staged transaction identity returned by the service.
    pub session_token: String,
}
impl SessionReceipt {
    /// Commit delivered native output or abort an unaccepted response.
    pub async fn settle(&self, success: bool, output: Option<Value>) -> Result<(), String> {
        SessionConfig {
            endpoint: self.endpoint.clone(),
            owner_id: self.owner_id.clone(),
            codec_sha256: None,
        }
        .validate()
        .map_err(|e| e.to_string())?;
        let client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|_| "ARC settlement client unavailable")?;
        let mut payload = json!({"owner_id":self.owner_id,"session_token":self.session_token});
        if success {
            payload["settlement"] = json!("successful_2xx_terminal_sent");
            payload["response_attribution"] =
                json!(if output.is_some() { "known" } else { "unknown" });
            payload["response_messages"] = json!(output);
        }
        let acknowledgement: Value = client
            .post(format!(
                "{}/{}",
                self.endpoint.trim_end_matches('/'),
                if success { "commit" } else { "abort" }
            ))
            .json(&payload)
            .send()
            .await
            .map_err(|_| "ARC settlement service unavailable")?
            .error_for_status()
            .map_err(|_| "ARC settlement refused")?
            .json()
            .await
            .map_err(|_| "ARC settlement acknowledgement invalid")?;
        if acknowledgement["state"] != if success { "committed" } else { "aborted" } {
            return Err("ARC settlement acknowledgement state mismatch".into());
        }
        Ok(())
    }
}

impl ArcRouter {
    pub(crate) async fn route_session(
        &self,
        mut request: Request,
    ) -> switchyard_libsy::Result<RoutingOutcome> {
        let session = self
            .config
            .session
            .as_ref()
            .ok_or_else(|| error("ARC session missing"))?;
        let metadata = request
            .metadata
            .as_ref()
            .ok_or_else(|| error("ARC session metadata missing"))?;
        if metadata.wire_format != Some(WireFormat::AnthropicMessages) {
            return Err(error("ARC sessions currently require native Messages"));
        }
        if metadata
            .session_id
            .as_deref()
            .is_none_or(|id| id.trim().is_empty())
            || metadata.subagent_identity_unsupported
        {
            return Err(error(
                "ARC requires a stable conversation identity and supported agent lineage",
            ));
        }
        let raw = request
            .raw_request
            .as_ref()
            .ok_or_else(|| error("ARC source body missing"))?;
        let source_stream = raw.get("stream").cloned();
        let available: Vec<_> = self
            .config
            .actions
            .iter()
            .filter(|(_, a)| {
                session.codec_sha256.is_some() || a.request_format == "anthropic_messages"
            })
            .map(|(id, _)| id.clone())
            .collect();
        if available.is_empty() {
            return Err(error("ARC has no eligible Messages actions"));
        }
        let operation = metadata.correlation_id.clone().unwrap_or_else(|| {
            format!(
                "{}-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos(),
                REQUESTS.fetch_add(1, Ordering::Relaxed)
            )
        });
        let compaction = metadata
            .http_headers
            .as_ref()
            .and_then(|h| h.get("x-switchyard-compaction-ordinal"))
            .map(|value| {
                value
                    .to_str()
                    .ok()
                    .and_then(|v| v.parse::<u64>().ok())
                    .filter(|n| *n > 0)
                    .ok_or_else(|| error("invalid ARC compaction ordinal"))
            })
            .transpose()?;
        let payload = json!({"owner_id":session.owner_id,"operation_id":operation,"metadata":{"session_id":metadata.session_id,"agent_id":metadata.agent_id,"parent_agent_id":metadata.parent_agent_id,"subagent_identity_unsupported":metadata.subagent_identity_unsupported},"request_format":"anthropic_messages","request":raw,"available_action_ids":available,"compaction_ordinal":compaction});
        let receipt: Value = self
            .client
            .post(format!(
                "{}/prepare",
                session.endpoint.trim_end_matches('/')
            ))
            .json(&payload)
            .send()
            .await
            .map_err(|_| error("ARC session service unavailable"))?
            .error_for_status()
            .map_err(|_| error("ARC session service refused request"))?
            .json()
            .await
            .map_err(|_| error("ARC session response invalid"))?;
        let token = receipt["session_token"]
            .as_str()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| error("ARC session token missing"))?;
        let transaction = SessionReceipt {
            endpoint: session.endpoint.clone(),
            owner_id: session.owner_id.clone(),
            session_token: token.into(),
        };
        let result = (|| {
            if receipt["package_sha256"] != self.config.package_sha256
                || receipt["decision"]["schema_version"]
                    != "rayline.arc.policy-decision-response.v1"
                || receipt["decision"]["package"]
                    != json!({"alias":self.config.package_alias,"package_sha256":self.config.package_sha256})
                || receipt["source_request_format"] != "anthropic_messages"
                || receipt["owner_id"] != session.owner_id
            {
                return Err(error("ARC prepared package/schema/format mismatch"));
            }
            let selected = receipt["decision"]["decision"]["selected_action_id"]
                .as_str()
                .ok_or_else(|| error("ARC selected action missing"))?;
            if receipt["action_id"] != selected
                || !available.iter().any(|id| id == selected)
                || !receipt["decision"]["decision"]["selected_arm_id"]
                    .as_str()
                    .is_some_and(sha256)
            {
                return Err(error("ARC selected action or arm invalid"));
            }
            let binding = &self.config.actions[selected];
            let body = &receipt["request"];
            // The pinned Messages codec omits the false default, but never true/null.
            let omitted_false = binding.request_format == "anthropic_messages"
                && source_stream == Some(Value::Bool(false))
                && body.get("stream").is_none();
            if body["model"] != binding.target
                || (body.get("stream") != source_stream.as_ref() && !omitted_false)
            {
                return Err(error("ARC prepared destination or streaming mode changed"));
            }
            let chat = binding.request_format == "openai_chat";
            if receipt["request_format"] != binding.request_format {
                return Err(error("ARC prepared provider format mismatch"));
            }
            let codec = if chat {
                let pin = session
                    .codec_sha256
                    .as_ref()
                    .ok_or_else(|| error("ARC Chat codec not enabled"))?;
                if receipt["response_codec"]
                    != json!({"schema_version":"rayline.arc.response-codec.v1","source":"openai_chat","target":"anthropic_messages","implementation_sha256":pin})
                {
                    return Err(error("ARC response codec identity mismatch"));
                }
                Some(PinnedResponseCodec {
                    endpoint: session.endpoint.clone(),
                    owner_id: session.owner_id.clone(),
                    session_token: token.into(),
                    implementation_sha256: pin.clone(),
                })
            } else {
                if binding.request_format != "anthropic_messages"
                    || receipt.get("response_codec").is_some()
                {
                    return Err(error("unsupported ARC native provider format"));
                }
                None
            };
            let decoded = TranslationEngine::default()
                .decode_request(
                    binding.request_format.as_str(),
                    body,
                    &TranslationPolicy::default(),
                )
                .map_err(|_| error("ARC prepared request invalid"))?;
            if !chat {
                let encoded = TranslationEngine::default()
                    .encode_request(
                        "anthropic_messages",
                        &decoded.request,
                        &TranslationPolicy::default(),
                    )
                    .map_err(|_| error("ARC prepared Messages cannot encode"))?;
                if encoded.body != *body {
                    return Err(error("ARC prepared Messages changed during translation"));
                }
            }
            request.llm_request = decoded.request;
            request.raw_request = Some(body.clone());
            let metadata = request
                .metadata
                .as_mut()
                .ok_or_else(|| error("ARC metadata missing"))?;
            if let Some(codec) = codec {
                metadata.prepared_chat = Some(PreparedChat {
                    body: body.clone(),
                    normalized: request.llm_request.clone(),
                    codec,
                });
            } else {
                metadata.prepared_messages = Some(PreparedMessages { body: body.clone() });
            }
            let mut outcome =
                RoutingOutcome::route_to(ModelId::from(binding.target.as_str()), vec![], request);
            outcome.metadata = Some(OutcomeMetadata::new(
                "arc".into(),
                Some(json!({"decision":receipt["decision"],"arc_session":transaction})),
            ));
            Ok(outcome)
        })();
        match result {
            Err(preparation) => match transaction.settle(false, None).await {
                Ok(()) => Err(preparation),
                Err(settlement) => Err(LibsyError::external(
                    "ARC preparation cleanup",
                    SessionRecoveryRequired {
                        preparation,
                        settlement,
                    },
                )),
            },
            Ok(outcome) => Ok(outcome),
        }
    }
}
