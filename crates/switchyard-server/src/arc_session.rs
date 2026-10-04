//! Settle ARC sessions from the native Messages bytes accepted by the HTTP body.
use axum::{body::Body, response::Response};
use http_body::{Body as HttpBody, Frame};
use http_body_util::BodyExt;
use parking_lot::Mutex;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, HashMap},
    convert::Infallible,
    pin::Pin,
    sync::{Arc, OnceLock, Weak},
    task::{Context, Poll},
};
use switchyard_arc_router::SessionReceipt;
use switchyard_llm_client::{RunObservation, RunObserver};
use switchyard_protocol::Metadata;
use tokio::sync::{OwnedMutexGuard, mpsc, oneshot};

#[derive(Default)]
struct State {
    receipt: Option<SessionReceipt>,
    settled: bool,
    gate: Option<OwnedMutexGuard<()>>,
}
struct Inner(Mutex<State>);
impl Drop for Inner {
    fn drop(&mut self) {
        let state = self.0.get_mut();
        if !state.settled
            && let Some(receipt) = state.receipt.take()
            && let Ok(runtime) = tokio::runtime::Handle::try_current()
        {
            let gate = state.gate.take();
            runtime.spawn(async move {
                let _gate = gate;
                if receipt.settle(false, None).await.is_err() {
                    tracing::error!("ARC abort failed; session requires recovery");
                }
            });
        }
    }
}
#[derive(Clone)]
pub(crate) struct Pending(Arc<Inner>);
impl Pending {
    pub(crate) fn new() -> Self {
        Self(Arc::new(Inner(Mutex::new(State::default()))))
    }
    pub(crate) async fn queue(&self, metadata: Option<&Metadata>) {
        type Key = (String, Option<String>, Option<String>);
        type Gates = Mutex<HashMap<Key, Weak<tokio::sync::Mutex<()>>>>;
        static GATES: OnceLock<Gates> = OnceLock::new();
        let Some(metadata) = metadata else {
            return;
        };
        let Some(session) = metadata.session_id.as_ref() else {
            return;
        };
        let gate = {
            let mut gates = GATES.get_or_init(Default::default).lock();
            gates.retain(|_, gate| gate.strong_count() > 0);
            let key = (
                session.clone(),
                metadata.agent_id.clone(),
                metadata.parent_agent_id.clone(),
            );
            match gates.get(&key).and_then(Weak::upgrade) {
                Some(gate) => gate,
                None => {
                    let gate = Arc::new(tokio::sync::Mutex::new(()));
                    gates.insert(key, Arc::downgrade(&gate));
                    gate
                }
            }
        };
        let guard = gate.lock_owned().await;
        self.0.0.lock().gate = Some(guard);
    }
    pub(crate) fn observer(&self, downstream: RunObserver) -> RunObserver {
        let pending = self.clone();
        Arc::new(move |event| {
            if let RunObservation::Outcome(metadata) = &event
                && metadata.algorithm == "arc"
                && let Some(value) = metadata
                    .evidence
                    .as_ref()
                    .and_then(|v| v.get("arc_session"))
            {
                pending.0.0.lock().receipt = serde_json::from_value(value.clone()).ok();
            }
            downstream(event);
        })
    }
    async fn settle(&self, success: bool, output: Option<Value>) {
        let receipt = {
            let mut state = self.0.0.lock();
            if state.settled {
                return;
            }
            state.settled = true;
            state.receipt.clone()
        };
        if let Some(receipt) = receipt
            && receipt.settle(success, output).await.is_err()
        {
            tracing::error!("ARC settlement failed; session requires recovery");
        }
        self.0.0.lock().gate.take();
    }
    pub(crate) fn transport(self, response: Response, streaming: bool) -> Response {
        if self.0.0.lock().receipt.is_none() {
            return response;
        }
        let (parts, mut body) = response.into_parts();
        let successful = parts.status.is_success();
        let (tx, rx) = mpsc::channel(1);
        tokio::spawn(async move {
            let mut capture = MessagesCapture::default();
            let mut buffered = Vec::new();
            loop {
                let frame = tokio::select! {frame=body.frame()=>frame,_=tx.closed()=>{self.settle(false,None).await;return;}};
                let Some(frame) = frame else {
                    break;
                };
                let Ok(frame) = frame else {
                    self.settle(false, None).await;
                    return;
                };
                if let Some(bytes) = frame.data_ref() {
                    if streaming {
                        capture.push(bytes);
                    } else {
                        buffered.extend_from_slice(bytes);
                    }
                }
                let (accepted, wait) = oneshot::channel();
                if tx.send(Queued { frame, accepted }).await.is_err() || wait.await.is_err() {
                    self.settle(false, None).await;
                    return;
                }
                if streaming && (capture.terminal || capture.failed) {
                    self.settle(
                        successful && capture.terminal && !capture.failed,
                        capture.output(),
                    )
                    .await;
                    return;
                }
            }
            if streaming {
                self.settle(false, None).await;
            } else {
                let output = serde_json::from_slice::<Value>(&buffered)
                    .ok()
                    .and_then(|v| assistant(&v));
                self.settle(successful && output.is_some(), output).await;
            }
        });
        Response::from_parts(parts, Body::new(Transport(rx)))
    }
}
fn assistant(body: &Value) -> Option<Value> {
    (body["role"] == "assistant" && body["content"].is_array())
        .then(|| json!([{"role":"assistant","content":body["content"]}]))
}
#[derive(Default)]
struct MessagesCapture {
    buffer: Vec<u8>,
    blocks: BTreeMap<usize, Value>,
    arguments: BTreeMap<usize, String>,
    started: bool,
    unknown: bool,
    terminal: bool,
    failed: bool,
}
impl MessagesCapture {
    fn push(&mut self, bytes: &[u8]) {
        self.buffer.extend_from_slice(bytes);
        loop {
            let boundary = self
                .buffer
                .windows(2)
                .position(|w| w == b"\n\n")
                .map(|i| (i, 2))
                .or_else(|| {
                    self.buffer
                        .windows(4)
                        .position(|w| w == b"\r\n\r\n")
                        .map(|i| (i, 4))
                });
            let Some((end, separator)) = boundary else {
                break;
            };
            let frame: Vec<_> = self.buffer.drain(..end + separator).collect();
            let Ok(text) = std::str::from_utf8(&frame) else {
                self.unknown = true;
                continue;
            };
            let data = text
                .lines()
                .filter_map(|l| l.strip_prefix("data:").map(str::trim_start))
                .collect::<Vec<_>>()
                .join("\n");
            if data.is_empty() {
                continue;
            }
            match serde_json::from_str::<Value>(&data) {
                Ok(event) => self.event(event),
                Err(_) => self.unknown = true,
            }
        }
    }
    fn event(&mut self, event: Value) {
        match event["type"].as_str() {
            Some("message_start") => {
                if self.started || event["message"]["role"] != "assistant" {
                    self.unknown = true;
                }
                self.started = true;
                if let Some(blocks) = event["message"]["content"].as_array() {
                    for (i, block) in blocks.iter().enumerate() {
                        self.blocks.insert(i, block.clone());
                    }
                } else {
                    self.unknown = true;
                }
            }
            Some("content_block_start") => {
                if let Some(i) = event["index"].as_u64() {
                    let i = i as usize;
                    if self
                        .blocks
                        .insert(i, event["content_block"].clone())
                        .is_some()
                    {
                        self.unknown = true;
                    }
                } else {
                    self.unknown = true;
                }
            }
            Some("content_block_delta") => {
                let Some(index) = event["index"].as_u64().map(|i| i as usize) else {
                    self.unknown = true;
                    return;
                };
                let Some(block) = self.blocks.get_mut(&index) else {
                    self.unknown = true;
                    return;
                };
                let delta = &event["delta"];
                match delta["type"].as_str() {
                    Some("input_json_delta") => {
                        if block["type"] != "tool_use" {
                            self.unknown = true;
                        }
                        if let Some(part) = delta["partial_json"].as_str() {
                            self.arguments.entry(index).or_default().push_str(part);
                        } else {
                            self.unknown = true;
                        }
                    }
                    Some(kind @ ("text_delta" | "thinking_delta" | "signature_delta")) => {
                        let field = match kind {
                            "text_delta" => "text",
                            "thinking_delta" => "thinking",
                            _ => "signature",
                        };
                        let expected = if kind == "text_delta" {
                            "text"
                        } else {
                            "thinking"
                        };
                        if block["type"] != expected {
                            self.unknown = true;
                            return;
                        }
                        if let Some(part) = delta[field].as_str() {
                            if block.get(field).is_none() {
                                block[field] = json!("");
                            }
                            if let Some(value) = block[field].as_str() {
                                block[field] = json!(format!("{value}{part}"));
                            } else {
                                self.unknown = true;
                            }
                        } else {
                            self.unknown = true;
                        }
                    }
                    _ => self.unknown = true,
                }
            }
            Some("message_stop") => {
                self.terminal = true;
                for (i, args) in &self.arguments {
                    match serde_json::from_str::<Value>(args) {
                        Ok(value) => self.blocks.get_mut(i).map(|block| {
                            block["input"] = value;
                        }),
                        Err(_) => {
                            self.unknown = true;
                            None
                        }
                    };
                }
            }
            Some("error") => self.failed = true,
            Some("content_block_stop" | "message_delta" | "ping") => {}
            _ => self.unknown = true,
        }
    }
    fn output(&self) -> Option<Value> {
        if !self.started
            || self.unknown
            || !self.terminal
            || self.failed
            || self.blocks.keys().copied().ne(0..self.blocks.len())
        {
            return None;
        }
        Some(json!([{"role":"assistant","content":self.blocks.values().collect::<Vec<_>>() }]))
    }
}
struct Queued {
    frame: Frame<axum::body::Bytes>,
    accepted: oneshot::Sender<()>,
}
struct Transport(mpsc::Receiver<Queued>);
impl HttpBody for Transport {
    type Data = axum::body::Bytes;
    type Error = Infallible;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        match self.0.poll_recv(cx) {
            Poll::Ready(Some(item)) => {
                let _ = item.accepted.send(());
                Poll::Ready(Some(Ok(item.frame)))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fragmented_tool_arguments_keep_identity_and_unknown_events_stay_unknown() {
        let events = [
            json!({"type":"message_start","message":{"role":"assistant","content":[]}}),
            json!({"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"tool-1","name":"Read","caller":{"type":"direct"},"input":{}}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"path\":"}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"\"sample.rs\"}"}}),
            json!({"type":"content_block_stop","index":0}),
            json!({"type":"message_stop"}),
        ];
        let bytes = events
            .iter()
            .map(|event| format!("data: {event}\r\n\r\n"))
            .collect::<String>();
        let mut capture = MessagesCapture::default();
        for chunk in bytes.as_bytes().chunks(3) {
            capture.push(chunk);
        }
        assert_eq!(
            capture.output().unwrap()[0]["content"][0],
            json!({"type":"tool_use","id":"tool-1","name":"Read","caller":{"type":"direct"},"input":{"path":"sample.rs"}})
        );
        capture.event(json!({"type":"unknown_extension"}));
        assert!(capture.output().is_none());
        assert!(capture.terminal);
    }
}
