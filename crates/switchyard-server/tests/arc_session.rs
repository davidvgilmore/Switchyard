#![cfg(feature = "arc-router")]

use axum::{
    Json, Router,
    extract::State,
    response::{IntoResponse, Response},
    routing::post,
};
use serde_json::{Value, json};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use switchyard_server::{build_switchyard_router, config::load_server_state};

#[derive(Clone, Default)]
struct Log {
    events: Arc<Mutex<Vec<(String, Value)>>>,
    commit_release: Arc<tokio::sync::Notify>,
}
impl std::ops::Deref for Log {
    type Target = Mutex<Vec<(String, Value)>>;
    fn deref(&self) -> &Self::Target {
        &self.events
    }
}
struct Host {
    url: String,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Host {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn serve(app: Router) -> Host {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    Host { url, task }
}
async fn prepare(State(log): State<Log>, Json(body): Json<Value>) -> Json<Value> {
    log.lock().unwrap().push(("prepare".into(), body.clone()));
    let mut prepared = body["request"].clone();
    prepared["model"] = json!("synthetic-model");
    // Match the pinned Messages codec's omitted false default.
    if prepared["stream"] == false || body["request"]["max_tokens"] == 9 {
        prepared.as_object_mut().unwrap().remove("stream");
    }
    if body["request"]["max_tokens"] == 3 {
        prepared["model"] = json!("wrong-model");
    }
    prepared["thinking"] = json!({"type":"adaptive"});
    prepared["output_config"] = json!({"effort":"high"});
    prepared["messages"].as_array_mut().unwrap().push(
        json!({"role":"user","content":[{"type":"text","text":"Synthetic private instruction"}]}),
    );
    Json(
        json!({"owner_id":if body["request"]["max_tokens"] == 6 { json!("wrong-owner") } else { body["owner_id"].clone() },"action_id":if body["request"]["max_tokens"] == 5 { "d".repeat(64) } else { "a".repeat(64) },"source_request_format":if body["request"]["max_tokens"] == 7 { "openai_chat" } else { "anthropic_messages" },"session_token":body["operation_id"],"package_sha256":"b".repeat(64),"request_format":"anthropic_messages","request":prepared,"decision":{"schema_version":"rayline.arc.policy-decision-response.v1","package":{"alias":"synthetic","package_sha256":"b".repeat(64)},"decision":{"selected_action_id":"a".repeat(64),"selected_arm_id":"c".repeat(64)}}}),
    )
}
async fn commit(State(log): State<Log>, Json(body): Json<Value>) -> Response {
    if body["session_token"] == "delayed-commit" {
        log.lock()
            .unwrap()
            .push(("commit-start".into(), body.clone()));
        log.commit_release.notified().await;
    }
    let token = body["session_token"].as_str().unwrap().to_owned();
    let wrong = token == "wrong-commit-ack";
    log.lock().unwrap().push(("commit".into(), body));
    if token == "missing-commit-ack" {
        return Json(json!({"ok":true})).into_response();
    }
    if token == "malformed-commit-ack" {
        return "not JSON".into_response();
    }
    if token == "lost-commit-ack" {
        return axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    Json(json!({"state":if wrong {"aborted"} else {"committed"}})).into_response()
}
async fn abort(State(log): State<Log>, Json(body): Json<Value>) -> Json<Value> {
    let wrong = body["session_token"] == "wrong-abort-ack";
    log.lock().unwrap().push(("abort".into(), body));
    Json(json!({"state":if wrong {"committed"} else {"aborted"}}))
}
async fn provider(State(log): State<Log>, Json(body): Json<Value>) -> Response {
    log.lock().unwrap().push(("provider".into(), body.clone()));
    if body["max_tokens"] == 1 {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            Json(json!({"error":{"type":"invalid_request_error","message":"synthetic refusal"}})),
        )
            .into_response();
    }
    if body["stream"] == true {
        let mut events = vec![
            json!({"type":"message_start","message":{"id":"synthetic","type":"message","role":"assistant","model":"synthetic-model","content":[],"usage":{"input_tokens":7,"output_tokens":0}}}),
            json!({"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":"","signature":""}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"Synthetic thought"}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"synthetic-signature"}}),
            json!({"type":"content_block_stop","index":0}),
            json!({"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":3}}),
        ];
        if body["max_tokens"] == 8 {
            events.insert(5, json!({"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"tool-1","name":"Read","input":{"path":"sample.rs"}}}));
            events.insert(6, json!({"type":"content_block_stop","index":1}));
            events[7]["delta"]["stop_reason"] = json!("tool_use");
        }
        if body["max_tokens"] != 2 && body["max_tokens"] != 4 {
            events.push(json!({"type":"message_stop"}));
        }
        let text = events
            .iter()
            .map(|e| format!("event: {}\ndata: {e}\n\n", e["type"].as_str().unwrap()))
            .collect::<String>();
        if body["max_tokens"] == 4 {
            let stream = async_stream::stream! {
                yield Ok::<_, std::convert::Infallible>(text);
                std::future::pending::<()>().await;
            };
            return (
                [("content-type", "text/event-stream")],
                axum::body::Body::from_stream(stream),
            )
                .into_response();
        }
        return ([("content-type", "text/event-stream")], text).into_response();
    }
    Json(json!({"id":"synthetic","type":"message","role":"assistant","model":"synthetic-model","content":[{"type":"tool_use","id":"tool-1","name":"Read","input":{"path":"sample.rs"}}],"stop_reason":"tool_use","stop_sequence":null,"usage":{"input_tokens":7,"output_tokens":3}})).into_response()
}
struct Fixture {
    host: Host,
    _worker: Host,
    _provider: Host,
    _directory: tempfile::TempDir,
    log: Log,
}
impl Fixture {
    async fn new() -> Self {
        let log = Log::default();
        let worker = serve(
            Router::new()
                .route("/prepare", post(prepare))
                .route("/commit", post(commit))
                .route("/abort", post(abort))
                .with_state(log.clone()),
        )
        .await;
        let provider = serve(
            Router::new()
                .route("/v1/messages", post(provider))
                .with_state(log.clone()),
        )
        .await;
        let directory = tempfile::tempdir().unwrap();
        let bindings = directory.path().join("bindings.json");
        let mut actions = serde_json::Map::new();
        actions.insert("a".repeat(64),json!({"target":"worker","request_format":"anthropic_messages","steering_suffix":"Synthetic private instruction","controls":{}}));
        std::fs::write(&bindings,serde_json::to_vec(&json!({"endpoint":format!("{}/v1/rayline/arc/policy/decide",worker.url),"package_alias":"synthetic","package_sha256":"b".repeat(64),"actions":actions,"session":{"endpoint":worker.url,"owner_id":"synthetic-host"}})).unwrap()).unwrap();
        let config = directory.path().join("server.toml");
        std::fs::write(
            &config,
            format!(
                r#"schema_version = 1
[llm_clients.native]
format = "anthropic_messages"
base_url = "{}/v1"
max_retries = 0
[targets.worker]
id = "synthetic-model"
llm_client = "native"
[routes.arc]
id = "arc"
type = "arc"
config = "{}"
targets = ["worker"]
"#,
                provider.url,
                bindings.display()
            ),
        )
        .unwrap();
        let host = serve(build_switchyard_router(load_server_state(&config).unwrap())).await;
        Self {
            host,
            _worker: worker,
            _provider: provider,
            _directory: directory,
            log,
        }
    }
    async fn send(&self, body: Value, operation: &str) -> (reqwest::StatusCode, String) {
        let response = reqwest::Client::new()
            .post(format!("{}/v1/messages", self.host.url))
            .header("x-switchyard-session-id", "synthetic-conversation")
            .header("x-switchyard-agent-id", "child")
            .header("x-switchyard-parent-agent-id", "parent")
            .header("x-switchyard-request-id", operation)
            .json(&body)
            .send()
            .await
            .unwrap();
        let status = response.status();
        (status, response.text().await.unwrap())
    }
    async fn settled(&self, kind: &str, count: usize) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if self
                    .log
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|(k, _)| k == kind)
                    .count()
                    >= count
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
}
fn request(tokens: u64, stream: bool) -> Value {
    json!({"model":"arc","max_tokens":tokens,"stream":stream,"messages":[{"role":"user","content":"Inspect sample"}],"tools":[{"name":"Read","description":"Read a file","input_schema":{"type":"object","properties":{"path":{"type":"string"}}}}]})
}

#[tokio::test]
async fn native_tool_turn_then_signed_stream_commit_exact_history() {
    let f = Fixture::new().await;
    let initial = request(32, false);
    let (status, text) = f.send(initial.clone(), "turn-1").await;
    assert!(status.is_success(), "{status}: {text}");
    let answer: Value = serde_json::from_str(&text).unwrap();
    f.settled("commit", 1).await;
    let mut next = request(32, true);
    next["messages"].as_array_mut().unwrap().extend([json!({"role":"assistant","content":answer["content"]}),json!({"role":"user","content":[{"type":"tool_result","tool_use_id":"tool-1","content":"fn sample() {}"}]})]);
    let (status, text) = f.send(next.clone(), "turn-2").await;
    assert!(status.is_success(), "{status}: {text}");
    assert!(text.contains("synthetic-signature"));
    f.settled("commit", 2).await;
    let log = f.log.lock().unwrap();
    let preparations: Vec<_> = log
        .iter()
        .filter(|(k, _)| k == "prepare")
        .map(|(_, v)| v)
        .collect();
    assert_eq!(preparations[0]["request"], initial);
    assert_eq!(preparations[1]["request"], next);
    assert_eq!(preparations[1]["metadata"]["agent_id"], "child");
    assert_eq!(preparations[1]["metadata"]["parent_agent_id"], "parent");
    let providers: Vec<_> = log
        .iter()
        .filter(|(k, _)| k == "provider")
        .map(|(_, v)| v)
        .collect();
    for body in &providers {
        assert_eq!(body["thinking"], json!({"type":"adaptive"}));
        assert_eq!(body["output_config"], json!({"effort":"high"}));
        assert!(!body.to_string().contains("cache_control"));
    }
    assert_eq!(providers[1]["messages"][2], next["messages"][2]);
    assert_eq!(
        providers[1]["messages"][3]["content"][0]["text"],
        "Synthetic private instruction"
    );
    let commits: Vec<_> = log
        .iter()
        .filter(|(k, _)| k == "commit")
        .map(|(_, v)| v)
        .collect();
    assert_eq!(
        commits[0]["response_messages"][0]["content"],
        answer["content"]
    );
    assert_eq!(
        commits[1]["response_messages"][0]["content"],
        json!([{"type":"thinking","thinking":"Synthetic thought","signature":"synthetic-signature"}])
    );
    assert_eq!(commits[1]["session_token"], "turn-2");
}
#[tokio::test]
async fn provider_refusal_and_missing_stream_terminal_abort() {
    let f = Fixture::new().await;
    let (status, _) = f.send(request(1, false), "refused").await;
    assert!(!status.is_success());
    f.settled("abort", 1).await;
    let (status, _) = f.send(request(2, true), "truncated").await;
    assert!(status.is_success());
    f.settled("abort", 2).await;
    assert!(!f.log.lock().unwrap().iter().any(|(k, _)| k == "commit"));
}

#[tokio::test]
async fn invalid_preparation_aborts_without_provider_dispatch() {
    let f = Fixture::new().await;
    let (status, _) = f.send(request(3, false), "invalid").await;
    assert!(!status.is_success());
    f.settled("abort", 1).await;
    assert!(!f.log.lock().unwrap().iter().any(|(k, _)| k == "provider"));
}

#[tokio::test]
async fn dropped_stream_aborts_prepared_operation() {
    use futures_util::StreamExt;
    let f = Fixture::new().await;
    let response = reqwest::Client::new()
        .post(format!("{}/v1/messages", f.host.url))
        .header("x-switchyard-session-id", "cancel-conversation")
        .json(&request(4, true))
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
    let mut stream = response.bytes_stream();
    assert!(stream.next().await.unwrap().is_ok());
    drop(stream);
    f.settled("abort", 1).await;
    assert!(!f.log.lock().unwrap().iter().any(|(k, _)| k == "commit"));
}

#[tokio::test]
async fn missing_identity_is_refused_before_prepare() {
    let f = Fixture::new().await;
    let response = reqwest::Client::new()
        .post(format!("{}/v1/messages", f.host.url))
        .json(&request(32, false))
        .send()
        .await
        .unwrap();
    assert!(!response.status().is_success());
    assert!(f.log.lock().unwrap().is_empty());
}

#[tokio::test]
async fn mismatched_receipt_action_owner_or_source_aborts_before_dispatch() {
    let f = Fixture::new().await;
    for (index, tokens) in [5, 6, 7].into_iter().enumerate() {
        let (status, _) = f
            .send(request(tokens, false), &format!("bad-receipt-{tokens}"))
            .await;
        assert!(!status.is_success());
        f.settled("abort", index + 1).await;
    }
    assert!(
        !f.log
            .lock()
            .unwrap()
            .iter()
            .any(|(kind, _)| kind == "provider")
    );
}

#[tokio::test]
async fn immediate_tool_continuation_waits_for_commit_without_blocking_other_sessions() {
    use futures_util::StreamExt;
    let f = Fixture::new().await;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(3))
        .build()
        .unwrap();
    let url = format!("{}/v1/messages", f.host.url);
    let response = client
        .post(&url)
        .header("x-switchyard-session-id", "delayed-conversation")
        .header("x-switchyard-request-id", "delayed-commit")
        .json(&request(8, true))
        .send()
        .await
        .unwrap();
    let mut stream = response.bytes_stream();
    let mut bytes = Vec::new();
    while !String::from_utf8_lossy(&bytes).contains("event: message_stop") {
        bytes.extend_from_slice(&stream.next().await.unwrap().unwrap());
    }
    drop(stream);
    f.settled("commit-start", 1).await;
    let next_client = client.clone();
    let next_url = url.clone();
    let continuation = tokio::spawn(async move {
        let mut body = request(32, false);
        body["messages"].as_array_mut().unwrap().extend([
            json!({"role":"assistant","content":[{"type":"tool_use","id":"tool-1","name":"Read","input":{"path":"sample.rs"}}]}),
            json!({"role":"user","content":[{"type":"tool_result","tool_use_id":"tool-1","content":"synthetic result"}]})
        ]);
        next_client
            .post(next_url)
            .header("x-switchyard-session-id", "delayed-conversation")
            .header("x-switchyard-request-id", "immediate-next")
            .json(&body)
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap()
    });
    let other = client
        .post(url)
        .header("x-switchyard-session-id", "independent-conversation")
        .header("x-switchyard-request-id", "independent")
        .json(&request(32, false))
        .send()
        .await
        .unwrap();
    assert!(other.status().is_success());
    other.text().await.unwrap();
    {
        let log = f.log.lock().unwrap();
        assert!(
            !log.iter()
                .any(|(kind, body)| kind == "commit" && body["session_token"] == "delayed-commit"),
            "independent session was blocked by delayed commit"
        );
    }
    f.log.commit_release.notify_one();
    continuation.await.unwrap();
    let log = f.log.lock().unwrap();
    let committed = log
        .iter()
        .position(|(kind, body)| kind == "commit" && body["session_token"] == "delayed-commit")
        .expect("delayed commit acknowledged");
    let prepared = log
        .iter()
        .position(|(kind, body)| kind == "prepare" && body["operation_id"] == "immediate-next")
        .unwrap();
    assert!(
        committed < prepared,
        "next tool turn prepared before previous commit acknowledgment"
    );
}

#[tokio::test]
async fn wrong_settlement_ack_fences_same_scope_without_blocking_another() {
    for (operation, tokens, kind) in [
        ("wrong-commit-ack", 32, "commit"),
        ("missing-commit-ack", 32, "commit"),
        ("malformed-commit-ack", 32, "commit"),
        ("lost-commit-ack", 32, "commit"),
        ("wrong-abort-ack", 2, "abort"),
        ("wrong-abort-ack", 3, "abort"),
    ] {
        let f = Fixture::new().await;
        let (_, first_body) = f.send(request(tokens, true), operation).await;
        if tokens == 3 {
            assert!(first_body.contains("ARC prepared destination"));
        }
        f.settled(kind, 1).await;
        let (status, _) = f.send(request(32, false), "blocked-next").await;
        assert_eq!(status, reqwest::StatusCode::CONFLICT);
        assert!(
            !f.log
                .lock()
                .unwrap()
                .iter()
                .any(|(kind, body)| kind == "prepare" && body["operation_id"] == "blocked-next")
        );
        let other = reqwest::Client::new()
            .post(format!("{}/v1/messages", f.host.url))
            .header("x-switchyard-session-id", "other-conversation")
            .header("x-switchyard-request-id", "other-operation")
            .json(&request(32, false))
            .send()
            .await
            .unwrap();
        assert!(other.status().is_success());
        other.text().await.unwrap();
    }
}

#[tokio::test]
async fn failed_abort_after_disconnect_fences_only_its_scope() {
    use futures_util::StreamExt;
    let f = Fixture::new().await;
    let client = reqwest::Client::new();
    let url = format!("{}/v1/messages", f.host.url);
    let response = client
        .post(&url)
        .header("x-switchyard-session-id", "cancel-fenced")
        .header("x-switchyard-request-id", "wrong-abort-ack")
        .json(&request(4, true))
        .send()
        .await
        .unwrap();
    let mut stream = response.bytes_stream();
    stream.next().await.unwrap().unwrap();
    drop(stream);
    f.settled("abort", 1).await;
    let next = client
        .post(&url)
        .header("x-switchyard-session-id", "cancel-fenced")
        .header("x-switchyard-request-id", "after-cancel")
        .json(&request(32, false))
        .send()
        .await
        .unwrap();
    assert_eq!(next.status(), reqwest::StatusCode::CONFLICT);
    assert!(
        !f.log
            .lock()
            .unwrap()
            .iter()
            .any(|(kind, body)| kind == "prepare" && body["operation_id"] == "after-cancel")
    );
    assert!(f.send(request(32, false), "unrelated").await.0.is_success());
}

#[tokio::test]
async fn omitted_false_codec_default_dispatches_but_true_omission_aborts() {
    let f = Fixture::new().await;
    let (status, _) = f.send(request(16, false), "false-default").await;
    assert!(status.is_success());
    f.settled("commit", 1).await;
    let (status, _) = f.send(request(9, true), "true-removed").await;
    assert!(!status.is_success());
    f.settled("abort", 1).await;
    let mut null_stream = request(9, false);
    null_stream["stream"] = Value::Null;
    let (status, _) = f.send(null_stream, "null-removed").await;
    assert!(!status.is_success());
    let log = f.log.lock().unwrap();
    let provider: Vec<_> = log.iter().filter(|(kind, _)| kind == "provider").collect();
    assert_eq!(provider.len(), 1);
    assert!(provider[0].1.get("stream").is_none());
}
