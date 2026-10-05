#![cfg(feature = "arc-router")]
use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
};
use serde_json::{Value, json};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use switchyard_server::{build_switchyard_router, config::load_server_state};
const PIN: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
#[derive(Clone, Default)]
struct Log {
    events: Arc<Mutex<Vec<(String, Value, HeaderMap)>>>,
    mode: Arc<Mutex<String>>,
    release: Arc<tokio::sync::Notify>,
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
async fn serve(router: Router) -> Host {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    Host { url, task }
}
fn native() -> Value {
    json!({"id":"synthetic","type":"message","role":"assistant","model":"codec-native-model","content":[{"type":"thinking","thinking":"synthetic thought","signature":"synthetic-signature"},{"type":"tool_use","id":"tool-1","name":"functions.Read","input":{"path":"example"}}],"stop_reason":"tool_use","usage":{"input_tokens":7,"output_tokens":3}})
}
fn frame(value: Value) -> String {
    format!(
        "event: {}\ndata: {}\n\n",
        value["type"].as_str().unwrap(),
        value
    )
}
fn frames() -> Vec<String> {
    vec![
        frame(
            json!({"type":"message_start","message":{"id":"synthetic","type":"message","role":"assistant","model":"codec-native-model","content":[]}}),
        ),
        frame(
            json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":"synthetic answer"}}),
        ),
        frame(json!({"type":"content_block_stop","index":0})),
        frame(json!({"type":"message_delta","delta":{"stop_reason":"end_turn"}})),
        frame(json!({"type":"message_stop"})),
    ]
}
async fn prepare(
    State(log): State<Log>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Json<Value> {
    log.events
        .lock()
        .unwrap()
        .push(("prepare".into(), body.clone(), headers));
    let mut messages = body["request"]["messages"].clone();
    messages
        .as_array_mut()
        .unwrap()
        .push(json!({"role":"user","content":"synthetic private tail"}));
    let mut result = json!({"owner_id":body["owner_id"],"session_token":body["operation_id"],"action_id":"a".repeat(64),"package_sha256":"b".repeat(64),"request_format":"openai_chat","source_request_format":"anthropic_messages","request":{"model":"synthetic-model","messages":messages,"reasoning_effort":"high","stream":body["request"]["stream"]},"response_codec":{"schema_version":"rayline.arc.response-codec.v1","source":"openai_chat","target":"anthropic_messages","implementation_sha256":PIN},"decision":{"schema_version":"rayline.arc.policy-decision-response.v1","package":{"alias":"synthetic","package_sha256":"b".repeat(64)},"decision":{"selected_action_id":"a".repeat(64),"selected_arm_id":"c".repeat(64)}}});
    match log.mode.lock().unwrap().as_str() {
        "bad-pin" => result["response_codec"]["implementation_sha256"] = json!("d".repeat(64)),
        "bad-source" => result["source_request_format"] = json!("openai_chat"),
        "bad-model" => result["request"]["model"] = json!("wrong"),
        _ => {}
    }
    Json(result)
}
async fn commit(
    State(log): State<Log>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Json<Value> {
    log.events
        .lock()
        .unwrap()
        .push(("commit".into(), body, headers));
    Json(json!({"state":if *log.mode.lock().unwrap()=="bad-ack"{"aborted"}else{"committed"}}))
}
async fn abort(State(log): State<Log>, headers: HeaderMap, Json(body): Json<Value>) -> Json<Value> {
    log.events
        .lock()
        .unwrap()
        .push(("abort".into(), body, headers));
    Json(json!({"state":"aborted"}))
}
async fn codec(State(log): State<Log>, headers: HeaderMap, Json(body): Json<Value>) -> Json<Value> {
    log.events
        .lock()
        .unwrap()
        .push(("codec".into(), body.clone(), headers));
    let mode = log.mode.lock().unwrap().clone();
    let mut reply = json!({"implementation_sha256":PIN});
    if mode == "reply-pin" {
        reply["implementation_sha256"] = json!("d".repeat(64));
    }
    match body["operation"].as_str().unwrap() {
        "response" => reply["body"] = native(),
        "stream_start" => {}
        "stream_push" => {
            reply["frames"] = if body["frame"].as_str().unwrap().contains("finish_reason") {
                json!(frames())
            } else {
                json!([])
            };
        }
        "stream_finish" => reply["frames"] = json!([]),
        _ => panic!("unexpected synthetic operation"),
    };
    Json(reply)
}
async fn provider(State(log): State<Log>, headers: HeaderMap, Json(body): Json<Value>) -> Response {
    log.events
        .lock()
        .unwrap()
        .push(("provider".into(), body.clone(), headers));
    let mode = log.mode.lock().unwrap().clone();
    if mode == "provider-error" {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    if body["stream"] != true {
        return Json(json!({"choices":[{"message":{"role":"assistant","content":"synthetic"},"finish_reason":"stop"}]})).into_response();
    }
    let finish = "data: {\"choices\":[{\"delta\":{\"content\":\"synthetic\"},\"finish_reason\":\"stop\"}]}\r\n\r\n";
    let stream = async_stream::stream! {yield Ok::<_,std::convert::Infallible>(finish.to_owned());if mode=="delay-done"{log.release.notified().await;}yield Ok(if mode=="truncated"{"data: [DONE]".to_owned()}else{"data: [DONE]\n\n".to_owned()});};
    (
        [("content-type", "text/event-stream")],
        axum::body::Body::from_stream(stream),
    )
        .into_response()
}
struct Fixture {
    host: Host,
    _service: Host,
    _provider: Host,
    _dir: tempfile::TempDir,
    log: Log,
}
impl Fixture {
    async fn new() -> Self {
        let log = Log::default();
        let service = serve(
            Router::new()
                .route("/prepare", post(prepare))
                .route("/commit", post(commit))
                .route("/abort", post(abort))
                .route("/codec", post(codec))
                .with_state(log.clone()),
        )
        .await;
        let provider = serve(
            Router::new()
                .route("/v1/chat/completions", post(provider))
                .with_state(log.clone()),
        )
        .await;
        let dir = tempfile::tempdir().unwrap();
        let binding = dir.path().join("bindings.json");
        let mut actions = serde_json::Map::new();
        actions.insert("a".repeat(64),json!({"target":"worker","request_format":"openai_chat","steering_suffix":"","controls":{}}));
        actions.insert("d".repeat(64),json!({"target":"worker","request_format":"anthropic_messages","steering_suffix":"","controls":{}}));
        std::fs::write(&binding,serde_json::to_vec(&json!({"endpoint":format!("{}/v1/rayline/arc/policy/decide",service.url),"package_alias":"synthetic","package_sha256":"b".repeat(64),"actions":actions,"session":{"endpoint":service.url,"owner_id":"synthetic-host","codec_sha256":PIN}})).unwrap()).unwrap();
        let config = dir.path().join("server.toml");
        std::fs::write(
            &config,
            format!(
                r#"schema_version=1
[llm_clients.chat]
format="openai_chat"
base_url="{}/v1"
api_key_env="USER"
max_retries=2
[targets.worker]
id="synthetic-model"
llm_client="chat"
[routes.arc]
id="arc"
type="arc"
config="{}"
targets=["worker"]
"#,
                provider.url,
                binding.display()
            ),
        )
        .unwrap();
        let host = serve(build_switchyard_router(load_server_state(&config).unwrap())).await;
        Self {
            host,
            _service: service,
            _provider: provider,
            _dir: dir,
            log,
        }
    }
    async fn send(&self, stream: bool, operation: &str, history: Value) -> reqwest::Response {
        reqwest::Client::new().post(format!("{}/v1/messages",self.host.url)).header("x-switchyard-session-id","native-session").header("x-switchyard-request-id",operation).header("authorization","Bearer synthetic-only-key").json(&json!({"model":"arc","stream":stream,"max_tokens":32,"messages":history,"tools":[{"name":"functions.Read","description":"Read","input_schema":{"type":"object"}}]})).send().await.unwrap()
    }
    async fn settled(&self, kind: &str) {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if self
                    .log
                    .events
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|(k, _, _)| k == kind)
                {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
}
fn history() -> Value {
    json!([{"role":"user","content":"synthetic task"}])
}
#[tokio::test]
async fn exact_prepared_wire_and_preserved_return_survive_two_turns() {
    let f = Fixture::new().await;
    let mut messages = history();
    for n in 0..2 {
        let response = f.send(false, &format!("op-{n}"), messages.clone()).await;
        let status = response.status();
        let text = response.text().await.unwrap();
        assert!(status.is_success(), "{text}");
        let output: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(output, native());
        messages.as_array_mut().unwrap().extend([json!({"role":"assistant","content":output["content"]}),json!({"role":"user","content":[{"type":"tool_result","tool_use_id":"tool-1","content":"synthetic result"}]})]);
    }
    f.settled("commit").await;
    let log = f.log.events.lock().unwrap();
    assert_eq!(log.iter().filter(|(k, _, _)| k == "provider").count(), 2);
    for (kind, body, headers) in log.iter() {
        if kind == "provider" {
            assert_eq!(body["model"], "synthetic-model");
            assert_eq!(body["reasoning_effort"], "high");
            assert_eq!(
                body["messages"].as_array().unwrap().last().unwrap()["content"],
                "synthetic private tail"
            );
            assert_eq!(
                headers["authorization"],
                format!("Bearer {}", std::env::var("USER").unwrap())
            );
            assert!(body.get("response_codec").is_none());
        } else {
            assert!(!headers.contains_key("authorization"));
        }
        if kind == "prepare" {
            assert_eq!(body["request_format"], "anthropic_messages");
            assert_eq!(body["available_action_ids"].as_array().unwrap().len(), 2);
        }
        if kind == "commit" {
            assert_eq!(
                body["response_messages"],
                json!([{"role":"assistant","content":native()["content"]}])
            );
        }
    }
}
#[tokio::test]
async fn incompatible_receipt_or_codec_never_commits() {
    for mode in [
        "bad-pin",
        "bad-source",
        "bad-model",
        "reply-pin",
        "provider-error",
    ] {
        let f = Fixture::new().await;
        *f.log.mode.lock().unwrap() = mode.into();
        let response = f.send(false, "bad", history()).await;
        assert!(!response.status().is_success());
        response.text().await.unwrap();
        f.settled("abort").await;
        let log = f.log.events.lock().unwrap();
        assert!(!log.iter().any(|(k, _, _)| k == "commit"));
        if mode == "provider-error" {
            assert_eq!(
                log.iter().filter(|(k, _, _)| k == "provider").count(),
                1,
                "prepared dispatch must not use configured retry budget"
            );
        }
    }
}
#[tokio::test]
async fn native_terminal_requires_done_and_codec_finish() {
    for mode in ["", "truncated"] {
        let f = Fixture::new().await;
        *f.log.mode.lock().unwrap() = mode.into();
        let text = f
            .send(true, "stream", history())
            .await
            .text()
            .await
            .unwrap();
        if mode.is_empty() {
            assert!(text.contains("codec-native-model"));
            assert!(text.contains("message_stop"));
            f.settled("commit").await;
        } else {
            assert!(!text.contains("event: message_stop"));
            f.settled("abort").await;
        }
    }
}
#[tokio::test]
async fn disconnect_before_provider_done_aborts() {
    use futures_util::StreamExt;
    let f = Fixture::new().await;
    *f.log.mode.lock().unwrap() = "delay-done".into();
    let response = f.send(true, "cancel", history()).await;
    assert!(response.status().is_success());
    let mut stream = response.bytes_stream();
    let first = stream.next().await.unwrap().unwrap();
    assert!(!String::from_utf8_lossy(&first).contains("message_stop"));
    drop(stream);
    f.settled("abort").await;
    assert!(
        !f.log
            .events
            .lock()
            .unwrap()
            .iter()
            .any(|(k, _, _)| k == "commit")
    );
}
#[tokio::test]
async fn codec_settlement_uncertainty_keeps_native_scope_fenced() {
    let f = Fixture::new().await;
    *f.log.mode.lock().unwrap() = "bad-ack".into();
    f.send(false, "bad", history()).await.text().await.unwrap();
    f.settled("commit").await;
    let next = f.send(false, "blocked", history()).await;
    assert_eq!(next.status(), StatusCode::CONFLICT);
    assert_eq!(
        f.log
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|(k, _, _)| k == "prepare")
            .count(),
        1
    );
}
