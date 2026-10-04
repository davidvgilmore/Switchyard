//! Local ARC policy decisions through Switchyard's native algorithm interface.

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use switchyard_libsy::{Algorithm, Driver, LibsyError, OutcomeMetadata, RoutingOutcome};
use switchyard_protocol::{ModelId, Request};
use switchyard_translation::{TranslationEngine, TranslationPolicy};

/// A configured action and its exact provider controls. Null removes a control.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActionBinding {
    pub target: String,
    /// Must be empty until steering placement is supported.
    pub steering_suffix: String,
    /// Source format this binding has been validated against.
    pub request_format: String,
    /// Top-level provider controls, such as reasoning_effort or thinking.
    pub controls: BTreeMap<String, Value>,
}

/// Connection and immutable package identity for a local worker.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArcConfig {
    pub endpoint: String,
    pub package_alias: String,
    pub package_sha256: String,
    pub actions: BTreeMap<String, ActionBinding>,
}

/// An ARC algorithm with no numerical model implementation in Switchyard.
pub struct ArcRouter {
    config: ArcConfig,
    client: reqwest::Client,
}

fn error(message: impl Into<String>) -> LibsyError {
    LibsyError::AlgorithmError {
        message: message.into(),
    }
}

impl ArcRouter {
    /// Validate the local worker address and create the HTTP client.
    pub fn new(config: ArcConfig) -> switchyard_libsy::Result<Self> {
        let url =
            reqwest::Url::parse(&config.endpoint).map_err(|_| error("invalid ARC endpoint"))?;
        if url.scheme() != "http"
            || !matches!(url.host_str(), Some("127.0.0.1" | "[::1]"))
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
            || url.path() != "/v1/rayline/arc/policy/decide"
        {
            return Err(error(
                "ARC endpoint must be a loopback HTTP policy/decide URL",
            ));
        }
        if !sha256(&config.package_sha256)
            || config.package_alias.is_empty()
            || config.actions.is_empty()
        {
            return Err(error(
                "ARC requires a package alias, SHA256, and action bindings",
            ));
        }
        for (id, action) in &config.actions {
            if !action.steering_suffix.is_empty() {
                return Err(error("ARC steering placement is not yet supported"));
            }
            if !sha256(id)
                || action.target.is_empty()
                || !matches!(
                    action.request_format.as_str(),
                    "openai_chat" | "anthropic_messages" | "openai_responses"
                )
            {
                return Err(error("invalid ARC action binding"));
            }
            if action.controls.get("output_config").is_some_and(|value| {
                !value.is_null()
                    && value
                        .as_object()
                        .is_none_or(|object| object.keys().any(|key| key != "effort"))
            }) {
                return Err(error("ARC output_config binding may only change effort"));
            }
            if action.controls.keys().any(|key| {
                !matches!(
                    key.as_str(),
                    "reasoning_effort" | "reasoning" | "thinking" | "output_config"
                )
            }) {
                return Err(error(
                    "ARC binding contains an unsupported control; steering is not yet supported",
                ));
            }
        }
        let client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(120))
            .build()
            .map_err(|_| error("cannot create ARC client"))?;
        Ok(Self { config, client })
    }

    fn prepare(&self, request: &Request) -> switchyard_libsy::Result<(Value, Value, String)> {
        let metadata = request
            .metadata
            .as_ref()
            .ok_or_else(|| error("ARC requires host metadata"))?;
        let context = metadata
            .extra_metadata
            .as_ref()
            .and_then(|extra| extra.get("arc_context"))
            .ok_or_else(|| error("ARC requires explicit arc_context metadata"))?;
        let mut context: Value =
            serde_json::from_str(context).map_err(|_| error("invalid arc_context JSON"))?;
        let format = metadata
            .wire_format
            .ok_or_else(|| error("ARC requires source wire format"))?
            .as_str();
        let body = request
            .raw_request
            .as_ref()
            .or_else(|| {
                request
                    .llm_request
                    .preservation
                    .requests
                    .get(&format.into())
            })
            .ok_or_else(|| error("ARC requires the complete source request"))?;
        let object = context
            .as_object_mut()
            .ok_or_else(|| error("arc_context must be an object"))?;
        if !object
            .get("episode_id_hash")
            .and_then(Value::as_str)
            .is_some_and(sha256)
            || !object
                .get("context_epoch")
                .and_then(Value::as_str)
                .is_some_and(|s| !s.is_empty())
        {
            return Err(error(
                "ARC requires explicit episode hash and context epoch",
            ));
        }
        let attributions = object
            .get("attribution")
            .and_then(Value::as_array)
            .ok_or_else(|| error("ARC requires explicit attribution"))?;
        let items = body
            .get(if format == "openai_responses" {
                "input"
            } else {
                "messages"
            })
            .and_then(Value::as_array)
            .ok_or_else(|| error("ARC requires materialized conversation history"))?;
        let mut seen = std::collections::BTreeSet::new();
        for attribution in attributions {
            let index = attribution["message"]
                .as_u64()
                .ok_or_else(|| error("invalid ARC attribution index"))?
                as usize;
            let item = items
                .get(index)
                .ok_or_else(|| error("ARC attribution outside history"))?;
            let attributable = item["role"] == "assistant"
                || (format == "openai_responses"
                    && matches!(item["type"].as_str(), Some("function_call" | "reasoning")));
            if !attributable
                || !seen.insert(index)
                || !attribution["action_id"].as_str().is_some_and(sha256)
                || (!attribution["arm_id"].is_null()
                    && !attribution["arm_id"].as_str().is_some_and(sha256))
            {
                return Err(error("invalid ARC attribution"));
            }
        }
        let available = object
            .get("selection")
            .and_then(|s| s.get("available_action_ids"))
            .and_then(Value::as_array)
            .ok_or_else(|| error("ARC requires explicit action eligibility"))?;
        if available.is_empty()
            || available.iter().any(|id| {
                id.as_str()
                    .is_none_or(|id| !self.config.actions.contains_key(id))
            })
        {
            return Err(error("ARC eligibility includes an unbound action"));
        }
        let mut policy_request = serde_json::Map::new();
        for key in if format == "openai_responses" {
            &["input", "instructions"][..]
        } else {
            &["messages", "system", "tools"][..]
        } {
            if let Some(value) = body.get(*key) {
                policy_request.insert((*key).to_owned(), value.clone());
            }
        }
        object.insert(
            "schema_version".into(),
            json!("rayline.arc.policy-decision-request.v1"),
        );
        object.insert("package".into(), json!({"alias": self.config.package_alias, "package_sha256": self.config.package_sha256}));
        object.insert("request_format".into(), json!(format));
        object.insert("request".into(), Value::Object(policy_request));
        Ok((context, body.clone(), format.to_owned()))
    }
}

fn sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

#[async_trait]
impl Algorithm for ArcRouter {
    fn name(&self) -> &str {
        "arc"
    }

    async fn route(
        self: Arc<Self>,
        _driver: Driver,
        mut request: Request,
    ) -> switchyard_libsy::Result<RoutingOutcome> {
        let (payload, mut body, format) = self.prepare(&request)?;
        let response = self
            .client
            .post(&self.config.endpoint)
            .json(&payload)
            .send()
            .await
            .map_err(|_| error("ARC worker unavailable"))?;
        if !response.status().is_success() {
            return Err(error(format!(
                "ARC worker refused decision: HTTP {}",
                response.status()
            )));
        }
        let decision: Value = response
            .json()
            .await
            .map_err(|_| error("ARC worker returned invalid JSON"))?;
        if decision["schema_version"] != "rayline.arc.policy-decision-response.v1"
            || decision["package"] != payload["package"]
        {
            return Err(error("ARC response schema or package mismatch"));
        }
        let selected = decision["decision"]["selected_action_id"]
            .as_str()
            .ok_or_else(|| error("ARC response has no selected action"))?;
        if !payload["selection"]["available_action_ids"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v == selected)
        {
            return Err(error("ARC selected an ineligible action"));
        }
        let binding = self
            .config
            .actions
            .get(selected)
            .ok_or_else(|| error("ARC selected an unbound action"))?;
        if binding.request_format != format {
            return Err(error(
                "ARC action has no validated controls for the source format",
            ));
        }
        let fields = body
            .as_object_mut()
            .ok_or_else(|| error("invalid source body"))?;
        // Clear every prior thinking control before applying this exact action.
        for name in ["reasoning_effort", "reasoning", "thinking"] {
            fields.remove(name);
        }
        // Anthropic output_config can also carry a structured-output schema.
        if let Some(output) = fields
            .get_mut("output_config")
            .and_then(Value::as_object_mut)
        {
            output.remove("effort");
        }
        for (name, value) in &binding.controls {
            if name == "output_config" {
                if let Some(effort) = value.get("effort") {
                    let output = fields.entry(name.clone()).or_insert_with(|| json!({}));
                    output
                        .as_object_mut()
                        .ok_or_else(|| error("invalid output_config"))?
                        .insert("effort".into(), effort.clone());
                }
            } else if !value.is_null() {
                fields.insert(name.clone(), value.clone());
            }
        }
        if fields
            .get("output_config")
            .and_then(Value::as_object)
            .is_some_and(|object| object.is_empty())
        {
            fields.remove("output_config");
        }
        fields.insert("model".into(), json!(binding.target));
        request.llm_request = TranslationEngine::default()
            .decode_request(format.as_str(), &body, &TranslationPolicy::default())
            .map_err(|_| error("cannot decode ARC-selected provider controls"))?
            .request;
        request.raw_request = Some(body);
        let mut outcome =
            RoutingOutcome::route_to(ModelId::from(binding.target.as_str()), vec![], request);
        outcome.metadata = Some(OutcomeMetadata::new("arc".into(), Some(decision)));
        Ok(outcome)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use switchyard_libsy::{RuntimeModels, drive};
    use switchyard_protocol::{Metadata, WireFormat};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn config(endpoint: String) -> ArcConfig {
        ArcConfig {
            endpoint,
            package_alias: "synthetic".into(),
            package_sha256: "a".repeat(64),
            actions: BTreeMap::from([(
                "b".repeat(64),
                ActionBinding {
                    target: "selected-target".into(),
                    steering_suffix: String::new(),
                    request_format: "openai_chat".into(),
                    controls: BTreeMap::from([("reasoning_effort".into(), json!("high"))]),
                },
            )]),
        }
    }
    fn request() -> Request {
        let body = json!({"model":"arc", "messages":[{"role":"user","content":"synthetic question"}], "reasoning_effort":"low", "tools":[{"type":"function","function":{"name":"inspect","parameters":{"type":"object"}}}]});
        Request {
            llm_request: TranslationEngine::default().decode_request("openai_chat", &body, &TranslationPolicy::default()).unwrap().request,
            raw_request: Some(body),
            metadata: Some(Metadata {
                wire_format: Some(WireFormat::OpenAiChat),
                extra_metadata: Some(BTreeMap::from([("arc_context".into(), json!({
                    "episode_id_hash":"c".repeat(64), "context_epoch":"synthetic-epoch", "attribution":[],
                    "selection":{"available_action_ids":["b".repeat(64)]}
                }).to_string())])), ..Default::default()
            }),
        }
    }
    async fn worker(bad_hash: bool) -> (String, tokio::task::JoinHandle<Value>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!(
            "http://{}/v1/rayline/arc/policy/decide",
            listener.local_addr().unwrap()
        );
        let task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            let (start, length) = loop {
                let mut part = [0; 4096];
                let n = socket.read(&mut part).await.unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&part[..n]);
                if let Some(start) = bytes.windows(4).position(|p| p == b"\r\n\r\n") {
                    let header = String::from_utf8_lossy(&bytes[..start]).to_ascii_lowercase();
                    let length: usize = header
                        .lines()
                        .find_map(|line| line.strip_prefix("content-length: "))
                        .unwrap()
                        .parse()
                        .unwrap();
                    if bytes.len() >= start + 4 + length {
                        break (start + 4, length);
                    }
                }
            };
            let received: Value = serde_json::from_slice(&bytes[start..start + length]).unwrap();
            let result = json!({"schema_version":"rayline.arc.policy-decision-response.v1", "package":{"alias":"synthetic", "package_sha256":if bad_hash {"d".repeat(64)} else {"a".repeat(64)}}, "decision":{"selected_action_id":"b".repeat(64)}}).to_string();
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", result.len(), result).as_bytes()).await.unwrap();
            received
        });
        (endpoint, task)
    }

    #[tokio::test]
    async fn routes_through_native_algorithm_and_encodes_selected_controls() {
        let (endpoint, worker) = worker(false).await;
        let input = request();
        let original = input.raw_request.clone().unwrap();
        let outcome = drive(
            Arc::new(ArcRouter::new(config(endpoint)).unwrap()),
            input,
            Arc::new(RuntimeModels::default()),
            |_| async { panic!("must not call a hosted LLM") },
        )
        .await
        .unwrap();
        let received = worker.await.unwrap();
        assert_eq!(received["request"]["messages"], original["messages"]);
        assert_eq!(received["request"]["tools"], original["tools"]);
        assert_eq!(
            outcome.selected_model_id().unwrap().as_str(),
            "selected-target"
        );
        assert_eq!(outcome.selected_model_ids.len(), 1);
        let encoded = TranslationEngine::default()
            .encode_request(
                "openai_chat",
                &outcome.request.llm_request,
                &TranslationPolicy::default(),
            )
            .unwrap()
            .body;
        assert_eq!(encoded["reasoning_effort"], "high");
        assert_eq!(encoded["messages"], original["messages"]);
        assert_eq!(encoded["tools"], original["tools"]);
    }

    #[tokio::test]
    async fn rejects_wrong_package_without_fallback() {
        let (endpoint, worker) = worker(true).await;
        let outcome = drive(
            Arc::new(ArcRouter::new(config(endpoint)).unwrap()),
            request(),
            Arc::new(RuntimeModels::default()),
            |_| async { panic!("no completion") },
        )
        .await;
        assert!(
            outcome
                .err()
                .unwrap()
                .to_string()
                .contains("package mismatch")
        );
        worker.await.unwrap();
    }

    #[test]
    fn preserves_unknown_attribution_without_inventing_an_arm() {
        let router = ArcRouter::new(config(
            "http://127.0.0.1:1/v1/rayline/arc/policy/decide".into(),
        ))
        .unwrap();
        let mut input = request();
        input.raw_request.as_mut().unwrap()["messages"]
            .as_array_mut()
            .unwrap()
            .push(json!({"role":"assistant","content":"previous answer"}));
        let (payload, _, _) = router.prepare(&input).unwrap();
        assert_eq!(payload["attribution"], json!([]));
    }

    #[test]
    fn refuses_nonlocal_worker_and_unsupported_controls() {
        let mut config = config("https://example.com/v1/rayline/arc/policy/decide".into());
        assert!(ArcRouter::new(config.clone()).is_err());
        config.endpoint = "http://127.0.0.1:1/v1/rayline/arc/policy/decide".into();
        config
            .actions
            .values_mut()
            .next()
            .unwrap()
            .controls
            .insert("steering_suffix".into(), json!("unvalidated"));
        assert!(ArcRouter::new(config).is_err());
    }
}
