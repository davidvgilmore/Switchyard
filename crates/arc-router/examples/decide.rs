//! Drive a local ARC worker through libsy without sending a completion request.
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Arc};
use switchyard_arc_router::{ArcConfig, ArcRouter};
use switchyard_libsy::{RuntimeModels, drive};
use switchyard_protocol::{Metadata, Request, WireFormat};
use switchyard_translation::{TranslationEngine, TranslationPolicy};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() != 2 {
        return Err("usage: decide <private-bindings.json> <private-decision-request.json>".into());
    }
    let config: ArcConfig = serde_json::from_slice(&std::fs::read(&args[0])?)?;
    let mut context: Value = serde_json::from_slice(&std::fs::read(&args[1])?)?;
    let wire_format: WireFormat = serde_json::from_value(context["request_format"].clone())?;
    if context["package"]
        != json!({"alias":config.package_alias,"package_sha256":config.package_sha256})
    {
        return Err("fixture package does not match configured pin".into());
    }
    let body = context
        .as_object_mut()
        .ok_or("request must be an object")?
        .remove("request")
        .ok_or("missing request")?;
    let engine = TranslationEngine::default();
    let request = Request {
        llm_request: engine
            .decode_request(wire_format, &body, &TranslationPolicy::default())?
            .request,
        raw_request: Some(body),
        metadata: Some(Metadata {
            wire_format: Some(wire_format),
            extra_metadata: Some(BTreeMap::from([(
                "arc_context".into(),
                context.to_string(),
            )])),
            ..Default::default()
        }),
    };
    let outcome = drive(
        Arc::new(ArcRouter::new(config)?),
        request,
        Arc::new(RuntimeModels::default()),
        |_| async { panic!("unexpected hosted model call") },
    )
    .await?;
    let dispatch = engine
        .encode_request(
            wire_format,
            &outcome.request.llm_request,
            &TranslationPolicy::default(),
        )?
        .body;
    println!(
        "{}",
        serde_json::to_string(
            &json!({"selected_model":outcome.selected_model_id()?.as_str(), "dispatch":dispatch,"decision":outcome.metadata.as_ref().and_then(|m|m.evidence.as_ref())})
        )?
    );
    Ok(())
}
