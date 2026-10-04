// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Response encoding glue for libsy server endpoints.

use std::error::Error;
use std::sync::Arc;

use axum::Json;
use axum::response::{IntoResponse, Response as HttpResponse};
use switchyard_protocol::{LlmResponse, ProviderExtensions, Response as AlgorithmResponse};
use switchyard_translation::{
    WireFormat, encode_aggregated_response_with_extensions, encode_stream_with_extensions,
};

use crate::sse::frame_stream;

type BoxError = Box<dyn Error + Send + Sync>;

/// Encodes a libsy response into the endpoint's wire format, reporting
/// `served_model` as the response model so the body names the model that
/// answered rather than the route the caller addressed.
pub(crate) fn into_http_response(
    response: AlgorithmResponse,
    target_format: WireFormat,
    served_model: Option<String>,
    request_extensions: ProviderExtensions,
    redactor: Arc<crate::redaction::Redactor>,
) -> Result<HttpResponse, BoxError> {
    if response
        .metadata
        .as_ref()
        .is_some_and(|m| m.prepared_chat.is_some())
    {
        return prepared_native_response(response, target_format, redactor);
    }
    match response.llm_response {
        LlmResponse::Agg(response) => {
            let body = encode_aggregated_response_with_extensions(
                &response,
                target_format,
                served_model.as_deref(),
                &request_extensions,
            )?;
            Ok(Json(body).into_response())
        }
        LlmResponse::Stream(stream) => {
            let events = encode_stream_with_extensions(
                stream,
                target_format,
                served_model,
                &request_extensions,
            )?;
            Ok(frame_stream(events, target_format, redactor).into_response())
        }
    }
}

// Telemetry can inspect the normalized view, but only preserved codec output is
// delivered. Generic model/tool-name rewriting must not touch this response.
fn prepared_native_response(
    response: AlgorithmResponse,
    target: WireFormat,
    redactor: Arc<crate::redaction::Redactor>,
) -> Result<HttpResponse, BoxError> {
    if target != WireFormat::AnthropicMessages {
        return Err("prepared codec return format mismatch".into());
    }
    match response.llm_response {
        LlmResponse::Agg(agg) => {
            let body = agg
                .preservation
                .responses
                .get(&target.into())
                .ok_or("prepared native response missing")?;
            Ok(Json(body.clone()).into_response())
        }
        LlmResponse::Stream(stream) => {
            use futures_util::StreamExt;
            let raw = stream
                .map(|item| {
                    let event = item.map_err(switchyard_translation::LlmStreamError::Client)?;
                    let preserved = event.preservation().ok_or_else(|| {
                        switchyard_translation::LlmStreamError::Client(
                            switchyard_protocol::LlmClientError::ResponseTranslation(
                                "prepared native event missing".into(),
                            ),
                        )
                    })?;
                    if preserved.source().as_str() != "anthropic_messages" {
                        return Err(switchyard_translation::LlmStreamError::Client(
                            switchyard_protocol::LlmClientError::ResponseTranslation(
                                "prepared native event format mismatch".into(),
                            ),
                        ));
                    }
                    Ok(preserved.raw().clone())
                })
                .boxed();
            Ok(frame_stream(raw, target, redactor).into_response())
        }
    }
}
