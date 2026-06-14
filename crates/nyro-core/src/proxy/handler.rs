//! Ingress handlers that remain in the legacy handler module.
//!
//! PR-16: The full proxy pipeline (`proxy_pipeline`, `handle_non_stream`,
//! `handle_stream`, etc.) has been moved to `proxy/dispatcher.rs`.
//! Old ingress handlers (`openai_proxy`, `anthropic_proxy`, etc.) have been
//! replaced by `proxy/ingress/*.rs` thin shells wired directly in `server.rs`.
//!
//! This file now contains only `models_list`, which is a read-only endpoint
//! that does not go through the proxy pipeline.

use std::collections::{BTreeSet, HashMap, HashSet};

use axum::Json;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};

use crate::db::models::ModelCapabilities;
use crate::Gateway;
use crate::proxy::security::{extract_api_key, is_key_expired};

// ── GET /v1/models ────────────────────────────────────────────────────────────

pub async fn models_list(State(gw): State<Gateway>, headers: HeaderMap) -> Response {
    let mut accessible_route_ids = HashSet::new();

    if let Some(raw_key) = extract_api_key(&headers)
        && let Some(store) = gw.storage.auth()
        && let Ok(Some(key_row)) = store.find_api_key(&raw_key).await
    {
        let key_active = key_row.is_enabled
            && key_row
                .expires_at
                .as_ref()
                .map(|expires| !is_key_expired(expires))
                .unwrap_or(true);

        if key_active && let Ok(bound_route_ids) = store.list_bound_model_ids(&key_row.id).await {
            accessible_route_ids.extend(bound_route_ids);
        }
    }

    // ── upstream: use model_cache with new naming ─────────────────────────────
    let cache = gw.model_cache.read().await;
    let active_models: Vec<_> = cache
        .models
        .iter()
        .filter(|model| !model.enable_auth || accessible_route_ids.contains(&model.id))
        .collect();

    // ── fork: pre-fetch capabilities for all model targets ────────────────────
    let mut target_set: Vec<(String, String)> = Vec::new();
    for model in &active_models {
        let pair = (model.target_provider.clone(), model.target_model.clone());
        if !target_set.contains(&pair) {
            target_set.push(pair);
        }
    }

    let admin = gw.admin();
    let mut cap_map: HashMap<String, Option<ModelCapabilities>> = HashMap::new();
    for (provider_id, target_model) in &target_set {
        let caps = admin.get_model_capabilities(provider_id, target_model).await.ok();
        cap_map.insert(target_model.clone(), caps);
    }

    // ── upstream: collect model names ──────────────────────────────────────────
    let model_names: BTreeSet<String> = active_models
        .iter()
        .map(|m| m.name.trim().to_string())
        .filter(|m| !m.is_empty())
        .collect();

    let data = model_names
        .into_iter()
        .map(|model| {
            let mut obj = serde_json::json!({
                "id": model,
                "object": "model",
                "created": 0,
                "owned_by": "Nyro"
            });

            // ── fork: attach capabilities from pre-fetched map ────────────────
            if let Some(m) = active_models.iter().find(|m| m.name.trim() == model) {
                if let Some(Some(caps)) = cap_map.get(&m.target_model) {
                    obj["max_context_length"] = serde_json::json!(caps.context_window);
                    if let Some(max_out) = caps.output_max_tokens {
                        obj["max_output_tokens"] = serde_json::json!(max_out);
                    }
                    obj["reasoning"] = serde_json::json!(caps.reasoning);
                    obj["tool_call"] = serde_json::json!(caps.tool_call);
                    obj["input_modalities"] = serde_json::json!(caps.input_modalities);
                    obj["output_modalities"] = serde_json::json!(caps.output_modalities);
                    if let Some(cost) = caps.input_cost {
                        obj["input_cost"] = serde_json::json!(cost);
                    }
                    if let Some(cost) = caps.output_cost {
                        obj["output_cost"] = serde_json::json!(cost);
                    }
                }
            }

            obj
        })
        .collect::<Vec<_>>();

    drop(cache);

    Json(serde_json::json!({
        "object": "list",
        "data": data
    }))
    .into_response()
}
