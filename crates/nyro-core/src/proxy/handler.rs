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

use axum::extract::State;
use axum::http::{HeaderMap, header};
use axum::response::{IntoResponse, Response};
use axum::Json;
use chrono::{NaiveDateTime, Utc};

use crate::db::models::ModelCapabilities;
use crate::Gateway;

// ── GET /v1/models ────────────────────────────────────────────────────────────

pub async fn models_list(State(gw): State<Gateway>, headers: HeaderMap) -> Response {
    let mut accessible_route_ids = HashSet::new();

    if let Some(raw_key) = extract_api_key(&headers)
        && let Some(store) = gw.storage.auth()
            && let Ok(Some(key_row)) = store.find_api_key(&raw_key).await {
                let key_active = key_row.is_enabled
                    && key_row
                        .expires_at
                        .as_ref()
                        .map(|expires| !is_key_expired(expires))
                        .unwrap_or(true);

                if key_active
                    && let Ok(bound_route_ids) = store.list_bound_route_ids(&key_row.id).await {
                        accessible_route_ids.extend(bound_route_ids);
                    }
            }

    let cache = gw.route_cache.read().await;
    let active_routes: Vec<_> = cache
        .routes
        .iter()
        .filter(|route| !route.access_control || accessible_route_ids.contains(&route.id))
        .collect();

    // Collect unique (provider_id, target_model) pairs for capability lookup
    let mut target_set: Vec<(String, String)> = Vec::new();
    for route in &active_routes {
        let pair = (route.target_provider.clone(), route.target_model.clone());
        if !target_set.contains(&pair) {
            target_set.push(pair);
        }
    }

    // Pre-fetch capabilities for all model targets
    let admin = gw.admin();
    let mut cap_map: HashMap<String, Option<ModelCapabilities>> = HashMap::new();
    for (provider_id, target_model) in &target_set {
        let caps = admin.get_model_capabilities(provider_id, target_model).await.ok();
        cap_map.insert(target_model.clone(), caps);
    }

    // Build model list with most capabilities from the route that matches
    let models: BTreeSet<String> = active_routes.iter().map(|r| r.virtual_model.trim().to_string()).filter(|m| !m.is_empty()).collect();

    let data = models
        .into_iter()
        .map(|model| {
            let mut obj = serde_json::json!({
                "id": model,
                "object": "model",
                "created": 0,
                "owned_by": "Nyro"
            });

            // Find the route for this virtual model and attach capabilities
            if let Some(route) = active_routes.iter().find(|r| r.virtual_model.trim() == model) {
                if let Some(Some(caps)) = cap_map.get(&route.target_model) {
                    obj["max_context_length"] = serde_json::json!(caps.context_window);
                    if let Some(max_out) = caps.output_max_tokens {
                        obj["max_output_tokens"] = serde_json::json!(max_out);
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

// ── Local helpers for models_list ────────────────────────────────────────────

fn extract_api_key(headers: &HeaderMap) -> Option<String> {
    if let Some(value) = headers.get(header::AUTHORIZATION).and_then(|v| v.to_str().ok())
        && let Some(token) = value.strip_prefix("Bearer ") {
            let token = token.trim();
            if !token.is_empty() {
                return Some(token.to_string());
            }
        }
    headers
        .get("x-api-key")
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(ToString::to_string)
}

fn is_key_expired(expires_at: &str) -> bool {
    if let Ok(parsed) = chrono::DateTime::parse_from_rfc3339(expires_at) {
        return parsed.with_timezone(&Utc) <= Utc::now();
    }
    NaiveDateTime::parse_from_str(expires_at, "%Y-%m-%d %H:%M:%S")
        .map(|parsed| parsed.and_utc() <= Utc::now())
        .unwrap_or(false)
}
