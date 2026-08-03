//! Streaming response handler.
//!
//! Two internal paths:
//! - PassThrough: ingress == egress protocol, no vendor mutations → forward raw
//!   SSE bytes; side-channel parser accumulates stats for logging.
//! - IR round-trip: parse → accumulate → format → re-emit as target-protocol SSE.

use std::convert::Infallible;

use axum::Json;
use axum::body::Body;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use futures::StreamExt;
use reqwest::header::HeaderMap as ReqwestHeaderMap;
use serde_json::Value;
use tokio_stream::wrappers::ReceiverStream;

use crate::protocol::ids::ProtocolEndpoint;
use crate::protocol::ir::AiStreamDelta;
use crate::proxy::client::ProxyClient;
use crate::proxy::observability::headers_to_json;

use super::{
    CallCtx, LogBuilder, RequestExtras, StreamResponseAccumulator, ai_response_to_deltas,
    error_response,
};

// ── Streaming response handler ────────────────────────────────────────────────

pub(super) async fn handle_stream(
    client: ProxyClient,
    url: &str,
    headers: ReqwestHeaderMap,
    body: Value,
    call_ctx: &CallCtx<'_>,
    req_extras: &RequestExtras,
    passthrough_resp: bool,
) -> Response {
    let egress = call_ctx.egress;
    let ingress = call_ctx.ingress;
    // Shared log builder: identity + request-side extras pre-filled.
    let log = LogBuilder::from_ctx(call_ctx)
        .with_req_extras(req_extras)
        .upstream_url(url);

    let upstream_start = std::time::Instant::now();
    let call_result = match client.call_stream(url, headers.clone(), body.clone()).await {
        Ok(r) => r,
        Err(e) => {
            log.status(502)
                .resp_body(Some(
                    serde_json::json!({ "error": { "message": format!("upstream error: {e}") } })
                        .to_string(),
                ))
                .emit();
            return error_response(502, &format!("upstream error: {e}"));
        }
    };
    let upstream_req_hdrs_str = crate::proxy::observability::reqwest_headers_to_json(&headers);
    let upstream_req_body_str = serde_json::to_string(&body).ok();

    let (resp, status) = call_result;
    let upstream_hdrs_str = headers_to_json(resp.headers());

    if status >= 400 {
        let err_body: Value = resp
            .json()
            .await
            .unwrap_or_else(|_| serde_json::json!({"error": {"message": "upstream error"}}));
        let err_body_str = serde_json::to_string(&err_body).ok();
        log.status(status)
            .upstream_status(status as i32)
            .with_upstream_request(upstream_req_hdrs_str, upstream_req_body_str)
            .upstream_resp_headers(upstream_hdrs_str.clone())
            .upstream_resp_body(err_body_str.clone())
            .resp_body(err_body_str)
            .emit();
        return (
            StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY),
            Json(err_body),
        )
            .into_response();
    }

    // ── Byte-level SSE passthrough ────────────────────────────────────────────
    // Used when ingress == egress protocol and the vendor declares no response
    // mutations (passthrough_resp=true). Upstream bytes are forwarded verbatim;
    // a side-channel parser accumulates usage stats for logging only.
    //
    // Tail handling on this path: frames are split on the SSE blank-line
    // delimiter and scanned cheaply (no full-stream buffering). Bare cost
    // footers like opencode.ai's {"choices":[],"cost":"0"} are absorbed, and
    // if the upstream ends without finish_reason/[DONE] a synthetic terminal
    // is appended before the channel closes (see passthrough_tail_events).
    if passthrough_resp {
        let (pt_tx, pt_rx) = tokio::sync::mpsc::channel::<Result<Bytes, Infallible>>(64);

        // Clone the log builder into the spawn: all identity + request-side
        // fields are already owned inside the builder, so no individual variable
        // cloning is needed.
        let log_pt = log.clone();
        let upstream_hdrs_pt = upstream_hdrs_str.clone();
        let upstream_req_hdrs_pt = upstream_req_hdrs_str.clone();
        let upstream_req_body_pt = upstream_req_body_str.clone();
        let upstream_start_pt = upstream_start;

        tokio::spawn(async move {
            let mut log_buf: Vec<u8> = Vec::new();
            let mut undecided_buf: Vec<u8> = Vec::new();
            let mut byte_stream = resp.bytes_stream();
            let mut stream_error: Option<String> = None;
            let mut chunks_count: i32 = 0;
            let mut first_chunk_ms: Option<i64> = None;
            let mut passthrough_mode = PassthroughBodyMode::Undecided;
            let mut converted_client_sse: Option<String> = None;
            let mut converted_ai_resp = None;
            let mut sse_frame_buf: Vec<u8> = Vec::new();
            let mut pt_flags = PassthroughFlags::default();

            while let Some(result) = byte_stream.next().await {
                match result {
                    Ok(b) => {
                        if first_chunk_ms.is_none() {
                            first_chunk_ms = Some(upstream_start_pt.elapsed().as_millis() as i64);
                        }
                        chunks_count += 1;
                        log_buf.extend_from_slice(&b);
                        match passthrough_mode {
                            PassthroughBodyMode::Undecided => {
                                undecided_buf.extend_from_slice(&b);
                                match classify_passthrough_body(&undecided_buf) {
                                    Some(PassthroughBodyMode::RawSse) => {
                                        passthrough_mode = PassthroughBodyMode::RawSse;
                                        let pending = std::mem::take(&mut undecided_buf);
                                        if !forward_passthrough_bytes(
                                            Bytes::from(pending),
                                            &mut sse_frame_buf,
                                            &mut pt_flags,
                                            &pt_tx,
                                        )
                                        .await
                                        {
                                            break; // client disconnected
                                        }
                                    }
                                    Some(PassthroughBodyMode::NonSseJson) => {
                                        passthrough_mode = PassthroughBodyMode::NonSseJson;
                                        undecided_buf.clear();
                                    }
                                    _ => {}
                                }
                            }
                            PassthroughBodyMode::RawSse => {
                                if !forward_passthrough_bytes(b, &mut sse_frame_buf, &mut pt_flags, &pt_tx)
                                    .await
                                {
                                    break; // client disconnected
                                }
                            }
                            PassthroughBodyMode::NonSseJson => {
                                // Upstream returned a complete JSON response to a stream endpoint.
                                // Buffer until EOF, then convert it to the downstream SSE shape.
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "upstream stream error during passthrough");
                        stream_error = Some(e.to_string());
                        // Emit an Anthropic-protocol error event so the client
                        // gets an explicit signal instead of a truncated stream.
                        let msg = e.to_string().replace('"', "\\\"");
                        let err_sse = format!(
                            "event: error\ndata: {{\"type\":\"error\",\"error\":{{\"type\":\"stream_error\",\"message\":\"{msg}\"}}}}\n\n"
                        );
                        let _ = pt_tx.send(Ok(Bytes::from(err_sse))).await;
                        break;
                    }
                }
            }

            // Tail handling for the raw-SSE passthrough: flush any partial
            // trailing frame, then synthesize a terminal finish_reason + [DONE]
            // when the upstream ended without one (e.g. opencode.ai stops at a
            // usage-only chunk followed by a bare cost footer). Never double-
            // emit when a terminal already passed through, and never after a
            // stream error (the client already got an explicit error event).
            if matches!(passthrough_mode, PassthroughBodyMode::RawSse) && stream_error.is_none() {
                if !sse_frame_buf.is_empty() {
                    let partial = std::mem::take(&mut sse_frame_buf);
                    pt_flags.observe_frame(&partial);
                    if !is_cost_footer_frame(&partial) {
                        let _ = pt_tx.send(Ok(Bytes::from(partial))).await;
                    }
                }
                for ev in passthrough_tail_events(
                    pt_flags.saw_done,
                    pt_flags.saw_finish_reason,
                    pt_flags.saw_tool_call,
                ) {
                    if pt_tx.send(Ok(ev)).await.is_err() {
                        break;
                    }
                }
            }

            let upstream_latency_ms = upstream_start_pt.elapsed().as_millis() as i64;
            let raw_sse = String::from_utf8_lossy(&log_buf).into_owned();

            if matches!(
                passthrough_mode,
                PassthroughBodyMode::NonSseJson | PassthroughBodyMode::Undecided
            ) && let Some((client_sse, ai_resp)) =
                format_non_sse_stream_response(&raw_sse, egress, ingress)
            {
                let _ = pt_tx.send(Ok(Bytes::from(client_sse.clone()))).await;
                converted_client_sse = Some(client_sse);
                converted_ai_resp = Some(ai_resp);
            }

            // Parse accumulated buffer for usage stats (best-effort).
            let mut log_parser = egress.handler().make_stream_response_decoder();
            let mut accumulator = StreamResponseAccumulator::default();
            if let Ok(ai_deltas) = log_parser.parse_chunk(&raw_sse) {
                accumulator.apply_all(&ai_deltas);
            }
            if let Ok(ai_deltas) = log_parser.finish() {
                accumulator.apply_all(&ai_deltas);
            }

            let mut ai_resp = converted_ai_resp.unwrap_or_else(|| accumulator.into_ai_response());
            if ai_resp.id.is_empty() {
                ai_resp.id = format!("msg_{}", uuid::Uuid::new_v4().simple());
            }
            if ai_resp.model.is_empty() {
                ai_resp.model = log_pt.upstream_model.clone();
            }

            log_pt
                .status(200)
                .upstream_status(200)
                .usage(ai_resp.usage.clone())
                .maybe_error(stream_error)
                .with_upstream_request(upstream_req_hdrs_pt, upstream_req_body_pt)
                .with_upstream_response(
                    200,
                    upstream_hdrs_pt,
                    Some(raw_sse.clone()),
                    Some(upstream_latency_ms),
                )
                .with_client_response(None, Some(converted_client_sse.unwrap_or(raw_sse)))
                .stream_metrics(chunks_count, first_chunk_ms)
                .emit();
        });

        let stream = ReceiverStream::new(pt_rx);
        let body = Body::from_stream(stream);
        let response = Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "text/event-stream")
            .header(header::CACHE_CONTROL, "no-cache")
            .header(header::CONNECTION, "keep-alive")
            .body(body)
            .unwrap();
        return response;
    }

    // ── IR round-trip path ────────────────────────────────────────────────────
    let mut stream_parser = egress.handler().make_stream_response_decoder();
    let mut stream_formatter = ingress.handler().make_stream_response_encoder();
    let mut byte_stream = resp.bytes_stream();
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<String, Infallible>>(64);

    // Move the log builder into the spawn.  Extract the fields we need AFTER
    // emit() consumes the builder, before passing it to the spawn.
    let log_ir = log;
    let act_model_ir = log_ir.upstream_model.clone();
    let upstream_hdrs_owned = upstream_hdrs_str;

    tokio::spawn(async move {
        let mut accumulator = StreamResponseAccumulator::default();
        let mut upstream_raw_buf: Vec<u8> = Vec::new();
        let mut client_sse_parts: Vec<String> = Vec::new();
        let mut chunks_count: i32 = 0;
        let mut first_chunk_ms: Option<i64> = None;

        while let Some(chunk) = byte_stream.next().await {
            let bytes = match chunk {
                Ok(b) => b,
                Err(e) => {
                    // P1: emit an explicit terminal event instead of silently breaking,
                    // so the client receives a defined stop_reason and does not hang.
                    tracing::warn!(error = %e, "upstream stream error; emitting terminal event");
                    let error_deltas = [AiStreamDelta::Done {
                        stop_reason: "error".to_string(),
                    }];
                    let events = stream_formatter.format_deltas(&error_deltas);
                    for ev in events {
                        let _ = tx.send(Ok(ev.to_sse_string())).await;
                    }
                    break;
                }
            };
            if first_chunk_ms.is_none() {
                first_chunk_ms = Some(upstream_start.elapsed().as_millis() as i64);
            }
            chunks_count += 1;
            upstream_raw_buf.extend_from_slice(&bytes);
            let text = String::from_utf8_lossy(&bytes);
            if let Ok(ai_deltas) = stream_parser.parse_chunk(&text) {
                accumulator.apply_all(&ai_deltas);
                let events = stream_formatter.format_deltas(&ai_deltas);
                for ev in events {
                    let sse = ev.to_sse_string();
                    client_sse_parts.push(sse.clone());
                    if tx.send(Ok(sse)).await.is_err() {
                        return;
                    }
                }
            }
            // Malformed upstream chunks with empty choices (e.g. opencode.ai's
            // {"choices":[],"cost":"0"} footer) yield no deltas from the parser,
            // so they are dropped here rather than forwarded raw to the client.
        }

        if let Ok(ai_deltas) = stream_parser.finish() {
            accumulator.apply_all(&ai_deltas);
            let events = stream_formatter.format_deltas(&ai_deltas);
            for ev in events {
                let sse = ev.to_sse_string();
                client_sse_parts.push(sse.clone());
                let _ = tx.send(Ok(sse)).await;
            }
        }

        let done_events = stream_formatter.format_done();
        for ev in done_events {
            let sse = ev.to_sse_string();
            client_sse_parts.push(sse.clone());
            let _ = tx.send(Ok(sse)).await;
        }

        let upstream_latency_ms = upstream_start.elapsed().as_millis() as i64;
        let upstream_raw_str = String::from_utf8_lossy(&upstream_raw_buf).into_owned();
        let client_sse_str = client_sse_parts.join("");

        let usage = stream_formatter.usage();
        let mut ai_resp = accumulator.into_ai_response();
        if ai_resp.usage.prompt_tokens == 0 && ai_resp.usage.completion_tokens == 0 {
            ai_resp.usage = usage.clone();
        }
        if ai_resp.id.is_empty() {
            ai_resp.id = format!("chatcmpl-{}", uuid::Uuid::new_v4().simple());
        }
        if ai_resp.model.is_empty() {
            ai_resp.model = act_model_ir.clone();
        }
        if ai_resp.stop_reason.is_none() {
            ai_resp.stop_reason = Some("stop".to_string());
        }

        log_ir
            .status(200)
            .upstream_status(200)
            .usage(ai_resp.usage.clone())
            .with_upstream_request(upstream_req_hdrs_str, upstream_req_body_str)
            .with_upstream_response(
                200,
                upstream_hdrs_owned,
                Some(upstream_raw_str),
                Some(upstream_latency_ms),
            )
            .with_client_response(None, Some(client_sse_str))
            .stream_metrics(chunks_count, first_chunk_ms)
            .emit();
    });

    let stream = ReceiverStream::new(rx);
    let body = Body::from_stream(stream);

    let response = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/event-stream")
        .header(header::CACHE_CONTROL, "no-cache")
        .header(header::CONNECTION, "keep-alive")
        .body(body)
        .unwrap();
    response
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PassthroughBodyMode {
    Undecided,
    RawSse,
    NonSseJson,
}

/// Terminal-state flags collected while forwarding raw SSE frames. Only tail
/// handling consumes them; the forward path stays strictly per-frame (no
/// full-stream buffering).
#[derive(Debug, Clone, Copy, Default)]
struct PassthroughFlags {
    saw_done: bool,
    saw_finish_reason: bool,
    saw_tool_call: bool,
}

impl PassthroughFlags {
    /// Scan one complete SSE frame and update the flags. Cheap substring /
    /// single-frame JSON checks only.
    fn observe_frame(&mut self, frame: &[u8]) {
        if frame_is_done(frame) {
            self.saw_done = true;
        }
        if frame_has_finish_reason(frame) {
            self.saw_finish_reason = true;
        }
        if contains_bytes(frame, b"\"tool_calls\"") {
            self.saw_tool_call = true;
        }
    }
}

/// Feed upstream bytes into the SSE frame buffer, forwarding complete frames
/// (dropping bare cost-footers) and updating the terminal flags. Returns false
/// if the client disconnected.
async fn forward_passthrough_bytes(
    bytes: Bytes,
    frame_buf: &mut Vec<u8>,
    flags: &mut PassthroughFlags,
    pt_tx: &tokio::sync::mpsc::Sender<Result<Bytes, Infallible>>,
) -> bool {
    frame_buf.extend_from_slice(&bytes);
    let mut start = 0;
    while let Some(end) = find_sse_frame_end(&frame_buf[start..]).map(|rel| start + rel) {
        let frame = &frame_buf[start..end];
        flags.observe_frame(frame);
        if !is_cost_footer_frame(frame) {
            if pt_tx.send(Ok(Bytes::from(frame.to_vec()))).await.is_err() {
                return false;
            }
        }
        start = end;
    }
    if start > 0 {
        frame_buf.drain(..start);
    }
    true
}

/// Find the byte offset just past the next complete SSE frame delimiter
/// (`\n\n` or `\r\n\r\n`) in `buf`, or None if no complete frame is present.
fn find_sse_frame_end(buf: &[u8]) -> Option<usize> {
    let lf = find_subslice(buf, b"\n\n").map(|p| p + 2);
    let crlf = find_subslice(buf, b"\r\n\r\n").map(|p| p + 4);
    match (lf, crlf) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || haystack.len() < needle.len() {
        return None;
    }
    haystack.windows(needle.len()).position(|w| w == needle)
}

fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
    find_subslice(haystack, needle).is_some()
}

/// Extract the payload of the first `data:` line of an SSE frame.
fn sse_frame_data(frame: &[u8]) -> Option<&str> {
    let text = std::str::from_utf8(frame).ok()?;
    let line = text.lines().find(|l| l.trim_start().starts_with("data:"))?;
    Some(line.trim_start().strip_prefix("data:")?.trim_start())
}

/// True when the frame is the standard `data: [DONE]` terminator.
fn frame_is_done(frame: &[u8]) -> bool {
    sse_frame_data(frame).map_or(false, |d| d.trim() == "[DONE]")
}

/// True when the frame's JSON payload carries a non-null finish_reason.
/// `"finish_reason":null` (sent mid-stream) does not count.
fn frame_has_finish_reason(frame: &[u8]) -> bool {
    let Some(data) = sse_frame_data(frame) else {
        return false;
    };
    let Ok(v) = serde_json::from_str::<Value>(data) else {
        return false;
    };
    !v["choices"][0]["finish_reason"].is_null()
}

/// True when the frame is a bare cost footer like opencode.ai's
/// `{"choices":[],"cost":"0"}` — empty choices array plus a cost key, no
/// content, no finish_reason. Matched loosely (empty choices + cost key
/// present) so these malformed frames never reach the client raw.
fn is_cost_footer_frame(frame: &[u8]) -> bool {
    let Some(data) = sse_frame_data(frame) else {
        return false;
    };
    let Ok(v) = serde_json::from_str::<Value>(data) else {
        return false;
    };
    v.get("cost").is_some()
        && v.get("choices")
            .and_then(|c| c.as_array())
            .is_some_and(|a| a.is_empty())
}

/// Synthetic terminal SSE frames for the byte-passthrough path, used when the
/// upstream ended without emitting a finish_reason or [DONE] (e.g. opencode.ai
/// streams stop at a usage-only chunk followed by a bare cost footer). Emits
/// nothing when a terminal already passed through upstream (no double-emission).
fn passthrough_tail_events(
    saw_done: bool,
    saw_finish_reason: bool,
    saw_tool_call: bool,
) -> Vec<Bytes> {
    if saw_done || saw_finish_reason {
        return vec![];
    }
    let finish_reason = if saw_tool_call { "tool_calls" } else { "stop" };
    let created = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let chunk = serde_json::json!({
        "id": "",
        "object": "chat.completion.chunk",
        "created": created,
        "model": "",
        "choices": [{"index": 0, "delta": {}, "finish_reason": finish_reason}],
    });
    vec![
        Bytes::from(format!("data: {chunk}\n\n")),
        Bytes::from("data: [DONE]\n\n"),
    ]
}

fn classify_passthrough_body(bytes: &[u8]) -> Option<PassthroughBodyMode> {
    let text = String::from_utf8_lossy(bytes);
    let trimmed = text.trim_start();
    if trimmed.is_empty() {
        return None;
    }
    if trimmed.starts_with("data:")
        || trimmed.starts_with("event:")
        || trimmed.starts_with("id:")
        || trimmed.starts_with("retry:")
        || trimmed.starts_with(':')
    {
        return Some(PassthroughBodyMode::RawSse);
    }
    if trimmed.starts_with('{') || trimmed.starts_with('[') {
        return Some(PassthroughBodyMode::NonSseJson);
    }
    Some(PassthroughBodyMode::RawSse)
}

fn format_non_sse_stream_response(
    raw: &str,
    egress: ProtocolEndpoint,
    ingress: ProtocolEndpoint,
) -> Option<(String, crate::protocol::ir::AiResponse)> {
    let value = serde_json::from_str::<Value>(raw).ok()?;
    let ai_resp = egress
        .handler()
        .make_response_decoder()
        .parse_response(value)
        .ok()?;
    let deltas = ai_response_to_deltas(&ai_resp);
    let mut stream_formatter = ingress.handler().make_stream_response_encoder();
    let mut client_sse_parts = Vec::new();

    for ev in stream_formatter.format_deltas(&deltas) {
        client_sse_parts.push(ev.to_sse_string());
    }
    for ev in stream_formatter.format_done() {
        client_sse_parts.push(ev.to_sse_string());
    }

    Some((client_sse_parts.join(""), ai_resp))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::ids::GOOGLE_GEMINI_GENERATE_CONTENT_V1BETA;

    // ── BUG 3: passthrough terminal injection ──

    #[test]
    fn passthrough_tail_injects_terminal_when_nothing_seen() {
        let events = passthrough_tail_events(false, false, false);
        assert_eq!(events.len(), 2, "finish_reason chunk + [DONE] expected");
        let frame = std::str::from_utf8(&events[0]).unwrap();
        let payload = frame.trim_start().strip_prefix("data: ").unwrap().trim();
        let v: Value = serde_json::from_str(payload).unwrap();
        assert_eq!(v["choices"][0]["finish_reason"], "stop");
        assert_eq!(v["choices"][0]["delta"], serde_json::json!({}));
        assert_eq!(
            std::str::from_utf8(&events[1]).unwrap(),
            "data: [DONE]\n\n"
        );
    }

    #[test]
    fn passthrough_tail_uses_tool_calls_reason_when_seen() {
        let events = passthrough_tail_events(false, false, true);
        let payload = std::str::from_utf8(&events[0])
            .unwrap()
            .trim_start()
            .strip_prefix("data: ")
            .unwrap()
            .trim();
        let v: Value = serde_json::from_str(payload).unwrap();
        assert_eq!(v["choices"][0]["finish_reason"], "tool_calls");
    }

    #[test]
    fn passthrough_tail_suppressed_when_terminal_already_seen() {
        assert!(passthrough_tail_events(true, false, false).is_empty());
        assert!(passthrough_tail_events(false, true, false).is_empty());
        assert!(passthrough_tail_events(true, true, true).is_empty());
    }

    #[test]
    fn cost_footer_frame_is_detected_loosely() {
        assert!(is_cost_footer_frame(b"data: {\"choices\":[],\"cost\":\"0\"}\n\n"));
        // usage-only chunk with id/usage but no cost key must NOT be dropped
        assert!(!is_cost_footer_frame(
            b"data: {\"id\":\"x\",\"object\":\"chat.completion.chunk\",\"choices\":[],\"usage\":{\"prompt_tokens\":49}}\n\n"
        ));
        // real content chunk must NOT be dropped
        assert!(!is_cost_footer_frame(
            b"data: {\"id\":\"x\",\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n"
        ));
    }

    #[test]
    fn passthrough_flags_observed_from_frames() {
        let mut flags = PassthroughFlags::default();
        flags.observe_frame(b"data: {\"id\":\"1\",\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n");
        assert!(!flags.saw_done && !flags.saw_finish_reason && !flags.saw_tool_call);
        // tool_calls with null finish_reason: tool_call flag set, finish NOT
        flags.observe_frame(
            b"data: {\"id\":\"1\",\"choices\":[{\"delta\":{\"tool_calls\":[{\"id\":\"c1\"}]},\"finish_reason\":null}]}\n\n",
        );
        assert!(flags.saw_tool_call);
        assert!(!flags.saw_finish_reason);
        flags.observe_frame(b"data: [DONE]\n\n");
        assert!(flags.saw_done);

        let mut done_flags = PassthroughFlags::default();
        done_flags.observe_frame(
            b"data: {\"id\":\"1\",\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
        );
        assert!(done_flags.saw_finish_reason);
    }

    #[test]
    fn find_sse_frame_end_handles_lf_and_crlf() {
        assert_eq!(find_sse_frame_end(b"data: a\n\n"), Some(9));
        assert_eq!(find_sse_frame_end(b"data: a\r\n\r\n"), Some(11));
        assert_eq!(find_sse_frame_end(b"data: a\n\ndata: b\r\n\r\n"), Some(9));
        assert_eq!(find_sse_frame_end(b"data: partial"), None);
    }

    #[test]
    fn non_sse_gemini_stream_response_is_formatted_as_sse() {
        let raw = serde_json::json!({
            "candidates": [{
                "content": {
                    "parts": [{"text": "hello"}],
                    "role": "model"
                },
                "finishReason": "STOP",
                "index": 0
            }],
            "modelVersion": "gemini-3.5-flash",
            "responseId": "resp-json-stream",
            "usageMetadata": {
                "candidatesTokenCount": 3,
                "promptTokenCount": 5,
                "totalTokenCount": 8
            }
        })
        .to_string();

        let (sse, ai_resp) = format_non_sse_stream_response(
            &raw,
            GOOGLE_GEMINI_GENERATE_CONTENT_V1BETA,
            GOOGLE_GEMINI_GENERATE_CONTENT_V1BETA,
        )
        .expect("complete JSON stream response should format as SSE");

        assert!(sse.starts_with("data: "), "SSE must use data frames: {sse}");
        assert!(
            sse.contains("\"usageMetadata\""),
            "terminal SSE must include Gemini usage metadata: {sse}"
        );
        assert_eq!(ai_resp.content, "hello");
        assert_eq!(ai_resp.usage.prompt_tokens, 5);
        assert_eq!(ai_resp.usage.completion_tokens, 3);
        assert_eq!(ai_resp.usage.total_tokens, 8);
    }
}
