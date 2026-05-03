# Fix: MLX-LM Reasoning Field Name Not Captured by Nyro Router

## Problem

When streaming chat completions from an **mlx-lm** server backend through the Nyro router, reasoning/thinking tokens were **silently dropped**. The final response would contain only the content text without any reasoning trace, even though the MLX server was correctly emitting reasoning tokens.

This was affecting all clients using Nyro as a proxy to MLX-backed models (e.g., `Qwen-3.6-35B-A3B-mlx`). The same models worked correctly when accessed directly via the MLX server on port 8000.

## Root Cause

### The Field Name Mismatch

MLX-LM's OpenAI-compatible server sends reasoning tokens using the field name `"reasoning"` in its streaming delta, while the **OpenAI API spec** (and therefore most clients) expects `"reasoning_content"`.

**What MLX sends (direct, port 8000):**
```json
{"choices":[{"delta":{"reasoning":"step-by-step thinking..."},"finish_reason":null,"index":0}]}
```

**What OpenAI clients expect:**
```json
{"choices":[{"delta":{"reasoning_content":"step-by-step thinking..."},"finish_reason":null,"index":0}]}
```

Nyro's `extract_reasoning_from_message()` function only checked for `"reasoning_content"` and `"reasoning_details"`, so MLX's `"reasoning"` field was simply ignored.

### Two Missing Paths

The bug manifested in two places within `crates/nyro-core/src/protocol/codec/openai/stream.rs`:

1. **Streaming path** (`format_streaming_event` / `extract_reasoning_from_message`):  
   When parsing each streaming delta chunk, the function `extract_reasoning_from_message()` looked at `message.get("reasoning_content")` but had no fallback for `message.get("reasoning")`. MLX's `"reasoning"` field was silently ignored.

2. **Non-streaming path** (`format_response`):  
   When the backend returned a non-streaming (buffered) response with a `reasoning` field, the response object's `reasoning_content` field was never populated from it, so the final response omitted reasoning entirely.

## The Fix

### File: `crates/nyro-core/src/protocol/codec/openai/stream.rs`

#### Change 1: `extract_reasoning_from_message()` — Add `"reasoning"` fallback

**Before (line ~502):**
```rust
pub(crate) fn extract_reasoning_from_message(message: &Value) -> Option<String> {
    if let Some(reasoning) = message.get("reasoning_content").and_then(|v| v.as_str()) {
        return Some(reasoning.to_string());
    }
    // ... check reasoning_details ...
}
```

**After:**
```rust
pub(crate) fn extract_reasoning_from_message(message: &Value) -> Option<String> {
    if let Some(reasoning) = message.get("reasoning_content").and_then(|v| v.as_str()) {
        return Some(reasoning.to_string());
    }
    // Some backends (e.g. mlx-lm) send the field as "reasoning" instead
    // of "reasoning_content".  Accept both.
    if let Some(reasoning) = message.get("reasoning").and_then(|v| v.as_str()) {
        return Some(reasoning.to_string());
    }
    let details = message.get("reasoning_details").and_then(|v| v.as_array())?;
    // ...
}
```

#### Change 2: `format_response()` — Populate `reasoning_content` in non-streaming output

**Before (around line 71):**
```rust
StreamOutput {
    content: message_content,
    reasoning_content: None,  // Was never populated from non-streaming response
    reasoning_signature: None,
}
```

**After:**
```rust
let reasoning_content = message.and_then(extract_reasoning_from_message);
// ...
StreamOutput {
    content: message_content,
    reasoning_content,
    reasoning_signature: None,
}
```

And in the JSON serialization:
```rust
if let Some(ref reasoning) = resp.reasoning_content {
    obj.as_object_mut()
        .unwrap()
        .insert("reasoning_content".into(), Value::String(reasoning.clone()));
}
```

## Data Flow Diagram

```
MLX-LM Server (port 8000)
  │
  │  Streaming delta chunk:
  │  {"choices":[{"delta":{"reasoning":"step 1..."}}]}
  │                                     ↑
  │                           Field name: "reasoning"
  │
  ▼
Nyro Router (port 19530)
  │
  │  extract_reasoning_from_message():
  │    ├── checks "reasoning_content" → None
  │    ├── checks "reasoning" → Some("step 1...")  ← NEW FALLBACK
  │    └── returns Some("step 1...")
  │
  │  Output to client:
  │  {"choices":[{"delta":{"reasoning_content":"step 1..."}}]}
  │                                     ↑
  │                           Field name: "reasoning_content"
  │
  ▼
Client (Codex, llama-webui, etc.)
  │  Receives standard OpenAI-compatible
  │  reasoning_content deltas
```

## Verification

### Direct MLX (port 8000) — raw `"reasoning"` field:
```bash
curl -s http://localhost:8000/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{"model":"<model-path>","messages":[{"role":"user","content":"..."}],"stream":true}' \
  | grep -o '"reasoning":"[^"]*"'
```
Shows: `"reasoning":"Here"`, `"reasoning":"'s"`, etc.

### Through Nyro (port 19530) — translated to `"reasoning_content"`:
```bash
curl -s http://localhost:19530/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{"model":"<route-name>","messages":[{"role":"user","content":"..."}],"stream":true}' \
  | grep -o '"reasoning_content":"[^"]*"'
```
Shows: `"reasoning_content":"Here"`, `"reasoning_content":"'s"`, etc.

### Count reasoning tokens:
```bash
# Direct MLX: Count "reasoning" field occurrences
curl -s http://localhost:8000/v1/chat/completions ... \
  | tr ',' '\n' | grep -c '"reasoning"'

# Through Nyro: Count "reasoning_content" field occurrences
curl -s http://localhost:19530/v1/chat/completions ... \
  | tr ',' '\n' | grep -c '"reasoning_content"'
```
Both should return approximately the same count (~200–300 for a reasoning-heavy prompt).

## Unit Tests

Four tests were added/updated to cover both field names and both streaming/non-streaming paths:

| Test | What it validates |
|------|------------------|
| `test_parse_response_with_reasoning_content` | Non-streaming response with `"reasoning_content"` |
| `test_parse_response_with_mlx_reasoning` | Non-streaming response with `"reasoning"` (MLX format) |
| `test_parse_streaming_delta_with_reasoning_content` | Streaming delta with `"reasoning_content"` |
| `test_parse_streaming_delta_with_mlx_reasoning` | Streaming delta with `"reasoning"` (MLX format) |

```bash
cargo test -p nyro-core --lib -- protocol::codec::openai::stream::tests
```

## Related Notes

- The continuous `/v1/models` polling spam on port 8000 was traced to **llama-webui**, not Nyro. Nyro uses a standard model discovery mechanism.
- The MLX server is started with `--chat-template-args '{"enable_thinking":true, "preserve_thinking": true}'` to ensure reasoning tokens are emitted and preserved.
