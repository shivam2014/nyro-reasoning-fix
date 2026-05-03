# Fix: mlx-lm Reasoning Not Visible Through Nyro Router

**Date:** 2026-05-03  
**Branch:** `fix>local-mlx-reasoning-not-captured`  
**Commit:** `afb489c`  
**File changed:** `crates/nyro-core/src/protocol/codec/openai/stream.rs`  
**Symptoms:** Reasoning/thinking output from mlx-lm is silently dropped when proxied through Nyro. Direct connection to mlx-lm works fine.

---

## 1. Background: How Reasoning Flows Through the System

When you send a chat request through Nyro to mlx-lm, the data crosses four layers:

```
  Client (Codex / llama-webui)
       │  POST /v1/chat/completions  {stream: true, messages: [...]}
       ▼
  ┌─────────────────────────────────────────┐
  │           Nyro Router (:19530)          │
  │                                         │
  │  1. IngressDecoder (OpenAI)             │
  │     body → InternalRequest              │
  │                                         │
  │  2. EgressEncoder (OpenAI)              │
  │     InternalRequest → JSON body         │
  │                                         │
  │  3. ProxyClient.call_stream()           │
  │     POST → upstream (mlx-lm :8000)      │
  │                                         │
  │  4. StreamParser (OpenAI)               │
  │     upstream SSE bytes → StreamDelta[]  │
  │     ◄── BUG LIVES HERE                  │
  │                                         │
  │  5. StreamFormatter (OpenAI)            │
  │     StreamDelta[] → downstream SSE      │
  │     ◄── BUG ALSO LIVES HERE (non-stream)│
  │                                         │
  └─────────────────────────────────────────┘
       │  SSE stream / JSON response
       ▼
  Client sees final answer but no reasoning
```

### The field-name convention

The OpenAI Chat Completions API spec defines reasoning content in the `reasoning_content` field:

```json
{
  "choices": [{
    "delta": {
      "reasoning_content": "the model's chain of thought"
    }
  }]
}
```

This is what DeepSeek, OpenAI o-series, and most reasoning models emit.

**mlx-lm uses a different field name.** Looking at `mlx_lm/server.py` line 1444:

```python
choice[key_name]["reasoning"] = reasoning_text
# key_name = "delta" for streaming, "message" for non-streaming
```

mlx-lm sends `reasoning` (without `_content` suffix).

---

## 2. Root Cause Analysis

### Bug A: `extract_reasoning_from_message` only checks `reasoning_content`

**File:** `crates/nyro-core/src/protocol/codec/openai/stream.rs`  
**BEFORE (line 496):**

```rust
pub(crate) fn extract_reasoning_from_message(message: &Value) -> Option<String> {
    // Only knows about "reasoning_content"
    if let Some(reasoning) = message.get("reasoning_content").and_then(|v| v.as_str()) {
        return Some(reasoning.to_string());
    }
    // Also handles "reasoning_details" array format, but not "reasoning"
    let details = message.get("reasoning_details")...
}
```

This function is called from **two places**, so a single fix covers both paths:

**Path 1 — Non-streaming response parser** (`OpenAIResponseParser::parse_response`, line 29):
```rust
let reasoning_content = message.and_then(extract_reasoning_from_message);
```

**Path 2 — Streaming chunk parser** (`OpenAIStreamParser::parse_openai_chunk`, line 230):
```rust
if let Some(reasoning) = extract_reasoning_from_message(delta) {
    if !reasoning.is_empty() {
        deltas.push(StreamDelta::ReasoningDelta(reasoning));
    }
}
```

Since the MLX server sends `{"delta": {"reasoning": "..."}}` (not `{"delta": {"reasoning_content": "..."}}`), extract returns `None` and **no ReasoningDelta is ever emitted** — the reasoning is silently discarded.

### Bug B: Non-streaming formatter drops reasoning_content

Even when `InternalResponse.reasoning_content` IS populated (e.g., from a spec-compliant backend), the non-streaming response formatter never includes it in the output.

**BEFORE (line 88):**

```rust
impl ResponseFormatter for OpenAIResponseFormatter {
    fn format_response(&self, resp: &InternalResponse) -> Value {
        let mut message = serde_json::json!({
            "role": "assistant",
            "content": resp.content,
        });
        // reasoning_content is never added to message
        // ... tool_calls handling ...
    }
}
```

So a non-streaming request would **always** lose reasoning, even from backends using the correct field name.

### Why llama-webui works fine

The llama-webui `server.py` has `SseTimingInjector._fix_reasoning_field()` which renames the field before forwarding:

```python
def _fix_reasoning_field(self, data):
    delta = data["choices"][0].get("delta")
    if delta is not None and "reasoning" in delta:
        delta["reasoning_content"] = delta.pop("reasoning")
```

Nyro had no equivalent code.

---

## 3. The Fix

### Change 1 — Accept both field names

**AFTER (line 496):**

```rust
pub(crate) fn extract_reasoning_from_message(message: &Value) -> Option<String> {
    if let Some(reasoning) = message.get("reasoning_content").and_then(|v| v.as_str()) {
        return Some(reasoning.to_string());
    }
    // NEW: Also accept "reasoning" (mlx-lm compat)
    if let Some(reasoning) = message.get("reasoning").and_then(|v| v.as_str()) {
        return Some(reasoning.to_string());
    }
    // ... rest unchanged
}
```

### Change 2 — Emit reasoning_content in non-streaming responses

**AFTER (line 88):**

```rust
let mut message = serde_json::json!({
    "role": "assistant",
    "content": resp.content,
});
// NEW: Include reasoning_content when present
if let Some(ref reasoning) = resp.reasoning_content {
    message
        .as_object_mut()
        .unwrap()
        .insert("reasoning_content".into(), Value::String(reasoning.clone()));
}
```

---

## 4. Test Coverage

Four tests were added to the `mod tests` block in the same file:

| Test | What it proves |
|------|---------------|
| `test_extract_reasoning_mlx_field_name` | `extract_reasoning_from_message` returns `Some("my reasoning")` given `{"reasoning": "my reasoning"}` |
| `test_parse_response_with_reasoning_field` | `OpenAIResponseParser` populates `reasoning_content` from a JSON response containing `"reasoning"` field |
| `test_format_response_includes_reasoning_content` | `OpenAIResponseFormatter` emits `"reasoning_content"` key in the output message when `InternalResponse.reasoning_content` is `Some` |
| `test_stream_reasoning_field_from_mlx` | `OpenAIStreamParser` emits `StreamDelta::ReasoningDelta` from SSE chunks containing `"reasoning"` in the delta object |

All 125 tests pass (58 unit + 67 integration) with zero regressions.

---

## 5. How to Verify on Real Hardware

```bash
# 1. Rebuild Nyro
cd /Users/shivam94/nyro-mod/nyro-src
cargo build --release

# 2. Restart Nyro with the new binary

# 3. Test streaming (SSE)
curl -N http://localhost:19530/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{"model":"qwen3-35b","messages":[{"role":"user","content":"Count to 3 slowly"}],"stream":true}' \
  | grep reasoning_content

# 4. Test non-streaming
curl http://localhost:19530/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{"model":"qwen3-35b","messages":[{"role":"user","content":"Count to 3 slowly"}],"stream":false}' \
  | python3 -m json.tool | grep reasoning_content
```

You should see `"reasoning_content"` appear in the response for both streaming and non-streaming calls.
