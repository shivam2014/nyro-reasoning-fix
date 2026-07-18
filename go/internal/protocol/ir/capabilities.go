package ir

// ModelCapabilities describes a model's static properties sourced from the
// models.dev catalog. Threaded from the dispatcher to encoders so they can
// clamp parameters (e.g. max_tokens) to provider-enforced limits.
type ModelCapabilities struct {
	Provider         string
	ModelID          string
	ContextWindow    uint64
	EmbeddingLength  *uint64
	OutputMaxTokens  *uint64
	ToolCall         bool
	Reasoning        bool
	InputModalities  []string
	OutputModalities []string
	InputCost        *float64
	OutputCost       *float64
}
