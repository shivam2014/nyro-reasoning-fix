// Package capabilities provides a read-only lookup of model metadata from the
// embedded models.dev.json snapshot. It is a leaf package with no dependencies
// on proxy or admin — safe to import from encoders and the dispatcher without
// introducing import cycles.
package capabilities

import (
	_ "embed"
	"encoding/json"
	"strings"
	"sync"

	"github.com/nyroway/nyro/go/internal/protocol/ir"
)

//go:embed assets/models.dev.json
var modelsDevJSON []byte

// providerEntry is the top-level shape of a vendor in models.dev.json.
type providerEntry struct {
	Models map[string]modelEntry `json:"models"`
}

// modelEntry is the per-model shape within a vendor.
type modelEntry struct {
	ID          string         `json:"id"`
	Reasoning   bool           `json:"reasoning"`
	ToolCall    bool           `json:"tool_call"`
	Modalities  modalities     `json:"modalities"`
	Cost        cost           `json:"cost"`
	Limit       limit          `json:"limit"`
	EmbedLength *uint64        `json:"embedding_length"`
}

type modalities struct {
	Input  []string `json:"input"`
	Output []string `json:"output"`
}

type cost struct {
	Input  *float64 `json:"input"`
	Output *float64 `json:"output"`
}

type limit struct {
	Context uint64  `json:"context"`
	Output  *uint64 `json:"output"`
}

var (
	once    sync.Once
	catalog map[string]*providerEntry // keyed by lowercased vendor id
)

func load() {
	once.Do(func() {
		catalog = make(map[string]*providerEntry)
		_ = json.Unmarshal(modelsDevJSON, &catalog) // lenient — missing fields are zero values
	})
}

// Lookup returns the capabilities for the given provider and model, or nil if
// either is not found in the catalog. provider is lowercased before lookup
// (matches the Rust vendor_key convention for simple cases; preset mapping is
// out of scope for this port).
func Lookup(provider, model string) *ir.ModelCapabilities {
	load()

	vendor := strings.ToLower(provider)
	pe, ok := catalog[vendor]
	if !ok {
		return nil
	}
	me, ok := pe.Models[model]
	if !ok {
		return nil
	}
	return &ir.ModelCapabilities{
		Provider:         vendor,
		ModelID:          me.ID,
		ContextWindow:    me.Limit.Context,
		EmbeddingLength:  me.EmbedLength,
		OutputMaxTokens:  me.Limit.Output,
		ToolCall:         me.ToolCall,
		Reasoning:        me.Reasoning,
		InputModalities:  me.Modalities.Input,
		OutputModalities: me.Modalities.Output,
		InputCost:        me.Cost.Input,
		OutputCost:       me.Cost.Output,
	}
}
