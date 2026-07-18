package proxy

import (
	"net/http"
	"slices"
	"sort"
	"strings"

	"github.com/nyroway/nyro/go/internal/capabilities"
	"github.com/nyroway/nyro/go/internal/webutil"
)

// handleModelsList serves GET /v1/models — the OpenAI-compatible client
// discovery endpoint. It lists the client-facing model names this gateway
// exposes, filtered by the caller's API key: open routes (enable_auth=false)
// are always listed; auth-gated routes appear only when the caller presents a
// valid, enabled, non-expired key granted access to them. Output mirrors the
// Rust proxy/handler.rs models_list: {object:"list", data:[{id, object:"model",
// created:0, owned_by:"Nyro"}]}, de-duplicated and sorted by name.
//
// Each model entry is enriched with max_context_length and max_output_tokens
// from the static capabilities catalog (models.dev.json) when available,
// allowing downstream clients to auto-detect the correct context window.
func handleModelsList(w http.ResponseWriter, r *http.Request, gw *Gateway) {
	var grantedRoutes []string
	snap := gw.snapshot()
	if raw := extractKey(r); raw != "" {
		if rec := snap.FindKey(raw); rec != nil && rec.Enabled {
			if rec.ExpiresAt == "" || !expired(rec.ExpiresAt) {
				grantedRoutes = rec.Routes
			}
		}
	}

	routes := snap.RoutesList()
	seen := map[string]struct{}{}
	var names []string
	// Track the first enabled upstream's provider for each model name, for
	// capabilities lookup. A model may have multiple route entries; the first
	// one with a resolvable provider wins.
	providerFor := map[string]string{}
	for _, rt := range routes {
		if rt.EnableAuth {
			if !slices.Contains(grantedRoutes, rt.Model) {
				continue
			}
		}
		name := strings.TrimSpace(rt.Model)
		if name == "" {
			continue
		}
		if _, dup := seen[name]; dup {
			continue
		}
		seen[name] = struct{}{}
		names = append(names, name)
		// Resolve the provider from the first enabled upstream target.
		for _, ru := range rt.Upstreams {
			if !ru.Enabled {
				continue
			}
			if u := snap.UpstreamGet(ru.UpstreamID); u != nil && u.Enabled {
				providerFor[name] = u.Provider
				break
			}
		}
	}
	sort.Strings(names)

	data := make([]map[string]any, 0, len(names))
	for _, n := range names {
		entry := map[string]any{"id": n, "object": "model", "created": 0, "owned_by": "Nyro"}
		// Enrich with capabilities from the static catalog (models.dev.json).
		if provider, ok := providerFor[n]; ok {
			if caps := capabilities.Lookup(provider, n); caps != nil {
				entry["max_context_length"] = caps.ContextWindow
				if caps.OutputMaxTokens != nil {
					entry["max_output_tokens"] = *caps.OutputMaxTokens
				}
			}
		}
		data = append(data, entry)
	}
	webutil.JSON(w, http.StatusOK, map[string]any{"object": "list", "data": data})
}
