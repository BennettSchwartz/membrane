// Package retrieval implements the layered memory retrieval service
// as specified in RFC 15.8 and RFC 15A.11.
package retrieval

import "github.com/GustyCube/membrane/pkg/schema"

// sensitivityOrder maps sensitivity levels to a numeric ordering.
// Higher values indicate more sensitive content.
var sensitivityOrder = map[schema.Sensitivity]int{
	schema.SensitivityPublic: 0,
	schema.SensitivityLow:    1,
	schema.SensitivityMedium: 2,
	schema.SensitivityHigh:   3,
	schema.SensitivityHyper:  4,
}

// SensitivityLevel returns the numeric level for a given sensitivity.
// Unknown sensitivity values return -1.
func SensitivityLevel(s schema.Sensitivity) int {
	level, ok := sensitivityOrder[s]
	if !ok {
		return -1
	}
	return level
}

// TrustContext gates retrieval based on the requester's trust attributes.
// Records that exceed the trust context's sensitivity or fall outside
// the allowed scopes are filtered out during retrieval.
type TrustContext struct {
	// MaxSensitivity is the maximum sensitivity level the requester is allowed to access.
	MaxSensitivity schema.Sensitivity

	// Authenticated indicates whether the requester has been authenticated.
	Authenticated bool

	// ActorID identifies who is making the retrieval request.
	ActorID string

	// Scopes lists the scopes the requester is allowed to access.
	// An empty slice means all scopes are allowed.
	Scopes []string
}

// NewTrustContext creates a new TrustContext with the given parameters.
func NewTrustContext(maxSensitivity schema.Sensitivity, authenticated bool, actorID string, scopes []string) *TrustContext {
	return &TrustContext{
		MaxSensitivity: maxSensitivity,
		Authenticated:  authenticated,
		ActorID:        actorID,
		Scopes:         scopes,
	}
}

// Allows checks whether the given record is accessible under this trust context.
// A record is allowed if:
//   - Its sensitivity level does not exceed the trust context's MaxSensitivity.
//   - Its scope matches one of the allowed scopes (or the trust context has no scope restrictions).
func (tc *TrustContext) Allows(record *schema.MemoryRecord) bool {
	if record == nil {
		return false
	}

	// Check sensitivity: record sensitivity must be <= max allowed.
	recordLevel := SensitivityLevel(record.Sensitivity)
	maxLevel := SensitivityLevel(tc.MaxSensitivity)
	if recordLevel > maxLevel {
		return false
	}

	// Check scope: if scopes are specified, the record's scope must match one.
	// Note: records with empty scope are unscoped and available to all contexts,
	// so they bypass this check (empty scope = not restricted to any particular scope).
	if len(tc.Scopes) > 0 && record.Scope != "" {
		found := false
		for _, s := range tc.Scopes {
			if s == record.Scope {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	return true
}
