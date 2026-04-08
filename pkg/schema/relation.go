package schema

import "time"

// Relation represents a relationship between memory records.
// RFC 15A.5: Relations form graph edges between MemoryRecords.
type Relation struct {
	// Predicate describes the relationship type.
	// RFC 15A.5: Required field. Examples: supports, contradicts, derived_from, supersedes.
	Predicate string `json:"predicate"`

	// TargetID is the ID of the related memory record.
	// RFC 15A.5: Required field.
	TargetID string `json:"target_id"`

	// Weight indicates the strength of the relationship.
	// RFC 15A.5: Optional field with range [0, 1].
	Weight float64 `json:"weight,omitempty"`

	// CreatedAt records when this relation was established.
	// Extension field for tracking relation history.
	CreatedAt time.Time `json:"created_at,omitempty"`
}

// GraphEdge is a concrete graph edge with both source and target IDs.
type GraphEdge struct {
	SourceID  string    `json:"source_id"`
	Predicate string    `json:"predicate"`
	TargetID  string    `json:"target_id"`
	Weight    float64   `json:"weight,omitempty"`
	CreatedAt time.Time `json:"created_at,omitempty"`
}
