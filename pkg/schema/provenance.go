package schema

import "time"

// Provenance tracks the sources and origins of a memory record.
// RFC 15A.4: Provenance links connect memory to source events or artifacts.
type Provenance struct {
	// Sources is the list of provenance sources that justify this record.
	// RFC 15A.4: Required field containing at least one source.
	Sources []ProvenanceSource `json:"sources"`

	// CreatedBy identifies what created this record (e.g., classifier/policy/consolidator version).
	// RFC 15A.4: Optional field for tracking creation origin.
	CreatedBy string `json:"created_by,omitempty"`
}

// ProvenanceSource represents a single source of evidence for a memory record.
// RFC 15A.4: Each source has a kind, reference, and optional hash.
type ProvenanceSource struct {
	// Kind specifies the type of source (event, artifact, tool_call, observation, outcome).
	// RFC 15A.4: Required field.
	Kind ProvenanceKind `json:"kind"`

	// Ref is an opaque reference into the host system.
	// RFC 15A.4: Required field.
	Ref string `json:"ref"`

	// Hash is an optional content hash for immutability verification.
	// RFC 15A.4: Optional field for content-addressed artifacts.
	Hash string `json:"hash,omitempty"`

	// CreatedBy identifies the actor or system that created this source.
	// Extension field for detailed provenance tracking.
	CreatedBy string `json:"created_by,omitempty"`

	// Timestamp records when this source was created or observed.
	// RFC 15A.8: Provenance references include timestamps.
	Timestamp time.Time `json:"timestamp,omitempty"`
}

// ProvenanceRef is a reference to evidence supporting a memory record.
// RFC 15A.8: Used for linking semantic payloads to their evidence.
type ProvenanceRef struct {
	// SourceType identifies the type of source (event, tool, observation, human).
	// RFC 15A.8: Required field.
	SourceType string `json:"source_type"`

	// SourceID is the unique identifier for the source.
	// RFC 15A.8: Required field.
	SourceID string `json:"source_id"`

	// Timestamp records when this evidence was created.
	// RFC 15A.8: Required field.
	Timestamp time.Time `json:"timestamp"`
}
