package schema

import (
	"encoding/json"
	"time"
)

// MemoryRecord is the atomic unit of storage in the memory substrate.
// RFC 15A.2, 15A.1: Every stored memory item MUST conform to the MemoryRecord shape.
//
// This is the base record type that MUST NOT be bypassed by any memory type.
// All memory records share this common structure with type-specific payloads.
type MemoryRecord struct {
	// ID is a globally unique identifier (UUID recommended).
	// RFC 15A.2: Required field, immutable once created.
	ID string `json:"id"`

	// Type declares the memory type (episodic, working, semantic, competence, plan_graph).
	// RFC 15A.2: Required field.
	Type MemoryType `json:"type"`

	// Sensitivity is the sensitivity classification for access control.
	// RFC 15A.2: Required field (public, low, medium, high, hyper).
	Sensitivity Sensitivity `json:"sensitivity"`

	// Confidence is the epistemic confidence in this record.
	// RFC 15A.2: Required field with range [0, 1].
	Confidence float64 `json:"confidence"`

	// Salience is the decay-weighted importance score.
	// RFC 15A.2: Required field with range [0, +inf).
	// Note: JSON schema shows max 1, but conceptual model allows unbounded salience.
	Salience float64 `json:"salience"`

	// Scope defines the visibility scope (implementation-defined).
	// RFC 15A.2: Optional field. Examples: user, device, project, workspace, global.
	Scope string `json:"scope,omitempty"`

	// Tags are free-form labels for categorization and retrieval.
	// RFC 15A.2: Optional array of strings.
	Tags []string `json:"tags,omitempty"`

	// CreatedAt is the timestamp when this record was created.
	// RFC 15A.2: Required field.
	CreatedAt time.Time `json:"created_at"`

	// UpdatedAt is the timestamp when this record was last updated.
	// RFC 15A.2: Required field.
	UpdatedAt time.Time `json:"updated_at"`

	// Lifecycle contains decay, reinforcement, and deletion metadata.
	// RFC 15A.2: Required field.
	Lifecycle Lifecycle `json:"lifecycle"`

	// Provenance links this record to its source events or artifacts.
	// RFC 15A.2: Required field.
	Provenance Provenance `json:"provenance"`

	// Relations are graph edges to other MemoryRecords.
	// RFC 15A.2: Optional array of relations.
	Relations []Relation `json:"relations,omitempty"`

	// Payload is the type-specific structured content.
	// RFC 15A.2: Required field, one of the five payload types.
	Payload Payload `json:"payload"`

	// Interpretation stores ingest-side extracted metadata and candidate links.
	Interpretation *Interpretation `json:"interpretation,omitempty"`

	// AuditLog tracks all actions performed on this record.
	// RFC 15A.8: Required for auditability.
	AuditLog []AuditEntry `json:"audit_log"`
}

// memoryRecordJSON is used for custom JSON marshaling/unmarshaling.
type memoryRecordJSON struct {
	ID             string          `json:"id"`
	Type           MemoryType      `json:"type"`
	Sensitivity    Sensitivity     `json:"sensitivity"`
	Confidence     float64         `json:"confidence"`
	Salience       float64         `json:"salience"`
	Scope          string          `json:"scope,omitempty"`
	Tags           []string        `json:"tags,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
	Lifecycle      Lifecycle       `json:"lifecycle"`
	Provenance     Provenance      `json:"provenance"`
	Relations      []Relation      `json:"relations,omitempty"`
	Payload        json.RawMessage `json:"payload"`
	Interpretation *Interpretation `json:"interpretation,omitempty"`
	AuditLog       []AuditEntry    `json:"audit_log"`
}

// MarshalJSON implements json.Marshaler for MemoryRecord.
func (mr MemoryRecord) MarshalJSON() ([]byte, error) {
	payloadBytes, err := json.Marshal(mr.Payload)
	if err != nil {
		return nil, err
	}

	return json.Marshal(memoryRecordJSON{
		ID:             mr.ID,
		Type:           mr.Type,
		Sensitivity:    mr.Sensitivity,
		Confidence:     mr.Confidence,
		Salience:       mr.Salience,
		Scope:          mr.Scope,
		Tags:           mr.Tags,
		CreatedAt:      mr.CreatedAt,
		UpdatedAt:      mr.UpdatedAt,
		Lifecycle:      mr.Lifecycle,
		Provenance:     mr.Provenance,
		Relations:      mr.Relations,
		Payload:        payloadBytes,
		Interpretation: mr.Interpretation,
		AuditLog:       mr.AuditLog,
	})
}

// UnmarshalJSON implements json.Unmarshaler for MemoryRecord.
func (mr *MemoryRecord) UnmarshalJSON(data []byte) error {
	var raw memoryRecordJSON
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	mr.ID = raw.ID
	mr.Type = raw.Type
	mr.Sensitivity = raw.Sensitivity
	mr.Confidence = raw.Confidence
	mr.Salience = raw.Salience
	mr.Scope = raw.Scope
	mr.Tags = raw.Tags
	mr.CreatedAt = raw.CreatedAt
	mr.UpdatedAt = raw.UpdatedAt
	mr.Lifecycle = raw.Lifecycle
	mr.Provenance = raw.Provenance
	mr.Relations = raw.Relations
	mr.Interpretation = raw.Interpretation
	mr.AuditLog = raw.AuditLog

	// Unmarshal payload based on type
	var wrapper PayloadWrapper
	if err := wrapper.UnmarshalJSON(raw.Payload); err != nil {
		return err
	}
	mr.Payload = wrapper.Payload

	return nil
}

// NewMemoryRecord creates a new MemoryRecord with required fields initialized.
// This is a convenience constructor that sets timestamps and creates empty slices.
func NewMemoryRecord(id string, memType MemoryType, sensitivity Sensitivity, payload Payload) *MemoryRecord {
	now := time.Now().UTC()
	return &MemoryRecord{
		ID:          id,
		Type:        memType,
		Sensitivity: sensitivity,
		Confidence:  1.0,
		Salience:    1.0,
		CreatedAt:   now,
		UpdatedAt:   now,
		Lifecycle: Lifecycle{
			Decay: DecayProfile{
				Curve:           DecayCurveExponential,
				HalfLifeSeconds: 86400, // 1 day default
			},
			LastReinforcedAt: now,
			DeletionPolicy:   DeletionPolicyAutoPrune,
		},
		Provenance: Provenance{
			Sources: []ProvenanceSource{},
		},
		Relations:      []Relation{},
		Payload:        payload,
		Interpretation: nil,
		AuditLog: []AuditEntry{
			{
				Action:    AuditActionCreate,
				Actor:     "system",
				Timestamp: now,
				Rationale: "Initial creation",
			},
		},
	}
}

// Validate performs basic validation on the MemoryRecord.
// Returns an error if required fields are missing or invalid.
func (mr *MemoryRecord) Validate() error {
	if mr.ID == "" {
		return &ValidationError{Field: "id", Message: "id is required"}
	}
	if mr.Type == "" {
		return &ValidationError{Field: "type", Message: "type is required"}
	}
	if mr.Sensitivity == "" {
		return &ValidationError{Field: "sensitivity", Message: "sensitivity is required"}
	}
	if !IsValidSensitivity(mr.Sensitivity) {
		return &ValidationError{Field: "sensitivity", Message: "sensitivity must be one of: public, low, medium, high, hyper"}
	}
	if mr.Confidence < 0 || mr.Confidence > 1 {
		return &ValidationError{Field: "confidence", Message: "confidence must be in range [0, 1]"}
	}
	if mr.Salience < 0 {
		return &ValidationError{Field: "salience", Message: "salience must be >= 0"}
	}
	if mr.Payload == nil {
		return &ValidationError{Field: "payload", Message: "payload is required"}
	}
	return nil
}

// ValidationError represents a validation failure for a specific field.
type ValidationError struct {
	Field   string
	Message string
}

func (e *ValidationError) Error() string {
	return "validation error for " + e.Field + ": " + e.Message
}
