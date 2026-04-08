package schema

// InterpretationStatus describes whether ingest-side interpretation is still
// tentative or has been resolved against known records/entities.
type InterpretationStatus string

const (
	InterpretationStatusTentative InterpretationStatus = "tentative"
	InterpretationStatusResolved  InterpretationStatus = "resolved"
)

// EntityKind describes a canonical entity category.
type EntityKind string

const (
	EntityKindPerson  EntityKind = "person"
	EntityKindTool    EntityKind = "tool"
	EntityKindProject EntityKind = "project"
	EntityKindFile    EntityKind = "file"
	EntityKindConcept EntityKind = "concept"
	EntityKindOther   EntityKind = "other"
)

// Mention is an extracted surface-form mention from captured content.
type Mention struct {
	Surface           string     `json:"surface"`
	EntityKind        EntityKind `json:"entity_kind,omitempty"`
	CanonicalEntityID string     `json:"canonical_entity_id,omitempty"`
	Confidence        float64    `json:"confidence,omitempty"`
	Aliases           []string   `json:"aliases,omitempty"`
}

// RelationCandidate is an extracted candidate relation that may or may not be
// resolved to a concrete target.
type RelationCandidate struct {
	Predicate      string  `json:"predicate"`
	TargetRecordID string  `json:"target_record_id,omitempty"`
	TargetEntityID string  `json:"target_entity_id,omitempty"`
	Confidence     float64 `json:"confidence,omitempty"`
	Resolved       bool    `json:"resolved,omitempty"`
}

// ReferenceCandidate is an extracted candidate reference to another entity or
// memory record.
type ReferenceCandidate struct {
	Ref            string  `json:"ref"`
	TargetRecordID string  `json:"target_record_id,omitempty"`
	TargetEntityID string  `json:"target_entity_id,omitempty"`
	Confidence     float64 `json:"confidence,omitempty"`
	Resolved       bool    `json:"resolved,omitempty"`
}

// Interpretation stores ingest-side structured interpretation metadata.
type Interpretation struct {
	Status               InterpretationStatus `json:"status,omitempty"`
	Summary              string               `json:"summary,omitempty"`
	ProposedType         MemoryType           `json:"proposed_type,omitempty"`
	TopicalLabels        []string             `json:"topical_labels,omitempty"`
	Mentions             []Mention            `json:"mentions,omitempty"`
	RelationCandidates   []RelationCandidate  `json:"relation_candidates,omitempty"`
	ReferenceCandidates  []ReferenceCandidate `json:"reference_candidates,omitempty"`
	ExtractionConfidence float64              `json:"extraction_confidence,omitempty"`
}
