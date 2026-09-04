package schema

import (
	"fmt"
	"math"
	"strings"
	"time"
	"unicode"
)

const (
	GraphPredicateMentionsEntity   = "mentions_entity"
	GraphPredicateMentionedIn      = "mentioned_in"
	GraphPredicateSubjectEntity    = "subject_entity"
	GraphPredicateFactSubjectOf    = "fact_subject_of"
	GraphPredicateObjectEntity     = "object_entity"
	GraphPredicateFactObjectOf     = "fact_object_of"
	GraphPredicateDerivedFrom      = "derived_from"
	GraphPredicateDerivedSemantic  = "derived_semantic"
	GraphPredicateReferencesRecord = "references_record"
	GraphPredicateReferencedBy     = "referenced_by"
	GraphPredicateDependsOn        = "depends_on"
	GraphPredicateDependencyOf     = "dependency_of"
	GraphPredicateUses             = "uses"
	GraphPredicateUsedBy           = "used_by"
	GraphPredicateCausedBy         = "caused_by"
	GraphPredicateCauses           = "causes"
	GraphPredicateSupports         = "supports"
	GraphPredicateSupportedBy      = "supported_by"
	GraphPredicateContradicts      = "contradicts"
	GraphPredicateContradictedBy   = "contradicted_by"
	GraphPredicateSupersedes       = "supersedes"
	GraphPredicateSupersededBy     = "superseded_by"
	GraphPredicateContestedBy      = "contested_by"
	GraphPredicateContests         = "contests"
)

// NormalizeGraphPredicate returns the canonical storage spelling for graph predicates.
func NormalizeGraphPredicate(predicate string) string {
	normalized := splitGraphPredicateCase(predicate)
	fields := strings.FieldsFunc(strings.ToLower(normalized), func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	})
	return strings.Join(fields, "_")
}

func splitGraphPredicateCase(predicate string) string {
	runes := []rune(strings.TrimSpace(predicate))
	if len(runes) == 0 {
		return ""
	}
	var b strings.Builder
	for i, r := range runes {
		if unicode.IsUpper(r) && i > 0 && graphPredicateCaseBoundary(runes, i) {
			b.WriteRune('_')
		}
		b.WriteRune(r)
	}
	return b.String()
}

func graphPredicateCaseBoundary(runes []rune, i int) bool {
	prev := runes[i-1]
	if !unicode.IsLetter(prev) && !unicode.IsDigit(prev) {
		return false
	}
	if unicode.IsLower(prev) || unicode.IsDigit(prev) {
		return true
	}
	return i+1 < len(runes) && unicode.IsLower(runes[i+1])
}

// InverseGraphPredicate returns the graph predicate used for the reverse edge.
func InverseGraphPredicate(predicate string) string {
	predicate = NormalizeGraphPredicate(predicate)
	switch predicate {
	case GraphPredicateMentionsEntity:
		return GraphPredicateMentionedIn
	case GraphPredicateMentionedIn:
		return GraphPredicateMentionsEntity
	case GraphPredicateReferencesRecord:
		return GraphPredicateReferencedBy
	case GraphPredicateReferencedBy:
		return GraphPredicateReferencesRecord
	case GraphPredicateDerivedSemantic:
		return GraphPredicateDerivedFrom
	case GraphPredicateDerivedFrom:
		return GraphPredicateDerivedSemantic
	case GraphPredicateSubjectEntity:
		return GraphPredicateFactSubjectOf
	case GraphPredicateFactSubjectOf:
		return GraphPredicateSubjectEntity
	case GraphPredicateObjectEntity:
		return GraphPredicateFactObjectOf
	case GraphPredicateFactObjectOf:
		return GraphPredicateObjectEntity
	case GraphPredicateDependsOn:
		return GraphPredicateDependencyOf
	case GraphPredicateDependencyOf:
		return GraphPredicateDependsOn
	case GraphPredicateUses:
		return GraphPredicateUsedBy
	case GraphPredicateUsedBy:
		return GraphPredicateUses
	case GraphPredicateCausedBy:
		return GraphPredicateCauses
	case GraphPredicateCauses:
		return GraphPredicateCausedBy
	case GraphPredicateSupports:
		return GraphPredicateSupportedBy
	case GraphPredicateSupportedBy:
		return GraphPredicateSupports
	case GraphPredicateContradicts:
		return GraphPredicateContradictedBy
	case GraphPredicateContradictedBy:
		return GraphPredicateContradicts
	case GraphPredicateSupersedes:
		return GraphPredicateSupersededBy
	case GraphPredicateSupersededBy:
		return GraphPredicateSupersedes
	case GraphPredicateContestedBy:
		return GraphPredicateContests
	case GraphPredicateContests:
		return GraphPredicateContestedBy
	default:
		return "inverse_of_" + predicate
	}
}

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

// Validate checks relation fields before an edge reaches storage.
func (rel Relation) Validate() error {
	return validateRelation(rel, "relation")
}

func validateRelation(rel Relation, field string) error {
	if NormalizeGraphPredicate(rel.Predicate) == "" {
		return &ValidationError{Field: field + ".predicate", Message: "predicate is required for relations"}
	}
	if strings.TrimSpace(rel.TargetID) == "" {
		return &ValidationError{Field: field + ".target_id", Message: "target_id is required for relations"}
	}
	if math.IsNaN(rel.Weight) || math.IsInf(rel.Weight, 0) || rel.Weight < 0 || rel.Weight > 1 {
		return &ValidationError{Field: field + ".weight", Message: "weight must be finite and in range [0, 1]"}
	}
	return nil
}

// GraphEdge is a concrete graph edge with both source and target IDs.
type GraphEdge struct {
	SourceID  string    `json:"source_id"`
	Predicate string    `json:"predicate"`
	TargetID  string    `json:"target_id"`
	Weight    float64   `json:"weight,omitempty"`
	CreatedAt time.Time `json:"created_at,omitempty"`
}

// Validate checks graph edge fields before a concrete edge is exposed or written.
func (edge GraphEdge) Validate() error {
	if strings.TrimSpace(edge.SourceID) == "" {
		return &ValidationError{Field: "edge.source_id", Message: "source_id is required for graph edges"}
	}
	if NormalizeGraphPredicate(edge.Predicate) == "" {
		return &ValidationError{Field: "edge.predicate", Message: "predicate is required for graph edges"}
	}
	if strings.TrimSpace(edge.TargetID) == "" {
		return &ValidationError{Field: "edge.target_id", Message: "target_id is required for graph edges"}
	}
	if math.IsNaN(edge.Weight) || math.IsInf(edge.Weight, 0) || edge.Weight < 0 || edge.Weight > 1 {
		return &ValidationError{Field: "edge.weight", Message: "weight must be finite and in range [0, 1]"}
	}
	return nil
}

func relationField(index int) string {
	return fmt.Sprintf("relations[%d]", index)
}
