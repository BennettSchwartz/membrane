package ingestion

import (
	"fmt"

	"github.com/GustyCube/membrane/pkg/schema"
)

// PolicyDefaults holds configurable default values for policy decisions.
type PolicyDefaults struct {
	// Sensitivity is the default sensitivity when not specified by the caller.
	Sensitivity schema.Sensitivity

	// EpisodicHalfLifeSeconds is the decay half-life for episodic memories.
	// Episodic memories are short-lived by design.
	EpisodicHalfLifeSeconds int64

	// SemanticHalfLifeSeconds is the decay half-life for semantic memories.
	// Semantic memories are long-lived stable knowledge.
	SemanticHalfLifeSeconds int64

	// WorkingHalfLifeSeconds is the decay half-life for working memories.
	// Working memories persist for the duration of a task.
	WorkingHalfLifeSeconds int64

	// DefaultInitialSalience is the starting salience for new records.
	DefaultInitialSalience float64

	// DefaultDeletionPolicy is the deletion policy for new records.
	DefaultDeletionPolicy schema.DeletionPolicy
}

// DefaultPolicyDefaults returns sensible default policy values.
func DefaultPolicyDefaults() PolicyDefaults {
	return PolicyDefaults{
		Sensitivity:             schema.SensitivityLow,
		EpisodicHalfLifeSeconds: 3600,    // 1 hour
		SemanticHalfLifeSeconds: 2592000, // 30 days
		WorkingHalfLifeSeconds:  86400,   // 1 day
		DefaultInitialSalience:  1.0,
		DefaultDeletionPolicy:   schema.DeletionPolicyAutoPrune,
	}
}

// PolicyResult contains the lifecycle metadata and validated fields produced
// by the policy engine for a given candidate.
type PolicyResult struct {
	// Sensitivity is the assigned sensitivity level.
	Sensitivity schema.Sensitivity

	// Confidence is the initial epistemic confidence.
	Confidence float64

	// Salience is the initial salience score.
	Salience float64

	// Lifecycle is the complete lifecycle configuration.
	Lifecycle schema.Lifecycle

	// DeletionPolicy is the deletion policy for the record.
	DeletionPolicy schema.DeletionPolicy
}

// PolicyEngine assigns lifecycle metadata and validates candidates before
// they are persisted as MemoryRecords.
type PolicyEngine struct {
	defaults PolicyDefaults
}

// NewPolicyEngine creates a new PolicyEngine with the given defaults.
func NewPolicyEngine(defaults PolicyDefaults) *PolicyEngine {
	return &PolicyEngine{defaults: defaults}
}

// Apply validates the candidate and produces a PolicyResult containing
// lifecycle metadata appropriate for the given memory type.
//
// Policy decisions:
//   - Sensitivity: uses candidate override if set, otherwise defaults.
//   - Confidence: based on source type (tool outputs get 0.9, observations 0.7, etc.).
//   - Decay profile: type-specific half-lives (episodic=short, semantic=long).
//   - Deletion policy: configurable default.
//   - Salience: configurable default initial value.
func (pe *PolicyEngine) Apply(candidate *MemoryCandidate, memType schema.MemoryType) (*PolicyResult, error) {
	if err := pe.validate(candidate); err != nil {
		return nil, err
	}

	result := &PolicyResult{
		Sensitivity:    pe.assignSensitivity(candidate),
		Confidence:     pe.assignConfidence(candidate),
		Salience:       pe.defaults.DefaultInitialSalience,
		DeletionPolicy: pe.defaults.DefaultDeletionPolicy,
	}

	result.Lifecycle = pe.assignLifecycle(memType, result.DeletionPolicy)

	return result, nil
}

// validate checks that the candidate has all required fields for its kind.
func (pe *PolicyEngine) validate(candidate *MemoryCandidate) error {
	if candidate.Kind == "" {
		return fmt.Errorf("ingestion policy: candidate kind is required")
	}
	if candidate.Source == "" {
		return fmt.Errorf("ingestion policy: candidate source is required")
	}
	if candidate.Timestamp.IsZero() {
		return fmt.Errorf("ingestion policy: candidate timestamp is required")
	}

	switch candidate.Kind {
	case CandidateKindEvent:
		if candidate.EventKind == "" {
			return fmt.Errorf("ingestion policy: event kind is required for event candidates")
		}
		if candidate.EventRef == "" {
			return fmt.Errorf("ingestion policy: event ref is required for event candidates")
		}
	case CandidateKindToolOutput:
		if candidate.ToolName == "" {
			return fmt.Errorf("ingestion policy: tool name is required for tool output candidates")
		}
	case CandidateKindObservation:
		if candidate.Subject == "" {
			return fmt.Errorf("ingestion policy: subject is required for observation candidates")
		}
		if candidate.Predicate == "" {
			return fmt.Errorf("ingestion policy: predicate is required for observation candidates")
		}
	case CandidateKindOutcome:
		if candidate.TargetRecordID == "" {
			return fmt.Errorf("ingestion policy: target record ID is required for outcome candidates")
		}
		if candidate.OutcomeStatus == "" {
			return fmt.Errorf("ingestion policy: outcome status is required for outcome candidates")
		}
	case CandidateKindWorkingState:
		if candidate.ThreadID == "" {
			return fmt.Errorf("ingestion policy: thread ID is required for working state candidates")
		}
		if candidate.TaskState == "" {
			return fmt.Errorf("ingestion policy: task state is required for working state candidates")
		}
	}

	return nil
}

// assignSensitivity returns the sensitivity level for the candidate.
// If the candidate has an explicit override, it is used; otherwise the default applies.
func (pe *PolicyEngine) assignSensitivity(candidate *MemoryCandidate) schema.Sensitivity {
	if candidate.Sensitivity != "" {
		return candidate.Sensitivity
	}
	return pe.defaults.Sensitivity
}

// assignConfidence returns the initial epistemic confidence based on source type.
// Tool outputs are generally more reliable than human observations.
func (pe *PolicyEngine) assignConfidence(candidate *MemoryCandidate) float64 {
	switch candidate.Kind {
	case CandidateKindEvent:
		return 0.8
	case CandidateKindToolOutput:
		return 0.9
	case CandidateKindObservation:
		return 0.7
	case CandidateKindOutcome:
		return 0.85
	case CandidateKindWorkingState:
		return 1.0
	default:
		return 0.5
	}
}

// assignLifecycle builds a Lifecycle struct with type-specific decay profiles.
func (pe *PolicyEngine) assignLifecycle(memType schema.MemoryType, deletionPolicy schema.DeletionPolicy) schema.Lifecycle {
	halfLife := pe.halfLifeForType(memType)

	return schema.Lifecycle{
		Decay: schema.DecayProfile{
			Curve:           schema.DecayCurveExponential,
			HalfLifeSeconds: halfLife,
		},
		DeletionPolicy: deletionPolicy,
	}
}

// halfLifeForType returns the decay half-life in seconds for the given memory type.
func (pe *PolicyEngine) halfLifeForType(memType schema.MemoryType) int64 {
	switch memType {
	case schema.MemoryTypeEpisodic:
		return pe.defaults.EpisodicHalfLifeSeconds
	case schema.MemoryTypeSemantic:
		return pe.defaults.SemanticHalfLifeSeconds
	case schema.MemoryTypeWorking:
		return pe.defaults.WorkingHalfLifeSeconds
	default:
		// Competence and plan_graph get semantic-level longevity.
		return pe.defaults.SemanticHalfLifeSeconds
	}
}
