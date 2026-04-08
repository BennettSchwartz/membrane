// Package schema defines the core data structures for the memory substrate
// as specified in RFC 15A (Schema Appendix).
package schema

// MemoryType defines the category of memory record.
// RFC 15A.1: MemoryType MUST be one of the defined values.
type MemoryType string

const (
	// MemoryTypeEpisodic captures raw experience such as user inputs, tool calls,
	// errors, and observations. Episodic memory is intentionally short-lived and
	// provides evidence for later learning.
	MemoryTypeEpisodic MemoryType = "episodic"

	// MemoryTypeWorking captures the current state of an ongoing task, enabling
	// resumption across sessions or devices.
	MemoryTypeWorking MemoryType = "working"

	// MemoryTypeSemantic stores stable knowledge such as preferences, environment
	// facts, and relationships. Semantic memory supports revisability.
	MemoryTypeSemantic MemoryType = "semantic"

	// MemoryTypeCompetence encodes procedural knowledge: how to achieve goals
	// reliably under specific conditions.
	MemoryTypeCompetence MemoryType = "competence"

	// MemoryTypePlanGraph stores reusable solution structures as directed graphs
	// of actions.
	MemoryTypePlanGraph MemoryType = "plan_graph"

	// MemoryTypeEntity stores canonical entities and their aliases for
	// graph-aware memory resolution and retrieval.
	MemoryTypeEntity MemoryType = "entity"
)

// IsValidMemoryType reports whether mt is one of the allowed memory types.
func IsValidMemoryType(mt MemoryType) bool {
	switch mt {
	case MemoryTypeEpisodic, MemoryTypeWorking, MemoryTypeSemantic, MemoryTypeCompetence, MemoryTypePlanGraph, MemoryTypeEntity:
		return true
	default:
		return false
	}
}

// Sensitivity defines the sensitivity classification for memory records.
// RFC 15A.1: Sensitivity MUST be one of the defined values.
// Higher sensitivity levels require stricter trust context for retrieval.
type Sensitivity string

const (
	// SensitivityPublic indicates content that can be freely shared.
	SensitivityPublic Sensitivity = "public"

	// SensitivityLow indicates content with minimal sensitivity.
	SensitivityLow Sensitivity = "low"

	// SensitivityMedium indicates moderately sensitive content.
	SensitivityMedium Sensitivity = "medium"

	// SensitivityHigh indicates highly sensitive content requiring elevated trust.
	SensitivityHigh Sensitivity = "high"

	// SensitivityHyper indicates extremely sensitive content with maximum protection.
	SensitivityHyper Sensitivity = "hyper"
)

// IsValidSensitivity reports whether s is one of the allowed sensitivity values.
func IsValidSensitivity(s Sensitivity) bool {
	switch s {
	case SensitivityPublic, SensitivityLow, SensitivityMedium, SensitivityHigh, SensitivityHyper:
		return true
	default:
		return false
	}
}

// DecayCurve defines the mathematical function used for salience decay.
// RFC 15A.3, 15A.7: Decay curves control how salience decreases over time.
type DecayCurve string

const (
	// DecayCurveExponential uses exponential decay with a half-life parameter.
	DecayCurveExponential DecayCurve = "exponential"
)

// DeletionPolicy defines how memory records may be deleted.
// RFC 15A.3: Controls whether records can be automatically pruned.
type DeletionPolicy string

const (
	// DeletionPolicyAutoPrune allows automatic deletion when salience drops
	// below threshold.
	DeletionPolicyAutoPrune DeletionPolicy = "auto_prune"

	// DeletionPolicyManualOnly requires explicit user action for deletion.
	DeletionPolicyManualOnly DeletionPolicy = "manual_only"

	// DeletionPolicyNever prevents deletion entirely.
	DeletionPolicyNever DeletionPolicy = "never"
)

// RevisionStatus indicates the current state of a semantic memory revision.
// RFC 15A.8: Semantic records support revision, replacement, conditionality.
type RevisionStatus string

const (
	// RevisionStatusActive indicates the record is currently valid.
	RevisionStatusActive RevisionStatus = "active"

	// RevisionStatusContested indicates uncertainty until resolved.
	RevisionStatusContested RevisionStatus = "contested"

	// RevisionStatusRetracted indicates the record has been withdrawn.
	RevisionStatusRetracted RevisionStatus = "retracted"
)

// ValidityMode defines how a semantic fact's validity is scoped.
// RFC 15A.8: Semantic records support conditional validity.
type ValidityMode string

const (
	// ValidityModeGlobal indicates the fact is universally valid.
	ValidityModeGlobal ValidityMode = "global"

	// ValidityModeConditional indicates the fact is valid under specific conditions.
	ValidityModeConditional ValidityMode = "conditional"

	// ValidityModeTimeboxed indicates the fact is valid within a time window.
	ValidityModeTimeboxed ValidityMode = "timeboxed"
)

// TaskState defines the current state of a working memory task.
// RFC 15A.7: Working memory tracks task state for resumption.
type TaskState string

const (
	// TaskStatePlanning indicates the task is in planning phase.
	TaskStatePlanning TaskState = "planning"

	// TaskStateExecuting indicates the task is actively being executed.
	TaskStateExecuting TaskState = "executing"

	// TaskStateBlocked indicates the task cannot proceed due to a blocker.
	TaskStateBlocked TaskState = "blocked"

	// TaskStateWaiting indicates the task is waiting for external input.
	TaskStateWaiting TaskState = "waiting"

	// TaskStateDone indicates the task has completed.
	TaskStateDone TaskState = "done"
)

// IsValidTaskState reports whether s is one of the allowed task states.
func IsValidTaskState(s TaskState) bool {
	switch s {
	case TaskStatePlanning, TaskStateExecuting, TaskStateBlocked, TaskStateWaiting, TaskStateDone:
		return true
	default:
		return false
	}
}

// OutcomeStatus defines the result of an episodic experience.
// RFC 15A.6: Episodic records track outcome status.
type OutcomeStatus string

const (
	// OutcomeStatusSuccess indicates the experience completed successfully.
	OutcomeStatusSuccess OutcomeStatus = "success"

	// OutcomeStatusFailure indicates the experience ended in failure.
	OutcomeStatusFailure OutcomeStatus = "failure"

	// OutcomeStatusPartial indicates partial success or incomplete outcome.
	OutcomeStatusPartial OutcomeStatus = "partial"
)

// IsValidOutcomeStatus reports whether s is one of the allowed outcome statuses.
func IsValidOutcomeStatus(s OutcomeStatus) bool {
	switch s {
	case OutcomeStatusSuccess, OutcomeStatusFailure, OutcomeStatusPartial:
		return true
	default:
		return false
	}
}

// AuditAction defines the type of action recorded in an audit entry.
// RFC 15A.8: Every revision MUST be auditable and traceable to evidence.
type AuditAction string

const (
	// AuditActionCreate indicates a new record was created.
	AuditActionCreate AuditAction = "create"

	// AuditActionRevise indicates an existing record was revised.
	AuditActionRevise AuditAction = "revise"

	// AuditActionFork indicates a record was forked into conditional variants.
	AuditActionFork AuditAction = "fork"

	// AuditActionMerge indicates records were merged together.
	AuditActionMerge AuditAction = "merge"

	// AuditActionDelete indicates a record was deleted.
	AuditActionDelete AuditAction = "delete"

	// AuditActionReinforce indicates a record's salience was reinforced.
	AuditActionReinforce AuditAction = "reinforce"

	// AuditActionDecay indicates a record's salience was decayed.
	AuditActionDecay AuditAction = "decay"
)

// ProvenanceKind defines the type of source in provenance tracking.
// RFC 15A.4: Provenance links connect memory to source events or artifacts.
type ProvenanceKind string

const (
	// ProvenanceKindEvent indicates the source is an event.
	ProvenanceKindEvent ProvenanceKind = "event"

	// ProvenanceKindArtifact indicates the source is an artifact (log, file, etc.).
	ProvenanceKindArtifact ProvenanceKind = "artifact"

	// ProvenanceKindToolCall indicates the source is a tool invocation.
	ProvenanceKindToolCall ProvenanceKind = "tool_call"

	// ProvenanceKindObservation indicates the source is an observation.
	ProvenanceKindObservation ProvenanceKind = "observation"

	// ProvenanceKindOutcome indicates the source is a task outcome.
	ProvenanceKindOutcome ProvenanceKind = "outcome"
)

// EdgeKind defines the type of edge in a plan graph.
// RFC 15A.10: Plan graph edges represent dependencies.
type EdgeKind string

const (
	// EdgeKindData indicates a data dependency edge.
	EdgeKindData EdgeKind = "data"

	// EdgeKindControl indicates a control flow edge.
	EdgeKindControl EdgeKind = "control"
)
