// Package ingestion provides the ingestion layer for the Membrane memory substrate.
// It classifies incoming data, applies lifecycle policies, and persists memory records.
package ingestion

import (
	"time"

	"github.com/GustyCube/membrane/pkg/schema"
)

// CandidateKind identifies the source type of a MemoryCandidate.
type CandidateKind string

const (
	// CandidateKindEvent indicates the candidate originated from an event.
	CandidateKindEvent CandidateKind = "event"

	// CandidateKindToolOutput indicates the candidate originated from a tool invocation.
	CandidateKindToolOutput CandidateKind = "tool_output"

	// CandidateKindObservation indicates the candidate originated from an observation.
	CandidateKindObservation CandidateKind = "observation"

	// CandidateKindOutcome indicates the candidate is an outcome update.
	CandidateKindOutcome CandidateKind = "outcome"

	// CandidateKindWorkingState indicates the candidate is a working state change.
	CandidateKindWorkingState CandidateKind = "working_state"
)

// MemoryCandidate is the intermediate representation used between ingestion
// request parsing, classification, and policy application. It carries all data
// needed to build a final MemoryRecord.
type MemoryCandidate struct {
	// Kind identifies the source type of this candidate.
	Kind CandidateKind

	// Source is the actor or system component that produced this candidate.
	Source string

	// Timestamp is when the source event or observation occurred.
	Timestamp time.Time

	// Tags are optional labels for categorization.
	Tags []string

	// Scope is the visibility scope (e.g., user, project, global).
	Scope string

	// --- Event fields ---

	// EventKind is the type of event (e.g., "user_input", "error", "system").
	EventKind string

	// EventRef is a reference identifier for the source event.
	EventRef string

	// Summary is a human-readable summary of the event or observation.
	Summary string

	// --- Tool output fields ---

	// ToolName is the name of the tool that produced output.
	ToolName string

	// ToolArgs are the arguments passed to the tool.
	ToolArgs map[string]any

	// ToolResult is the output produced by the tool.
	ToolResult any

	// ToolDependsOn lists IDs of tool nodes this depends on.
	ToolDependsOn []string

	// --- Observation fields ---

	// Subject is the entity the observation is about (for semantic memory).
	Subject string

	// Predicate is the relationship or property observed.
	Predicate string

	// Object is the value or related entity observed.
	Object any

	// --- Outcome fields ---

	// TargetRecordID is the ID of an existing record to update with outcome data.
	TargetRecordID string

	// OutcomeStatus is the result status (success, failure, partial).
	OutcomeStatus schema.OutcomeStatus

	// --- Working state fields ---

	// ThreadID is the thread/session identifier for working memory.
	ThreadID string

	// TaskState is the current state of the task.
	TaskState schema.TaskState

	// ContextSummary provides context for working memory.
	ContextSummary string

	// NextActions lists planned next actions for working memory.
	NextActions []string

	// OpenQuestions lists unresolved questions for working memory.
	OpenQuestions []string

	// --- Sensitivity override ---

	// Sensitivity allows the caller to override the default sensitivity.
	// If empty, the policy engine assigns a default.
	Sensitivity schema.Sensitivity
}

// Classifier determines what type of memory to create from a MemoryCandidate.
type Classifier struct{}

// NewClassifier creates a new Classifier instance.
func NewClassifier() *Classifier {
	return &Classifier{}
}

// Classify examines a MemoryCandidate and returns the appropriate MemoryType.
//
// Classification rules:
//   - Events and tool outputs produce episodic memory.
//   - Observations produce semantic memory (subject-predicate-object facts).
//   - Outcomes update existing episodic records (returned as episodic).
//   - Working state changes produce working memory.
func (c *Classifier) Classify(candidate *MemoryCandidate) schema.MemoryType {
	switch candidate.Kind {
	case CandidateKindEvent:
		return schema.MemoryTypeEpisodic
	case CandidateKindToolOutput:
		return schema.MemoryTypeEpisodic
	case CandidateKindObservation:
		return schema.MemoryTypeSemantic
	case CandidateKindOutcome:
		// Outcomes update existing episodic records.
		return schema.MemoryTypeEpisodic
	case CandidateKindWorkingState:
		return schema.MemoryTypeWorking
	default:
		// Default to episodic for unknown candidate kinds.
		return schema.MemoryTypeEpisodic
	}
}
