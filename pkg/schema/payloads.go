package schema

import (
	"encoding/json"
	"time"
)

// Payload is the interface that all memory payload types must implement.
// RFC 15A.2: Payload is a oneOf of the five payload types.
type Payload interface {
	// PayloadKind returns the kind identifier for this payload type.
	// This corresponds to the "kind" field in each payload schema.
	PayloadKind() string

	// isPayload is a marker method to ensure only defined payload types
	// can implement this interface.
	isPayload()
}

// ============================================================================
// Episodic Payload (RFC 15A.6, 15A.2)
// ============================================================================

// EpisodicPayload captures raw experience as a time-ordered sequence of events.
// RFC 15A.6: Episodic payloads MUST be append-only. Semantic correction is forbidden.
type EpisodicPayload struct {
	// Kind identifies this as an episodic payload.
	// RFC 15A.6: Required field with const value "episodic".
	Kind string `json:"kind"`

	// Timeline is a time-ordered sequence of events.
	// RFC 15A.6: Required field.
	Timeline []TimelineEvent `json:"timeline"`

	// ToolGraph captures tool calls and data flow during the episode.
	// RFC 15A.2: Extension for detailed tool tracking.
	ToolGraph []ToolNode `json:"tool_graph,omitempty"`

	// Environment captures the environment snapshot during the episode.
	// RFC 15A.2: OS, versions, context.
	Environment *EnvironmentSnapshot `json:"environment,omitempty"`

	// Outcome indicates the result of the episode.
	// RFC 15A.2: success | failure | partial.
	Outcome OutcomeStatus `json:"outcome,omitempty"`

	// Artifacts references external artifacts (logs, screenshots, files).
	// RFC 15A.6: Optional array of artifact references.
	Artifacts []string `json:"artifacts,omitempty"`

	// ToolGraphRef is an optional reference to an external tool graph.
	// RFC 15A.6: Optional field for external graph storage.
	ToolGraphRef string `json:"tool_graph_ref,omitempty"`
}

func (EpisodicPayload) PayloadKind() string { return "episodic" }
func (EpisodicPayload) isPayload()          {}

// TimelineEvent represents a single event in an episodic timeline.
// RFC 15A.6: Events have timestamp, kind, reference, and optional summary.
type TimelineEvent struct {
	// T is the timestamp of the event.
	// RFC 15A.6: Required field.
	T time.Time `json:"t"`

	// EventKind identifies the type of event.
	// RFC 15A.6: Required field.
	EventKind string `json:"event_kind"`

	// Ref is a reference to the event details.
	// RFC 15A.6: Required field.
	Ref string `json:"ref"`

	// Summary is an optional human-readable summary.
	// RFC 15A.6: Optional field.
	Summary string `json:"summary,omitempty"`
}

// ToolNode represents a tool call in the tool graph.
// RFC 15A.2: Tool graph captures tool calls and data flow.
type ToolNode struct {
	// ID is the unique identifier for this tool node.
	ID string `json:"id"`

	// Tool is the name or identifier of the tool.
	Tool string `json:"tool"`

	// Args contains the arguments passed to the tool.
	Args map[string]any `json:"args,omitempty"`

	// Result contains the output from the tool.
	Result any `json:"result,omitempty"`

	// Timestamp records when the tool was invoked.
	Timestamp time.Time `json:"timestamp,omitempty"`

	// DependsOn lists IDs of tool nodes this depends on.
	DependsOn []string `json:"depends_on,omitempty"`
}

// EnvironmentSnapshot captures the environment state during an episode.
// RFC 15A.2: Includes OS, versions, context.
type EnvironmentSnapshot struct {
	// OS is the operating system identifier.
	OS string `json:"os,omitempty"`

	// OSVersion is the operating system version.
	OSVersion string `json:"os_version,omitempty"`

	// ToolVersions maps tool names to their versions.
	ToolVersions map[string]string `json:"tool_versions,omitempty"`

	// WorkingDirectory is the current working directory.
	WorkingDirectory string `json:"working_directory,omitempty"`

	// Context contains additional contextual information.
	Context map[string]any `json:"context,omitempty"`
}

// ============================================================================
// Working Payload (RFC 15A.7, 15A.3)
// ============================================================================

// WorkingPayload captures the current state of an ongoing task.
// RFC 15A.7: Working memory MAY be freely edited and discarded when the task ends.
type WorkingPayload struct {
	// Kind identifies this as a working payload.
	// RFC 15A.7: Required field with const value "working".
	Kind string `json:"kind"`

	// ThreadID is the identifier for the current thread/session.
	// RFC 15A.7: Required field.
	ThreadID string `json:"thread_id"`

	// State indicates the current task state.
	// RFC 15A.7: Required field (planning, executing, blocked, waiting, done).
	State TaskState `json:"state"`

	// ActiveConstraints lists constraints currently active for the task.
	// RFC 15A.3: Working memory tracks active constraints.
	ActiveConstraints []Constraint `json:"active_constraints,omitempty"`

	// NextActions lists the next planned actions.
	// RFC 15A.7: Optional array of action hints.
	NextActions []string `json:"next_actions,omitempty"`

	// OpenQuestions lists unresolved questions for the task.
	// RFC 15A.7: Optional array of questions.
	OpenQuestions []string `json:"open_questions,omitempty"`

	// ContextSummary provides a summary of the current context.
	// RFC 15A.7: Optional field for context tracking.
	ContextSummary string `json:"context_summary,omitempty"`
}

func (WorkingPayload) PayloadKind() string { return "working" }
func (WorkingPayload) isPayload()          {}

// Constraint represents a constraint on task execution.
// RFC 15A.3, 15A.6: Constraints affect plan selection and execution.
type Constraint struct {
	// Type identifies the kind of constraint.
	Type string `json:"type"`

	// Key is the constraint key or name.
	Key string `json:"key"`

	// Value is the constraint value.
	Value any `json:"value"`

	// Required indicates if the constraint is mandatory.
	Required bool `json:"required,omitempty"`
}

// ============================================================================
// Semantic Payload (RFC 15A.8, 15A.4)
// ============================================================================

// SemanticPayload stores revisable facts as subject-predicate-object triples.
// RFC 15A.8: Semantic payloads MUST support coexistence of multiple conditional truths.
type SemanticPayload struct {
	// Kind identifies this as a semantic payload.
	// RFC 15A.8: Required field with const value "semantic".
	Kind string `json:"kind"`

	// Subject is the entity the fact is about.
	// RFC 15A.8: Required field.
	Subject string `json:"subject"`

	// Predicate is the relationship or property.
	// RFC 15A.8: Required field.
	Predicate string `json:"predicate"`

	// Object is the value or related entity.
	// RFC 15A.8: Required field. Can be string, number, boolean, object, or array.
	Object any `json:"object"`

	// Validity defines when this fact is valid.
	// RFC 15A.8: Required field.
	Validity Validity `json:"validity"`

	// Evidence links to provenance supporting this fact.
	// RFC 15A.4: Optional array of evidence references.
	Evidence []ProvenanceRef `json:"evidence,omitempty"`

	// RevisionPolicy defines how this fact should be revised.
	// RFC 15A.4: replace | fork | contest.
	RevisionPolicy string `json:"revision_policy,omitempty"`

	// Revision tracks revision state for this fact.
	// RFC 15A.8: Optional field for revision tracking.
	Revision *RevisionState `json:"revision,omitempty"`
}

func (SemanticPayload) PayloadKind() string { return "semantic" }
func (SemanticPayload) isPayload()          {}

// EntityPayload stores canonical entity metadata used for graph-aware memory
// resolution and retrieval.
type EntityPayload struct {
	// Kind identifies this as an entity payload.
	Kind string `json:"kind"`

	// CanonicalName is the preferred display/lookup name for the entity.
	CanonicalName string `json:"canonical_name"`

	// PrimaryType is the preferred ontology type for the entity.
	PrimaryType string `json:"primary_type,omitempty"`

	// Types lists ontology types for the entity. Values are registry-backed
	// strings rather than a closed enum, so callers can add domain-specific
	// types without schema migrations.
	Types []string `json:"types,omitempty"`

	// Aliases contains alternate surface forms for the entity.
	Aliases []EntityAlias `json:"aliases,omitempty"`

	// Identifiers contains stable external identifiers for the entity.
	Identifiers []EntityIdentifier `json:"identifiers,omitempty"`

	// Summary is a short description of the entity.
	Summary string `json:"summary,omitempty"`
}

func (EntityPayload) PayloadKind() string { return "entity" }
func (EntityPayload) isPayload()          {}

// EntityAlias is an alternate surface form for an entity.
type EntityAlias struct {
	Value  string `json:"value"`
	Kind   string `json:"kind,omitempty"`
	Locale string `json:"locale,omitempty"`
}

// EntityIdentifier is a stable external identifier for an entity.
type EntityIdentifier struct {
	Namespace string `json:"namespace"`
	Value     string `json:"value"`
}

// MarshalJSON emits a compact string form for plain aliases while preserving
// room for typed aliases when kind/locale are present.
func (a EntityAlias) MarshalJSON() ([]byte, error) {
	if a.Kind == "" && a.Locale == "" {
		return json.Marshal(a.Value)
	}
	type alias EntityAlias
	return json.Marshal(alias(a))
}

// UnmarshalJSON accepts both the new object form and the previous string form.
func (a *EntityAlias) UnmarshalJSON(data []byte) error {
	var value string
	if err := json.Unmarshal(data, &value); err == nil {
		a.Value = value
		a.Kind = ""
		a.Locale = ""
		return nil
	}
	type alias EntityAlias
	var raw alias
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	*a = EntityAlias(raw)
	return nil
}

// Validity defines when a semantic fact is valid.
// RFC 15A.8: Supports global, conditional, and timeboxed validity.
type Validity struct {
	// Mode specifies the validity mode.
	// RFC 15A.8: Required field (global, conditional, timeboxed).
	Mode ValidityMode `json:"mode"`

	// Conditions contains implementation-defined conditional keys.
	// RFC 15A.8: Optional field for conditional validity.
	Conditions map[string]any `json:"conditions,omitempty"`

	// Start is the start of the validity window for timeboxed validity.
	// RFC 15A.8: Optional field for timeboxed mode.
	Start *time.Time `json:"start,omitempty"`

	// End is the end of the validity window for timeboxed validity.
	// RFC 15A.8: Optional field for timeboxed mode.
	End *time.Time `json:"end,omitempty"`
}

// RevisionState tracks the revision status of a semantic fact.
// RFC 15A.8: Supports supersedes relationships and status tracking.
type RevisionState struct {
	// Supersedes is the ID of the record this supersedes.
	// RFC 15A.8: Optional field.
	Supersedes string `json:"supersedes,omitempty"`

	// SupersededBy is the ID of the record that supersedes this.
	// RFC 15A.8: Optional field.
	SupersededBy string `json:"superseded_by,omitempty"`

	// Status is the current revision status.
	// RFC 15A.8: Optional field (active, contested, retracted).
	Status RevisionStatus `json:"status,omitempty"`
}

// ============================================================================
// Competence Payload (RFC 15A.9, 15A.5)
// ============================================================================

// CompetencePayload encodes procedural knowledge: how to achieve goals.
// RFC 15A.9: Competence payloads represent "knowing how" rather than "knowing that".
type CompetencePayload struct {
	// Kind identifies this as a competence payload.
	// RFC 15A.9: Required field with const value "competence".
	Kind string `json:"kind"`

	// SkillName is the name of the skill or procedure.
	// RFC 15A.9: Required field.
	SkillName string `json:"skill_name"`

	// Triggers define when this competence applies.
	// RFC 15A.9: Required field.
	Triggers []Trigger `json:"triggers"`

	// Recipe contains the ordered or conditional steps.
	// RFC 15A.9: Required field.
	Recipe []RecipeStep `json:"recipe"`

	// RequiredTools lists tools needed for this competence.
	// RFC 15A.5: Optional array of tool references.
	RequiredTools []string `json:"required_tools,omitempty"`

	// FailureModes documents known failure cases.
	// RFC 15A.9: Optional array of failure descriptions.
	FailureModes []string `json:"failure_modes,omitempty"`

	// Fallbacks provides alternative steps when primary recipe fails.
	// RFC 15A.9: Optional array of fallback strategies.
	Fallbacks []string `json:"fallbacks,omitempty"`

	// Performance tracks success and failure statistics.
	// RFC 15A.5: Performance stats for competence selection.
	Performance *PerformanceStats `json:"performance,omitempty"`

	// Version is the version identifier for this competence.
	// RFC 15A.9: Optional version tracking.
	Version string `json:"version,omitempty"`
}

func (CompetencePayload) PayloadKind() string { return "competence" }
func (CompetencePayload) isPayload()          {}

// Trigger defines when a competence should be activated.
// RFC 15A.9: Triggers have a signal and optional conditions.
type Trigger struct {
	// Signal is the trigger signal (e.g., error signature, intent label).
	// RFC 15A.9: Required field.
	Signal string `json:"signal"`

	// Conditions are additional matching conditions.
	// RFC 15A.9: Optional field with implementation-defined keys.
	Conditions map[string]any `json:"conditions,omitempty"`
}

// RecipeStep represents a single step in a competence procedure.
// RFC 15A.9: Steps have a description, optional tool, and validation.
type RecipeStep struct {
	// Step is the human-readable step description.
	// RFC 15A.9: Required field.
	Step string `json:"step"`

	// Tool is the tool to use for this step.
	// RFC 15A.9: Optional field.
	Tool string `json:"tool,omitempty"`

	// ArgsSchema defines the expected arguments for the tool.
	// RFC 15A.9: Optional schema definition.
	ArgsSchema map[string]any `json:"args_schema,omitempty"`

	// Validation describes how to verify step success.
	// RFC 15A.9: Optional validation criteria.
	Validation string `json:"validation,omitempty"`
}

// PerformanceStats tracks success and failure statistics.
// RFC 15A.5, 15A.6: Used for competence and plan selection.
type PerformanceStats struct {
	// SuccessCount is the number of successful uses.
	SuccessCount int64 `json:"success_count,omitempty"`

	// FailureCount is the number of failed uses.
	FailureCount int64 `json:"failure_count,omitempty"`

	// SuccessRate is the computed success rate.
	// Value range: [0, 1]
	SuccessRate float64 `json:"success_rate,omitempty"`

	// AvgLatencyMs is the average execution time in milliseconds.
	AvgLatencyMs float64 `json:"avg_latency_ms,omitempty"`

	// LastUsedAt is when this was last used.
	LastUsedAt *time.Time `json:"last_used_at,omitempty"`
}

// ============================================================================
// Plan Graph Payload (RFC 15A.10, 15A.6)
// ============================================================================

// PlanGraphPayload stores reusable solution structures as directed graphs.
// RFC 15A.10: Plan graphs MUST be reusable, versioned, and selectable by constraint matching.
type PlanGraphPayload struct {
	// Kind identifies this as a plan_graph payload.
	// RFC 15A.10: Required field with const value "plan_graph".
	Kind string `json:"kind"`

	// PlanID is the unique identifier for this plan.
	// RFC 15A.10: Required field.
	PlanID string `json:"plan_id"`

	// Version is the version identifier for this plan.
	// RFC 15A.10: Required field.
	Version string `json:"version"`

	// Intent is the high-level intent label (e.g., setup_project).
	// RFC 15A.10: Optional field for intent matching.
	Intent string `json:"intent,omitempty"`

	// Constraints define trust requirements, sensitivity limits, etc.
	// RFC 15A.10: Optional constraint object.
	Constraints map[string]any `json:"constraints,omitempty"`

	// InputsSchema defines the expected inputs for the plan.
	// RFC 15A.10: Optional schema definition.
	InputsSchema map[string]any `json:"inputs_schema,omitempty"`

	// OutputsSchema defines the expected outputs from the plan.
	// RFC 15A.10: Optional schema definition.
	OutputsSchema map[string]any `json:"outputs_schema,omitempty"`

	// Nodes are the action nodes in the plan graph.
	// RFC 15A.10: Required field.
	Nodes []PlanNode `json:"nodes"`

	// Edges are the dependency edges in the plan graph.
	// RFC 15A.10: Required field.
	Edges []PlanEdge `json:"edges"`

	// Metrics track plan execution statistics.
	// RFC 15A.10: Optional performance metrics.
	Metrics *PlanMetrics `json:"metrics,omitempty"`
}

func (PlanGraphPayload) PayloadKind() string { return "plan_graph" }
func (PlanGraphPayload) isPayload()          {}

// PlanNode represents an action node in a plan graph.
// RFC 15A.10: Nodes have ID, operation, params, and guards.
type PlanNode struct {
	// ID is the unique identifier for this node within the plan.
	// RFC 15A.10: Required field.
	ID string `json:"id"`

	// Op is the action or tool identifier.
	// RFC 15A.10: Required field.
	Op string `json:"op"`

	// Params contains parameters for the operation.
	// RFC 15A.10: Optional parameter object.
	Params map[string]any `json:"params,omitempty"`

	// Guards define conditional execution criteria.
	// RFC 15A.10: Optional guards for conditional execution.
	Guards map[string]any `json:"guards,omitempty"`
}

// PlanEdge represents a dependency edge in a plan graph.
// RFC 15A.10: Edges connect nodes with data or control dependencies.
type PlanEdge struct {
	// From is the source node ID.
	// RFC 15A.10: Required field.
	From string `json:"from"`

	// To is the target node ID.
	// RFC 15A.10: Required field.
	To string `json:"to"`

	// Kind specifies the edge type (data or control).
	// RFC 15A.10: Required field.
	Kind EdgeKind `json:"kind"`
}

// PlanMetrics tracks execution statistics for a plan.
// RFC 15A.10: Includes latency and failure rate metrics.
type PlanMetrics struct {
	// AvgLatencyMs is the average execution time in milliseconds.
	AvgLatencyMs float64 `json:"avg_latency_ms,omitempty"`

	// FailureRate is the rate of failed executions.
	// Value range: [0, 1]
	FailureRate float64 `json:"failure_rate,omitempty"`

	// ExecutionCount is the total number of executions.
	ExecutionCount int64 `json:"execution_count,omitempty"`

	// LastExecutedAt is when this plan was last executed.
	LastExecutedAt *time.Time `json:"last_executed_at,omitempty"`
}

// ============================================================================
// Payload Marshaling Helpers
// ============================================================================

// PayloadWrapper is used for JSON marshaling/unmarshaling of the Payload interface.
type PayloadWrapper struct {
	Payload Payload
}

// MarshalJSON implements json.Marshaler for PayloadWrapper.
func (pw PayloadWrapper) MarshalJSON() ([]byte, error) {
	return json.Marshal(pw.Payload)
}

// UnmarshalJSON implements json.Unmarshaler for PayloadWrapper.
// It uses the "kind" field to determine the concrete payload type.
func (pw *PayloadWrapper) UnmarshalJSON(data []byte) error {
	// First, extract the kind field
	var kindHolder struct {
		Kind string `json:"kind"`
	}
	if err := json.Unmarshal(data, &kindHolder); err != nil {
		return err
	}

	// Create the appropriate payload type based on kind
	var payload Payload
	switch kindHolder.Kind {
	case "episodic":
		payload = &EpisodicPayload{}
	case "working":
		payload = &WorkingPayload{}
	case "semantic":
		payload = &SemanticPayload{}
	case "competence":
		payload = &CompetencePayload{}
	case "plan_graph":
		payload = &PlanGraphPayload{}
	case "entity":
		payload = &EntityPayload{}
	default:
		pw.Payload = nil
		return nil
	}

	// Unmarshal into the concrete type
	if err := json.Unmarshal(data, payload); err != nil {
		return err
	}

	pw.Payload = payload
	return nil
}
