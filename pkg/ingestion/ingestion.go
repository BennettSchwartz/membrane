package ingestion

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/GustyCube/membrane/pkg/schema"
	"github.com/GustyCube/membrane/pkg/storage"
)

// IngestEventRequest contains the parameters for ingesting an event.
type IngestEventRequest struct {
	// Source identifies the actor or system that produced the event.
	Source string

	// EventKind is the type of event (e.g., "user_input", "error", "system").
	EventKind string

	// Ref is a reference identifier for the source event.
	Ref string

	// Summary is an optional human-readable summary.
	Summary string

	// Timestamp is when the event occurred. If zero, the current time is used.
	Timestamp time.Time

	// Tags are optional labels for categorization.
	Tags []string

	// Scope is the visibility scope.
	Scope string

	// Sensitivity overrides the default sensitivity if set.
	Sensitivity schema.Sensitivity
}

// IngestToolOutputRequest contains the parameters for ingesting a tool output.
type IngestToolOutputRequest struct {
	// Source identifies the actor or system that invoked the tool.
	Source string

	// ToolName is the name of the tool that was invoked.
	ToolName string

	// Args are the arguments passed to the tool.
	Args map[string]any

	// Result is the output produced by the tool.
	Result any

	// DependsOn lists IDs of tool nodes this output depends on.
	DependsOn []string

	// Timestamp is when the tool was invoked. If zero, the current time is used.
	Timestamp time.Time

	// Tags are optional labels for categorization.
	Tags []string

	// Scope is the visibility scope.
	Scope string

	// Sensitivity overrides the default sensitivity if set.
	Sensitivity schema.Sensitivity
}

// IngestObservationRequest contains the parameters for ingesting an observation.
type IngestObservationRequest struct {
	// Source identifies the actor or system that made the observation.
	Source string

	// Subject is the entity the observation is about.
	Subject string

	// Predicate is the relationship or property observed.
	Predicate string

	// Object is the value or related entity observed.
	Object any

	// Timestamp is when the observation was made. If zero, the current time is used.
	Timestamp time.Time

	// Tags are optional labels for categorization.
	Tags []string

	// Scope is the visibility scope.
	Scope string

	// Sensitivity overrides the default sensitivity if set.
	Sensitivity schema.Sensitivity
}

// IngestOutcomeRequest contains the parameters for updating an existing record
// with outcome data.
type IngestOutcomeRequest struct {
	// Source identifies the actor or system reporting the outcome.
	Source string

	// TargetRecordID is the ID of the existing record to update.
	TargetRecordID string

	// OutcomeStatus is the result (success, failure, partial).
	OutcomeStatus schema.OutcomeStatus

	// Timestamp is when the outcome was determined. If zero, the current time is used.
	Timestamp time.Time
}

// IngestWorkingStateRequest contains the parameters for ingesting working memory state.
type IngestWorkingStateRequest struct {
	// Source identifies the actor or system that produced the working state.
	Source string

	// ThreadID is the identifier for the current thread/session.
	ThreadID string

	// State indicates the current task state.
	State schema.TaskState

	// NextActions lists the next planned actions.
	NextActions []string

	// OpenQuestions lists unresolved questions for the task.
	OpenQuestions []string

	// ContextSummary provides a summary of the current context.
	ContextSummary string

	// ActiveConstraints lists constraints currently active for the task.
	ActiveConstraints []schema.Constraint

	// Timestamp is when the working state was captured. If zero, the current time is used.
	Timestamp time.Time

	// Tags are optional labels for categorization.
	Tags []string

	// Scope is the visibility scope.
	Scope string

	// Sensitivity overrides the default sensitivity if set.
	Sensitivity schema.Sensitivity
}

// Service orchestrates ingestion of raw data into the memory substrate.
// It coordinates classification, policy application, and storage.
type Service struct {
	store       storage.Store
	classifier  *Classifier
	policy      *PolicyEngine
	interpreter Interpreter
}

// NewService creates a new ingestion Service.
func NewService(store storage.Store, classifier *Classifier, policy *PolicyEngine) *Service {
	return &Service{
		store:      store,
		classifier: classifier,
		policy:     policy,
	}
}

// NewServiceWithInterpreter creates a new ingestion Service with ingest-side
// interpretation support.
func NewServiceWithInterpreter(store storage.Store, classifier *Classifier, policy *PolicyEngine, interpreter Interpreter) *Service {
	return &Service{
		store:       store,
		classifier:  classifier,
		policy:      policy,
		interpreter: interpreter,
	}
}

// IngestEvent creates an episodic memory record from an event.
func (s *Service) IngestEvent(ctx context.Context, req IngestEventRequest) (*schema.MemoryRecord, error) {
	ts := req.Timestamp
	if ts.IsZero() {
		ts = time.Now().UTC()
	}

	candidate := &MemoryCandidate{
		Kind:        CandidateKindEvent,
		Source:      req.Source,
		Timestamp:   ts,
		Tags:        req.Tags,
		Scope:       req.Scope,
		EventKind:   req.EventKind,
		EventRef:    req.Ref,
		Summary:     req.Summary,
		Sensitivity: req.Sensitivity,
	}

	memType := s.classifier.Classify(candidate)

	policyResult, err := s.policy.Apply(candidate, memType)
	if err != nil {
		return nil, fmt.Errorf("ingestion: classify event: %w", err)
	}

	payload := schema.EpisodicPayload{
		Kind: "episodic",
		Timeline: []schema.TimelineEvent{
			{
				T:         ts,
				EventKind: req.EventKind,
				Ref:       req.Ref,
				Summary:   req.Summary,
			},
		},
	}

	record := s.buildRecord(candidate, memType, policyResult, &payload)

	if err := s.store.Create(ctx, record); err != nil {
		return nil, fmt.Errorf("ingestion: store event: %w", err)
	}

	return record, nil
}

// IngestToolOutput creates an episodic memory record with tool graph data from
// a tool invocation.
func (s *Service) IngestToolOutput(ctx context.Context, req IngestToolOutputRequest) (*schema.MemoryRecord, error) {
	ts := req.Timestamp
	if ts.IsZero() {
		ts = time.Now().UTC()
	}

	candidate := &MemoryCandidate{
		Kind:          CandidateKindToolOutput,
		Source:        req.Source,
		Timestamp:     ts,
		Tags:          req.Tags,
		Scope:         req.Scope,
		ToolName:      req.ToolName,
		ToolArgs:      req.Args,
		ToolResult:    req.Result,
		ToolDependsOn: req.DependsOn,
		Sensitivity:   req.Sensitivity,
	}

	memType := s.classifier.Classify(candidate)

	policyResult, err := s.policy.Apply(candidate, memType)
	if err != nil {
		return nil, fmt.Errorf("ingestion: classify tool output: %w", err)
	}

	toolNodeID := uuid.New().String()
	payload := schema.EpisodicPayload{
		Kind: "episodic",
		Timeline: []schema.TimelineEvent{
			{
				T:         ts,
				EventKind: "tool_call",
				Ref:       toolNodeID,
				Summary:   fmt.Sprintf("tool_call: %s", req.ToolName),
			},
		},
		ToolGraph: []schema.ToolNode{
			{
				ID:        toolNodeID,
				Tool:      req.ToolName,
				Args:      req.Args,
				Result:    req.Result,
				Timestamp: ts,
				DependsOn: req.DependsOn,
			},
		},
	}

	record := s.buildRecord(candidate, memType, policyResult, &payload)

	if err := s.store.Create(ctx, record); err != nil {
		return nil, fmt.Errorf("ingestion: store tool output: %w", err)
	}

	return record, nil
}

// IngestObservation creates a semantic or working memory record from an
// observation, extracting subject-predicate-object structure.
func (s *Service) IngestObservation(ctx context.Context, req IngestObservationRequest) (*schema.MemoryRecord, error) {
	ts := req.Timestamp
	if ts.IsZero() {
		ts = time.Now().UTC()
	}

	candidate := &MemoryCandidate{
		Kind:        CandidateKindObservation,
		Source:      req.Source,
		Timestamp:   ts,
		Tags:        req.Tags,
		Scope:       req.Scope,
		Subject:     req.Subject,
		Predicate:   req.Predicate,
		Object:      req.Object,
		Sensitivity: req.Sensitivity,
	}

	memType := s.classifier.Classify(candidate)

	policyResult, err := s.policy.Apply(candidate, memType)
	if err != nil {
		return nil, fmt.Errorf("ingestion: classify observation: %w", err)
	}

	payload := schema.SemanticPayload{
		Kind:      "semantic",
		Subject:   req.Subject,
		Predicate: req.Predicate,
		Object:    req.Object,
		Validity: schema.Validity{
			Mode: schema.ValidityModeGlobal,
		},
		Evidence: []schema.ProvenanceRef{
			{
				SourceType: "observation",
				SourceID:   candidate.Source,
				Timestamp:  ts,
			},
		},
		RevisionPolicy: "replace",
	}

	record := s.buildRecord(candidate, memType, policyResult, &payload)

	if err := s.store.Create(ctx, record); err != nil {
		return nil, fmt.Errorf("ingestion: store observation: %w", err)
	}

	return record, nil
}

// IngestOutcome updates an existing episodic record with outcome data.
// It retrieves the target record, sets the outcome status, appends an audit
// entry, and persists the update.
func (s *Service) IngestOutcome(ctx context.Context, req IngestOutcomeRequest) (*schema.MemoryRecord, error) {
	ts := req.Timestamp
	if ts.IsZero() {
		ts = time.Now().UTC()
	}

	// Retrieve the existing record.
	record, err := s.store.Get(ctx, req.TargetRecordID)
	if err != nil {
		return nil, fmt.Errorf("ingestion: get target record for outcome: %w", err)
	}

	// Verify the target is an episodic record with an EpisodicPayload.
	ep, ok := record.Payload.(*schema.EpisodicPayload)
	if !ok {
		return nil, fmt.Errorf("ingestion: target record %s is not episodic", req.TargetRecordID)
	}

	// Update the outcome.
	ep.Outcome = req.OutcomeStatus
	record.Payload = ep
	record.UpdatedAt = ts

	// Add provenance source for the outcome.
	record.Provenance.Sources = append(record.Provenance.Sources, schema.ProvenanceSource{
		Kind:      schema.ProvenanceKindOutcome,
		Ref:       fmt.Sprintf("outcome:%s:%s", req.TargetRecordID, req.OutcomeStatus),
		CreatedBy: req.Source,
		Timestamp: ts,
	})

	// Append audit entry.
	record.AuditLog = append(record.AuditLog, schema.AuditEntry{
		Action:    schema.AuditActionRevise,
		Actor:     req.Source,
		Timestamp: ts,
		Rationale: fmt.Sprintf("Outcome recorded: %s", req.OutcomeStatus),
	})

	if err := s.store.Update(ctx, record); err != nil {
		return nil, fmt.Errorf("ingestion: update outcome: %w", err)
	}

	return record, nil
}

// IngestWorkingState creates a working memory record from a working state snapshot.
func (s *Service) IngestWorkingState(ctx context.Context, req IngestWorkingStateRequest) (*schema.MemoryRecord, error) {
	ts := req.Timestamp
	if ts.IsZero() {
		ts = time.Now().UTC()
	}

	candidate := &MemoryCandidate{
		Kind:           CandidateKindWorkingState,
		Source:         req.Source,
		Timestamp:      ts,
		Tags:           req.Tags,
		Scope:          req.Scope,
		ThreadID:       req.ThreadID,
		TaskState:      req.State,
		ContextSummary: req.ContextSummary,
		NextActions:    req.NextActions,
		OpenQuestions:  req.OpenQuestions,
		Sensitivity:    req.Sensitivity,
	}

	memType := s.classifier.Classify(candidate)

	policyResult, err := s.policy.Apply(candidate, memType)
	if err != nil {
		return nil, fmt.Errorf("ingestion: classify working state: %w", err)
	}

	payload := schema.WorkingPayload{
		Kind:              "working",
		ThreadID:          req.ThreadID,
		State:             req.State,
		ActiveConstraints: req.ActiveConstraints,
		NextActions:       req.NextActions,
		OpenQuestions:     req.OpenQuestions,
		ContextSummary:    req.ContextSummary,
	}

	record := s.buildRecord(candidate, memType, policyResult, &payload)

	if err := s.store.Create(ctx, record); err != nil {
		return nil, fmt.Errorf("ingestion: store working state: %w", err)
	}

	return record, nil
}

// buildRecord constructs a MemoryRecord from a candidate, classified type,
// policy result, and payload. It generates a UUID, sets timestamps, and
// populates provenance and audit log.
func (s *Service) buildRecord(
	candidate *MemoryCandidate,
	memType schema.MemoryType,
	policyResult *PolicyResult,
	payload schema.Payload,
) *schema.MemoryRecord {
	now := time.Now().UTC()
	id := uuid.New().String()

	lifecycle := policyResult.Lifecycle
	lifecycle.LastReinforcedAt = now

	provenanceKind := provenanceKindForCandidate(candidate.Kind)

	return &schema.MemoryRecord{
		ID:          id,
		Type:        memType,
		Sensitivity: policyResult.Sensitivity,
		Confidence:  policyResult.Confidence,
		Salience:    policyResult.Salience,
		Scope:       candidate.Scope,
		Tags:        candidate.Tags,
		CreatedAt:   now,
		UpdatedAt:   now,
		Lifecycle:   lifecycle,
		Provenance: schema.Provenance{
			Sources: []schema.ProvenanceSource{
				{
					Kind:      provenanceKind,
					Ref:       candidate.EventRef,
					CreatedBy: candidate.Source,
					Timestamp: candidate.Timestamp,
				},
			},
			CreatedBy: "ingestion-service",
		},
		Relations: []schema.Relation{},
		Payload:   payload,
		AuditLog: []schema.AuditEntry{
			{
				Action:    schema.AuditActionCreate,
				Actor:     "ingestion-service",
				Timestamp: now,
				Rationale: fmt.Sprintf("Ingested %s from %s", candidate.Kind, candidate.Source),
			},
		},
	}
}

// provenanceKindForCandidate maps a CandidateKind to a ProvenanceKind.
func provenanceKindForCandidate(kind CandidateKind) schema.ProvenanceKind {
	switch kind {
	case CandidateKindEvent:
		return schema.ProvenanceKindEvent
	case CandidateKindToolOutput:
		return schema.ProvenanceKindToolCall
	case CandidateKindObservation:
		return schema.ProvenanceKindObservation
	case CandidateKindOutcome:
		return schema.ProvenanceKindOutcome
	default:
		return schema.ProvenanceKindEvent
	}
}
