package ingestion

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/BennettSchwartz/membrane/pkg/schema"
	"github.com/BennettSchwartz/membrane/pkg/storage"
	sqlitestore "github.com/BennettSchwartz/membrane/pkg/storage/sqlite"
)

type observationCreateFailStore struct {
	storage.Store
}

func (s *observationCreateFailStore) Begin(context.Context) (storage.Transaction, error) {
	return &observationCreateFailTx{}, nil
}

type observationCreateFailTx struct {
	storage.Transaction
}

func (tx *observationCreateFailTx) Create(context.Context, *schema.MemoryRecord) error {
	return errors.New("create failed")
}

func (tx *observationCreateFailTx) Commit() error {
	return nil
}

func (tx *observationCreateFailTx) Rollback() error {
	return nil
}

type outcomeErrorStore struct {
	storage.Store
	record    *schema.MemoryRecord
	updateErr error
}

func (s *outcomeErrorStore) Get(context.Context, string) (*schema.MemoryRecord, error) {
	return s.record, nil
}

func (s *outcomeErrorStore) Update(context.Context, *schema.MemoryRecord) error {
	return s.updateErr
}

func TestIngestEventDirectTimestampAndErrors(t *testing.T) {
	ctx := context.Background()
	svc, _ := newCaptureTestService(t, nil)

	rec, err := svc.IngestEvent(ctx, IngestEventRequest{
		Source:    "tester",
		EventKind: "event",
		Ref:       "evt-zero-time",
		Summary:   "zero timestamp uses now",
	})
	if err != nil {
		t.Fatalf("IngestEvent zero timestamp: %v", err)
	}
	if rec.CreatedAt.IsZero() {
		t.Fatalf("IngestEvent CreatedAt is zero, want current timestamp")
	}

	if _, err := svc.IngestEvent(ctx, IngestEventRequest{EventKind: "event", Ref: "evt-missing-source"}); err == nil || !strings.Contains(err.Error(), "candidate source is required") {
		t.Fatalf("IngestEvent validation error = %v, want source validation", err)
	}

	store, err := sqlitestore.Open(":memory:", "")
	if err != nil {
		t.Fatalf("Open sqlite: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close sqlite: %v", err)
	}
	closedSvc := NewService(store, NewClassifier(), NewPolicyEngine(DefaultPolicyDefaults()))
	if _, err := closedSvc.IngestEvent(ctx, IngestEventRequest{Source: "tester", EventKind: "event", Ref: "evt-store-error"}); err == nil || !strings.Contains(err.Error(), "store event") {
		t.Fatalf("IngestEvent closed store error = %v, want store event wrapper", err)
	}
}

func TestDirectIngestCreateErrors(t *testing.T) {
	ctx := context.Background()
	store, err := sqlitestore.Open(":memory:", "")
	if err != nil {
		t.Fatalf("Open sqlite: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close sqlite: %v", err)
	}
	svc := NewService(store, NewClassifier(), NewPolicyEngine(DefaultPolicyDefaults()))

	if _, err := svc.IngestToolOutput(ctx, IngestToolOutputRequest{Source: "tester", ToolName: "rg"}); err == nil || !strings.Contains(err.Error(), "store tool output") {
		t.Fatalf("IngestToolOutput closed store error = %v, want store tool output wrapper", err)
	}
	if _, err := svc.IngestWorkingState(ctx, IngestWorkingStateRequest{Source: "tester", ThreadID: "thread", State: schema.TaskStatePlanning}); err == nil || !strings.Contains(err.Error(), "store working state") {
		t.Fatalf("IngestWorkingState closed store error = %v, want store working state wrapper", err)
	}
}

func TestIngestObservationCreateErrorAndCanonicalizeGuards(t *testing.T) {
	ctx := context.Background()
	svc := NewService(&observationCreateFailStore{}, NewClassifier(), NewPolicyEngine(DefaultPolicyDefaults()))
	_, err := svc.IngestObservation(ctx, IngestObservationRequest{
		Source:    "observer",
		Subject:   "Orchid",
		Predicate: "is",
		Object:    "ready",
	})
	if err == nil || !strings.Contains(err.Error(), "store observation") {
		t.Fatalf("IngestObservation create error = %v, want store observation wrapper", err)
	}

	if edges := svc.canonicalizeSemanticEntities(ctx, schema.NewMemoryRecord("working", schema.MemoryTypeWorking, schema.SensitivityLow, &schema.WorkingPayload{
		Kind:     "working",
		ThreadID: "thread",
		State:    schema.TaskStatePlanning,
	})); edges != nil {
		t.Fatalf("canonicalize non-semantic edges = %+v, want nil", edges)
	}
	if edges := svc.canonicalizeSemanticEntities(ctx, schema.NewMemoryRecord("nil-payload", schema.MemoryTypeSemantic, schema.SensitivityLow, nil)); edges != nil {
		t.Fatalf("canonicalize nil payload edges = %+v, want nil", edges)
	}
	rec := schema.NewMemoryRecord("semantic", schema.MemoryTypeSemantic, schema.SensitivityLow, &schema.SemanticPayload{
		Kind:      "semantic",
		Subject:   "Orchid",
		Predicate: "is",
		Object:    "ready",
		Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
	})
	rec.CreatedAt = time.Time{}
	if edges := svc.canonicalizeSemanticEntities(ctx, rec); len(edges) != 0 {
		t.Fatalf("canonicalize without lookup edges = %+v, want empty", edges)
	}
}

func TestIngestOutcomeNilTargetAndUpdateErrors(t *testing.T) {
	ctx := context.Background()
	nilSvc := NewService(&outcomeErrorStore{}, NewClassifier(), NewPolicyEngine(DefaultPolicyDefaults()))
	if _, err := nilSvc.IngestOutcome(ctx, IngestOutcomeRequest{Source: "tester", TargetRecordID: "missing", OutcomeStatus: schema.OutcomeStatusSuccess}); err == nil || !strings.Contains(err.Error(), "was nil") {
		t.Fatalf("IngestOutcome nil record error = %v, want nil target error", err)
	}

	rec := schema.NewMemoryRecord("episode", schema.MemoryTypeEpisodic, schema.SensitivityLow, &schema.EpisodicPayload{
		Kind:     "episodic",
		Timeline: []schema.TimelineEvent{{EventKind: "event", Ref: "evt-1"}},
	})
	updateErr := errors.New("update failed")
	updateSvc := NewService(&outcomeErrorStore{record: rec, updateErr: updateErr}, NewClassifier(), NewPolicyEngine(DefaultPolicyDefaults()))
	if _, err := updateSvc.IngestOutcome(ctx, IngestOutcomeRequest{Source: "tester", TargetRecordID: rec.ID, OutcomeStatus: schema.OutcomeStatusSuccess}); !errors.Is(err, updateErr) {
		t.Fatalf("IngestOutcome update error = %v, want %v", err, updateErr)
	}
}
