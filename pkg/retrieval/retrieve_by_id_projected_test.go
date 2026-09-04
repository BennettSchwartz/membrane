package retrieval

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/BennettSchwartz/membrane/pkg/schema"
	"github.com/BennettSchwartz/membrane/pkg/storage"
)

type projectedByIDStore struct {
	storage.Store
	result   storage.BoundedListResult
	err      error
	options  []storage.ListOptions
	getCalls int
}

func (s *projectedByIDStore) Get(context.Context, string) (*schema.MemoryRecord, error) {
	s.getCalls++
	return nil, errors.New("full Get must not be used")
}

func (s *projectedByIDStore) ListBounded(_ context.Context, opts storage.ListOptions) (storage.BoundedListResult, error) {
	s.options = append(s.options, opts)
	return s.result, s.err
}

func TestRetrieveProjectedByIDUsesExactBoundedProjection(t *testing.T) {
	record := newSemanticRetrievalRecord("projected-id", 0.8, schema.SensitivityLow)
	record.Scope = "project:alpha"
	record.Relations = []schema.Relation{{Predicate: "related_to", TargetID: "neighbor"}}
	record.AuditLog = []schema.AuditEntry{{Action: schema.AuditActionReinforce, Actor: "fixture"}}
	record.Provenance.Sources = []schema.ProvenanceSource{{Kind: schema.ProvenanceKindObservation, Ref: "fixture"}}

	store := &projectedByIDStore{result: storage.BoundedListResult{
		Records:        []*schema.MemoryRecord{record},
		ProjectedBytes: storage.MaxBoundedHydrationBytes + 1, // dishonest metadata must be ignored
	}}
	result, err := NewService(store, nil).RetrieveProjectedByID(context.Background(), record.ID,
		NewTrustContext(schema.SensitivityLow, true, "tester", []string{"project:alpha"}))
	if err != nil {
		t.Fatalf("RetrieveProjectedByID: %v", err)
	}
	if store.getCalls != 0 || len(store.options) != 1 {
		t.Fatalf("store calls = Get:%d ListBounded:%d, want 0/1", store.getCalls, len(store.options))
	}
	opts := store.options[0]
	if opts.ID != record.ID || opts.Limit != 1 || !opts.OmitRelations || !opts.OmitHistory || opts.MaxHydratedBytes != MaxProjectedResponseBytes ||
		opts.MaxSensitivity != schema.SensitivityLow || len(opts.Scopes) != 1 || opts.Scopes[0] != "project:alpha" || !opts.IncludeUnscoped {
		t.Fatalf("ListBounded options = %+v, want exact-ID bounded projection", opts)
	}
	if result == nil || result.Record == nil || result.Record.ID != record.ID {
		t.Fatalf("result = %+v, want %q", result, record.ID)
	}
	if len(result.Record.Relations) != 0 || len(result.Record.AuditLog) != 0 || len(result.Record.Provenance.Sources) != 0 {
		t.Fatalf("projected record leaked omitted fields: %+v", result.Record)
	}
	if !result.Projection.RelationsOmitted || !result.Projection.HistoryOmitted || result.Projection.RelationsTruncated || result.Projection.RecordsTruncated {
		t.Fatalf("projection = %+v, want truthful single-record omission metadata", result.Projection)
	}
	if len(record.Relations) != 1 || len(record.AuditLog) != 1 || len(record.Provenance.Sources) != 1 {
		t.Fatal("service projection mutated store-owned record")
	}
}

func TestRetrieveProjectedByIDFailsClosedAndRejectsOversize(t *testing.T) {
	trust := NewTrustContext(schema.SensitivityLow, true, "tester", nil)
	if _, err := NewService(&failingRetrievalStore{}, nil).RetrieveProjectedByID(context.Background(), "id", trust); !errors.Is(err, ErrBoundedRetrievalUnsupported) {
		t.Fatalf("unsupported error = %v, want ErrBoundedRetrievalUnsupported", err)
	}

	oversize := newSemanticRetrievalRecord("oversize", 1, schema.SensitivityLow)
	oversize.Tags = []string{strings.Repeat("x", int(MaxProjectedResponseBytes))}
	store := &projectedByIDStore{result: storage.BoundedListResult{Records: []*schema.MemoryRecord{oversize}}}
	_, err := NewService(store, nil).RetrieveProjectedByID(context.Background(), oversize.ID, trust)
	var tooLarge *ProjectedRecordTooLargeError
	if !errors.As(err, &tooLarge) || tooLarge.Limit != MaxProjectedResponseBytes {
		t.Fatalf("oversize error = %v, want ProjectedRecordTooLargeError limit %d", err, MaxProjectedResponseBytes)
	}

	truncated := &projectedByIDStore{result: storage.BoundedListResult{HydrationBytesTruncated: true}}
	_, err = NewService(truncated, nil).RetrieveProjectedByID(context.Background(), "too-large-before-hydration", trust)
	if !errors.As(err, &tooLarge) {
		t.Fatalf("pre-hydration truncation error = %v, want ProjectedRecordTooLargeError", err)
	}
}

func TestRetrieveProjectedByIDSanitizesWrongIDAndRechecksTrust(t *testing.T) {
	wrong := newSemanticRetrievalRecord("wrong", 1, schema.SensitivityLow)
	store := &projectedByIDStore{result: storage.BoundedListResult{Records: []*schema.MemoryRecord{wrong, newSemanticRetrievalRecord("wanted", 1, schema.SensitivityLow)}}}
	_, err := NewService(store, nil).RetrieveProjectedByID(context.Background(), "wanted",
		NewTrustContext(schema.SensitivityLow, true, "tester", nil))
	if !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("wrong-ID contract result = %v, want ErrNotFound without scanning excess rows", err)
	}

	denied := newSemanticRetrievalRecord("denied", 1, schema.SensitivityHigh)
	store = &projectedByIDStore{result: storage.BoundedListResult{Records: []*schema.MemoryRecord{denied}}}
	_, err = NewService(store, nil).RetrieveProjectedByID(context.Background(), denied.ID,
		NewTrustContext(schema.SensitivityLow, true, "tester", nil))
	if !errors.Is(err, ErrAccessDenied) {
		t.Fatalf("denied error = %v, want ErrAccessDenied", err)
	}
}
