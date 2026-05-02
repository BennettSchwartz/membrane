package revision

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/BennettSchwartz/membrane/pkg/schema"
	"github.com/BennettSchwartz/membrane/pkg/storage/sqlite"
)

type recordingEmbedder struct {
	ids []string
	err error
}

func (e *recordingEmbedder) EmbedRecord(_ context.Context, rec *schema.MemoryRecord) error {
	e.ids = append(e.ids, rec.ID)
	return e.err
}

func newRevisionTestStore(t *testing.T) *sqlite.SQLiteStore {
	t.Helper()
	store, err := sqlite.Open(":memory:", "")
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func semanticRevisionRecord(id, subject, object string) *schema.MemoryRecord {
	now := time.Date(2026, 5, 1, 11, 0, 0, 0, time.UTC)
	rec := schema.NewMemoryRecord(id, schema.MemoryTypeSemantic, schema.SensitivityLow, &schema.SemanticPayload{
		Kind:      "semantic",
		Subject:   subject,
		Predicate: "is",
		Object:    object,
		Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
	})
	rec.CreatedAt = now
	rec.UpdatedAt = now
	rec.Lifecycle.LastReinforcedAt = now
	rec.Provenance.Sources = append(rec.Provenance.Sources, schema.ProvenanceSource{
		Kind:      schema.ProvenanceKindObservation,
		Ref:       "evidence-" + id,
		Timestamp: now,
	})
	return rec
}

func TestServiceWithEmbedderEmbedsCreatedRevisionRecords(t *testing.T) {
	ctx := context.Background()
	store := newRevisionTestStore(t)
	embedder := &recordingEmbedder{err: errors.New("embedding backend unavailable")}
	svc := NewServiceWithEmbedder(store, embedder)

	old := semanticRevisionRecord("old", "runtime", "go1.24")
	if err := store.Create(ctx, old); err != nil {
		t.Fatalf("Create old: %v", err)
	}
	replacement := semanticRevisionRecord("replacement", "runtime", "go1.25")
	if _, err := svc.Supersede(ctx, old.ID, replacement, "tester", "version update"); err != nil {
		t.Fatalf("Supersede: %v", err)
	}

	source := semanticRevisionRecord("source", "database", "postgres")
	if err := store.Create(ctx, source); err != nil {
		t.Fatalf("Create source: %v", err)
	}
	forked := semanticRevisionRecord("forked", "database", "sqlite")
	if _, err := svc.Fork(ctx, source.ID, forked, "tester", "local variant"); err != nil {
		t.Fatalf("Fork: %v", err)
	}

	mergeA := semanticRevisionRecord("merge-a", "editor", "vim")
	mergeB := semanticRevisionRecord("merge-b", "editor", "neovim")
	for _, rec := range []*schema.MemoryRecord{mergeA, mergeB} {
		if err := store.Create(ctx, rec); err != nil {
			t.Fatalf("Create %s: %v", rec.ID, err)
		}
	}
	merged := semanticRevisionRecord("merged", "editor", "neovim")
	if _, err := svc.Merge(ctx, []string{mergeA.ID, mergeB.ID}, merged, "tester", "merge variants"); err != nil {
		t.Fatalf("Merge: %v", err)
	}

	want := []string{replacement.ID, forked.ID, merged.ID}
	if len(embedder.ids) != len(want) {
		t.Fatalf("embedded IDs = %v, want %v", embedder.ids, want)
	}
	for i := range want {
		if embedder.ids[i] != want[i] {
			t.Fatalf("embedded IDs = %v, want %v", embedder.ids, want)
		}
	}
}
