package tests_test

import (
	"context"
	"errors"
	"testing"

	"github.com/BennettSchwartz/membrane/pkg/ingestion"
	"github.com/BennettSchwartz/membrane/pkg/membrane"
	"github.com/BennettSchwartz/membrane/pkg/revision"
	"github.com/BennettSchwartz/membrane/pkg/schema"
)

// ingestSemanticRecord is a helper that ingests an observation and returns the record.
func ingestSemanticRecord(t *testing.T, m *membrane.Membrane, subject, predicate string, object any) *schema.MemoryRecord {
	t.Helper()
	rec, err := captureObservationRecord(context.Background(), m, ingestion.IngestObservationRequest{
		Source:    "test",
		Subject:   subject,
		Predicate: predicate,
		Object:    object,
		Tags:      []string{"test"},
	})
	if err != nil {
		t.Fatalf("ingestSemanticRecord: %v", err)
	}
	return rec
}

// ingestEpisodicRecord is a helper that ingests an event and returns the record.
func ingestEpisodicRecord(t *testing.T, m *membrane.Membrane) *schema.MemoryRecord {
	t.Helper()
	rec, err := captureEventRecord(context.Background(), m, ingestion.IngestEventRequest{
		Source:    "test",
		EventKind: "test_event",
		Ref:       "test-ref",
		Summary:   "test event",
	})
	if err != nil {
		t.Fatalf("ingestEpisodicRecord: %v", err)
	}
	return rec
}

func TestSupersede(t *testing.T) {
	ctx := context.Background()
	m := newTestMembrane(t)
	trust := fullTrust()

	// Create an original semantic record.
	original := ingestSemanticRecord(t, m, "Go", "version", "1.21")

	// Create a new record to supersede it (with evidence as required by RFC §7).
	newRec := schema.NewMemoryRecord("", schema.MemoryTypeSemantic, schema.SensitivityLow,
		&schema.SemanticPayload{
			Kind:      "semantic",
			Subject:   "Go",
			Predicate: "version",
			Object:    "1.22",
			Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
		},
	)
	newRec.Provenance.Sources = append(newRec.Provenance.Sources, schema.ProvenanceSource{
		Kind: "observation",
		Ref:  "evidence-for-supersede",
	})

	superseded, err := m.Supersede(ctx, original.ID, newRec, "test-actor", "version update")
	if err != nil {
		t.Fatalf("Supersede: %v", err)
	}

	// Verify old record is retracted (salience = 0).
	oldRec, err := m.RetrieveByID(ctx, original.ID, trust)
	if err != nil {
		t.Fatalf("RetrieveByID old: %v", err)
	}
	if oldRec.Salience != 0 {
		t.Errorf("expected old record salience=0, got %.2f", oldRec.Salience)
	}

	// Verify the semantic revision status is retracted.
	if sp, ok := oldRec.Payload.(*schema.SemanticPayload); ok {
		if sp.Revision == nil || sp.Revision.Status != schema.RevisionStatusRetracted {
			t.Errorf("expected old record revision status=retracted, got %v", sp.Revision)
		}
	}

	// Verify new record has a "supersedes" relation to the old.
	foundRelation := false
	for _, rel := range superseded.Relations {
		if rel.Predicate == "supersedes" && rel.TargetID == original.ID {
			foundRelation = true
			break
		}
	}
	if !foundRelation {
		t.Error("expected new record to have 'supersedes' relation to old record")
	}

	// Verify new record is retrievable.
	retrieved, err := m.RetrieveByID(ctx, superseded.ID, trust)
	if err != nil {
		t.Fatalf("RetrieveByID new: %v", err)
	}
	if retrieved.Salience <= 0 {
		t.Errorf("expected new record salience > 0, got %.2f", retrieved.Salience)
	}
}

func TestFork(t *testing.T) {
	ctx := context.Background()
	m := newTestMembrane(t)
	trust := fullTrust()

	// Create a source semantic record.
	source := ingestSemanticRecord(t, m, "database", "type", "PostgreSQL")

	// Fork it (with evidence as required by RFC §7).
	forkedRec := schema.NewMemoryRecord("", schema.MemoryTypeSemantic, schema.SensitivityLow,
		&schema.SemanticPayload{
			Kind:      "semantic",
			Subject:   "database",
			Predicate: "type",
			Object:    "SQLite",
			Validity: schema.Validity{
				Mode:       schema.ValidityModeConditional,
				Conditions: map[string]any{"env": "development"},
			},
		},
	)
	forkedRec.Provenance.Sources = append(forkedRec.Provenance.Sources, schema.ProvenanceSource{
		Kind: "observation",
		Ref:  "evidence-for-fork",
	})

	forked, err := m.Fork(ctx, source.ID, forkedRec, "test-actor", "conditional variant for dev")
	if err != nil {
		t.Fatalf("Fork: %v", err)
	}

	// Verify both records are still active (salience > 0).
	sourceAfter, err := m.RetrieveByID(ctx, source.ID, trust)
	if err != nil {
		t.Fatalf("RetrieveByID source: %v", err)
	}
	if sourceAfter.Salience <= 0 {
		t.Errorf("expected source salience > 0 after fork, got %.2f", sourceAfter.Salience)
	}

	forkedAfter, err := m.RetrieveByID(ctx, forked.ID, trust)
	if err != nil {
		t.Fatalf("RetrieveByID forked: %v", err)
	}
	if forkedAfter.Salience <= 0 {
		t.Errorf("expected forked salience > 0, got %.2f", forkedAfter.Salience)
	}

	// Verify the forked record has a "derived_from" relation.
	foundRelation := false
	for _, rel := range forked.Relations {
		if rel.Predicate == "derived_from" && rel.TargetID == source.ID {
			foundRelation = true
			break
		}
	}
	if !foundRelation {
		t.Error("expected forked record to have 'derived_from' relation to source")
	}
}

func TestRetract(t *testing.T) {
	ctx := context.Background()
	m := newTestMembrane(t)
	trust := fullTrust()

	// Create a semantic record.
	rec := ingestSemanticRecord(t, m, "fact", "is", "true")

	// Retract it.
	err := m.Retract(ctx, rec.ID, "test-actor", "fact was wrong")
	if err != nil {
		t.Fatalf("Retract: %v", err)
	}

	// Verify salience = 0.
	retracted, err := m.RetrieveByID(ctx, rec.ID, trust)
	if err != nil {
		t.Fatalf("RetrieveByID: %v", err)
	}
	if retracted.Salience != 0 {
		t.Errorf("expected salience=0 after retract, got %.2f", retracted.Salience)
	}

	// Verify revision status is retracted for semantic records.
	if sp, ok := retracted.Payload.(*schema.SemanticPayload); ok {
		if sp.Revision == nil || sp.Revision.Status != schema.RevisionStatusRetracted {
			t.Errorf("expected revision status=retracted, got %v", sp.Revision)
		}
	}
}

func TestMerge(t *testing.T) {
	ctx := context.Background()
	m := newTestMembrane(t)
	trust := fullTrust()

	// Create multiple semantic records.
	rec1 := ingestSemanticRecord(t, m, "tool", "uses", "vim")
	rec2 := ingestSemanticRecord(t, m, "tool", "uses", "neovim")
	rec3 := ingestSemanticRecord(t, m, "tool", "uses", "editor")

	// Merge them (with evidence as required by RFC §7).
	mergedRec := schema.NewMemoryRecord("", schema.MemoryTypeSemantic, schema.SensitivityLow,
		&schema.SemanticPayload{
			Kind:      "semantic",
			Subject:   "tool",
			Predicate: "uses",
			Object:    "neovim-based editor",
			Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
		},
	)
	mergedRec.Provenance.Sources = append(mergedRec.Provenance.Sources, schema.ProvenanceSource{
		Kind: "observation",
		Ref:  "evidence-for-merge",
	})

	merged, err := m.Merge(ctx, []string{rec1.ID, rec2.ID, rec3.ID}, mergedRec, "test-actor", "consolidating editor preferences")
	if err != nil {
		t.Fatalf("Merge: %v", err)
	}

	// Verify all source records are retracted (salience = 0).
	for _, id := range []string{rec1.ID, rec2.ID, rec3.ID} {
		r, err := m.RetrieveByID(ctx, id, trust)
		if err != nil {
			t.Fatalf("RetrieveByID(%s): %v", id, err)
		}
		if r.Salience != 0 {
			t.Errorf("expected source %s salience=0, got %.2f", id, r.Salience)
		}
	}

	// Verify merged record has "derived_from" relations to all sources.
	targetIDs := make(map[string]bool)
	for _, rel := range merged.Relations {
		if rel.Predicate == "derived_from" {
			targetIDs[rel.TargetID] = true
		}
	}
	for _, id := range []string{rec1.ID, rec2.ID, rec3.ID} {
		if !targetIDs[id] {
			t.Errorf("expected merged record to have 'derived_from' relation to %s", id)
		}
	}

	// Verify merged record is retrievable and active.
	mergedAfter, err := m.RetrieveByID(ctx, merged.ID, trust)
	if err != nil {
		t.Fatalf("RetrieveByID merged: %v", err)
	}
	if mergedAfter.Salience <= 0 {
		t.Errorf("expected merged record salience > 0, got %.2f", mergedAfter.Salience)
	}
}

func TestEpisodicRevisionRejected(t *testing.T) {
	ctx := context.Background()
	m := newTestMembrane(t)

	// Create an episodic record.
	episodic := ingestEpisodicRecord(t, m)

	// Attempt to supersede should fail (episodic source is immutable).
	newRec := schema.NewMemoryRecord("", schema.MemoryTypeSemantic, schema.SensitivityLow,
		&schema.SemanticPayload{
			Kind:      "semantic",
			Subject:   "test",
			Predicate: "is",
			Object:    "replaced",
			Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
		},
	)
	newRec.Provenance.Sources = append(newRec.Provenance.Sources, schema.ProvenanceSource{
		Kind: "observation",
		Ref:  "evidence",
	})
	_, err := m.Supersede(ctx, episodic.ID, newRec, "test", "should fail")
	if err == nil {
		t.Fatal("expected error when superseding episodic record")
	}
	if !errors.Is(err, revision.ErrEpisodicImmutable) {
		t.Errorf("expected ErrEpisodicImmutable, got: %v", err)
	}

	// Attempt to fork should fail.
	forkRec := schema.NewMemoryRecord("", schema.MemoryTypeSemantic, schema.SensitivityLow,
		&schema.SemanticPayload{
			Kind:      "semantic",
			Subject:   "test",
			Predicate: "is",
			Object:    "forked",
			Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
		},
	)
	forkRec.Provenance.Sources = append(forkRec.Provenance.Sources, schema.ProvenanceSource{
		Kind: "observation",
		Ref:  "evidence",
	})
	_, err = m.Fork(ctx, episodic.ID, forkRec, "test", "should fail")
	if err == nil {
		t.Fatal("expected error when forking episodic record")
	}
	if !errors.Is(err, revision.ErrEpisodicImmutable) {
		t.Errorf("expected ErrEpisodicImmutable, got: %v", err)
	}

	// Attempt to retract should fail.
	err = m.Retract(ctx, episodic.ID, "test", "should fail")
	if err == nil {
		t.Fatal("expected error when retracting episodic record")
	}
	if !errors.Is(err, revision.ErrEpisodicImmutable) {
		t.Errorf("expected ErrEpisodicImmutable, got: %v", err)
	}

	// Attempt to merge should fail.
	mergeRec := schema.NewMemoryRecord("", schema.MemoryTypeSemantic, schema.SensitivityLow,
		&schema.SemanticPayload{
			Kind:      "semantic",
			Subject:   "test",
			Predicate: "is",
			Object:    "merged",
			Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
		},
	)
	mergeRec.Provenance.Sources = append(mergeRec.Provenance.Sources, schema.ProvenanceSource{
		Kind: "observation",
		Ref:  "evidence",
	})
	_, err = m.Merge(ctx, []string{episodic.ID}, mergeRec, "test", "should fail")
	if err == nil {
		t.Fatal("expected error when merging episodic record")
	}
	if !errors.Is(err, revision.ErrEpisodicImmutable) {
		t.Errorf("expected ErrEpisodicImmutable, got: %v", err)
	}
}
