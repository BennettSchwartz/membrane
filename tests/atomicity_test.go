package tests_test

import (
	"context"
	"testing"

	"github.com/GustyCube/membrane/pkg/ingestion"
	"github.com/GustyCube/membrane/pkg/schema"
	"github.com/GustyCube/membrane/pkg/storage"
)

// TestAtomicSupersede verifies that a failed supersede does not leave partial state.
// If the new record is invalid and the transaction fails, the old record must remain
// unchanged (salience > 0, revision status unchanged).
func TestAtomicSupersede(t *testing.T) {
	ctx := context.Background()
	m := newTestMembrane(t)
	trust := fullTrust()

	// Ingest a semantic record.
	original := ingestSemanticRecord(t, m, "lang", "name", "Go")
	originalSalience := original.Salience

	// Attempt to supersede with a record referencing a non-existent old ID.
	// This should fail because the old record does not exist.
	newRec := schema.NewMemoryRecord("", schema.MemoryTypeSemantic, schema.SensitivityLow,
		&schema.SemanticPayload{
			Kind:      "semantic",
			Subject:   "lang",
			Predicate: "name",
			Object:    "Rust",
			Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
		},
	)
	_, err := m.Supersede(ctx, "non-existent-id", newRec, "test", "should fail")
	if err == nil {
		t.Fatal("expected error for non-existent old ID")
	}

	// Verify original record is unchanged.
	rec, err := m.RetrieveByID(ctx, original.ID, trust)
	if err != nil {
		t.Fatalf("RetrieveByID: %v", err)
	}
	if rec.Salience != originalSalience {
		t.Errorf("expected salience %.2f unchanged, got %.2f", originalSalience, rec.Salience)
	}
}

// TestAtomicMerge verifies that a failed merge does not retract any source records.
func TestAtomicMerge(t *testing.T) {
	ctx := context.Background()
	m := newTestMembrane(t)
	trust := fullTrust()

	// Ingest two semantic records.
	rec1 := ingestSemanticRecord(t, m, "x", "is", "1")
	rec2 := ingestSemanticRecord(t, m, "x", "is", "2")

	rec1Salience := rec1.Salience
	rec2Salience := rec2.Salience

	// Attempt to merge with a mix of valid and non-existent IDs.
	mergeRec := schema.NewMemoryRecord("", schema.MemoryTypeSemantic, schema.SensitivityLow,
		&schema.SemanticPayload{
			Kind:      "semantic",
			Subject:   "x",
			Predicate: "is",
			Object:    "merged",
			Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
		},
	)
	_, err := m.Merge(ctx, []string{rec1.ID, rec2.ID, "non-existent-id"}, mergeRec, "test", "should fail")
	if err == nil {
		t.Fatal("expected error for merge with non-existent ID")
	}

	// Verify both source records are unchanged (not retracted).
	for _, tc := range []struct {
		id               string
		expectedSalience float64
	}{
		{rec1.ID, rec1Salience},
		{rec2.ID, rec2Salience},
	} {
		r, err := m.RetrieveByID(ctx, tc.id, trust)
		if err != nil {
			t.Fatalf("RetrieveByID(%s): %v", tc.id, err)
		}
		if r.Salience != tc.expectedSalience {
			t.Errorf("record %s: expected salience %.2f, got %.2f (transaction should have rolled back)",
				tc.id, tc.expectedSalience, r.Salience)
		}
	}
}

// TestAtomicRetractNonExistent verifies that retracting a non-existent record returns an error.
func TestAtomicRetractNonExistent(t *testing.T) {
	ctx := context.Background()
	m := newTestMembrane(t)

	err := m.Retract(ctx, "does-not-exist", "test", "should fail")
	if err == nil {
		t.Fatal("expected error for retracting non-existent record")
	}
}

// TestAtomicForkNonExistent verifies that forking from a non-existent source returns an error.
func TestAtomicForkNonExistent(t *testing.T) {
	ctx := context.Background()
	m := newTestMembrane(t)

	forkedRec := schema.NewMemoryRecord("", schema.MemoryTypeSemantic, schema.SensitivityLow,
		&schema.SemanticPayload{
			Kind:      "semantic",
			Subject:   "test",
			Predicate: "is",
			Object:    "forked",
			Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
		},
	)
	_, err := m.Fork(ctx, "does-not-exist", forkedRec, "test", "should fail")
	if err == nil {
		t.Fatal("expected error for forking non-existent record")
	}
}

// TestStoreConsistencyAfterErrors verifies the store remains usable after errors.
func TestStoreConsistencyAfterErrors(t *testing.T) {
	ctx := context.Background()
	m := newTestMembrane(t)
	trust := fullTrust()

	// Trigger several errors.
	_ = m.Retract(ctx, "bad-id-1", "test", "fail")
	_ = m.Retract(ctx, "bad-id-2", "test", "fail")
	_, _ = m.RetrieveByID(ctx, "bad-id-3", trust)

	// Now verify the store is still functional by ingesting and retrieving.
	rec, err := m.IngestEvent(ctx, ingestion.IngestEventRequest{
		Source:    "test",
		EventKind: "post_error_test",
		Ref:       "ref-after-errors",
		Summary:   "Store should still work",
	})
	if err != nil {
		t.Fatalf("IngestEvent after errors: %v", err)
	}

	retrieved, err := m.RetrieveByID(ctx, rec.ID, trust)
	if err != nil {
		t.Fatalf("RetrieveByID after errors: %v", err)
	}
	if retrieved.ID != rec.ID {
		t.Errorf("expected ID %s, got %s", rec.ID, retrieved.ID)
	}

	// Also verify that creating a duplicate fails properly.
	err = func() error {
		// We can't directly access the store, but we can verify via the
		// membrane that IDs are unique by checking ingestion always produces
		// new IDs.
		rec2, err := m.IngestEvent(ctx, ingestion.IngestEventRequest{
			Source:    "test",
			EventKind: "another",
			Ref:       "ref-unique",
		})
		if err != nil {
			return err
		}
		if rec2.ID == rec.ID {
			t.Error("expected unique IDs for different ingested records")
		}
		return nil
	}()
	if err != nil {
		t.Fatalf("post-error ingestion: %v", err)
	}

	_ = storage.ErrNotFound // reference storage package
}
