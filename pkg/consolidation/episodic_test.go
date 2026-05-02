package consolidation

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/BennettSchwartz/membrane/pkg/schema"
	"github.com/BennettSchwartz/membrane/pkg/storage/sqlite"
)

func newConsolidationTestStore(t *testing.T) *sqlite.SQLiteStore {
	t.Helper()

	store, err := sqlite.Open(":memory:", "")
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	return store
}

func episodicRecordForCompression(id string, createdAt time.Time, salience float64) *schema.MemoryRecord {
	rec := schema.NewMemoryRecord(id, schema.MemoryTypeEpisodic, schema.SensitivityLow, &schema.EpisodicPayload{
		Kind: "episodic",
		Timeline: []schema.TimelineEvent{{
			T:         createdAt,
			EventKind: "event",
			Ref:       id,
			Summary:   "old event",
		}},
	})
	rec.CreatedAt = createdAt
	rec.UpdatedAt = createdAt
	rec.Lifecycle.LastReinforcedAt = createdAt
	rec.Salience = salience
	return rec
}

func TestEpisodicConsolidateCompressesEligibleRecords(t *testing.T) {
	ctx := context.Background()
	store := newConsolidationTestStore(t)
	consolidator := NewEpisodicConsolidator(store)
	now := time.Now().UTC()

	old := episodicRecordForCompression("old", now.Add(-25*time.Hour), 0.8)
	nearFloor := episodicRecordForCompression("near-floor", now.Add(-48*time.Hour), 0.08)
	recent := episodicRecordForCompression("recent", now.Add(-2*time.Hour), 0.8)
	atFloor := episodicRecordForCompression("at-floor", now.Add(-48*time.Hour), compressedSalienceFloor)
	pinned := episodicRecordForCompression("pinned", now.Add(-48*time.Hour), 0.8)
	pinned.Lifecycle.Pinned = true

	for _, rec := range []*schema.MemoryRecord{old, nearFloor, recent, atFloor, pinned} {
		if err := store.Create(ctx, rec); err != nil {
			t.Fatalf("Create %s: %v", rec.ID, err)
		}
	}

	count, err := consolidator.Consolidate(ctx)
	if err != nil {
		t.Fatalf("Consolidate: %v", err)
	}
	if count != 2 {
		t.Fatalf("compressed count = %d, want 2", count)
	}

	assertSalience(t, ctx, store, old.ID, 0.4)
	assertSalience(t, ctx, store, nearFloor.ID, compressedSalienceFloor)
	assertSalience(t, ctx, store, recent.ID, 0.8)
	assertSalience(t, ctx, store, atFloor.ID, compressedSalienceFloor)
	assertSalience(t, ctx, store, pinned.ID, 0.8)

	got, err := store.Get(ctx, old.ID)
	if err != nil {
		t.Fatalf("Get old: %v", err)
	}
	if !auditLogContains(got.AuditLog, schema.AuditActionDecay, "consolidation/episodic") {
		t.Fatalf("AuditLog = %+v, want episodic decay audit entry", got.AuditLog)
	}
}

func TestConsolidationRunAllIncludesEpisodicCompression(t *testing.T) {
	ctx := context.Background()
	store := newConsolidationTestStore(t)
	old := episodicRecordForCompression("old-run-all", time.Now().UTC().Add(-48*time.Hour), 0.6)
	if err := store.Create(ctx, old); err != nil {
		t.Fatalf("Create old episode: %v", err)
	}

	result, err := NewService(store).RunAll(ctx)
	if err != nil {
		t.Fatalf("RunAll: %v", err)
	}
	if result.EpisodicCompressed != 1 {
		t.Fatalf("EpisodicCompressed = %d, want 1", result.EpisodicCompressed)
	}
	if result.SemanticExtracted != 0 || result.CompetenceExtracted != 0 || result.PlanGraphsExtracted != 0 {
		t.Fatalf("unexpected extra consolidation result: %+v", result)
	}
	assertSalience(t, ctx, store, old.ID, 0.3)
}

func assertSalience(t *testing.T, ctx context.Context, store *sqlite.SQLiteStore, id string, want float64) {
	t.Helper()

	got, err := store.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get %s: %v", id, err)
	}
	if math.Abs(got.Salience-want) > 0.0001 {
		t.Fatalf("%s salience = %.4f, want %.4f", id, got.Salience, want)
	}
}

func auditLogContains(entries []schema.AuditEntry, action schema.AuditAction, actor string) bool {
	for _, entry := range entries {
		if entry.Action == action && entry.Actor == actor {
			return true
		}
	}
	return false
}
