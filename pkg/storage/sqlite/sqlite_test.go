package sqlite

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/GustyCube/membrane/pkg/schema"
	"github.com/GustyCube/membrane/pkg/storage"
)

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// newTestStore opens an in-memory SQLite store for testing.
func newTestStore(t *testing.T) *SQLiteStore {
	t.Helper()
	store, err := Open(":memory:", "")
	if err != nil {
		t.Fatalf("open test store: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

// newSemanticRecord builds a valid MemoryRecord with a SemanticPayload.
func newSemanticRecord(id string) *schema.MemoryRecord {
	now := time.Date(2025, 1, 15, 12, 0, 0, 0, time.UTC)
	return &schema.MemoryRecord{
		ID:          id,
		Type:        schema.MemoryTypeSemantic,
		Sensitivity: schema.SensitivityLow,
		Confidence:  0.9,
		Salience:    0.8,
		Scope:       "project",
		Tags:        []string{"test", "semantic"},
		CreatedAt:   now,
		UpdatedAt:   now,
		Lifecycle: schema.Lifecycle{
			Decay: schema.DecayProfile{
				Curve:             schema.DecayCurveExponential,
				HalfLifeSeconds:   86400,
				MinSalience:       0.1,
				MaxAgeSeconds:     604800,
				ReinforcementGain: 0.2,
			},
			LastReinforcedAt: now,
			Pinned:           false,
			DeletionPolicy:   schema.DeletionPolicyAutoPrune,
		},
		Provenance: schema.Provenance{
			Sources: []schema.ProvenanceSource{
				{
					Kind:      schema.ProvenanceKindObservation,
					Ref:       "obs-001",
					Hash:      "sha256:abc123",
					CreatedBy: "test-agent",
					Timestamp: now,
				},
			},
		},
		Relations: []schema.Relation{},
		Payload: &schema.SemanticPayload{
			Kind:      "semantic",
			Subject:   "Go",
			Predicate: "is_language",
			Object:    "programming",
			Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
		},
		AuditLog: []schema.AuditEntry{
			{
				Action:    schema.AuditActionCreate,
				Actor:     "test",
				Timestamp: now,
				Rationale: "initial creation",
			},
		},
	}
}

// newEpisodicRecord builds a valid MemoryRecord with an EpisodicPayload.
func newEpisodicRecord(id string) *schema.MemoryRecord {
	now := time.Date(2025, 1, 15, 12, 0, 0, 0, time.UTC)
	return &schema.MemoryRecord{
		ID:          id,
		Type:        schema.MemoryTypeEpisodic,
		Sensitivity: schema.SensitivityMedium,
		Confidence:  0.7,
		Salience:    0.5,
		Scope:       "session",
		Tags:        []string{"episodic", "debug"},
		CreatedAt:   now,
		UpdatedAt:   now,
		Lifecycle: schema.Lifecycle{
			Decay: schema.DecayProfile{
				Curve:             schema.DecayCurveExponential,
				HalfLifeSeconds:   3600,
				MinSalience:       0.0,
				ReinforcementGain: 0.1,
			},
			LastReinforcedAt: now,
			DeletionPolicy:   schema.DeletionPolicyAutoPrune,
		},
		Provenance: schema.Provenance{
			Sources: []schema.ProvenanceSource{
				{
					Kind:      schema.ProvenanceKindEvent,
					Ref:       "evt-001",
					Timestamp: now,
				},
			},
		},
		Relations: []schema.Relation{},
		Payload: &schema.EpisodicPayload{
			Kind: "episodic",
			Timeline: []schema.TimelineEvent{
				{
					T:         now,
					EventKind: "user_input",
					Ref:       "msg-001",
					Summary:   "User asked a question",
				},
			},
			Outcome: schema.OutcomeStatusSuccess,
		},
		AuditLog: []schema.AuditEntry{
			{
				Action:    schema.AuditActionCreate,
				Actor:     "system",
				Timestamp: now,
				Rationale: "captured episode",
			},
		},
	}
}

// newCompetenceRecord builds a valid MemoryRecord with a CompetencePayload.
func newCompetenceRecord(id string) *schema.MemoryRecord {
	now := time.Date(2025, 1, 15, 12, 0, 0, 0, time.UTC)
	return &schema.MemoryRecord{
		ID:          id,
		Type:        schema.MemoryTypeCompetence,
		Sensitivity: schema.SensitivityPublic,
		Confidence:  0.95,
		Salience:    0.9,
		Tags:        []string{"competence"},
		CreatedAt:   now,
		UpdatedAt:   now,
		Lifecycle: schema.Lifecycle{
			Decay: schema.DecayProfile{
				Curve:             schema.DecayCurveExponential,
				HalfLifeSeconds:   172800,
				MinSalience:       0.2,
				ReinforcementGain: 0.15,
			},
			LastReinforcedAt: now,
			Pinned:           true,
			DeletionPolicy:   schema.DeletionPolicyManualOnly,
		},
		Provenance: schema.Provenance{
			Sources: []schema.ProvenanceSource{
				{
					Kind:      schema.ProvenanceKindOutcome,
					Ref:       "outcome-001",
					Timestamp: now,
				},
			},
		},
		Relations: []schema.Relation{},
		Payload: &schema.CompetencePayload{
			Kind:      "competence",
			SkillName: "error_handling",
			Triggers:  []schema.Trigger{{Signal: "error_detected"}},
			Recipe:    []schema.RecipeStep{{Step: "check logs"}},
			Performance: &schema.PerformanceStats{
				SuccessCount: 10,
				FailureCount: 2,
			},
		},
		AuditLog: []schema.AuditEntry{
			{
				Action:    schema.AuditActionCreate,
				Actor:     "consolidator",
				Timestamp: now,
				Rationale: "learned from episodes",
			},
		},
	}
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestCreate(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	rec := newSemanticRecord("create-001")
	if err := store.Create(ctx, rec); err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Verify it can be retrieved.
	got, err := store.Get(ctx, "create-001")
	if err != nil {
		t.Fatalf("Get after Create: %v", err)
	}
	if got.ID != rec.ID {
		t.Errorf("ID = %q, want %q", got.ID, rec.ID)
	}
	if got.Type != rec.Type {
		t.Errorf("Type = %q, want %q", got.Type, rec.Type)
	}
	if got.Sensitivity != rec.Sensitivity {
		t.Errorf("Sensitivity = %q, want %q", got.Sensitivity, rec.Sensitivity)
	}
	if got.Confidence != rec.Confidence {
		t.Errorf("Confidence = %v, want %v", got.Confidence, rec.Confidence)
	}
	if got.Salience != rec.Salience {
		t.Errorf("Salience = %v, want %v", got.Salience, rec.Salience)
	}
	if got.Scope != rec.Scope {
		t.Errorf("Scope = %q, want %q", got.Scope, rec.Scope)
	}
	if len(got.Tags) != len(rec.Tags) {
		t.Errorf("Tags len = %d, want %d", len(got.Tags), len(rec.Tags))
	}
	if len(got.AuditLog) != 1 {
		t.Errorf("AuditLog len = %d, want 1", len(got.AuditLog))
	}
	if len(got.Provenance.Sources) != 1 {
		t.Errorf("Provenance.Sources len = %d, want 1", len(got.Provenance.Sources))
	}
}

func TestGet(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	rec := newSemanticRecord("get-001")
	if err := store.Create(ctx, rec); err != nil {
		t.Fatalf("Create: %v", err)
	}

	got, err := store.Get(ctx, "get-001")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	// Verify payload deserialization.
	sp, ok := got.Payload.(*schema.SemanticPayload)
	if !ok {
		t.Fatalf("Payload type = %T, want *schema.SemanticPayload", got.Payload)
	}
	if sp.Kind != "semantic" {
		t.Errorf("Payload.Kind = %q, want %q", sp.Kind, "semantic")
	}
	if sp.Subject != "Go" {
		t.Errorf("Payload.Subject = %q, want %q", sp.Subject, "Go")
	}
	if sp.Predicate != "is_language" {
		t.Errorf("Payload.Predicate = %q, want %q", sp.Predicate, "is_language")
	}

	// Verify lifecycle fields.
	if got.Lifecycle.Decay.Curve != schema.DecayCurveExponential {
		t.Errorf("Decay.Curve = %q, want %q", got.Lifecycle.Decay.Curve, schema.DecayCurveExponential)
	}
	if got.Lifecycle.Decay.HalfLifeSeconds != 86400 {
		t.Errorf("Decay.HalfLifeSeconds = %d, want 86400", got.Lifecycle.Decay.HalfLifeSeconds)
	}
	if got.Lifecycle.DeletionPolicy != schema.DeletionPolicyAutoPrune {
		t.Errorf("DeletionPolicy = %q, want %q", got.Lifecycle.DeletionPolicy, schema.DeletionPolicyAutoPrune)
	}
	if got.Lifecycle.Decay.MaxAgeSeconds != 604800 {
		t.Errorf("Decay.MaxAgeSeconds = %d, want 604800", got.Lifecycle.Decay.MaxAgeSeconds)
	}

	// Verify provenance.
	if len(got.Provenance.Sources) != 1 {
		t.Fatalf("Provenance.Sources len = %d, want 1", len(got.Provenance.Sources))
	}
	src := got.Provenance.Sources[0]
	if src.Kind != schema.ProvenanceKindObservation {
		t.Errorf("Source.Kind = %q, want %q", src.Kind, schema.ProvenanceKindObservation)
	}
	if src.Hash != "sha256:abc123" {
		t.Errorf("Source.Hash = %q, want %q", src.Hash, "sha256:abc123")
	}

	// Verify timestamps round-trip.
	if !got.CreatedAt.Equal(rec.CreatedAt) {
		t.Errorf("CreatedAt = %v, want %v", got.CreatedAt, rec.CreatedAt)
	}
	if !got.UpdatedAt.Equal(rec.UpdatedAt) {
		t.Errorf("UpdatedAt = %v, want %v", got.UpdatedAt, rec.UpdatedAt)
	}
}

func TestGetNotFound(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	_, err := store.Get(ctx, "nonexistent-id")
	if !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("Get non-existent: err = %v, want ErrNotFound", err)
	}
}

func TestUpdate(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	rec := newSemanticRecord("update-001")
	if err := store.Create(ctx, rec); err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Modify and update.
	rec.Salience = 0.5
	rec.Sensitivity = schema.SensitivityHigh
	rec.Tags = []string{"updated", "modified"}
	rec.UpdatedAt = rec.UpdatedAt.Add(time.Hour)
	rec.Payload = &schema.SemanticPayload{
		Kind:      "semantic",
		Subject:   "Rust",
		Predicate: "is_language",
		Object:    "systems",
		Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
	}

	if err := store.Update(ctx, rec); err != nil {
		t.Fatalf("Update: %v", err)
	}

	got, err := store.Get(ctx, "update-001")
	if err != nil {
		t.Fatalf("Get after Update: %v", err)
	}

	if got.Salience != 0.5 {
		t.Errorf("Salience = %v, want 0.5", got.Salience)
	}
	if got.Sensitivity != schema.SensitivityHigh {
		t.Errorf("Sensitivity = %q, want %q", got.Sensitivity, schema.SensitivityHigh)
	}
	if len(got.Tags) != 2 {
		t.Errorf("Tags len = %d, want 2", len(got.Tags))
	}
	tagSet := map[string]bool{}
	for _, tag := range got.Tags {
		tagSet[tag] = true
	}
	if !tagSet["updated"] || !tagSet["modified"] {
		t.Errorf("Tags = %v, want to contain 'updated' and 'modified'", got.Tags)
	}

	sp, ok := got.Payload.(*schema.SemanticPayload)
	if !ok {
		t.Fatalf("Payload type = %T, want *schema.SemanticPayload", got.Payload)
	}
	if sp.Subject != "Rust" {
		t.Errorf("Payload.Subject = %q, want %q", sp.Subject, "Rust")
	}
}

func TestDelete(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	rec := newSemanticRecord("delete-001")
	if err := store.Create(ctx, rec); err != nil {
		t.Fatalf("Create: %v", err)
	}

	if err := store.Delete(ctx, "delete-001"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	_, err := store.Get(ctx, "delete-001")
	if !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("Get after Delete: err = %v, want ErrNotFound", err)
	}

	// Deleting again should return ErrNotFound.
	err = store.Delete(ctx, "delete-001")
	if !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("Delete again: err = %v, want ErrNotFound", err)
	}
}

func TestList(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	// Create a set of records with varying attributes.
	saliences := []float64{0.0, 0.25, 0.5, 0.75, 1.0}
	for i := 0; i < 5; i++ {
		rec := newSemanticRecord(fmt.Sprintf("list-%03d", i))
		rec.Salience = saliences[i]
		rec.Scope = "project"
		rec.Sensitivity = schema.SensitivityLow
		rec.Tags = []string{"batch", fmt.Sprintf("idx-%d", i)}
		if err := store.Create(ctx, rec); err != nil {
			t.Fatalf("Create list-%03d: %v", i, err)
		}
	}

	tests := []struct {
		name    string
		opts    storage.ListOptions
		wantLen int
	}{
		{
			name:    "no filters",
			opts:    storage.ListOptions{},
			wantLen: 5,
		},
		{
			name:    "filter by type",
			opts:    storage.ListOptions{Type: schema.MemoryTypeSemantic},
			wantLen: 5,
		},
		{
			name:    "filter by type no match",
			opts:    storage.ListOptions{Type: schema.MemoryTypeEpisodic},
			wantLen: 0,
		},
		{
			name:    "filter by scope",
			opts:    storage.ListOptions{Scope: "project"},
			wantLen: 5,
		},
		{
			name:    "filter by scope no match",
			opts:    storage.ListOptions{Scope: "nonexistent"},
			wantLen: 0,
		},
		{
			name:    "filter by sensitivity",
			opts:    storage.ListOptions{Sensitivity: schema.SensitivityLow},
			wantLen: 5,
		},
		{
			name:    "filter by sensitivity no match",
			opts:    storage.ListOptions{Sensitivity: schema.SensitivityHyper},
			wantLen: 0,
		},
		{
			name:    "min salience",
			opts:    storage.ListOptions{MinSalience: 0.5},
			wantLen: 3, // 0.5, 0.75, 1.0
		},
		{
			name:    "max salience",
			opts:    storage.ListOptions{MaxSalience: 0.25},
			wantLen: 2, // 0.0, 0.25
		},
		{
			name:    "min and max salience",
			opts:    storage.ListOptions{MinSalience: 0.25, MaxSalience: 0.75},
			wantLen: 3, // 0.25, 0.5, 0.75
		},
		{
			name:    "filter by single tag",
			opts:    storage.ListOptions{Tags: []string{"batch"}},
			wantLen: 5,
		},
		{
			name:    "filter by specific tag",
			opts:    storage.ListOptions{Tags: []string{"idx-2"}},
			wantLen: 1,
		},
		{
			name:    "filter by multiple tags (AND)",
			opts:    storage.ListOptions{Tags: []string{"batch", "idx-3"}},
			wantLen: 1,
		},
		{
			name:    "filter by tag no match",
			opts:    storage.ListOptions{Tags: []string{"nonexistent"}},
			wantLen: 0,
		},
		{
			name:    "limit",
			opts:    storage.ListOptions{Limit: 2},
			wantLen: 2,
		},
		{
			name:    "limit and offset",
			opts:    storage.ListOptions{Limit: 2, Offset: 3},
			wantLen: 2,
		},
		{
			name:    "offset past end",
			opts:    storage.ListOptions{Limit: 100, Offset: 10},
			wantLen: 0,
		},
		{
			name: "combined filters",
			opts: storage.ListOptions{
				Type:        schema.MemoryTypeSemantic,
				Scope:       "project",
				Sensitivity: schema.SensitivityLow,
				Tags:        []string{"batch"},
				MinSalience: 0.5,
				Limit:       10,
			},
			wantLen: 3, // 0.5, 0.75, 1.0
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results, err := store.List(ctx, tt.opts)
			if err != nil {
				t.Fatalf("List: %v", err)
			}
			if len(results) != tt.wantLen {
				t.Errorf("List returned %d records, want %d", len(results), tt.wantLen)
			}
		})
	}
}

func TestListByType(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	sem := newSemanticRecord("lbt-sem-001")
	epi := newEpisodicRecord("lbt-epi-001")
	comp := newCompetenceRecord("lbt-comp-001")

	for _, rec := range []*schema.MemoryRecord{sem, epi, comp} {
		if err := store.Create(ctx, rec); err != nil {
			t.Fatalf("Create %s: %v", rec.ID, err)
		}
	}

	tests := []struct {
		memType schema.MemoryType
		wantLen int
	}{
		{schema.MemoryTypeSemantic, 1},
		{schema.MemoryTypeEpisodic, 1},
		{schema.MemoryTypeCompetence, 1},
		{schema.MemoryTypeWorking, 0},
		{schema.MemoryTypePlanGraph, 0},
	}

	for _, tt := range tests {
		t.Run(string(tt.memType), func(t *testing.T) {
			results, err := store.ListByType(ctx, tt.memType)
			if err != nil {
				t.Fatalf("ListByType: %v", err)
			}
			if len(results) != tt.wantLen {
				t.Errorf("ListByType(%s) = %d records, want %d", tt.memType, len(results), tt.wantLen)
			}
		})
	}
}

func TestUpdateSalience(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	rec := newSemanticRecord("salience-001")
	if err := store.Create(ctx, rec); err != nil {
		t.Fatalf("Create: %v", err)
	}

	if err := store.UpdateSalience(ctx, "salience-001", 0.42); err != nil {
		t.Fatalf("UpdateSalience: %v", err)
	}

	got, err := store.Get(ctx, "salience-001")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Salience != 0.42 {
		t.Errorf("Salience = %v, want 0.42", got.Salience)
	}

	// UpdateSalience on non-existent record.
	err = store.UpdateSalience(ctx, "nonexistent", 0.5)
	if !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("UpdateSalience nonexistent: err = %v, want ErrNotFound", err)
	}
}

func TestAddAuditEntry(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	rec := newSemanticRecord("audit-001")
	if err := store.Create(ctx, rec); err != nil {
		t.Fatalf("Create: %v", err)
	}

	entry := schema.AuditEntry{
		Action:    schema.AuditActionRevise,
		Actor:     "test-user",
		Timestamp: time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC),
		Rationale: "updated confidence",
	}
	if err := store.AddAuditEntry(ctx, "audit-001", entry); err != nil {
		t.Fatalf("AddAuditEntry: %v", err)
	}

	got, err := store.Get(ctx, "audit-001")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if len(got.AuditLog) != 2 {
		t.Fatalf("AuditLog len = %d, want 2", len(got.AuditLog))
	}
	last := got.AuditLog[1]
	if last.Action != schema.AuditActionRevise {
		t.Errorf("AuditLog[1].Action = %q, want %q", last.Action, schema.AuditActionRevise)
	}
	if last.Actor != "test-user" {
		t.Errorf("AuditLog[1].Actor = %q, want %q", last.Actor, "test-user")
	}
	if last.Rationale != "updated confidence" {
		t.Errorf("AuditLog[1].Rationale = %q, want %q", last.Rationale, "updated confidence")
	}

	// AddAuditEntry on non-existent record.
	err = store.AddAuditEntry(ctx, "nonexistent", entry)
	if !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("AddAuditEntry nonexistent: err = %v, want ErrNotFound", err)
	}
}

func TestAddRelation(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	// Create two records so the foreign key on target_id is satisfied.
	src := newSemanticRecord("rel-src")
	tgt := newSemanticRecord("rel-tgt")
	for _, r := range []*schema.MemoryRecord{src, tgt} {
		if err := store.Create(ctx, r); err != nil {
			t.Fatalf("Create %s: %v", r.ID, err)
		}
	}

	rel := schema.Relation{
		Predicate: "supports",
		TargetID:  "rel-tgt",
		Weight:    0.75,
		CreatedAt: time.Date(2025, 3, 1, 0, 0, 0, 0, time.UTC),
	}
	if err := store.AddRelation(ctx, "rel-src", rel); err != nil {
		t.Fatalf("AddRelation: %v", err)
	}

	// Verify via Get.
	got, err := store.Get(ctx, "rel-src")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if len(got.Relations) != 1 {
		t.Fatalf("Relations len = %d, want 1", len(got.Relations))
	}
	r := got.Relations[0]
	if r.Predicate != "supports" {
		t.Errorf("Relation.Predicate = %q, want %q", r.Predicate, "supports")
	}
	if r.TargetID != "rel-tgt" {
		t.Errorf("Relation.TargetID = %q, want %q", r.TargetID, "rel-tgt")
	}
	if r.Weight != 0.75 {
		t.Errorf("Relation.Weight = %v, want 0.75", r.Weight)
	}

	// AddRelation on non-existent source.
	err = store.AddRelation(ctx, "nonexistent", rel)
	if !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("AddRelation nonexistent: err = %v, want ErrNotFound", err)
	}
}

func TestGetRelations(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	src := newSemanticRecord("gr-src")
	tgt1 := newSemanticRecord("gr-tgt1")
	tgt2 := newSemanticRecord("gr-tgt2")
	for _, r := range []*schema.MemoryRecord{src, tgt1, tgt2} {
		if err := store.Create(ctx, r); err != nil {
			t.Fatalf("Create %s: %v", r.ID, err)
		}
	}

	rels := []schema.Relation{
		{Predicate: "supports", TargetID: "gr-tgt1", Weight: 0.9, CreatedAt: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)},
		{Predicate: "contradicts", TargetID: "gr-tgt2", Weight: 0.3, CreatedAt: time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)},
	}
	for _, rel := range rels {
		if err := store.AddRelation(ctx, "gr-src", rel); err != nil {
			t.Fatalf("AddRelation: %v", err)
		}
	}

	got, err := store.GetRelations(ctx, "gr-src")
	if err != nil {
		t.Fatalf("GetRelations: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("GetRelations len = %d, want 2", len(got))
	}
	if got[0].Predicate != "supports" {
		t.Errorf("Relations[0].Predicate = %q, want %q", got[0].Predicate, "supports")
	}
	if got[1].Predicate != "contradicts" {
		t.Errorf("Relations[1].Predicate = %q, want %q", got[1].Predicate, "contradicts")
	}

	// GetRelations for a record with no relations should return empty slice.
	got, err = store.GetRelations(ctx, "gr-tgt1")
	if err != nil {
		t.Fatalf("GetRelations empty: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("GetRelations empty len = %d, want 0", len(got))
	}
}

func TestTransaction(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	// Test commit persists.
	t.Run("commit", func(t *testing.T) {
		tx, err := store.Begin(ctx)
		if err != nil {
			t.Fatalf("Begin: %v", err)
		}

		rec := newSemanticRecord("tx-commit-001")
		if err := tx.Create(ctx, rec); err != nil {
			t.Fatalf("tx.Create: %v", err)
		}

		// Visible within the transaction.
		if _, err := tx.Get(ctx, "tx-commit-001"); err != nil {
			t.Fatalf("tx.Get before commit: %v", err)
		}

		if err := tx.Commit(); err != nil {
			t.Fatalf("Commit: %v", err)
		}

		// Visible outside after commit.
		got, err := store.Get(ctx, "tx-commit-001")
		if err != nil {
			t.Fatalf("Get after commit: %v", err)
		}
		if got.ID != "tx-commit-001" {
			t.Errorf("ID = %q, want %q", got.ID, "tx-commit-001")
		}
	})

	// Test rollback reverts.
	t.Run("rollback", func(t *testing.T) {
		tx, err := store.Begin(ctx)
		if err != nil {
			t.Fatalf("Begin: %v", err)
		}

		rec := newSemanticRecord("tx-rollback-001")
		if err := tx.Create(ctx, rec); err != nil {
			t.Fatalf("tx.Create: %v", err)
		}

		if err := tx.Rollback(); err != nil {
			t.Fatalf("Rollback: %v", err)
		}

		// Not visible outside after rollback.
		_, err = store.Get(ctx, "tx-rollback-001")
		if !errors.Is(err, storage.ErrNotFound) {
			t.Errorf("Get after rollback: err = %v, want ErrNotFound", err)
		}
	})

	// Test closed transaction returns ErrTxClosed.
	t.Run("closed", func(t *testing.T) {
		tx, err := store.Begin(ctx)
		if err != nil {
			t.Fatalf("Begin: %v", err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("Commit: %v", err)
		}

		rec := newSemanticRecord("tx-closed-001")
		if err := tx.Create(ctx, rec); !errors.Is(err, storage.ErrTxClosed) {
			t.Errorf("Create after close: err = %v, want ErrTxClosed", err)
		}
		if err := tx.Commit(); !errors.Is(err, storage.ErrTxClosed) {
			t.Errorf("Commit after close: err = %v, want ErrTxClosed", err)
		}
		if err := tx.Rollback(); !errors.Is(err, storage.ErrTxClosed) {
			t.Errorf("Rollback after close: err = %v, want ErrTxClosed", err)
		}
	})
}

func TestTransactionAtomicity(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	// Pre-create a record so the second create in the tx will fail on duplicate.
	existing := newSemanticRecord("atom-dup")
	if err := store.Create(ctx, existing); err != nil {
		t.Fatalf("Create existing: %v", err)
	}

	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}

	// First operation succeeds.
	rec1 := newSemanticRecord("atom-new")
	if err := tx.Create(ctx, rec1); err != nil {
		t.Fatalf("tx.Create rec1: %v", err)
	}

	// Second operation fails (duplicate).
	rec2 := newSemanticRecord("atom-dup")
	err = tx.Create(ctx, rec2)
	if !errors.Is(err, storage.ErrAlreadyExists) {
		t.Fatalf("tx.Create duplicate: err = %v, want ErrAlreadyExists", err)
	}

	// Rollback the transaction since it had a failure.
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	// The first record should not be visible because we rolled back.
	_, err = store.Get(ctx, "atom-new")
	if !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("Get atom-new after rollback: err = %v, want ErrNotFound", err)
	}
}

func TestCreateDuplicate(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	rec := newSemanticRecord("dup-001")
	if err := store.Create(ctx, rec); err != nil {
		t.Fatalf("Create first: %v", err)
	}

	err := store.Create(ctx, rec)
	if !errors.Is(err, storage.ErrAlreadyExists) {
		t.Errorf("Create duplicate: err = %v, want ErrAlreadyExists", err)
	}
}

func TestCascadeDelete(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	// Create two records with a relation between them.
	src := newSemanticRecord("cascade-src")
	tgt := newSemanticRecord("cascade-tgt")
	for _, r := range []*schema.MemoryRecord{src, tgt} {
		if err := store.Create(ctx, r); err != nil {
			t.Fatalf("Create %s: %v", r.ID, err)
		}
	}

	// Add a relation from src to tgt.
	if err := store.AddRelation(ctx, "cascade-src", schema.Relation{
		Predicate: "derived_from",
		TargetID:  "cascade-tgt",
		Weight:    1.0,
		CreatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("AddRelation: %v", err)
	}

	// Add an audit entry.
	if err := store.AddAuditEntry(ctx, "cascade-src", schema.AuditEntry{
		Action:    schema.AuditActionRevise,
		Actor:     "cascade-test",
		Timestamp: time.Now().UTC(),
		Rationale: "testing cascade",
	}); err != nil {
		t.Fatalf("AddAuditEntry: %v", err)
	}

	// Delete the source record.
	if err := store.Delete(ctx, "cascade-src"); err != nil {
		t.Fatalf("Delete cascade-src: %v", err)
	}

	// The source record should be gone.
	_, err := store.Get(ctx, "cascade-src")
	if !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("Get cascade-src after delete: err = %v, want ErrNotFound", err)
	}

	// The target record should still exist (cascade only removes children, not targets).
	got, err := store.Get(ctx, "cascade-tgt")
	if err != nil {
		t.Fatalf("Get cascade-tgt after delete: %v", err)
	}
	if got.ID != "cascade-tgt" {
		t.Errorf("cascade-tgt ID = %q, want %q", got.ID, "cascade-tgt")
	}

	// Relations referencing cascade-src as source should be gone.
	// Since cascade-src is deleted, we verify indirectly: cascade-tgt should have
	// no inbound relations visible (relations table rows are cascade-deleted).
	// We can verify by querying relations for cascade-tgt (it had none as source).
	rels, err := store.GetRelations(ctx, "cascade-tgt")
	if err != nil {
		t.Fatalf("GetRelations cascade-tgt: %v", err)
	}
	if len(rels) != 0 {
		t.Errorf("GetRelations cascade-tgt = %d, want 0 (target had no outgoing rels)", len(rels))
	}
}
