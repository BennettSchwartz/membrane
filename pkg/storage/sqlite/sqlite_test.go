package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/BennettSchwartz/membrane/pkg/schema"
	"github.com/BennettSchwartz/membrane/pkg/storage"
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

func TestOpenWithEncryptionAndSchemaError(t *testing.T) {
	encrypted, err := Open(":memory:", "test-key")
	if err != nil {
		t.Fatalf("Open encrypted memory DB: %v", err)
	}
	if err := encrypted.Close(); err != nil {
		t.Fatalf("Close encrypted memory DB: %v", err)
	}

	_, err = Open(filepath.Join(t.TempDir(), "missing", "membrane.db"), "")
	if err == nil || !strings.Contains(err.Error(), "apply schema") {
		t.Fatalf("Open missing directory error = %v, want apply schema error", err)
	}
}

func TestOpenInjectedDBBranches(t *testing.T) {
	oldSQLOpen := sqlOpen
	defer func() { sqlOpen = oldSQLOpen }()

	t.Run("sql open error", func(t *testing.T) {
		sqlOpen = func(string, string) (*sql.DB, error) {
			return nil, errors.New("open failed")
		}
		if _, err := Open("membrane.db", ""); err == nil || !strings.Contains(err.Error(), "open sqlite") {
			t.Fatalf("Open sql error = %v, want open sqlite error", err)
		}
	})

	t.Run("encryption key error closes db", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		defer func() {
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("unmet sql expectations: %v", err)
			}
		}()
		sqlOpen = func(driverName, dsn string) (*sql.DB, error) {
			if driverName != "sqlite3" || !strings.Contains(dsn, "_foreign_keys=on") {
				t.Fatalf("sqlOpen args = %q/%q, want sqlite3 DSN with options", driverName, dsn)
			}
			return db, nil
		}

		mock.ExpectExec(`PRAGMA key`).
			WillReturnError(errors.New("key failed"))
		mock.ExpectClose()

		if _, err := Open("membrane.db", "secret"); err == nil || !strings.Contains(err.Error(), "set encryption key") {
			t.Fatalf("Open key error = %v, want set encryption key error", err)
		}
	})

	t.Run("encryption verification error closes db", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		defer func() {
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("unmet sql expectations: %v", err)
			}
		}()
		sqlOpen = func(string, string) (*sql.DB, error) {
			return db, nil
		}

		mock.ExpectExec(`PRAGMA key`).
			WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec(`SELECT count\(\*\) FROM sqlite_master`).
			WillReturnError(errors.New("verification failed"))
		mock.ExpectClose()

		if _, err := Open("membrane.db", "secret"); err == nil || !strings.Contains(err.Error(), "encryption key verification failed") {
			t.Fatalf("Open verification error = %v, want verification error", err)
		}
	})

	t.Run("success with escaped encryption key", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		defer func() {
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("unmet sql expectations: %v", err)
			}
		}()
		sqlOpen = func(string, string) (*sql.DB, error) {
			return db, nil
		}

		mock.ExpectExec(`PRAGMA key = 'a''b'`).
			WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec(`SELECT count\(\*\) FROM sqlite_master`).
			WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec(`CREATE TABLE IF NOT EXISTS memory_records`).
			WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectClose()

		store, err := Open("membrane.db", "a'b")
		if err != nil {
			t.Fatalf("Open injected success: %v", err)
		}
		if err := store.Close(); err != nil {
			t.Fatalf("Close injected store: %v", err)
		}
	})
}

func TestDSNWithOptionsPreservesExistingQuery(t *testing.T) {
	if got := dsnWithOptions("membrane.db"); got != "membrane.db?_foreign_keys=on&_journal_mode=WAL&_busy_timeout=5000" {
		t.Fatalf("dsnWithOptions plain = %q", got)
	}
	if got := dsnWithOptions("file:memdb1?mode=memory&cache=shared"); got != "file:memdb1?mode=memory&cache=shared&_foreign_keys=on&_journal_mode=WAL&_busy_timeout=5000" {
		t.Fatalf("dsnWithOptions query = %q", got)
	}

	store, err := Open("file:membrane-query-test?mode=memory&cache=shared", "")
	if err != nil {
		t.Fatalf("Open DSN with existing query: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close query DSN store: %v", err)
	}
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

type staticResult int64

func (r staticResult) LastInsertId() (int64, error) {
	return 0, nil
}

func (r staticResult) RowsAffected() (int64, error) {
	return int64(r), nil
}

type failingExecQueryable struct {
	failAt       int
	rowsAffected int64
	calls        int
	err          error
}

func (q *failingExecQueryable) ExecContext(context.Context, string, ...any) (sql.Result, error) {
	q.calls++
	if q.calls == q.failAt {
		if q.err != nil {
			return nil, q.err
		}
		return nil, errors.New("injected exec failure")
	}
	rowsAffected := q.rowsAffected
	if rowsAffected == 0 {
		rowsAffected = 1
	}
	return staticResult(rowsAffected), nil
}

func (*failingExecQueryable) QueryContext(context.Context, string, ...any) (*sql.Rows, error) {
	return nil, errors.New("unexpected query")
}

func (*failingExecQueryable) QueryRowContext(context.Context, string, ...any) *sql.Row {
	return nil
}

func TestCreateRecordDirectErrorBranches(t *testing.T) {
	ctx := context.Background()
	if err := createRecord(ctx, &failingExecQueryable{}, &schema.MemoryRecord{}); err == nil {
		t.Fatalf("createRecord invalid record error = nil")
	}

	rec := newSemanticRecord("direct-create-generic-insert")
	if err := createRecord(ctx, &failingExecQueryable{failAt: 1, err: errors.New("insert failed")}, rec); err == nil || !strings.Contains(err.Error(), "insert memory_records") {
		t.Fatalf("createRecord generic insert error = %v, want memory_records error", err)
	}

	rec = newSemanticRecord("direct-create-default-policy")
	rec.Lifecycle.DeletionPolicy = ""
	if err := createRecord(ctx, &failingExecQueryable{failAt: 100}, rec); err != nil {
		t.Fatalf("createRecord default deletion policy: %v", err)
	}

	rec = newSemanticRecord("direct-create-bad-payload")
	rec.Payload = &schema.SemanticPayload{Kind: "semantic", Subject: "x", Predicate: "is", Object: make(chan int), Validity: schema.Validity{Mode: schema.ValidityModeGlobal}}
	if err := createRecord(ctx, &failingExecQueryable{failAt: 100}, rec); err == nil || !strings.Contains(err.Error(), "marshal payload") {
		t.Fatalf("createRecord payload marshal error = %v, want marshal payload", err)
	}

	rec = newSemanticRecord("direct-create-bad-interpretation")
	rec.Interpretation = &schema.Interpretation{ExtractionConfidence: math.NaN()}
	if err := createRecord(ctx, &failingExecQueryable{failAt: 100}, rec); err == nil || !strings.Contains(err.Error(), "marshal interpretation") {
		t.Fatalf("createRecord interpretation marshal error = %v, want marshal interpretation", err)
	}
}

func TestUpdateRecordDirectErrorBranches(t *testing.T) {
	ctx := context.Background()
	if err := updateRecord(ctx, &failingExecQueryable{}, &schema.MemoryRecord{}); err == nil {
		t.Fatalf("updateRecord invalid record error = nil")
	}

	rec := newSemanticRecord("direct-update-generic")
	if err := updateRecord(ctx, &failingExecQueryable{failAt: 1, err: errors.New("update failed")}, rec); err == nil || !strings.Contains(err.Error(), "update memory_records") {
		t.Fatalf("updateRecord memory update error = %v, want wrapped update error", err)
	}

	rec = newSemanticRecord("direct-update-defaults")
	rec.Lifecycle.Pinned = true
	rec.Lifecycle.DeletionPolicy = ""
	if err := updateRecord(ctx, &failingExecQueryable{failAt: 100}, rec); err != nil {
		t.Fatalf("updateRecord pinned/default deletion policy: %v", err)
	}

	rec = newSemanticRecord("direct-update-bad-payload")
	rec.Payload = &schema.SemanticPayload{Kind: "semantic", Subject: "x", Predicate: "is", Object: make(chan int), Validity: schema.Validity{Mode: schema.ValidityModeGlobal}}
	if err := updateRecord(ctx, &failingExecQueryable{failAt: 100}, rec); err == nil || !strings.Contains(err.Error(), "marshal payload") {
		t.Fatalf("updateRecord payload marshal error = %v, want marshal payload", err)
	}

	rec = newSemanticRecord("direct-update-delete-interpretation")
	if err := updateRecord(ctx, &failingExecQueryable{failAt: 4, err: errors.New("delete interpretation failed")}, rec); err == nil || !strings.Contains(err.Error(), "delete interpretations") {
		t.Fatalf("updateRecord delete interpretation error = %v, want delete interpretations", err)
	}

	rec = newSemanticRecord("direct-update-bad-interpretation")
	rec.Interpretation = &schema.Interpretation{ExtractionConfidence: math.NaN()}
	if err := updateRecord(ctx, &failingExecQueryable{failAt: 100}, rec); err == nil || !strings.Contains(err.Error(), "marshal interpretation") {
		t.Fatalf("updateRecord interpretation marshal error = %v, want marshal interpretation", err)
	}

	rec = newSemanticRecord("direct-update-insert-interpretation")
	rec.Interpretation = &schema.Interpretation{Summary: "ok"}
	if err := updateRecord(ctx, &failingExecQueryable{failAt: 5, err: errors.New("insert interpretation failed")}, rec); err == nil || !strings.Contains(err.Error(), "insert interpretations") {
		t.Fatalf("updateRecord insert interpretation error = %v, want insert interpretations", err)
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

func TestCreateUpdateAndBeginReturnClosedStoreErrors(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	rec := newSemanticRecord("closed-store")
	if err := store.Close(); err != nil {
		t.Fatalf("Close store: %v", err)
	}

	if _, err := store.Begin(ctx); err == nil || !strings.Contains(err.Error(), "begin tx") {
		t.Fatalf("Begin closed store error = %v, want begin tx error", err)
	}
	if err := store.Create(ctx, rec); err == nil || !strings.Contains(err.Error(), "begin tx") {
		t.Fatalf("Create closed store error = %v, want begin tx error", err)
	}
	if err := store.Update(ctx, rec); err == nil || !strings.Contains(err.Error(), "begin tx") {
		t.Fatalf("Update closed store error = %v, want begin tx error", err)
	}

	if err := store.Create(ctx, &schema.MemoryRecord{}); err == nil {
		t.Fatalf("Create invalid record error = nil, want validation error")
	}
	if err := store.Update(ctx, &schema.MemoryRecord{}); err == nil {
		t.Fatalf("Update invalid record error = nil, want validation error")
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

func TestCreateUpdatePreservesInterpretationAndRelations(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	entity := schema.NewMemoryRecord("entity-001", schema.MemoryTypeEntity, schema.SensitivityLow, &schema.EntityPayload{
		Kind:          "entity",
		CanonicalName: "Orchid",
		PrimaryType:   schema.EntityTypeProject,
		Types:         []string{schema.EntityTypeProject},
		Aliases:       []schema.EntityAlias{{Value: "orchid"}},
		Summary:       "Orchid entity",
	})
	if err := store.Create(ctx, entity); err != nil {
		t.Fatalf("Create entity: %v", err)
	}

	rec := newSemanticRecord("interp-001")
	rec.Interpretation = &schema.Interpretation{
		Status:               schema.InterpretationStatusTentative,
		Summary:              "Potential Orchid mention",
		ProposedType:         schema.MemoryTypeSemantic,
		TopicalLabels:        []string{"deploy"},
		ExtractionConfidence: 0.6,
		Mentions: []schema.Mention{{
			Surface:    "Orchid",
			EntityKind: schema.EntityKindProject,
			Confidence: 0.6,
			Aliases:    []string{"orchid"},
		}},
	}
	if err := store.Create(ctx, rec); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := store.AddRelation(ctx, rec.ID, schema.Relation{
		TargetID:  "entity-001",
		Predicate: "mentions_entity",
		Weight:    1.0,
		CreatedAt: rec.CreatedAt,
	}); err != nil {
		t.Fatalf("AddRelation: %v", err)
	}

	rec.Interpretation.Status = schema.InterpretationStatusResolved
	rec.Interpretation.Mentions[0].CanonicalEntityID = "entity-001"
	rec.Relations = []schema.Relation{{
		TargetID:  "entity-001",
		Predicate: "mentions_entity",
		Weight:    1.0,
		CreatedAt: rec.CreatedAt,
	}}
	if err := store.Update(ctx, rec); err != nil {
		t.Fatalf("Update: %v", err)
	}

	got, err := store.Get(ctx, rec.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Interpretation == nil {
		t.Fatalf("Interpretation = nil, want interpretation data")
	}
	if got.Interpretation.Status != schema.InterpretationStatusResolved {
		t.Fatalf("Interpretation.Status = %q, want %q", got.Interpretation.Status, schema.InterpretationStatusResolved)
	}
	if got.Interpretation.Mentions[0].CanonicalEntityID != "entity-001" {
		t.Fatalf("Interpretation canonical_entity_id = %q, want entity-001", got.Interpretation.Mentions[0].CanonicalEntityID)
	}
	rels, err := store.GetRelations(ctx, rec.ID)
	if err != nil {
		t.Fatalf("GetRelations: %v", err)
	}
	if len(rels) != 1 || rels[0].Predicate != "mentions_entity" || rels[0].TargetID != "entity-001" {
		t.Fatalf("Relations = %+v, want mentions_entity -> entity-001", rels)
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

func expectSQLiteBaseRecord(mock sqlmock.Sqlmock, id string, now time.Time) {
	formatted := now.Format(time.RFC3339Nano)
	mock.ExpectQuery(`SELECT id, type, sensitivity, confidence, salience, scope, created_at, updated_at FROM memory_records WHERE id = \?`).
		WithArgs(id).
		WillReturnRows(sqlmock.NewRows([]string{"id", "type", "sensitivity", "confidence", "salience", "scope", "created_at", "updated_at"}).
			AddRow(id, string(schema.MemoryTypeSemantic), string(schema.SensitivityLow), 0.9, 0.8, "project", formatted, formatted))
}

func expectSQLiteDecayNoRows(mock sqlmock.Sqlmock, id string) {
	mock.ExpectQuery(`SELECT curve, half_life_seconds, min_salience, max_age_seconds, reinforcement_gain, last_reinforced_at, pinned, deletion_policy FROM decay_profiles WHERE record_id = \?`).
		WithArgs(id).
		WillReturnError(sql.ErrNoRows)
}

func expectSQLitePayloadNoRows(mock sqlmock.Sqlmock, id string) {
	mock.ExpectQuery(`SELECT payload_json FROM payloads WHERE record_id = \?`).
		WithArgs(id).
		WillReturnError(sql.ErrNoRows)
}

func expectSQLiteInterpretationNoRows(mock sqlmock.Sqlmock, id string) {
	mock.ExpectQuery(`SELECT interpretation_json FROM interpretations WHERE record_id = \?`).
		WithArgs(id).
		WillReturnError(sql.ErrNoRows)
}

func expectSQLiteTagsEmpty(mock sqlmock.Sqlmock, id string) {
	mock.ExpectQuery(`SELECT tag FROM tags WHERE record_id = \?`).
		WithArgs(id).
		WillReturnRows(sqlmock.NewRows([]string{"tag"}))
}

func expectSQLiteProvenanceEmpty(mock sqlmock.Sqlmock, id string) {
	mock.ExpectQuery(`SELECT kind, ref, hash, created_by, timestamp FROM provenance_sources WHERE record_id = \? ORDER BY id`).
		WithArgs(id).
		WillReturnRows(sqlmock.NewRows([]string{"kind", "ref", "hash", "created_by", "timestamp"}))
}

func expectSQLiteRelationsEmpty(mock sqlmock.Sqlmock, id string) {
	mock.ExpectQuery(`SELECT predicate, target_id, weight, created_at FROM relations WHERE source_id = \? ORDER BY id`).
		WithArgs(id).
		WillReturnRows(sqlmock.NewRows([]string{"predicate", "target_id", "weight", "created_at"}))
}

func TestGetRecordHydrationErrors(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 5, 1, 15, 0, 0, 0, time.UTC)

	t.Run("base query", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		t.Cleanup(func() {
			_ = db.Close()
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("unmet sql expectations: %v", err)
			}
		})
		mock.ExpectQuery(`SELECT id, type, sensitivity, confidence, salience, scope, created_at, updated_at FROM memory_records WHERE id = \?`).
			WithArgs("rec-1").
			WillReturnError(errors.New("base failed"))
		if _, err := getRecord(ctx, db, "rec-1"); err == nil || !strings.Contains(err.Error(), "query memory_records") {
			t.Fatalf("getRecord base error = %v, want wrapped base query error", err)
		}
	})

	t.Run("decay query", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		t.Cleanup(func() {
			_ = db.Close()
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("unmet sql expectations: %v", err)
			}
		})
		expectSQLiteBaseRecord(mock, "rec-1", now)
		mock.ExpectQuery(`SELECT curve, half_life_seconds, min_salience, max_age_seconds, reinforcement_gain, last_reinforced_at, pinned, deletion_policy FROM decay_profiles WHERE record_id = \?`).
			WithArgs("rec-1").
			WillReturnError(errors.New("decay failed"))
		if _, err := getRecord(ctx, db, "rec-1"); err == nil || !strings.Contains(err.Error(), "query decay_profiles") {
			t.Fatalf("getRecord decay error = %v, want wrapped decay query error", err)
		}
	})

	t.Run("payload query", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		t.Cleanup(func() {
			_ = db.Close()
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("unmet sql expectations: %v", err)
			}
		})
		expectSQLiteBaseRecord(mock, "rec-1", now)
		expectSQLiteDecayNoRows(mock, "rec-1")
		mock.ExpectQuery(`SELECT payload_json FROM payloads WHERE record_id = \?`).
			WithArgs("rec-1").
			WillReturnError(errors.New("payload failed"))
		if _, err := getRecord(ctx, db, "rec-1"); err == nil || !strings.Contains(err.Error(), "query payloads") {
			t.Fatalf("getRecord payload query error = %v, want wrapped payload query error", err)
		}
	})

	t.Run("payload unmarshal", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		t.Cleanup(func() {
			_ = db.Close()
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("unmet sql expectations: %v", err)
			}
		})
		expectSQLiteBaseRecord(mock, "rec-1", now)
		expectSQLiteDecayNoRows(mock, "rec-1")
		mock.ExpectQuery(`SELECT payload_json FROM payloads WHERE record_id = \?`).
			WithArgs("rec-1").
			WillReturnRows(sqlmock.NewRows([]string{"payload_json"}).AddRow(`{"kind":"semantic"`))
		if _, err := getRecord(ctx, db, "rec-1"); err == nil || !strings.Contains(err.Error(), "unmarshal payload") {
			t.Fatalf("getRecord payload unmarshal error = %v, want unmarshal payload error", err)
		}
	})

	t.Run("interpretation unmarshal", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		t.Cleanup(func() {
			_ = db.Close()
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("unmet sql expectations: %v", err)
			}
		})
		expectSQLiteBaseRecord(mock, "rec-1", now)
		expectSQLiteDecayNoRows(mock, "rec-1")
		expectSQLitePayloadNoRows(mock, "rec-1")
		mock.ExpectQuery(`SELECT interpretation_json FROM interpretations WHERE record_id = \?`).
			WithArgs("rec-1").
			WillReturnRows(sqlmock.NewRows([]string{"interpretation_json"}).AddRow(`{"status":"resolved"`))
		if _, err := getRecord(ctx, db, "rec-1"); err == nil || !strings.Contains(err.Error(), "unmarshal interpretation") {
			t.Fatalf("getRecord interpretation unmarshal error = %v, want unmarshal interpretation error", err)
		}
	})

	t.Run("interpretation query", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		t.Cleanup(func() {
			_ = db.Close()
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("unmet sql expectations: %v", err)
			}
		})
		expectSQLiteBaseRecord(mock, "rec-1", now)
		expectSQLiteDecayNoRows(mock, "rec-1")
		expectSQLitePayloadNoRows(mock, "rec-1")
		mock.ExpectQuery(`SELECT interpretation_json FROM interpretations WHERE record_id = \?`).
			WithArgs("rec-1").
			WillReturnError(errors.New("interpretation failed"))
		if _, err := getRecord(ctx, db, "rec-1"); err == nil || !strings.Contains(err.Error(), "query interpretations") {
			t.Fatalf("getRecord interpretation query error = %v, want wrapped interpretation query error", err)
		}
	})

	t.Run("tag query", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		t.Cleanup(func() {
			_ = db.Close()
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("unmet sql expectations: %v", err)
			}
		})
		expectSQLiteBaseRecord(mock, "rec-1", now)
		expectSQLiteDecayNoRows(mock, "rec-1")
		expectSQLitePayloadNoRows(mock, "rec-1")
		expectSQLiteInterpretationNoRows(mock, "rec-1")
		mock.ExpectQuery(`SELECT tag FROM tags WHERE record_id = \?`).
			WithArgs("rec-1").
			WillReturnError(errors.New("tag failed"))
		if _, err := getRecord(ctx, db, "rec-1"); err == nil || !strings.Contains(err.Error(), "query tags") {
			t.Fatalf("getRecord tag query error = %v, want wrapped tag query error", err)
		}
	})

	t.Run("tag scan", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		t.Cleanup(func() {
			_ = db.Close()
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("unmet sql expectations: %v", err)
			}
		})
		expectSQLiteBaseRecord(mock, "rec-1", now)
		expectSQLiteDecayNoRows(mock, "rec-1")
		expectSQLitePayloadNoRows(mock, "rec-1")
		expectSQLiteInterpretationNoRows(mock, "rec-1")
		mock.ExpectQuery(`SELECT tag FROM tags WHERE record_id = \?`).
			WithArgs("rec-1").
			WillReturnRows(sqlmock.NewRows([]string{"tag", "extra"}).AddRow("tag", "extra"))
		if _, err := getRecord(ctx, db, "rec-1"); err == nil || !strings.Contains(err.Error(), "scan tag") {
			t.Fatalf("getRecord tag scan error = %v, want scan tag error", err)
		}
	})

	t.Run("tag iteration", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		t.Cleanup(func() {
			_ = db.Close()
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("unmet sql expectations: %v", err)
			}
		})
		expectSQLiteBaseRecord(mock, "rec-1", now)
		expectSQLiteDecayNoRows(mock, "rec-1")
		expectSQLitePayloadNoRows(mock, "rec-1")
		expectSQLiteInterpretationNoRows(mock, "rec-1")
		mock.ExpectQuery(`SELECT tag FROM tags WHERE record_id = \?`).
			WithArgs("rec-1").
			WillReturnRows(sqlmock.NewRows([]string{"tag"}).
				AddRow("tag").
				RowError(0, errors.New("tag rows failed")))
		if _, err := getRecord(ctx, db, "rec-1"); err == nil || !strings.Contains(err.Error(), "iterate tags") {
			t.Fatalf("getRecord tag iteration error = %v, want iterate tags error", err)
		}
	})

	t.Run("provenance query", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		t.Cleanup(func() {
			_ = db.Close()
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("unmet sql expectations: %v", err)
			}
		})
		expectSQLiteBaseRecord(mock, "rec-1", now)
		expectSQLiteDecayNoRows(mock, "rec-1")
		expectSQLitePayloadNoRows(mock, "rec-1")
		expectSQLiteInterpretationNoRows(mock, "rec-1")
		expectSQLiteTagsEmpty(mock, "rec-1")
		mock.ExpectQuery(`SELECT kind, ref, hash, created_by, timestamp FROM provenance_sources WHERE record_id = \? ORDER BY id`).
			WithArgs("rec-1").
			WillReturnError(errors.New("provenance failed"))
		if _, err := getRecord(ctx, db, "rec-1"); err == nil || !strings.Contains(err.Error(), "query provenance_sources") {
			t.Fatalf("getRecord provenance query error = %v, want wrapped provenance query error", err)
		}
	})

	t.Run("provenance scan", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		t.Cleanup(func() {
			_ = db.Close()
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("unmet sql expectations: %v", err)
			}
		})
		expectSQLiteBaseRecord(mock, "rec-1", now)
		expectSQLiteDecayNoRows(mock, "rec-1")
		expectSQLitePayloadNoRows(mock, "rec-1")
		expectSQLiteInterpretationNoRows(mock, "rec-1")
		expectSQLiteTagsEmpty(mock, "rec-1")
		mock.ExpectQuery(`SELECT kind, ref, hash, created_by, timestamp FROM provenance_sources WHERE record_id = \? ORDER BY id`).
			WithArgs("rec-1").
			WillReturnRows(sqlmock.NewRows([]string{"kind"}).AddRow("input"))
		if _, err := getRecord(ctx, db, "rec-1"); err == nil || !strings.Contains(err.Error(), "scan provenance_source") {
			t.Fatalf("getRecord provenance scan error = %v, want scan provenance_source error", err)
		}
	})

	t.Run("provenance iteration", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		t.Cleanup(func() {
			_ = db.Close()
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("unmet sql expectations: %v", err)
			}
		})
		expectSQLiteBaseRecord(mock, "rec-1", now)
		expectSQLiteDecayNoRows(mock, "rec-1")
		expectSQLitePayloadNoRows(mock, "rec-1")
		expectSQLiteInterpretationNoRows(mock, "rec-1")
		expectSQLiteTagsEmpty(mock, "rec-1")
		mock.ExpectQuery(`SELECT kind, ref, hash, created_by, timestamp FROM provenance_sources WHERE record_id = \? ORDER BY id`).
			WithArgs("rec-1").
			WillReturnRows(sqlmock.NewRows([]string{"kind", "ref", "hash", "created_by", "timestamp"}).
				AddRow("input", "ref", nil, nil, now.Format(time.RFC3339Nano)).
				RowError(0, errors.New("provenance rows failed")))
		if _, err := getRecord(ctx, db, "rec-1"); err == nil || !strings.Contains(err.Error(), "iterate provenance_sources") {
			t.Fatalf("getRecord provenance iteration error = %v, want iterate provenance_sources error", err)
		}
	})

	t.Run("relations", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		t.Cleanup(func() {
			_ = db.Close()
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("unmet sql expectations: %v", err)
			}
		})
		expectSQLiteBaseRecord(mock, "rec-1", now)
		expectSQLiteDecayNoRows(mock, "rec-1")
		expectSQLitePayloadNoRows(mock, "rec-1")
		expectSQLiteInterpretationNoRows(mock, "rec-1")
		expectSQLiteTagsEmpty(mock, "rec-1")
		expectSQLiteProvenanceEmpty(mock, "rec-1")
		mock.ExpectQuery(`SELECT predicate, target_id, weight, created_at FROM relations WHERE source_id = \? ORDER BY id`).
			WithArgs("rec-1").
			WillReturnError(errors.New("relations failed"))
		if _, err := getRecord(ctx, db, "rec-1"); err == nil || !strings.Contains(err.Error(), "query relations") {
			t.Fatalf("getRecord relations error = %v, want query relations error", err)
		}
	})

	t.Run("audit query", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		t.Cleanup(func() {
			_ = db.Close()
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("unmet sql expectations: %v", err)
			}
		})
		expectSQLiteBaseRecord(mock, "rec-1", now)
		expectSQLiteDecayNoRows(mock, "rec-1")
		expectSQLitePayloadNoRows(mock, "rec-1")
		expectSQLiteInterpretationNoRows(mock, "rec-1")
		expectSQLiteTagsEmpty(mock, "rec-1")
		expectSQLiteProvenanceEmpty(mock, "rec-1")
		expectSQLiteRelationsEmpty(mock, "rec-1")
		mock.ExpectQuery(`SELECT action, actor, timestamp, rationale FROM audit_log WHERE record_id = \? ORDER BY id`).
			WithArgs("rec-1").
			WillReturnError(errors.New("audit failed"))
		if _, err := getRecord(ctx, db, "rec-1"); err == nil || !strings.Contains(err.Error(), "query audit_log") {
			t.Fatalf("getRecord audit query error = %v, want wrapped audit query error", err)
		}
	})

	t.Run("audit scan", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		t.Cleanup(func() {
			_ = db.Close()
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("unmet sql expectations: %v", err)
			}
		})
		expectSQLiteBaseRecord(mock, "rec-1", now)
		expectSQLiteDecayNoRows(mock, "rec-1")
		expectSQLitePayloadNoRows(mock, "rec-1")
		expectSQLiteInterpretationNoRows(mock, "rec-1")
		expectSQLiteTagsEmpty(mock, "rec-1")
		expectSQLiteProvenanceEmpty(mock, "rec-1")
		expectSQLiteRelationsEmpty(mock, "rec-1")
		mock.ExpectQuery(`SELECT action, actor, timestamp, rationale FROM audit_log WHERE record_id = \? ORDER BY id`).
			WithArgs("rec-1").
			WillReturnRows(sqlmock.NewRows([]string{"action"}).AddRow("create"))
		if _, err := getRecord(ctx, db, "rec-1"); err == nil || !strings.Contains(err.Error(), "scan audit_log") {
			t.Fatalf("getRecord audit scan error = %v, want scan audit_log error", err)
		}
	})

	t.Run("audit iteration", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		t.Cleanup(func() {
			_ = db.Close()
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("unmet sql expectations: %v", err)
			}
		})
		expectSQLiteBaseRecord(mock, "rec-1", now)
		expectSQLiteDecayNoRows(mock, "rec-1")
		expectSQLitePayloadNoRows(mock, "rec-1")
		expectSQLiteInterpretationNoRows(mock, "rec-1")
		expectSQLiteTagsEmpty(mock, "rec-1")
		expectSQLiteProvenanceEmpty(mock, "rec-1")
		expectSQLiteRelationsEmpty(mock, "rec-1")
		mock.ExpectQuery(`SELECT action, actor, timestamp, rationale FROM audit_log WHERE record_id = \? ORDER BY id`).
			WithArgs("rec-1").
			WillReturnRows(sqlmock.NewRows([]string{"action", "actor", "timestamp", "rationale"}).
				AddRow("create", "tester", now.Format(time.RFC3339Nano), "created").
				RowError(0, errors.New("audit rows failed")))
		if _, err := getRecord(ctx, db, "rec-1"); err == nil || !strings.Contains(err.Error(), "iterate audit_log") {
			t.Fatalf("getRecord audit iteration error = %v, want iterate audit_log error", err)
		}
	})
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

	missing := newSemanticRecord("update-missing")
	if err := store.Update(ctx, missing); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("Update missing err = %v, want ErrNotFound", err)
	}
}

func TestCreateRollsBackOnRelationFailure(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	rec := newSemanticRecord("create-rollback")
	rec.Relations = []schema.Relation{{
		Predicate: "depends_on",
		TargetID:  "missing-target",
		Weight:    0.5,
		CreatedAt: rec.CreatedAt,
	}}

	if err := store.Create(ctx, rec); err == nil {
		t.Fatalf("Create with missing relation target error = nil, want error")
	}
	if _, err := store.Get(ctx, rec.ID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("Get after failed Create err = %v, want ErrNotFound", err)
	}
}

func TestUpdateRollsBackOnRelationFailure(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	rec := newSemanticRecord("update-rollback")
	if err := store.Create(ctx, rec); err != nil {
		t.Fatalf("Create: %v", err)
	}

	rec.Salience = 0.2
	rec.Tags = []string{"should-not-persist"}
	rec.Relations = []schema.Relation{{
		Predicate: "depends_on",
		TargetID:  "missing-target",
		Weight:    0.5,
		CreatedAt: rec.CreatedAt,
	}}
	if err := store.Update(ctx, rec); err == nil {
		t.Fatalf("Update with missing relation target error = nil, want error")
	}

	got, err := store.Get(ctx, rec.ID)
	if err != nil {
		t.Fatalf("Get after failed Update: %v", err)
	}
	if got.Salience != 0.8 {
		t.Fatalf("Salience after failed Update = %v, want original 0.8", got.Salience)
	}
	if strings.Join(got.Tags, ",") != "semantic,test" && strings.Join(got.Tags, ",") != "test,semantic" {
		t.Fatalf("Tags after failed Update = %#v, want original tags", got.Tags)
	}
	if len(got.Relations) != 0 {
		t.Fatalf("Relations after failed Update = %#v, want none", got.Relations)
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

func TestListLargeBatchHydratesInterpretationsAndRelations(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)

	ids := []string{"batch-rich-0", "batch-rich-1", "batch-rich-2", "batch-rich-3"}
	for i, id := range ids {
		rec := newSemanticRecord(id)
		rec.Scope = "rich-batch"
		rec.Salience = 0.9 - float64(i)*0.1
		rec.CreatedAt = now.Add(time.Duration(i) * time.Minute)
		rec.UpdatedAt = rec.CreatedAt
		rec.Lifecycle.LastReinforcedAt = rec.CreatedAt
		if i == 0 {
			rec.Interpretation = &schema.Interpretation{
				Status:        schema.InterpretationStatusResolved,
				Summary:       "rich batch record",
				TopicalLabels: []string{"batch"},
			}
		}
		if err := store.Create(ctx, rec); err != nil {
			t.Fatalf("Create %s: %v", id, err)
		}
	}

	if err := store.AddRelation(ctx, ids[0], schema.Relation{
		Predicate: "supports",
		TargetID:  ids[1],
		Weight:    0.7,
		CreatedAt: now,
	}); err != nil {
		t.Fatalf("AddRelation: %v", err)
	}

	records, err := store.List(ctx, storage.ListOptions{Scope: "rich-batch", Limit: 10})
	if err != nil {
		t.Fatalf("List rich batch: %v", err)
	}
	if len(records) != len(ids) {
		t.Fatalf("List rich batch len = %d, want %d", len(records), len(ids))
	}
	first := records[0]
	if first.ID != ids[0] {
		t.Fatalf("records[0].ID = %q, want %q", first.ID, ids[0])
	}
	if first.Interpretation == nil || first.Interpretation.Status != schema.InterpretationStatusResolved {
		t.Fatalf("Interpretation = %+v, want resolved interpretation", first.Interpretation)
	}
	if _, ok := first.Payload.(*schema.SemanticPayload); !ok {
		t.Fatalf("Payload type = %T, want semantic", first.Payload)
	}
	if len(first.Tags) == 0 || len(first.Provenance.Sources) == 0 || len(first.AuditLog) == 0 {
		t.Fatalf("hydrated metadata = tags:%v provenance:%v audit:%v, want non-empty", first.Tags, first.Provenance.Sources, first.AuditLog)
	}
	if len(first.Relations) != 1 || first.Relations[0].TargetID != ids[1] || first.Relations[0].Weight != 0.7 {
		t.Fatalf("Relations = %+v, want supports -> %s", first.Relations, ids[1])
	}
}

func expectSQLiteBatchBaseRows(mock sqlmock.Sqlmock, now time.Time) {
	ts := now.Format(time.RFC3339Nano)
	mock.ExpectQuery(`SELECT id, type, sensitivity, confidence, salience, scope, created_at, updated_at FROM memory_records WHERE id IN`).
		WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
		WillReturnRows(sqlmock.NewRows([]string{"id", "type", "sensitivity", "confidence", "salience", "scope", "created_at", "updated_at"}).
			AddRow("batch-1", string(schema.MemoryTypeSemantic), string(schema.SensitivityLow), 0.9, 0.8, "project", ts, ts).
			AddRow("batch-2", string(schema.MemoryTypeSemantic), string(schema.SensitivityLow), 0.8, 0.7, "project", ts, ts).
			AddRow("batch-3", string(schema.MemoryTypeSemantic), string(schema.SensitivityLow), 0.7, 0.6, "project", ts, ts).
			AddRow("batch-4", string(schema.MemoryTypeSemantic), string(schema.SensitivityLow), 0.6, 0.5, "project", ts, ts))
}

func expectSQLiteBatchEmptyDecayRows(mock sqlmock.Sqlmock) {
	mock.ExpectQuery(`SELECT record_id, curve, half_life_seconds, min_salience, max_age_seconds, reinforcement_gain, last_reinforced_at, pinned, deletion_policy FROM decay_profiles WHERE record_id IN`).
		WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
		WillReturnRows(sqlmock.NewRows([]string{"record_id", "curve", "half_life_seconds", "min_salience", "max_age_seconds", "reinforcement_gain", "last_reinforced_at", "pinned", "deletion_policy"}))
}

func expectSQLiteBatchEmptyPayloadRows(mock sqlmock.Sqlmock) {
	mock.ExpectQuery(`SELECT record_id, payload_json FROM payloads WHERE record_id IN`).
		WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
		WillReturnRows(sqlmock.NewRows([]string{"record_id", "payload_json"}))
}

func expectSQLiteBatchEmptyInterpretationRows(mock sqlmock.Sqlmock) {
	mock.ExpectQuery(`SELECT record_id, interpretation_json FROM interpretations WHERE record_id IN`).
		WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
		WillReturnRows(sqlmock.NewRows([]string{"record_id", "interpretation_json"}))
}

func expectSQLiteBatchEmptyTagRows(mock sqlmock.Sqlmock) {
	mock.ExpectQuery(`SELECT record_id, tag FROM tags WHERE record_id IN`).
		WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
		WillReturnRows(sqlmock.NewRows([]string{"record_id", "tag"}))
}

func expectSQLiteBatchEmptyProvenanceRows(mock sqlmock.Sqlmock) {
	mock.ExpectQuery(`SELECT record_id, kind, ref, hash, created_by, timestamp FROM provenance_sources WHERE record_id IN`).
		WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
		WillReturnRows(sqlmock.NewRows([]string{"record_id", "kind", "ref", "hash", "created_by", "timestamp"}))
}

func expectSQLiteBatchEmptyRelationRows(mock sqlmock.Sqlmock) {
	mock.ExpectQuery(`SELECT source_id, predicate, target_id, weight, created_at FROM relations WHERE source_id IN`).
		WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
		WillReturnRows(sqlmock.NewRows([]string{"source_id", "predicate", "target_id", "weight", "created_at"}))
}

func expectSQLiteBatchEmptyAuditRows(mock sqlmock.Sqlmock) {
	mock.ExpectQuery(`SELECT record_id, action, actor, timestamp, rationale FROM audit_log WHERE record_id IN`).
		WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
		WillReturnRows(sqlmock.NewRows([]string{"record_id", "action", "actor", "timestamp", "rationale"}))
}

func TestGetRecordsBatchErrorBranches(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 5, 1, 16, 30, 0, 0, time.UTC)
	ids := []string{"batch-1", "batch-2", "batch-3", "batch-4"}

	newMockDB := func(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
		t.Helper()
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		t.Cleanup(func() {
			_ = db.Close()
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("unmet sql expectations: %v", err)
			}
		})
		return db, mock
	}

	t.Run("empty batch", func(t *testing.T) {
		db, _ := newMockDB(t)
		records, err := getRecordsBatch(ctx, db, nil)
		if err != nil || len(records) != 0 {
			t.Fatalf("getRecordsBatch empty = %+v, %v; want empty nil", records, err)
		}
	})

	t.Run("small batch propagates get error", func(t *testing.T) {
		db, mock := newMockDB(t)
		mock.ExpectQuery(`SELECT id, type, sensitivity, confidence, salience, scope, created_at, updated_at FROM memory_records WHERE id = \?`).
			WithArgs("missing").
			WillReturnError(sql.ErrNoRows)

		if _, err := getRecordsBatch(ctx, db, []string{"missing"}); !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("getRecordsBatch small error = %v, want ErrNotFound", err)
		}
	})

	t.Run("base query error", func(t *testing.T) {
		db, mock := newMockDB(t)
		mock.ExpectQuery(`SELECT id, type, sensitivity, confidence, salience, scope, created_at, updated_at FROM memory_records WHERE id IN`).
			WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
			WillReturnError(errors.New("base failed"))

		if _, err := getRecordsBatch(ctx, db, ids); err == nil || !strings.Contains(err.Error(), "batch query memory_records") {
			t.Fatalf("getRecordsBatch base query error = %v, want wrapped base query error", err)
		}
	})

	t.Run("base scan error", func(t *testing.T) {
		db, mock := newMockDB(t)
		ts := now.Format(time.RFC3339Nano)
		mock.ExpectQuery(`SELECT id, type, sensitivity, confidence, salience, scope, created_at, updated_at FROM memory_records WHERE id IN`).
			WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
			WillReturnRows(sqlmock.NewRows([]string{"id", "type", "sensitivity", "confidence", "salience", "scope", "created_at", "updated_at"}).
				AddRow("batch-1", string(schema.MemoryTypeSemantic), string(schema.SensitivityLow), "bad-confidence", 0.8, "project", ts, ts))

		if _, err := getRecordsBatch(ctx, db, ids); err == nil || !strings.Contains(err.Error(), "batch scan memory_records") {
			t.Fatalf("getRecordsBatch base scan error = %v, want wrapped base scan error", err)
		}
	})

	t.Run("base iterate error", func(t *testing.T) {
		db, mock := newMockDB(t)
		ts := now.Format(time.RFC3339Nano)
		mock.ExpectQuery(`SELECT id, type, sensitivity, confidence, salience, scope, created_at, updated_at FROM memory_records WHERE id IN`).
			WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
			WillReturnRows(sqlmock.NewRows([]string{"id", "type", "sensitivity", "confidence", "salience", "scope", "created_at", "updated_at"}).
				AddRow("batch-1", string(schema.MemoryTypeSemantic), string(schema.SensitivityLow), 0.9, 0.8, "project", ts, ts).
				RowError(0, errors.New("base rows failed")))

		if _, err := getRecordsBatch(ctx, db, ids); err == nil || !strings.Contains(err.Error(), "batch iterate memory_records") {
			t.Fatalf("getRecordsBatch base iterate error = %v, want wrapped base iteration error", err)
		}
	})

	t.Run("decay query error", func(t *testing.T) {
		db, mock := newMockDB(t)
		expectSQLiteBatchBaseRows(mock, now)
		mock.ExpectQuery(`SELECT record_id, curve, half_life_seconds, min_salience, max_age_seconds, reinforcement_gain, last_reinforced_at, pinned, deletion_policy FROM decay_profiles WHERE record_id IN`).
			WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
			WillReturnError(errors.New("decay failed"))

		if _, err := getRecordsBatch(ctx, db, ids); err == nil || !strings.Contains(err.Error(), "batch query decay_profiles") {
			t.Fatalf("getRecordsBatch decay query error = %v, want wrapped decay query error", err)
		}
	})

	t.Run("decay scan error", func(t *testing.T) {
		db, mock := newMockDB(t)
		expectSQLiteBatchBaseRows(mock, now)
		mock.ExpectQuery(`SELECT record_id, curve, half_life_seconds, min_salience, max_age_seconds, reinforcement_gain, last_reinforced_at, pinned, deletion_policy FROM decay_profiles WHERE record_id IN`).
			WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
			WillReturnRows(sqlmock.NewRows([]string{"record_id"}).AddRow("batch-1"))

		if _, err := getRecordsBatch(ctx, db, ids); err == nil || !strings.Contains(err.Error(), "batch scan decay_profiles") {
			t.Fatalf("getRecordsBatch decay scan error = %v, want wrapped decay scan error", err)
		}
	})

	t.Run("decay iterate error", func(t *testing.T) {
		db, mock := newMockDB(t)
		expectSQLiteBatchBaseRows(mock, now)
		ts := now.Format(time.RFC3339Nano)
		mock.ExpectQuery(`SELECT record_id, curve, half_life_seconds, min_salience, max_age_seconds, reinforcement_gain, last_reinforced_at, pinned, deletion_policy FROM decay_profiles WHERE record_id IN`).
			WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
			WillReturnRows(sqlmock.NewRows([]string{"record_id", "curve", "half_life_seconds", "min_salience", "max_age_seconds", "reinforcement_gain", "last_reinforced_at", "pinned", "deletion_policy"}).
				AddRow("batch-1", string(schema.DecayCurveExponential), int64(3600), 0.1, int64(7200), 0.2, ts, 1, string(schema.DeletionPolicyAutoPrune)).
				RowError(0, errors.New("decay rows failed")))

		if _, err := getRecordsBatch(ctx, db, ids); err == nil || !strings.Contains(err.Error(), "batch iterate decay_profiles") {
			t.Fatalf("getRecordsBatch decay iterate error = %v, want wrapped decay iterate error", err)
		}
	})

	t.Run("payload query error", func(t *testing.T) {
		db, mock := newMockDB(t)
		expectSQLiteBatchBaseRows(mock, now)
		expectSQLiteBatchEmptyDecayRows(mock)
		mock.ExpectQuery(`SELECT record_id, payload_json FROM payloads WHERE record_id IN`).
			WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
			WillReturnError(errors.New("payload failed"))

		if _, err := getRecordsBatch(ctx, db, ids); err == nil || !strings.Contains(err.Error(), "batch query payloads") {
			t.Fatalf("getRecordsBatch payload query error = %v, want wrapped payload query error", err)
		}
	})

	t.Run("payload scan error", func(t *testing.T) {
		db, mock := newMockDB(t)
		expectSQLiteBatchBaseRows(mock, now)
		expectSQLiteBatchEmptyDecayRows(mock)
		mock.ExpectQuery(`SELECT record_id, payload_json FROM payloads WHERE record_id IN`).
			WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
			WillReturnRows(sqlmock.NewRows([]string{"record_id"}).AddRow("batch-1"))

		if _, err := getRecordsBatch(ctx, db, ids); err == nil || !strings.Contains(err.Error(), "batch scan payloads") {
			t.Fatalf("getRecordsBatch payload scan error = %v, want wrapped payload scan error", err)
		}
	})

	t.Run("payload iterate error", func(t *testing.T) {
		db, mock := newMockDB(t)
		expectSQLiteBatchBaseRows(mock, now)
		expectSQLiteBatchEmptyDecayRows(mock)
		mock.ExpectQuery(`SELECT record_id, payload_json FROM payloads WHERE record_id IN`).
			WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
			WillReturnRows(sqlmock.NewRows([]string{"record_id", "payload_json"}).
				AddRow("batch-1", "").
				RowError(0, errors.New("payload rows failed")))

		if _, err := getRecordsBatch(ctx, db, ids); err == nil || !strings.Contains(err.Error(), "batch iterate payloads") {
			t.Fatalf("getRecordsBatch payload iterate error = %v, want wrapped payload iterate error", err)
		}
	})

	t.Run("payload unmarshal error", func(t *testing.T) {
		db, mock := newMockDB(t)
		expectSQLiteBatchBaseRows(mock, now)
		expectSQLiteBatchEmptyDecayRows(mock)
		mock.ExpectQuery(`SELECT record_id, payload_json FROM payloads WHERE record_id IN`).
			WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
			WillReturnRows(sqlmock.NewRows([]string{"record_id", "payload_json"}).
				AddRow("batch-1", `{"kind":"semantic"`))

		if _, err := getRecordsBatch(ctx, db, ids); err == nil || !strings.Contains(err.Error(), "unmarshal payload for batch-1") {
			t.Fatalf("getRecordsBatch payload unmarshal error = %v, want wrapped unmarshal error", err)
		}
	})

	t.Run("interpretation query error", func(t *testing.T) {
		db, mock := newMockDB(t)
		expectSQLiteBatchBaseRows(mock, now)
		expectSQLiteBatchEmptyDecayRows(mock)
		expectSQLiteBatchEmptyPayloadRows(mock)
		mock.ExpectQuery(`SELECT record_id, interpretation_json FROM interpretations WHERE record_id IN`).
			WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
			WillReturnError(errors.New("interpretation failed"))

		if _, err := getRecordsBatch(ctx, db, ids); err == nil || !strings.Contains(err.Error(), "batch query interpretations") {
			t.Fatalf("getRecordsBatch interpretation query error = %v, want wrapped interpretation query error", err)
		}
	})

	t.Run("interpretation scan error", func(t *testing.T) {
		db, mock := newMockDB(t)
		expectSQLiteBatchBaseRows(mock, now)
		expectSQLiteBatchEmptyDecayRows(mock)
		expectSQLiteBatchEmptyPayloadRows(mock)
		mock.ExpectQuery(`SELECT record_id, interpretation_json FROM interpretations WHERE record_id IN`).
			WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
			WillReturnRows(sqlmock.NewRows([]string{"record_id"}).AddRow("batch-1"))

		if _, err := getRecordsBatch(ctx, db, ids); err == nil || !strings.Contains(err.Error(), "batch scan interpretations") {
			t.Fatalf("getRecordsBatch interpretation scan error = %v, want wrapped interpretation scan error", err)
		}
	})

	t.Run("interpretation unmarshal error", func(t *testing.T) {
		db, mock := newMockDB(t)
		expectSQLiteBatchBaseRows(mock, now)
		expectSQLiteBatchEmptyDecayRows(mock)
		expectSQLiteBatchEmptyPayloadRows(mock)
		mock.ExpectQuery(`SELECT record_id, interpretation_json FROM interpretations WHERE record_id IN`).
			WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
			WillReturnRows(sqlmock.NewRows([]string{"record_id", "interpretation_json"}).
				AddRow("batch-1", `{"status":"resolved"`))

		if _, err := getRecordsBatch(ctx, db, ids); err == nil || !strings.Contains(err.Error(), "unmarshal interpretation for batch-1") {
			t.Fatalf("getRecordsBatch interpretation unmarshal error = %v, want wrapped interpretation unmarshal error", err)
		}
	})

	t.Run("interpretation iterate error", func(t *testing.T) {
		db, mock := newMockDB(t)
		expectSQLiteBatchBaseRows(mock, now)
		expectSQLiteBatchEmptyDecayRows(mock)
		expectSQLiteBatchEmptyPayloadRows(mock)
		mock.ExpectQuery(`SELECT record_id, interpretation_json FROM interpretations WHERE record_id IN`).
			WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
			WillReturnRows(sqlmock.NewRows([]string{"record_id", "interpretation_json"}).
				AddRow("batch-1", "").
				RowError(0, errors.New("interpretation rows failed")))

		if _, err := getRecordsBatch(ctx, db, ids); err == nil || !strings.Contains(err.Error(), "batch iterate interpretations") {
			t.Fatalf("getRecordsBatch interpretation iterate error = %v, want wrapped interpretation iterate error", err)
		}
	})

	t.Run("tag query error", func(t *testing.T) {
		db, mock := newMockDB(t)
		expectSQLiteBatchBaseRows(mock, now)
		expectSQLiteBatchEmptyDecayRows(mock)
		expectSQLiteBatchEmptyPayloadRows(mock)
		expectSQLiteBatchEmptyInterpretationRows(mock)
		mock.ExpectQuery(`SELECT record_id, tag FROM tags WHERE record_id IN`).
			WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
			WillReturnError(errors.New("tag failed"))

		if _, err := getRecordsBatch(ctx, db, ids); err == nil || !strings.Contains(err.Error(), "batch query tags") {
			t.Fatalf("getRecordsBatch tag query error = %v, want wrapped tag query error", err)
		}
	})

	t.Run("tag scan error", func(t *testing.T) {
		db, mock := newMockDB(t)
		expectSQLiteBatchBaseRows(mock, now)
		expectSQLiteBatchEmptyDecayRows(mock)
		expectSQLiteBatchEmptyPayloadRows(mock)
		expectSQLiteBatchEmptyInterpretationRows(mock)
		mock.ExpectQuery(`SELECT record_id, tag FROM tags WHERE record_id IN`).
			WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
			WillReturnRows(sqlmock.NewRows([]string{"record_id"}).AddRow("batch-1"))

		if _, err := getRecordsBatch(ctx, db, ids); err == nil || !strings.Contains(err.Error(), "batch scan tags") {
			t.Fatalf("getRecordsBatch tag scan error = %v, want wrapped tag scan error", err)
		}
	})

	t.Run("tag iterate error", func(t *testing.T) {
		db, mock := newMockDB(t)
		expectSQLiteBatchBaseRows(mock, now)
		expectSQLiteBatchEmptyDecayRows(mock)
		expectSQLiteBatchEmptyPayloadRows(mock)
		expectSQLiteBatchEmptyInterpretationRows(mock)
		mock.ExpectQuery(`SELECT record_id, tag FROM tags WHERE record_id IN`).
			WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
			WillReturnRows(sqlmock.NewRows([]string{"record_id", "tag"}).
				AddRow("batch-1", "tag").
				RowError(0, errors.New("tag rows failed")))

		if _, err := getRecordsBatch(ctx, db, ids); err == nil || !strings.Contains(err.Error(), "batch iterate tags") {
			t.Fatalf("getRecordsBatch tag iterate error = %v, want wrapped tag iterate error", err)
		}
	})

	t.Run("provenance query error", func(t *testing.T) {
		db, mock := newMockDB(t)
		expectSQLiteBatchBaseRows(mock, now)
		expectSQLiteBatchEmptyDecayRows(mock)
		expectSQLiteBatchEmptyPayloadRows(mock)
		expectSQLiteBatchEmptyInterpretationRows(mock)
		expectSQLiteBatchEmptyTagRows(mock)
		mock.ExpectQuery(`SELECT record_id, kind, ref, hash, created_by, timestamp FROM provenance_sources WHERE record_id IN`).
			WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
			WillReturnError(errors.New("provenance failed"))

		if _, err := getRecordsBatch(ctx, db, ids); err == nil || !strings.Contains(err.Error(), "batch query provenance_sources") {
			t.Fatalf("getRecordsBatch provenance query error = %v, want wrapped provenance query error", err)
		}
	})

	t.Run("provenance scan error", func(t *testing.T) {
		db, mock := newMockDB(t)
		expectSQLiteBatchBaseRows(mock, now)
		expectSQLiteBatchEmptyDecayRows(mock)
		expectSQLiteBatchEmptyPayloadRows(mock)
		expectSQLiteBatchEmptyInterpretationRows(mock)
		expectSQLiteBatchEmptyTagRows(mock)
		mock.ExpectQuery(`SELECT record_id, kind, ref, hash, created_by, timestamp FROM provenance_sources WHERE record_id IN`).
			WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
			WillReturnRows(sqlmock.NewRows([]string{"record_id"}).AddRow("batch-1"))

		if _, err := getRecordsBatch(ctx, db, ids); err == nil || !strings.Contains(err.Error(), "batch scan provenance_sources") {
			t.Fatalf("getRecordsBatch provenance scan error = %v, want wrapped provenance scan error", err)
		}
	})

	t.Run("provenance iterate error", func(t *testing.T) {
		db, mock := newMockDB(t)
		expectSQLiteBatchBaseRows(mock, now)
		expectSQLiteBatchEmptyDecayRows(mock)
		expectSQLiteBatchEmptyPayloadRows(mock)
		expectSQLiteBatchEmptyInterpretationRows(mock)
		expectSQLiteBatchEmptyTagRows(mock)
		ts := now.Format(time.RFC3339Nano)
		mock.ExpectQuery(`SELECT record_id, kind, ref, hash, created_by, timestamp FROM provenance_sources WHERE record_id IN`).
			WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
			WillReturnRows(sqlmock.NewRows([]string{"record_id", "kind", "ref", "hash", "created_by", "timestamp"}).
				AddRow("batch-1", string(schema.ProvenanceKindObservation), "ref", nil, nil, ts).
				RowError(0, errors.New("provenance rows failed")))

		if _, err := getRecordsBatch(ctx, db, ids); err == nil || !strings.Contains(err.Error(), "batch iterate provenance_sources") {
			t.Fatalf("getRecordsBatch provenance iterate error = %v, want wrapped provenance iterate error", err)
		}
	})

	t.Run("relation query error", func(t *testing.T) {
		db, mock := newMockDB(t)
		expectSQLiteBatchBaseRows(mock, now)
		expectSQLiteBatchEmptyDecayRows(mock)
		expectSQLiteBatchEmptyPayloadRows(mock)
		expectSQLiteBatchEmptyInterpretationRows(mock)
		expectSQLiteBatchEmptyTagRows(mock)
		expectSQLiteBatchEmptyProvenanceRows(mock)
		mock.ExpectQuery(`SELECT source_id, predicate, target_id, weight, created_at FROM relations WHERE source_id IN`).
			WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
			WillReturnError(errors.New("relation failed"))

		if _, err := getRecordsBatch(ctx, db, ids); err == nil || !strings.Contains(err.Error(), "batch query relations") {
			t.Fatalf("getRecordsBatch relation query error = %v, want wrapped relation query error", err)
		}
	})

	t.Run("relation scan error", func(t *testing.T) {
		db, mock := newMockDB(t)
		expectSQLiteBatchBaseRows(mock, now)
		expectSQLiteBatchEmptyDecayRows(mock)
		expectSQLiteBatchEmptyPayloadRows(mock)
		expectSQLiteBatchEmptyInterpretationRows(mock)
		expectSQLiteBatchEmptyTagRows(mock)
		expectSQLiteBatchEmptyProvenanceRows(mock)
		mock.ExpectQuery(`SELECT source_id, predicate, target_id, weight, created_at FROM relations WHERE source_id IN`).
			WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
			WillReturnRows(sqlmock.NewRows([]string{"source_id"}).AddRow("batch-1"))

		if _, err := getRecordsBatch(ctx, db, ids); err == nil || !strings.Contains(err.Error(), "batch scan relations") {
			t.Fatalf("getRecordsBatch relation scan error = %v, want wrapped relation scan error", err)
		}
	})

	t.Run("relation iterate error", func(t *testing.T) {
		db, mock := newMockDB(t)
		expectSQLiteBatchBaseRows(mock, now)
		expectSQLiteBatchEmptyDecayRows(mock)
		expectSQLiteBatchEmptyPayloadRows(mock)
		expectSQLiteBatchEmptyInterpretationRows(mock)
		expectSQLiteBatchEmptyTagRows(mock)
		expectSQLiteBatchEmptyProvenanceRows(mock)
		ts := now.Format(time.RFC3339Nano)
		mock.ExpectQuery(`SELECT source_id, predicate, target_id, weight, created_at FROM relations WHERE source_id IN`).
			WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
			WillReturnRows(sqlmock.NewRows([]string{"source_id", "predicate", "target_id", "weight", "created_at"}).
				AddRow("batch-1", "relates_to", "batch-2", 0.4, ts).
				RowError(0, errors.New("relation rows failed")))

		if _, err := getRecordsBatch(ctx, db, ids); err == nil || !strings.Contains(err.Error(), "batch iterate relations") {
			t.Fatalf("getRecordsBatch relation iterate error = %v, want wrapped relation iterate error", err)
		}
	})

	t.Run("audit query error", func(t *testing.T) {
		db, mock := newMockDB(t)
		expectSQLiteBatchBaseRows(mock, now)
		expectSQLiteBatchEmptyDecayRows(mock)
		expectSQLiteBatchEmptyPayloadRows(mock)
		expectSQLiteBatchEmptyInterpretationRows(mock)
		expectSQLiteBatchEmptyTagRows(mock)
		expectSQLiteBatchEmptyProvenanceRows(mock)
		expectSQLiteBatchEmptyRelationRows(mock)
		mock.ExpectQuery(`SELECT record_id, action, actor, timestamp, rationale FROM audit_log WHERE record_id IN`).
			WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
			WillReturnError(errors.New("audit failed"))

		if _, err := getRecordsBatch(ctx, db, ids); err == nil || !strings.Contains(err.Error(), "batch query audit_log") {
			t.Fatalf("getRecordsBatch audit query error = %v, want wrapped audit query error", err)
		}
	})

	t.Run("audit scan error", func(t *testing.T) {
		db, mock := newMockDB(t)
		expectSQLiteBatchBaseRows(mock, now)
		expectSQLiteBatchEmptyDecayRows(mock)
		expectSQLiteBatchEmptyPayloadRows(mock)
		expectSQLiteBatchEmptyInterpretationRows(mock)
		expectSQLiteBatchEmptyTagRows(mock)
		expectSQLiteBatchEmptyProvenanceRows(mock)
		expectSQLiteBatchEmptyRelationRows(mock)
		mock.ExpectQuery(`SELECT record_id, action, actor, timestamp, rationale FROM audit_log WHERE record_id IN`).
			WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
			WillReturnRows(sqlmock.NewRows([]string{"record_id"}).AddRow("batch-1"))

		if _, err := getRecordsBatch(ctx, db, ids); err == nil || !strings.Contains(err.Error(), "batch scan audit_log") {
			t.Fatalf("getRecordsBatch audit scan error = %v, want wrapped audit scan error", err)
		}
	})

	t.Run("audit iterate error", func(t *testing.T) {
		db, mock := newMockDB(t)
		expectSQLiteBatchBaseRows(mock, now)
		expectSQLiteBatchEmptyDecayRows(mock)
		expectSQLiteBatchEmptyPayloadRows(mock)
		expectSQLiteBatchEmptyInterpretationRows(mock)
		expectSQLiteBatchEmptyTagRows(mock)
		expectSQLiteBatchEmptyProvenanceRows(mock)
		expectSQLiteBatchEmptyRelationRows(mock)
		ts := now.Format(time.RFC3339Nano)
		mock.ExpectQuery(`SELECT record_id, action, actor, timestamp, rationale FROM audit_log WHERE record_id IN`).
			WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
			WillReturnRows(sqlmock.NewRows([]string{"record_id", "action", "actor", "timestamp", "rationale"}).
				AddRow("batch-1", string(schema.AuditActionCreate), "tester", ts, "ok").
				RowError(0, errors.New("audit rows failed")))

		if _, err := getRecordsBatch(ctx, db, ids); err == nil || !strings.Contains(err.Error(), "batch iterate audit_log") {
			t.Fatalf("getRecordsBatch audit iterate error = %v, want wrapped audit iterate error", err)
		}
	})
}

func TestListRowsIterationError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet sql expectations: %v", err)
		}
	})

	mock.ExpectQuery(`SELECT id FROM memory_records`).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).
			AddRow("rec-1").
			RowError(0, errors.New("driver iteration failed")))

	_, err = listRecords(context.Background(), db, storage.ListOptions{})
	if err == nil || !strings.Contains(err.Error(), "iterate ids") {
		t.Fatalf("listRecords row iteration error = %v, want iterate ids error", err)
	}

	mock.ExpectQuery(`SELECT id FROM memory_records`).
		WillReturnError(errors.New("list failed"))
	if _, err := listRecords(context.Background(), db, storage.ListOptions{}); err == nil || !strings.Contains(err.Error(), "list query") {
		t.Fatalf("listRecords query error = %v, want list query error", err)
	}

	mock.ExpectQuery(`SELECT id FROM memory_records`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "extra"}).AddRow("rec-1", "extra"))
	if _, err := listRecords(context.Background(), db, storage.ListOptions{}); err == nil || !strings.Contains(err.Error(), "scan id") {
		t.Fatalf("listRecords scan error = %v, want scan id error", err)
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

func TestMutationHelperErrorBranches(t *testing.T) {
	ctx := context.Background()
	ts := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	entry := schema.AuditEntry{
		Action:    schema.AuditActionRevise,
		Actor:     "tester",
		Timestamp: ts,
		Rationale: "covered",
	}
	rel := schema.Relation{Predicate: "supports", TargetID: "target", Weight: 0.5, CreatedAt: ts}

	t.Run("update salience exec error", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		t.Cleanup(func() {
			_ = db.Close()
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("unmet sql expectations: %v", err)
			}
		})
		mock.ExpectExec(`UPDATE memory_records SET salience = \?`).
			WithArgs(0.5, sqlmock.AnyArg(), "rec-1").
			WillReturnError(errors.New("salience failed"))
		if err := updateSalience(ctx, db, "rec-1", 0.5); err == nil || !strings.Contains(err.Error(), "update salience") {
			t.Fatalf("updateSalience error = %v, want wrapped update error", err)
		}
	})

	t.Run("audit check error", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		t.Cleanup(func() {
			_ = db.Close()
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("unmet sql expectations: %v", err)
			}
		})
		mock.ExpectQuery(`SELECT 1 FROM memory_records WHERE id = \?`).
			WithArgs("rec-1").
			WillReturnError(errors.New("check failed"))
		if err := addAuditEntry(ctx, db, "rec-1", entry); err == nil || !strings.Contains(err.Error(), "check record existence") {
			t.Fatalf("addAuditEntry check error = %v, want wrapped check error", err)
		}
	})

	t.Run("audit insert error", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		t.Cleanup(func() {
			_ = db.Close()
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("unmet sql expectations: %v", err)
			}
		})
		mock.ExpectQuery(`SELECT 1 FROM memory_records WHERE id = \?`).
			WithArgs("rec-1").
			WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(1))
		mock.ExpectExec(`INSERT INTO audit_log`).
			WithArgs("rec-1", string(entry.Action), entry.Actor, entry.Timestamp.UTC().Format(time.RFC3339Nano), entry.Rationale, nil).
			WillReturnError(errors.New("audit insert failed"))
		if err := addAuditEntry(ctx, db, "rec-1", entry); err == nil || !strings.Contains(err.Error(), "insert audit_log") {
			t.Fatalf("addAuditEntry insert error = %v, want wrapped insert error", err)
		}
	})

	t.Run("relation check error", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		t.Cleanup(func() {
			_ = db.Close()
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("unmet sql expectations: %v", err)
			}
		})
		mock.ExpectQuery(`SELECT 1 FROM memory_records WHERE id = \?`).
			WithArgs("src").
			WillReturnError(errors.New("check failed"))
		if err := addRelation(ctx, db, "src", rel); err == nil || !strings.Contains(err.Error(), "check record existence") {
			t.Fatalf("addRelation check error = %v, want wrapped check error", err)
		}
	})

	t.Run("relation insert error", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		t.Cleanup(func() {
			_ = db.Close()
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("unmet sql expectations: %v", err)
			}
		})
		mock.ExpectQuery(`SELECT 1 FROM memory_records WHERE id = \?`).
			WithArgs("src").
			WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(1))
		mock.ExpectExec(`INSERT INTO relations`).
			WithArgs("src", rel.Predicate, rel.TargetID, rel.Weight, rel.CreatedAt.UTC().Format(time.RFC3339Nano)).
			WillReturnError(errors.New("relation insert failed"))
		if err := addRelation(ctx, db, "src", rel); err == nil || !strings.Contains(err.Error(), "insert relations") {
			t.Fatalf("addRelation insert error = %v, want wrapped insert error", err)
		}
	})
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

	rel.Weight = 0.5
	if err := store.AddRelation(ctx, "rel-src", rel); err != nil {
		t.Fatalf("AddRelation duplicate: %v", err)
	}
	gotRelations, err := store.GetRelations(ctx, "rel-src")
	if err != nil {
		t.Fatalf("GetRelations duplicate: %v", err)
	}
	if len(gotRelations) != 1 {
		t.Fatalf("Relations len after duplicate add = %d, want 1", len(gotRelations))
	}
	if gotRelations[0].Weight != 0.5 {
		t.Fatalf("Duplicate relation weight = %v, want updated weight 0.5", gotRelations[0].Weight)
	}

	defaultSrc := newSemanticRecord("rel-default-src")
	defaultTgt := newSemanticRecord("rel-default-tgt")
	for _, rec := range []*schema.MemoryRecord{defaultSrc, defaultTgt} {
		if err := store.Create(ctx, rec); err != nil {
			t.Fatalf("Create %s: %v", rec.ID, err)
		}
	}
	if err := store.AddRelation(ctx, defaultSrc.ID, schema.Relation{
		Predicate: "defaults_to",
		TargetID:  defaultTgt.ID,
	}); err != nil {
		t.Fatalf("AddRelation default fields: %v", err)
	}
	defaultRelations, err := store.GetRelations(ctx, defaultSrc.ID)
	if err != nil {
		t.Fatalf("GetRelations default fields: %v", err)
	}
	if len(defaultRelations) != 1 {
		t.Fatalf("Default relations len = %d, want 1", len(defaultRelations))
	}
	if defaultRelations[0].Weight != 1 {
		t.Fatalf("Default relation weight = %v, want 1", defaultRelations[0].Weight)
	}
	if defaultRelations[0].CreatedAt.IsZero() {
		t.Fatalf("Default relation CreatedAt is zero, want generated timestamp")
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

	if _, err := store.GetRelations(ctx, "missing-rel-source"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("GetRelations missing source err = %v, want ErrNotFound", err)
	}
}

func TestGetRelationsErrorBranches(t *testing.T) {
	ctx := context.Background()
	ts := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC).Format(time.RFC3339Nano)

	t.Run("query error", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		t.Cleanup(func() {
			_ = db.Close()
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("unmet sql expectations: %v", err)
			}
		})
		mock.ExpectQuery(`SELECT predicate, target_id, weight, created_at FROM relations WHERE source_id = \? ORDER BY id`).
			WithArgs("src").
			WillReturnError(errors.New("relations query failed"))
		if _, err := getRelations(ctx, db, "src", true); err == nil || !strings.Contains(err.Error(), "query relations") {
			t.Fatalf("getRelations query error = %v, want wrapped query error", err)
		}
	})

	t.Run("scan error", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		t.Cleanup(func() {
			_ = db.Close()
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("unmet sql expectations: %v", err)
			}
		})
		mock.ExpectQuery(`SELECT predicate, target_id, weight, created_at FROM relations WHERE source_id = \? ORDER BY id`).
			WithArgs("src").
			WillReturnRows(sqlmock.NewRows([]string{"predicate"}).AddRow("supports"))
		if _, err := getRelations(ctx, db, "src", true); err == nil || !strings.Contains(err.Error(), "scan relation") {
			t.Fatalf("getRelations scan error = %v, want wrapped scan error", err)
		}
	})

	t.Run("iteration error", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		t.Cleanup(func() {
			_ = db.Close()
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("unmet sql expectations: %v", err)
			}
		})
		mock.ExpectQuery(`SELECT predicate, target_id, weight, created_at FROM relations WHERE source_id = \? ORDER BY id`).
			WithArgs("src").
			WillReturnRows(sqlmock.NewRows([]string{"predicate", "target_id", "weight", "created_at"}).
				AddRow("supports", "target", 1.0, ts).
				RowError(0, errors.New("relations rows failed")))
		if _, err := getRelations(ctx, db, "src", true); err == nil || !strings.Contains(err.Error(), "iterate relations") {
			t.Fatalf("getRelations iteration error = %v, want wrapped iteration error", err)
		}
	})

	t.Run("empty without source check", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		t.Cleanup(func() {
			_ = db.Close()
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("unmet sql expectations: %v", err)
			}
		})
		mock.ExpectQuery(`SELECT predicate, target_id, weight, created_at FROM relations WHERE source_id = \? ORDER BY id`).
			WithArgs("src").
			WillReturnRows(sqlmock.NewRows([]string{"predicate", "target_id", "weight", "created_at"}))
		rels, err := getRelations(ctx, db, "src", false)
		if err != nil || len(rels) != 0 {
			t.Fatalf("getRelations empty no source check = %+v, %v; want empty nil", rels, err)
		}
	})

	t.Run("source existence error", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		t.Cleanup(func() {
			_ = db.Close()
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("unmet sql expectations: %v", err)
			}
		})
		mock.ExpectQuery(`SELECT predicate, target_id, weight, created_at FROM relations WHERE source_id = \? ORDER BY id`).
			WithArgs("src").
			WillReturnRows(sqlmock.NewRows([]string{"predicate", "target_id", "weight", "created_at"}))
		mock.ExpectQuery(`SELECT 1 FROM memory_records WHERE id = \?`).
			WithArgs("src").
			WillReturnError(errors.New("existence failed"))
		if _, err := getRelations(ctx, db, "src", true); err == nil || !strings.Contains(err.Error(), "check record existence") {
			t.Fatalf("getRelations existence error = %v, want wrapped existence error", err)
		}
	})
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
		if _, err := tx.Get(ctx, "tx-closed-001"); !errors.Is(err, storage.ErrTxClosed) {
			t.Errorf("Get after close: err = %v, want ErrTxClosed", err)
		}
		if err := tx.Update(ctx, rec); !errors.Is(err, storage.ErrTxClosed) {
			t.Errorf("Update after close: err = %v, want ErrTxClosed", err)
		}
		if err := tx.Delete(ctx, "tx-closed-001"); !errors.Is(err, storage.ErrTxClosed) {
			t.Errorf("Delete after close: err = %v, want ErrTxClosed", err)
		}
		if err := tx.UpdateSalience(ctx, "tx-closed-001", 0.5); !errors.Is(err, storage.ErrTxClosed) {
			t.Errorf("UpdateSalience after close: err = %v, want ErrTxClosed", err)
		}
		if err := tx.AddAuditEntry(ctx, "tx-closed-001", schema.AuditEntry{}); !errors.Is(err, storage.ErrTxClosed) {
			t.Errorf("AddAuditEntry after close: err = %v, want ErrTxClosed", err)
		}
		if err := tx.AddRelation(ctx, "tx-closed-001", schema.Relation{}); !errors.Is(err, storage.ErrTxClosed) {
			t.Errorf("AddRelation after close: err = %v, want ErrTxClosed", err)
		}
		if err := tx.Commit(); !errors.Is(err, storage.ErrTxClosed) {
			t.Errorf("Commit after close: err = %v, want ErrTxClosed", err)
		}
		if err := tx.Rollback(); !errors.Is(err, storage.ErrTxClosed) {
			t.Errorf("Rollback after close: err = %v, want ErrTxClosed", err)
		}
	})
}

func TestTransactionListByTypeAndGetRelations(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}

	src := newSemanticRecord("tx-list-src")
	tgt := newSemanticRecord("tx-list-tgt")
	episode := newEpisodicRecord("tx-list-episode")
	for _, rec := range []*schema.MemoryRecord{src, tgt, episode} {
		if err := tx.Create(ctx, rec); err != nil {
			t.Fatalf("tx.Create %s: %v", rec.ID, err)
		}
	}

	rel := schema.Relation{
		Predicate: "related_to",
		TargetID:  tgt.ID,
		Weight:    0.8,
		CreatedAt: time.Date(2025, 5, 1, 9, 0, 0, 0, time.UTC),
	}
	if err := tx.AddRelation(ctx, src.ID, rel); err != nil {
		t.Fatalf("tx.AddRelation: %v", err)
	}

	allRecords, err := tx.List(ctx, storage.ListOptions{Limit: 10})
	if err != nil {
		t.Fatalf("tx.List: %v", err)
	}
	if len(allRecords) != 3 {
		t.Fatalf("tx.List len = %d, want 3", len(allRecords))
	}

	semanticRecords, err := tx.ListByType(ctx, schema.MemoryTypeSemantic)
	if err != nil {
		t.Fatalf("tx.ListByType: %v", err)
	}
	if len(semanticRecords) != 2 {
		t.Fatalf("tx.ListByType semantic len = %d, want 2", len(semanticRecords))
	}

	relations, err := tx.GetRelations(ctx, src.ID)
	if err != nil {
		t.Fatalf("tx.GetRelations: %v", err)
	}
	if len(relations) != 1 {
		t.Fatalf("tx.GetRelations len = %d, want 1", len(relations))
	}
	if relations[0].Predicate != rel.Predicate || relations[0].TargetID != rel.TargetID || relations[0].Weight != rel.Weight {
		t.Fatalf("tx.GetRelations[0] = %+v, want %+v", relations[0], rel)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	if _, err := tx.List(ctx, storage.ListOptions{}); !errors.Is(err, storage.ErrTxClosed) {
		t.Fatalf("tx.List after commit: err = %v, want ErrTxClosed", err)
	}
	if _, err := tx.ListByType(ctx, schema.MemoryTypeSemantic); !errors.Is(err, storage.ErrTxClosed) {
		t.Fatalf("tx.ListByType after commit: err = %v, want ErrTxClosed", err)
	}
	if _, err := tx.GetRelations(ctx, src.ID); !errors.Is(err, storage.ErrTxClosed) {
		t.Fatalf("tx.GetRelations after commit: err = %v, want ErrTxClosed", err)
	}
}

func TestTransactionUpdateAuditSalienceAndDelete(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	rec := newSemanticRecord("tx-update-rec")
	if err := store.Create(ctx, rec); err != nil {
		t.Fatalf("Create: %v", err)
	}
	target := newSemanticRecord("tx-update-target")
	if err := store.Create(ctx, target); err != nil {
		t.Fatalf("Create target: %v", err)
	}
	deleted := newSemanticRecord("tx-update-deleted")
	if err := store.Create(ctx, deleted); err != nil {
		t.Fatalf("Create deleted: %v", err)
	}

	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}

	rec.Salience = 0.4
	rec.Tags = []string{"updated"}
	if err := tx.Update(ctx, rec); err != nil {
		t.Fatalf("tx.Update: %v", err)
	}
	if err := tx.UpdateSalience(ctx, rec.ID, 0.6); err != nil {
		t.Fatalf("tx.UpdateSalience: %v", err)
	}
	entry := schema.AuditEntry{
		Action:    schema.AuditActionReinforce,
		Actor:     "tester",
		Timestamp: time.Date(2025, 5, 1, 10, 0, 0, 0, time.UTC),
		Rationale: "transaction test",
	}
	if err := tx.AddAuditEntry(ctx, rec.ID, entry); err != nil {
		t.Fatalf("tx.AddAuditEntry: %v", err)
	}
	rel := schema.Relation{
		Predicate: "supports",
		TargetID:  target.ID,
		Weight:    0.75,
		CreatedAt: time.Date(2025, 5, 1, 11, 0, 0, 0, time.UTC),
	}
	if err := tx.AddRelation(ctx, rec.ID, rel); err != nil {
		t.Fatalf("tx.AddRelation: %v", err)
	}
	got, err := tx.Get(ctx, rec.ID)
	if err != nil {
		t.Fatalf("tx.Get: %v", err)
	}
	if got.Salience != 0.6 || len(got.Tags) != 1 || got.Tags[0] != "updated" {
		t.Fatalf("tx.Get = %+v, want updated salience and tags", got)
	}
	if len(got.AuditLog) != 2 {
		t.Fatalf("AuditLog len = %d, want original plus transaction entry", len(got.AuditLog))
	}
	if err := tx.Delete(ctx, deleted.ID); err != nil {
		t.Fatalf("tx.Delete: %v", err)
	}
	if _, err := tx.Get(ctx, deleted.ID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("tx.Get deleted target error = %v, want ErrNotFound", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	persisted, err := store.Get(ctx, rec.ID)
	if err != nil {
		t.Fatalf("Get persisted: %v", err)
	}
	if persisted.Salience != 0.6 || len(persisted.Relations) != 1 {
		t.Fatalf("persisted = %+v, want transaction updates committed", persisted)
	}
	if _, err := store.Get(ctx, deleted.ID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("Get deleted target after commit error = %v, want ErrNotFound", err)
	}
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

func TestCreateRecordPropagatesExecErrors(t *testing.T) {
	ctx := context.Background()
	baseRecord := func() *schema.MemoryRecord {
		rec := newSemanticRecord("create-exec-error")
		rec.Tags = nil
		rec.Provenance.Sources = nil
		rec.Relations = nil
		rec.AuditLog = nil
		rec.Interpretation = nil
		return rec
	}

	tests := []struct {
		name    string
		failAt  int
		mutate  func(*schema.MemoryRecord)
		wantErr string
	}{
		{name: "decay profile", failAt: 2, wantErr: "insert decay_profiles"},
		{name: "payload", failAt: 3, wantErr: "insert payloads"},
		{name: "interpretation", failAt: 4, mutate: func(rec *schema.MemoryRecord) {
			rec.Interpretation = &schema.Interpretation{Status: schema.InterpretationStatusTentative}
		}, wantErr: "insert interpretations"},
		{name: "tag", failAt: 4, mutate: func(rec *schema.MemoryRecord) {
			rec.Tags = []string{"tag"}
		}, wantErr: "insert tag"},
		{name: "provenance", failAt: 4, mutate: func(rec *schema.MemoryRecord) {
			rec.Provenance.Sources = []schema.ProvenanceSource{{Kind: schema.ProvenanceKindObservation, Ref: "obs", Timestamp: rec.CreatedAt}}
		}, wantErr: "insert provenance_sources"},
		{name: "relation", failAt: 4, mutate: func(rec *schema.MemoryRecord) {
			rec.Relations = []schema.Relation{{Predicate: "supports", TargetID: "target", CreatedAt: rec.CreatedAt}}
		}, wantErr: "insert relations"},
		{name: "audit log", failAt: 4, mutate: func(rec *schema.MemoryRecord) {
			rec.AuditLog = []schema.AuditEntry{{Action: schema.AuditActionCreate, Actor: "tester", Timestamp: rec.CreatedAt}}
		}, wantErr: "insert audit_log"},
		{name: "entity index cleanup", failAt: 4, wantErr: "delete entity_terms"},
		{name: "competence stats", failAt: 4, mutate: func(rec *schema.MemoryRecord) {
			competence := newCompetenceRecord("create-competence-error")
			competence.Tags = nil
			competence.Provenance.Sources = nil
			competence.AuditLog = nil
			*rec = *competence
		}, wantErr: "insert competence_stats"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := baseRecord()
			if tt.mutate != nil {
				tt.mutate(rec)
			}
			err := createRecord(ctx, &failingExecQueryable{failAt: tt.failAt}, rec)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("createRecord error = %v, want %q", err, tt.wantErr)
			}
		})
	}
}

func TestUpdateRecordPropagatesExecErrors(t *testing.T) {
	ctx := context.Background()
	baseRecord := func() *schema.MemoryRecord {
		rec := newSemanticRecord("update-exec-error")
		rec.Tags = nil
		rec.Provenance.Sources = nil
		rec.Relations = nil
		rec.AuditLog = nil
		rec.Interpretation = nil
		return rec
	}

	tests := []struct {
		name    string
		failAt  int
		mutate  func(*schema.MemoryRecord)
		wantErr string
	}{
		{name: "base record", failAt: 1, wantErr: "update memory_records"},
		{name: "decay profile", failAt: 2, wantErr: "upsert decay_profiles"},
		{name: "payload", failAt: 3, wantErr: "upsert payloads"},
		{name: "delete interpretation", failAt: 4, wantErr: "delete interpretations"},
		{name: "insert interpretation", failAt: 5, mutate: func(rec *schema.MemoryRecord) {
			rec.Interpretation = &schema.Interpretation{Status: schema.InterpretationStatusResolved}
		}, wantErr: "insert interpretations"},
		{name: "delete tags", failAt: 5, wantErr: "delete tags"},
		{name: "insert tag", failAt: 6, mutate: func(rec *schema.MemoryRecord) {
			rec.Tags = []string{"tag"}
		}, wantErr: "insert tag"},
		{name: "delete provenance", failAt: 6, wantErr: "delete provenance_sources"},
		{name: "insert provenance", failAt: 7, mutate: func(rec *schema.MemoryRecord) {
			rec.Provenance.Sources = []schema.ProvenanceSource{{Kind: schema.ProvenanceKindObservation, Ref: "obs", Timestamp: rec.CreatedAt}}
		}, wantErr: "insert provenance_sources"},
		{name: "delete relations", failAt: 7, wantErr: "delete relations"},
		{name: "insert relation", failAt: 8, mutate: func(rec *schema.MemoryRecord) {
			rec.Relations = []schema.Relation{{Predicate: "supports", TargetID: "target"}}
		}, wantErr: "insert relations"},
		{name: "entity index cleanup", failAt: 8, wantErr: "delete entity_terms"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := baseRecord()
			if tt.mutate != nil {
				tt.mutate(rec)
			}
			err := updateRecord(ctx, &failingExecQueryable{failAt: tt.failAt, rowsAffected: 1}, rec)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("updateRecord error = %v, want %q", err, tt.wantErr)
			}
		})
	}
}

func TestDeleteRecordPropagatesExecError(t *testing.T) {
	err := deleteRecord(context.Background(), &failingExecQueryable{failAt: 1}, "delete-error")
	if err == nil || !strings.Contains(err.Error(), "delete memory_records") {
		t.Fatalf("deleteRecord error = %v, want wrapped delete error", err)
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

func TestEntityLookupIndexesTermsTypesAndIdentifiers(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	entity := schema.NewMemoryRecord("entity-lookup", schema.MemoryTypeEntity, schema.SensitivityLow, &schema.EntityPayload{
		Kind:          "entity",
		CanonicalName: "Orchid",
		PrimaryType:   schema.EntityTypeProject,
		Types:         []string{schema.EntityTypeProject, schema.EntityTypeRepository},
		Aliases:       []schema.EntityAlias{{Value: "Project Orchid", Kind: "surface"}},
		Identifiers:   []schema.EntityIdentifier{{Namespace: "github", Value: "BennettSchwartz/orchid"}},
		Summary:       "Orchid repository",
	})
	entity.Scope = "project:alpha"
	if err := store.Create(ctx, entity); err != nil {
		t.Fatalf("Create entity: %v", err)
	}

	byAlias, err := store.FindEntitiesByTerm(ctx, "project orchid", "project:alpha", 5)
	if err != nil {
		t.Fatalf("FindEntitiesByTerm alias: %v", err)
	}
	if len(byAlias) != 1 || byAlias[0].ID != entity.ID {
		t.Fatalf("FindEntitiesByTerm alias = %+v, want %s", byAlias, entity.ID)
	}
	byIdentifier, err := store.FindEntityByIdentifier(ctx, "github", "BennettSchwartz/orchid", "project:alpha")
	if err != nil {
		t.Fatalf("FindEntityByIdentifier: %v", err)
	}
	if byIdentifier.ID != entity.ID {
		t.Fatalf("FindEntityByIdentifier ID = %q, want %q", byIdentifier.ID, entity.ID)
	}

	payload := entity.Payload.(*schema.EntityPayload)
	payload.CanonicalName = "Lotus"
	payload.Aliases = []schema.EntityAlias{{Value: "Project Lotus"}}
	payload.Identifiers = []schema.EntityIdentifier{{Namespace: "github", Value: "BennettSchwartz/lotus"}}
	if err := store.Update(ctx, entity); err != nil {
		t.Fatalf("Update entity: %v", err)
	}

	oldAlias, err := store.FindEntitiesByTerm(ctx, "project orchid", "project:alpha", 5)
	if err != nil {
		t.Fatalf("FindEntitiesByTerm old alias: %v", err)
	}
	if len(oldAlias) != 0 {
		t.Fatalf("Old alias lookup = %+v, want none after reindex", oldAlias)
	}
	newIdentifier, err := store.FindEntityByIdentifier(ctx, "github", "BennettSchwartz/lotus", "project:alpha")
	if err != nil {
		t.Fatalf("FindEntityByIdentifier new: %v", err)
	}
	if newIdentifier.ID != entity.ID {
		t.Fatalf("New identifier ID = %q, want %q", newIdentifier.ID, entity.ID)
	}

	emptyTerm, err := store.FindEntitiesByTerm(ctx, "   ", "project:alpha", 0)
	if err != nil {
		t.Fatalf("FindEntitiesByTerm empty: %v", err)
	}
	if len(emptyTerm) != 0 {
		t.Fatalf("FindEntitiesByTerm empty = %+v, want empty result", emptyTerm)
	}
	if _, err := store.FindEntityByIdentifier(ctx, " ", "BennettSchwartz/lotus", "project:alpha"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("FindEntityByIdentifier empty namespace err = %v, want ErrNotFound", err)
	}
	if _, err := store.FindEntityByIdentifier(ctx, "github", "missing", "project:alpha"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("FindEntityByIdentifier missing err = %v, want ErrNotFound", err)
	}
}

func TestReplaceEntityIndexesBranchesAndErrors(t *testing.T) {
	ctx := context.Background()
	entityRecord := func() *schema.MemoryRecord {
		rec := schema.NewMemoryRecord("entity-index", schema.MemoryTypeEntity, schema.SensitivityLow, &schema.EntityPayload{
			Kind:          "entity",
			CanonicalName: "Orchid",
			PrimaryType:   schema.EntityTypeProject,
			Aliases:       []schema.EntityAlias{{Value: "Project Orchid"}},
			Identifiers:   []schema.EntityIdentifier{{Namespace: "slug", Value: "orchid"}},
		})
		rec.Scope = "project:alpha"
		return rec
	}

	if err := replaceEntityIndexes(ctx, &failingExecQueryable{}, newSemanticRecord("non-entity-index")); err != nil {
		t.Fatalf("replaceEntityIndexes non-entity: %v", err)
	}
	if err := replaceEntityIndexes(ctx, &failingExecQueryable{}, schema.NewMemoryRecord("entity-nil-payload", schema.MemoryTypeEntity, schema.SensitivityLow, nil)); err != nil {
		t.Fatalf("replaceEntityIndexes nil entity payload: %v", err)
	}
	entityWithBlankIdentifier := entityRecord()
	entityWithBlankIdentifier.Payload.(*schema.EntityPayload).Identifiers = []schema.EntityIdentifier{{Namespace: " ", Value: "orchid"}}
	if err := replaceEntityIndexes(ctx, &failingExecQueryable{}, entityWithBlankIdentifier); err != nil {
		t.Fatalf("replaceEntityIndexes blank identifier: %v", err)
	}

	tests := []struct {
		name    string
		failAt  int
		wantErr string
	}{
		{name: "delete types", failAt: 2, wantErr: "delete entity_types"},
		{name: "delete identifiers", failAt: 3, wantErr: "delete entity_identifiers"},
		{name: "canonical term", failAt: 4, wantErr: "insert entity canonical term"},
		{name: "alias term", failAt: 5, wantErr: "insert entity alias term"},
		{name: "entity type", failAt: 6, wantErr: "insert entity type"},
		{name: "identifier", failAt: 7, wantErr: "insert entity identifier"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := replaceEntityIndexes(ctx, &failingExecQueryable{failAt: tt.failAt}, entityRecord())
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("replaceEntityIndexes error = %v, want %q", err, tt.wantErr)
			}
		})
	}
}

func TestEntityLookupRecordsFromRowsErrors(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	rows, err := store.db.QueryContext(ctx, `SELECT 'entity-id', 'extra'`)
	if err != nil {
		t.Fatalf("query scan-error rows: %v", err)
	}
	if _, err := store.recordsFromRows(ctx, rows); err == nil || !strings.Contains(err.Error(), "scan entity id") {
		t.Fatalf("recordsFromRows scan error = %v, want scan entity id error", err)
	}
	_ = rows.Close()

	rows, err = store.db.QueryContext(ctx, `SELECT 'missing-entity'`)
	if err != nil {
		t.Fatalf("query missing rows: %v", err)
	}
	if _, err := store.recordsFromRows(ctx, rows); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("recordsFromRows missing error = %v, want ErrNotFound", err)
	}
	_ = rows.Close()
}

func TestEntityLookupQueryErrors(t *testing.T) {
	ctx := context.Background()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet sql expectations: %v", err)
		}
	})
	store := &SQLiteStore{db: db}

	mock.ExpectQuery(`SELECT DISTINCT record_id FROM entity_terms`).
		WithArgs("orchid", "project", "project", 10).
		WillReturnError(errors.New("term query failed"))
	if records, err := store.FindEntitiesByTerm(ctx, "Orchid", "project", 0); err == nil || !strings.Contains(err.Error(), "query entity terms") {
		t.Fatalf("FindEntitiesByTerm query error = %v, records = %#v; want query entity terms error", err, records)
	}

	mock.ExpectQuery(`SELECT record_id FROM entity_identifiers`).
		WithArgs("slug", "orchid", "project", "project").
		WillReturnError(errors.New("identifier query failed"))
	if record, err := store.FindEntityByIdentifier(ctx, " slug ", " orchid ", "project"); err == nil || !strings.Contains(err.Error(), "query entity identifiers") {
		t.Fatalf("FindEntityByIdentifier query error = %v, record = %#v; want query entity identifiers error", err, record)
	}

	mock.ExpectQuery(`SELECT record_id FROM entity_identifiers`).
		WithArgs("slug", "bad", "project", "project").
		WillReturnRows(sqlmock.NewRows([]string{"record_id"}).AddRow("bad-entity"))
	mock.ExpectQuery(`SELECT id, type, sensitivity, confidence, salience, scope, created_at, updated_at FROM memory_records WHERE id = \?`).
		WithArgs("bad-entity").
		WillReturnError(errors.New("record lookup failed"))
	if record, err := store.FindEntityByIdentifier(ctx, "slug", "bad", "project"); err == nil || !strings.Contains(err.Error(), "query memory_records") {
		t.Fatalf("FindEntityByIdentifier record lookup error = %v, record = %#v; want query memory_records error", err, record)
	}

	mock.ExpectQuery(`SELECT record_id FROM entity_terms`).
		WillReturnRows(sqlmock.NewRows([]string{"record_id"}).
			AddRow("entity-id").
			RowError(0, errors.New("entity ids failed")))
	rows, err := db.QueryContext(ctx, `SELECT record_id FROM entity_terms`)
	if err != nil {
		t.Fatalf("query rows for iteration error: %v", err)
	}
	defer rows.Close()
	if _, err := store.recordsFromRows(ctx, rows); err == nil || !strings.Contains(err.Error(), "iterate entity ids") {
		t.Fatalf("recordsFromRows iterate error = %v, want iterate entity ids error", err)
	}
}

func TestFirstNonEmpty(t *testing.T) {
	if got := firstNonEmpty("", "  value  ", "next"); got != "value" {
		t.Fatalf("firstNonEmpty = %q, want value", got)
	}
	if got := firstNonEmpty("", "   "); got != "" {
		t.Fatalf("firstNonEmpty empty = %q, want empty", got)
	}
}
