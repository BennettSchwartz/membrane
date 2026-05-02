package postgres

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/BennettSchwartz/membrane/pkg/schema"
	"github.com/BennettSchwartz/membrane/pkg/storage"
)

func newMockStore(t *testing.T, cfg EmbeddingConfig) (*PostgresStore, sqlmock.Sqlmock) {
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
	return &PostgresStore{db: db, embeddingConfig: cfg}, mock
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

func TestOpenRejectsEmptyDSN(t *testing.T) {
	if _, err := Open("", EmbeddingConfig{}); err == nil {
		t.Fatalf("Open empty DSN error = nil, want error")
	}
}

func TestOpenRejectsInvalidDSN(t *testing.T) {
	store, err := Open("postgres://%", EmbeddingConfig{})
	if store != nil {
		t.Cleanup(func() { _ = store.Close() })
	}
	if err == nil || !strings.Contains(err.Error(), "ping postgres") {
		t.Fatalf("Open invalid DSN error = %v, want ping postgres error", err)
	}
}

func TestOpenWithInjectedDBSuccessAndErrors(t *testing.T) {
	oldSQLOpen := sqlOpen
	defer func() { sqlOpen = oldSQLOpen }()

	t.Run("success default dimensions and metadata insert", func(t *testing.T) {
		db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		defer func() {
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("unmet sql expectations: %v", err)
			}
		}()
		sqlOpen = func(driverName, dataSourceName string) (*sql.DB, error) {
			if driverName != "pgx" || dataSourceName != "postgres://fake/db" {
				t.Fatalf("sqlOpen args = %q/%q, want pgx/postgres://fake/db", driverName, dataSourceName)
			}
			return db, nil
		}

		mock.ExpectPing()
		mock.ExpectExec(`CREATE EXTENSION IF NOT EXISTS vector`).
			WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectQuery(`SELECT value FROM embedding_metadata WHERE key = \$1`).
			WithArgs("dimensions").
			WillReturnError(sql.ErrNoRows)
		mock.ExpectExec(`INSERT INTO embedding_metadata`).
			WithArgs("dimensions", "1536").
			WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectQuery(`SELECT value FROM embedding_metadata WHERE key = \$1`).
			WithArgs("model").
			WillReturnError(sql.ErrNoRows)
		mock.ExpectExec(`INSERT INTO embedding_metadata`).
			WithArgs("model", "test-model").
			WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectClose()

		store, err := Open("postgres://fake/db", EmbeddingConfig{Model: "test-model"})
		if err != nil {
			t.Fatalf("Open injected success: %v", err)
		}
		if store.embeddingConfig.Dimensions != 1536 || store.embeddingConfig.Model != "test-model" {
			t.Fatalf("embedding config = %+v, want default dimensions and model", store.embeddingConfig)
		}
		if err := store.Close(); err != nil {
			t.Fatalf("Close injected store: %v", err)
		}
	})

	t.Run("sql open error", func(t *testing.T) {
		sqlOpen = func(string, string) (*sql.DB, error) {
			return nil, errors.New("open failed")
		}
		if _, err := Open("postgres://fake/db", EmbeddingConfig{}); err == nil || !strings.Contains(err.Error(), "open postgres") {
			t.Fatalf("Open sql error = %v, want open postgres error", err)
		}
	})

	t.Run("schema error closes db", func(t *testing.T) {
		db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
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

		mock.ExpectPing()
		mock.ExpectExec(`CREATE EXTENSION IF NOT EXISTS vector`).
			WillReturnError(errors.New("schema failed"))
		mock.ExpectClose()

		if _, err := Open("postgres://fake/db", EmbeddingConfig{Dimensions: 4}); err == nil || !strings.Contains(err.Error(), "apply schema") {
			t.Fatalf("Open schema error = %v, want apply schema error", err)
		}
	})

	t.Run("metadata error closes db", func(t *testing.T) {
		db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
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

		mock.ExpectPing()
		mock.ExpectExec(`CREATE EXTENSION IF NOT EXISTS vector`).
			WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectQuery(`SELECT value FROM embedding_metadata WHERE key = \$1`).
			WithArgs("dimensions").
			WillReturnError(errors.New("metadata failed"))
		mock.ExpectClose()

		if _, err := Open("postgres://fake/db", EmbeddingConfig{Dimensions: 4}); err == nil || !strings.Contains(err.Error(), "read embedding metadata dimensions") {
			t.Fatalf("Open metadata error = %v, want metadata read error", err)
		}
	})
}

func TestCloseClosesDB(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	mock.ExpectClose()

	store := &PostgresStore{db: db}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}

func TestCloseAllowsNilStoreAndNilDB(t *testing.T) {
	var store *PostgresStore
	if err := store.Close(); err != nil {
		t.Fatalf("nil store Close: %v", err)
	}
	if err := (&PostgresStore{}).Close(); err != nil {
		t.Fatalf("nil DB Close: %v", err)
	}
}

func TestEnsureEmbeddingMetadata(t *testing.T) {
	ctx := context.Background()
	store, mock := newMockStore(t, EmbeddingConfig{Dimensions: 3, Model: "test-model"})

	mock.ExpectQuery(`SELECT value FROM embedding_metadata WHERE key = \$1`).
		WithArgs("dimensions").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectExec(`INSERT INTO embedding_metadata`).
		WithArgs("dimensions", "3").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(`SELECT value FROM embedding_metadata WHERE key = \$1`).
		WithArgs("model").
		WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow("test-model"))

	if err := store.ensureEmbeddingMetadata(ctx); err != nil {
		t.Fatalf("ensureEmbeddingMetadata: %v", err)
	}
}

func TestEnsureEmbeddingMetadataRejectsMismatch(t *testing.T) {
	ctx := context.Background()
	store, mock := newMockStore(t, EmbeddingConfig{Dimensions: 3})

	mock.ExpectQuery(`SELECT value FROM embedding_metadata WHERE key = \$1`).
		WithArgs("dimensions").
		WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow("4"))

	if err := store.ensureEmbeddingMetadata(ctx); err == nil {
		t.Fatalf("ensureEmbeddingMetadata mismatch error = nil, want error")
	}
}

func TestEnsureEmbeddingMetadataRejectsModelMismatch(t *testing.T) {
	ctx := context.Background()
	store, mock := newMockStore(t, EmbeddingConfig{Dimensions: 3, Model: "expected-model"})

	mock.ExpectQuery(`SELECT value FROM embedding_metadata WHERE key = \$1`).
		WithArgs("dimensions").
		WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow("3"))
	mock.ExpectQuery(`SELECT value FROM embedding_metadata WHERE key = \$1`).
		WithArgs("model").
		WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow("other-model"))

	if err := store.ensureEmbeddingMetadata(ctx); err == nil || !strings.Contains(err.Error(), "embedding metadata mismatch for model") {
		t.Fatalf("ensureEmbeddingMetadata model mismatch error = %v, want mismatch error", err)
	}
}

func TestEnsureEmbeddingMetadataPropagatesReadAndInsertErrors(t *testing.T) {
	ctx := context.Background()

	t.Run("read error", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{Dimensions: 3})
		mock.ExpectQuery(`SELECT value FROM embedding_metadata WHERE key = \$1`).
			WithArgs("dimensions").
			WillReturnError(errors.New("read failed"))

		if err := store.ensureEmbeddingMetadata(ctx); err == nil || !strings.Contains(err.Error(), "read embedding metadata dimensions") {
			t.Fatalf("ensureEmbeddingMetadata read error = %v, want wrapped read error", err)
		}
	})

	t.Run("insert error", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{Dimensions: 3})
		mock.ExpectQuery(`SELECT value FROM embedding_metadata WHERE key = \$1`).
			WithArgs("dimensions").
			WillReturnError(sql.ErrNoRows)
		mock.ExpectExec(`INSERT INTO embedding_metadata`).
			WithArgs("dimensions", "3").
			WillReturnError(errors.New("insert failed"))

		if err := store.ensureEmbeddingMetadata(ctx); err == nil || !strings.Contains(err.Error(), "insert embedding metadata dimensions") {
			t.Fatalf("ensureEmbeddingMetadata insert error = %v, want wrapped insert error", err)
		}
	})
}

func TestStoreTriggerEmbeddingValidationAndUpsert(t *testing.T) {
	ctx := context.Background()
	store, mock := newMockStore(t, EmbeddingConfig{Dimensions: 3, Model: "fallback-model"})

	if err := store.StoreTriggerEmbedding(ctx, "rec-1", nil, ""); err == nil {
		t.Fatalf("StoreTriggerEmbedding empty vector error = nil, want error")
	}
	if err := store.StoreTriggerEmbedding(ctx, "rec-1", []float32{1, 2}, ""); err == nil {
		t.Fatalf("StoreTriggerEmbedding dimension mismatch error = nil, want error")
	}

	mock.ExpectExec(`INSERT INTO trigger_embeddings`).
		WithArgs("rec-1", "[1,2,3]", "fallback-model", sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	if err := store.StoreTriggerEmbedding(ctx, "rec-1", []float32{1, 2, 3}, ""); err != nil {
		t.Fatalf("StoreTriggerEmbedding: %v", err)
	}

	mock.ExpectExec(`INSERT INTO trigger_embeddings`).
		WithArgs("rec-2", "[1,2,3]", "explicit-model", sqlmock.AnyArg()).
		WillReturnError(errors.New("upsert failed"))
	if err := store.StoreTriggerEmbedding(ctx, "rec-2", []float32{1, 2, 3}, "explicit-model"); err == nil || !strings.Contains(err.Error(), "store trigger embedding") {
		t.Fatalf("StoreTriggerEmbedding exec error = %v, want wrapped store error", err)
	}
}

func TestGetTriggerEmbedding(t *testing.T) {
	ctx := context.Background()
	store, mock := newMockStore(t, EmbeddingConfig{})

	mock.ExpectQuery(`SELECT embedding::text FROM trigger_embeddings WHERE record_id = \$1`).
		WithArgs("missing").
		WillReturnError(sql.ErrNoRows)
	got, err := store.GetTriggerEmbedding(ctx, "missing")
	if err != nil {
		t.Fatalf("GetTriggerEmbedding missing: %v", err)
	}
	if got != nil {
		t.Fatalf("GetTriggerEmbedding missing = %#v, want nil", got)
	}

	mock.ExpectQuery(`SELECT embedding::text FROM trigger_embeddings WHERE record_id = \$1`).
		WithArgs("rec-1").
		WillReturnRows(sqlmock.NewRows([]string{"embedding"}).AddRow("[0.25,1.5]"))
	got, err = store.GetTriggerEmbedding(ctx, "rec-1")
	if err != nil {
		t.Fatalf("GetTriggerEmbedding: %v", err)
	}
	if len(got) != 2 || got[0] != 0.25 || got[1] != 1.5 {
		t.Fatalf("GetTriggerEmbedding = %#v, want [0.25 1.5]", got)
	}

	mock.ExpectQuery(`SELECT embedding::text FROM trigger_embeddings WHERE record_id = \$1`).
		WithArgs("query-error").
		WillReturnError(errors.New("query failed"))
	if _, err := store.GetTriggerEmbedding(ctx, "query-error"); err == nil || !strings.Contains(err.Error(), "get trigger embedding") {
		t.Fatalf("GetTriggerEmbedding query error = %v, want wrapped get error", err)
	}

	mock.ExpectQuery(`SELECT embedding::text FROM trigger_embeddings WHERE record_id = \$1`).
		WithArgs("bad-vector").
		WillReturnRows(sqlmock.NewRows([]string{"embedding"}).AddRow("[not-a-number]"))
	if _, err := store.GetTriggerEmbedding(ctx, "bad-vector"); err == nil {
		t.Fatalf("GetTriggerEmbedding bad vector error = nil, want parse error")
	}
}

func TestSearchByEmbedding(t *testing.T) {
	ctx := context.Background()
	store, mock := newMockStore(t, EmbeddingConfig{})

	ids, err := store.SearchByEmbedding(ctx, nil, 5)
	if err != nil {
		t.Fatalf("SearchByEmbedding empty: %v", err)
	}
	if ids != nil {
		t.Fatalf("SearchByEmbedding empty = %#v, want nil", ids)
	}

	mock.ExpectQuery(`SELECT record_id FROM trigger_embeddings`).
		WithArgs("[1,0]", 10).
		WillReturnRows(sqlmock.NewRows([]string{"record_id"}).AddRow("rec-1").AddRow("rec-2"))

	ids, err = store.SearchByEmbedding(ctx, []float32{1, 0}, 0)
	if err != nil {
		t.Fatalf("SearchByEmbedding: %v", err)
	}
	if len(ids) != 2 || ids[0] != "rec-1" || ids[1] != "rec-2" {
		t.Fatalf("SearchByEmbedding = %#v, want [rec-1 rec-2]", ids)
	}

	mock.ExpectQuery(`SELECT record_id FROM trigger_embeddings`).
		WithArgs("[1,0]", 2).
		WillReturnRows(sqlmock.NewRows([]string{"record_id"}).
			AddRow("rec-1").
			RowError(0, errors.New("embedding rows failed")))
	if _, err := store.SearchByEmbedding(ctx, []float32{1, 0}, 2); err == nil || !strings.Contains(err.Error(), "iterate trigger embedding results") {
		t.Fatalf("SearchByEmbedding rows error = %v, want iterate error", err)
	}

	mock.ExpectQuery(`SELECT record_id FROM trigger_embeddings`).
		WithArgs("[1,0]", 2).
		WillReturnError(errors.New("search failed"))
	if _, err := store.SearchByEmbedding(ctx, []float32{1, 0}, 2); err == nil || !strings.Contains(err.Error(), "search trigger embeddings") {
		t.Fatalf("SearchByEmbedding query error = %v, want wrapped search error", err)
	}

	mock.ExpectQuery(`SELECT record_id FROM trigger_embeddings`).
		WithArgs("[1,0]", 2).
		WillReturnRows(sqlmock.NewRows([]string{"record_id", "extra"}).AddRow("rec-1", "extra"))
	if _, err := store.SearchByEmbedding(ctx, []float32{1, 0}, 2); err == nil || !strings.Contains(err.Error(), "scan trigger embedding result") {
		t.Fatalf("SearchByEmbedding scan error = %v, want scan error", err)
	}
}

func TestEntityLookupValidationAndNoMatches(t *testing.T) {
	ctx := context.Background()
	store, mock := newMockStore(t, EmbeddingConfig{})

	records, err := store.FindEntitiesByTerm(ctx, "   ", "project", 5)
	if err != nil {
		t.Fatalf("FindEntitiesByTerm empty: %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("FindEntitiesByTerm empty = %#v, want empty", records)
	}

	mock.ExpectQuery(`SELECT record_id`).
		WithArgs("orchid", "project", 10).
		WillReturnRows(sqlmock.NewRows([]string{"record_id"}))
	records, err = store.FindEntitiesByTerm(ctx, "Orchid", "project", 0)
	if err != nil {
		t.Fatalf("FindEntitiesByTerm no matches: %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("FindEntitiesByTerm no matches = %#v, want empty", records)
	}

	record, err := store.FindEntityByIdentifier(ctx, "", "orchid", "project")
	if !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("FindEntityByIdentifier empty namespace: err = %v, want ErrNotFound", err)
	}
	if record != nil {
		t.Fatalf("FindEntityByIdentifier empty namespace = %#v, want nil", record)
	}

	mock.ExpectQuery(`SELECT record_id FROM entity_identifiers`).
		WithArgs("slug", "orchid", "project").
		WillReturnRows(sqlmock.NewRows([]string{"record_id"}))
	record, err = store.FindEntityByIdentifier(ctx, " slug ", " orchid ", "project")
	if !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("FindEntityByIdentifier no matches: err = %v, want ErrNotFound", err)
	}
	if record != nil {
		t.Fatalf("FindEntityByIdentifier no matches = %#v, want nil", record)
	}

	mock.ExpectQuery(`SELECT record_id FROM entity_identifiers`).
		WithArgs("slug", "orchid", "project").
		WillReturnError(errors.New("identifier query failed"))
	record, err = store.FindEntityByIdentifier(ctx, "slug", "orchid", "project")
	if err == nil || !strings.Contains(err.Error(), "query entity identifiers") {
		t.Fatalf("FindEntityByIdentifier query error = %v, want wrapped query error", err)
	}
	if record != nil {
		t.Fatalf("FindEntityByIdentifier query error record = %#v, want nil", record)
	}

	mock.ExpectQuery(`SELECT record_id FROM entity_identifiers`).
		WithArgs("slug", "bad", "project").
		WillReturnRows(sqlmock.NewRows([]string{"record_id"}).AddRow("bad-entity"))
	mock.ExpectQuery(`SELECT id, type, sensitivity, confidence, salience, scope, created_at, updated_at FROM memory_records WHERE id = \$1`).
		WithArgs("bad-entity").
		WillReturnError(errors.New("record lookup failed"))
	record, err = store.FindEntityByIdentifier(ctx, "slug", "bad", "project")
	if err == nil || !strings.Contains(err.Error(), "query memory_records") {
		t.Fatalf("FindEntityByIdentifier record lookup error = %v, want query memory_records error", err)
	}
	if record != nil {
		t.Fatalf("FindEntityByIdentifier record lookup error record = %#v, want nil", record)
	}

	now := time.Date(2026, 5, 1, 18, 0, 0, 0, time.UTC)
	mock.ExpectQuery(`SELECT record_id FROM entity_identifiers`).
		WithArgs("slug", "orchid", "project").
		WillReturnRows(sqlmock.NewRows([]string{"record_id"}).AddRow("entity-1"))
	expectGetSemanticRecord(mock, "entity-1", now)
	record, err = store.FindEntityByIdentifier(ctx, " slug ", " orchid ", "project")
	if err != nil {
		t.Fatalf("FindEntityByIdentifier success: %v", err)
	}
	if record.ID != "entity-1" {
		t.Fatalf("FindEntityByIdentifier ID = %q, want entity-1", record.ID)
	}
}

func TestEntityLookupQueryAndRowErrors(t *testing.T) {
	ctx := context.Background()
	store, mock := newMockStore(t, EmbeddingConfig{})

	mock.ExpectQuery(`SELECT record_id`).
		WithArgs("orchid", "project", 10).
		WillReturnError(errors.New("term query failed"))
	if records, err := store.FindEntitiesByTerm(ctx, "Orchid", "project", 0); err == nil || !strings.Contains(err.Error(), "query entity terms") {
		t.Fatalf("FindEntitiesByTerm query error = %v, records = %#v; want query entity terms error", err, records)
	}

	mock.ExpectQuery(`SELECT record_id`).
		WillReturnRows(sqlmock.NewRows([]string{"record_id", "extra"}).AddRow("entity-id", "extra"))
	scanRows, err := store.db.QueryContext(ctx, `SELECT record_id`)
	if err != nil {
		t.Fatalf("query rows for scan error: %v", err)
	}
	if _, err := store.recordsFromRows(ctx, scanRows); err == nil || !strings.Contains(err.Error(), "scan entity id") {
		t.Fatalf("recordsFromRows scan error = %v, want scan entity id error", err)
	}
	_ = scanRows.Close()

	mock.ExpectQuery(`SELECT record_id`).
		WillReturnRows(sqlmock.NewRows([]string{"record_id"}).
			AddRow("entity-id").
			RowError(0, errors.New("entity ids failed")))
	iterRows, err := store.db.QueryContext(ctx, `SELECT record_id`)
	if err != nil {
		t.Fatalf("query rows for iteration error: %v", err)
	}
	if _, err := store.recordsFromRows(ctx, iterRows); err == nil || !strings.Contains(err.Error(), "iterate entity ids") {
		t.Fatalf("recordsFromRows iterate error = %v, want iterate entity ids error", err)
	}
	_ = iterRows.Close()

	mock.ExpectQuery(`SELECT record_id`).
		WithArgs("orchid", "project", 1).
		WillReturnRows(sqlmock.NewRows([]string{"record_id"}).AddRow("missing-entity"))
	mock.ExpectQuery(`SELECT id, type, sensitivity, confidence, salience, scope, created_at, updated_at FROM memory_records WHERE id = \$1`).
		WithArgs("missing-entity").
		WillReturnError(sql.ErrNoRows)
	if records, err := store.FindEntitiesByTerm(ctx, "orchid", "project", 1); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("FindEntitiesByTerm missing record error = %v, records = %#v; want ErrNotFound", err, records)
	}
}

func TestListEmptyResults(t *testing.T) {
	ctx := context.Background()
	store, mock := newMockStore(t, EmbeddingConfig{})

	mock.ExpectQuery(`SELECT id FROM memory_records`).
		WithArgs(
			string(schema.MemoryTypeSemantic),
			"project",
			string(schema.SensitivityLow),
			0.2,
			0.9,
			"tag-a",
			"tag-b",
			5,
			2,
		).
		WillReturnRows(sqlmock.NewRows([]string{"id"}))

	records, err := store.List(ctx, storage.ListOptions{
		Type:        schema.MemoryTypeSemantic,
		Scope:       "project",
		Sensitivity: schema.SensitivityLow,
		MinSalience: 0.2,
		MaxSalience: 0.9,
		Tags:        []string{"tag-a", "tag-b"},
		Limit:       5,
		Offset:      2,
	})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("List = %#v, want empty", records)
	}

	mock.ExpectQuery(`SELECT id FROM memory_records`).
		WithArgs(string(schema.MemoryTypeEpisodic)).
		WillReturnRows(sqlmock.NewRows([]string{"id"}))
	records, err = store.ListByType(ctx, schema.MemoryTypeEpisodic)
	if err != nil {
		t.Fatalf("ListByType: %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("ListByType = %#v, want empty", records)
	}

	mock.ExpectQuery(`SELECT id FROM memory_records`).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).
			AddRow("rec-1").
			RowError(0, errors.New("list rows failed")))
	_, err = store.List(ctx, storage.ListOptions{})
	if err == nil || !strings.Contains(err.Error(), "iterate ids") {
		t.Fatalf("List rows error = %v, want iterate ids error", err)
	}

	mock.ExpectQuery(`SELECT id FROM memory_records`).
		WillReturnError(errors.New("list failed"))
	if _, err := store.List(ctx, storage.ListOptions{}); err == nil || !strings.Contains(err.Error(), "list query") {
		t.Fatalf("List query error = %v, want list query error", err)
	}

	mock.ExpectQuery(`SELECT id FROM memory_records`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "extra"}).AddRow("rec-1", "extra"))
	if _, err := store.List(ctx, storage.ListOptions{}); err == nil || !strings.Contains(err.Error(), "scan id") {
		t.Fatalf("List scan error = %v, want scan id error", err)
	}

	mock.ExpectQuery(`SELECT id FROM memory_records`).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("missing"))
	mock.ExpectQuery(`SELECT id, type, sensitivity, confidence, salience, scope, created_at, updated_at FROM memory_records WHERE id = \$1`).
		WithArgs("missing").
		WillReturnError(sql.ErrNoRows)
	if _, err := store.List(ctx, storage.ListOptions{}); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("List batch get error = %v, want ErrNotFound", err)
	}
}

func TestMutatingHelpers(t *testing.T) {
	ctx := context.Background()
	ts := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)

	t.Run("delete", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		mock.ExpectExec(`DELETE FROM memory_records WHERE id = \$1`).
			WithArgs("rec-1").
			WillReturnResult(sqlmock.NewResult(0, 1))
		if err := store.Delete(ctx, "rec-1"); err != nil {
			t.Fatalf("Delete: %v", err)
		}

		mock.ExpectExec(`DELETE FROM memory_records WHERE id = \$1`).
			WithArgs("missing").
			WillReturnResult(sqlmock.NewResult(0, 0))
		if err := store.Delete(ctx, "missing"); !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("Delete missing: err = %v, want ErrNotFound", err)
		}

		mock.ExpectExec(`DELETE FROM memory_records WHERE id = \$1`).
			WithArgs("error").
			WillReturnError(errors.New("delete failed"))
		if err := store.Delete(ctx, "error"); err == nil || !strings.Contains(err.Error(), "delete memory_records") {
			t.Fatalf("Delete exec error = %v, want wrapped delete error", err)
		}
	})

	t.Run("salience", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		mock.ExpectExec(`UPDATE memory_records SET salience = \$1`).
			WithArgs(0.42, sqlmock.AnyArg(), "rec-1").
			WillReturnResult(sqlmock.NewResult(0, 1))
		if err := store.UpdateSalience(ctx, "rec-1", 0.42); err != nil {
			t.Fatalf("UpdateSalience: %v", err)
		}

		mock.ExpectExec(`UPDATE memory_records SET salience = \$1`).
			WithArgs(0.5, sqlmock.AnyArg(), "missing").
			WillReturnResult(sqlmock.NewResult(0, 0))
		if err := store.UpdateSalience(ctx, "missing", 0.5); !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("UpdateSalience missing: err = %v, want ErrNotFound", err)
		}
	})

	t.Run("audit", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		entry := schema.AuditEntry{
			Action:    schema.AuditActionReinforce,
			Actor:     "tester",
			Timestamp: ts,
			Rationale: "covered by test",
		}
		mock.ExpectQuery(`SELECT 1 FROM memory_records WHERE id = \$1`).
			WithArgs("rec-1").
			WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(1))
		mock.ExpectExec(`INSERT INTO audit_log`).
			WithArgs("rec-1", string(entry.Action), entry.Actor, entry.Timestamp.UTC(), entry.Rationale, nil).
			WillReturnResult(sqlmock.NewResult(1, 1))
		if err := store.AddAuditEntry(ctx, "rec-1", entry); err != nil {
			t.Fatalf("AddAuditEntry: %v", err)
		}

		mock.ExpectQuery(`SELECT 1 FROM memory_records WHERE id = \$1`).
			WithArgs("missing").
			WillReturnError(sql.ErrNoRows)
		if err := store.AddAuditEntry(ctx, "missing", entry); !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("AddAuditEntry missing: err = %v, want ErrNotFound", err)
		}
	})

	t.Run("relation", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		mock.ExpectQuery(`SELECT 1 FROM memory_records WHERE id = \$1`).
			WithArgs("src").
			WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(1))
		mock.ExpectExec(`INSERT INTO relations`).
			WithArgs("src", "related_to", "target", 1.0, sqlmock.AnyArg()).
			WillReturnResult(sqlmock.NewResult(1, 1))
		if err := store.AddRelation(ctx, "src", schema.Relation{Predicate: "related_to", TargetID: "target"}); err != nil {
			t.Fatalf("AddRelation: %v", err)
		}

		mock.ExpectQuery(`SELECT 1 FROM memory_records WHERE id = \$1`).
			WithArgs("missing").
			WillReturnError(sql.ErrNoRows)
		if err := store.AddRelation(ctx, "missing", schema.Relation{Predicate: "related_to", TargetID: "target"}); !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("AddRelation missing: err = %v, want ErrNotFound", err)
		}
	})
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
		store, mock := newMockStore(t, EmbeddingConfig{})
		mock.ExpectExec(`UPDATE memory_records SET salience = \$1`).
			WithArgs(0.5, sqlmock.AnyArg(), "rec-1").
			WillReturnError(errors.New("salience failed"))
		if err := store.UpdateSalience(ctx, "rec-1", 0.5); err == nil || !strings.Contains(err.Error(), "update salience") {
			t.Fatalf("UpdateSalience error = %v, want wrapped update error", err)
		}
	})

	t.Run("audit check error", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		mock.ExpectQuery(`SELECT 1 FROM memory_records WHERE id = \$1`).
			WithArgs("rec-1").
			WillReturnError(errors.New("check failed"))
		if err := store.AddAuditEntry(ctx, "rec-1", entry); err == nil || !strings.Contains(err.Error(), "check record existence") {
			t.Fatalf("AddAuditEntry check error = %v, want wrapped check error", err)
		}
	})

	t.Run("audit insert error", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		mock.ExpectQuery(`SELECT 1 FROM memory_records WHERE id = \$1`).
			WithArgs("rec-1").
			WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(1))
		mock.ExpectExec(`INSERT INTO audit_log`).
			WithArgs("rec-1", string(entry.Action), entry.Actor, entry.Timestamp.UTC(), entry.Rationale, nil).
			WillReturnError(errors.New("audit insert failed"))
		if err := store.AddAuditEntry(ctx, "rec-1", entry); err == nil || !strings.Contains(err.Error(), "insert audit_log") {
			t.Fatalf("AddAuditEntry insert error = %v, want wrapped insert error", err)
		}
	})

	t.Run("relation check error", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		mock.ExpectQuery(`SELECT 1 FROM memory_records WHERE id = \$1`).
			WithArgs("src").
			WillReturnError(errors.New("check failed"))
		if err := store.AddRelation(ctx, "src", rel); err == nil || !strings.Contains(err.Error(), "check record existence") {
			t.Fatalf("AddRelation check error = %v, want wrapped check error", err)
		}
	})

	t.Run("relation insert error", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		mock.ExpectQuery(`SELECT 1 FROM memory_records WHERE id = \$1`).
			WithArgs("src").
			WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(1))
		mock.ExpectExec(`INSERT INTO relations`).
			WithArgs("src", rel.Predicate, rel.TargetID, rel.Weight, rel.CreatedAt.UTC()).
			WillReturnError(errors.New("relation insert failed"))
		if err := store.AddRelation(ctx, "src", rel); err == nil || !strings.Contains(err.Error(), "insert relations") {
			t.Fatalf("AddRelation insert error = %v, want wrapped insert error", err)
		}
	})
}

func TestCreateEntityRecordWritesIndexes(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	store, mock := newMockStore(t, EmbeddingConfig{})
	rec := schema.NewMemoryRecord("entity-1", schema.MemoryTypeEntity, schema.SensitivityLow, &schema.EntityPayload{
		Kind:          "entity",
		CanonicalName: "Orchid Project",
		PrimaryType:   schema.EntityTypeProject,
		Types:         []string{schema.EntityTypeProject, schema.EntityTypeTool},
		Aliases: []schema.EntityAlias{
			{Value: "Orchid"},
			{Value: "Project Orchid", Kind: "display"},
		},
		Identifiers: []schema.EntityIdentifier{
			{Namespace: "slug", Value: "orchid"},
			{Namespace: " ", Value: "ignored"},
		},
	})
	rec.Scope = "project"
	rec.CreatedAt = now
	rec.UpdatedAt = now
	rec.Lifecycle.LastReinforcedAt = now
	rec.Interpretation = &schema.Interpretation{Status: schema.InterpretationStatusResolved, Summary: "entity interpretation"}
	rec.Tags = []string{"entity-tag"}
	rec.Provenance.Sources = []schema.ProvenanceSource{{
		Kind:      schema.ProvenanceKindObservation,
		Ref:       "obs-1",
		Hash:      "hash",
		CreatedBy: "tester",
		Timestamp: now,
	}}
	rec.Relations = []schema.Relation{{Predicate: "related_to", TargetID: "entity-2"}}
	rec.AuditLog = []schema.AuditEntry{{Action: schema.AuditActionCreate, Actor: "tester", Timestamp: now, Rationale: "entity create"}}

	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO memory_records`).
		WithArgs(rec.ID, string(rec.Type), string(rec.Sensitivity), rec.Confidence, rec.Salience, rec.Scope, rec.CreatedAt.UTC(), rec.UpdatedAt.UTC()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO decay_profiles`).
		WithArgs(rec.ID, string(rec.Lifecycle.Decay.Curve), rec.Lifecycle.Decay.HalfLifeSeconds, rec.Lifecycle.Decay.MinSalience, nil, rec.Lifecycle.Decay.ReinforcementGain, rec.Lifecycle.LastReinforcedAt.UTC(), false, string(schema.DeletionPolicyAutoPrune)).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO payloads`).
		WithArgs(rec.ID, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO interpretations`).
		WithArgs(rec.ID, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO tags`).
		WithArgs(rec.ID, "entity-tag").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO provenance_sources`).
		WithArgs(rec.ID, string(schema.ProvenanceKindObservation), "obs-1", "hash", "tester", now.UTC()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO relations`).
		WithArgs(rec.ID, "related_to", "entity-2", 1.0, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(rec.ID, string(schema.AuditActionCreate), "tester", now.UTC(), "entity create", nil).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`DELETE FROM entity_terms WHERE record_id = \$1`).
		WithArgs(rec.ID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`DELETE FROM entity_types WHERE record_id = \$1`).
		WithArgs(rec.ID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`DELETE FROM entity_identifiers WHERE record_id = \$1`).
		WithArgs(rec.ID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO entity_terms`).
		WithArgs(rec.ID, "orchid project", schema.EntityTermKindCanonical, rec.Scope).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO entity_terms`).
		WithArgs(rec.ID, "orchid", schema.EntityTermKindAlias, rec.Scope).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO entity_terms`).
		WithArgs(rec.ID, "project orchid", "display", rec.Scope).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO entity_types`).
		WithArgs(rec.ID, schema.EntityTypeProject).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO entity_types`).
		WithArgs(rec.ID, schema.EntityTypeTool).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO entity_identifiers`).
		WithArgs(rec.ID, "slug", "orchid", rec.Scope).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	if err := store.Create(ctx, rec); err != nil {
		t.Fatalf("Create entity: %v", err)
	}
}

func TestCreateAndUpdateValidateBeforeDB(t *testing.T) {
	ctx := context.Background()
	store := &PostgresStore{}
	invalid := &schema.MemoryRecord{}

	if err := store.Create(ctx, invalid); err == nil {
		t.Fatalf("Create invalid error = nil, want validation error")
	}
	if err := store.Update(ctx, invalid); err == nil {
		t.Fatalf("Update invalid error = nil, want validation error")
	}
}

func TestCreateDuplicateAndUpdateMissingOrFailed(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 5, 1, 18, 0, 0, 0, time.UTC)

	t.Run("create duplicate", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		rec := newSemanticRecord("duplicate")
		rec.CreatedAt = now
		rec.UpdatedAt = now
		mock.ExpectBegin()
		mock.ExpectExec(`INSERT INTO memory_records`).
			WithArgs(rec.ID, string(rec.Type), string(rec.Sensitivity), rec.Confidence, rec.Salience, rec.Scope, rec.CreatedAt.UTC(), rec.UpdatedAt.UTC()).
			WillReturnError(&pgconn.PgError{Code: "23505"})
		mock.ExpectRollback()

		if err := store.Create(ctx, rec); !errors.Is(err, storage.ErrAlreadyExists) {
			t.Fatalf("Create duplicate err = %v, want ErrAlreadyExists", err)
		}
	})

	t.Run("update missing", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		rec := newSemanticRecord("missing-update")
		rec.UpdatedAt = now
		mock.ExpectBegin()
		mock.ExpectExec(`UPDATE memory_records`).
			WithArgs(string(rec.Type), string(rec.Sensitivity), rec.Confidence, rec.Salience, rec.Scope, rec.UpdatedAt.UTC(), rec.ID).
			WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectRollback()

		if err := store.Update(ctx, rec); !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("Update missing err = %v, want ErrNotFound", err)
		}
	})

	t.Run("update exec error", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		rec := newSemanticRecord("failed-update")
		rec.UpdatedAt = now
		mock.ExpectBegin()
		mock.ExpectExec(`UPDATE memory_records`).
			WithArgs(string(rec.Type), string(rec.Sensitivity), rec.Confidence, rec.Salience, rec.Scope, rec.UpdatedAt.UTC(), rec.ID).
			WillReturnError(errors.New("update failed"))
		mock.ExpectRollback()

		if err := store.Update(ctx, rec); err == nil || !strings.Contains(err.Error(), "update memory_records") {
			t.Fatalf("Update exec error = %v, want wrapped update error", err)
		}
	})
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
			rec.Type = schema.MemoryTypeCompetence
			rec.Payload = &schema.CompetencePayload{
				Kind:        "competence",
				SkillName:   "debugging",
				Performance: &schema.PerformanceStats{SuccessCount: 1},
			}
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

func TestCreateRecordDirectBranches(t *testing.T) {
	ctx := context.Background()

	if err := createRecord(ctx, &failingExecQueryable{}, &schema.MemoryRecord{}); err == nil {
		t.Fatalf("createRecord invalid record error = nil, want validation error")
	}

	rec := newSemanticRecord("create-direct-generic")
	rec.Tags = nil
	rec.Provenance.Sources = nil
	rec.Relations = nil
	rec.AuditLog = nil
	rec.Interpretation = nil
	if err := createRecord(ctx, &failingExecQueryable{failAt: 1, err: errors.New("insert failed")}, rec); err == nil || !strings.Contains(err.Error(), "insert memory_records") {
		t.Fatalf("createRecord generic insert error = %v, want insert memory_records error", err)
	}

	rec = newSemanticRecord("create-direct-duplicate")
	rec.Tags = nil
	rec.Provenance.Sources = nil
	rec.Relations = nil
	rec.AuditLog = nil
	rec.Interpretation = nil
	if err := createRecord(ctx, &failingExecQueryable{failAt: 1, err: &pgconn.PgError{Code: "23505"}}, rec); !errors.Is(err, storage.ErrAlreadyExists) {
		t.Fatalf("createRecord duplicate error = %v, want ErrAlreadyExists", err)
	}

	rec = newSemanticRecord("create-direct-default-policy")
	rec.Tags = nil
	rec.Provenance.Sources = nil
	rec.Relations = nil
	rec.AuditLog = nil
	rec.Interpretation = nil
	rec.Lifecycle.DeletionPolicy = ""
	if err := createRecord(ctx, &failingExecQueryable{failAt: 100}, rec); err != nil {
		t.Fatalf("createRecord default policy: %v", err)
	}

	rec = newSemanticRecord("create-direct-bad-payload")
	rec.Tags = nil
	rec.Provenance.Sources = nil
	rec.Relations = nil
	rec.AuditLog = nil
	rec.Interpretation = nil
	rec.Payload = &schema.SemanticPayload{
		Kind:      "semantic",
		Subject:   "Go",
		Predicate: "cannot_marshal",
		Object:    make(chan int),
		Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
	}
	if err := createRecord(ctx, &failingExecQueryable{failAt: 100}, rec); err == nil || !strings.Contains(err.Error(), "marshal payload") {
		t.Fatalf("createRecord payload marshal error = %v, want marshal payload error", err)
	}

	rec = newSemanticRecord("create-direct-bad-interpretation")
	rec.Tags = nil
	rec.Provenance.Sources = nil
	rec.Relations = nil
	rec.AuditLog = nil
	rec.Interpretation = &schema.Interpretation{ExtractionConfidence: math.NaN()}
	if err := createRecord(ctx, &failingExecQueryable{failAt: 100}, rec); err == nil || !strings.Contains(err.Error(), "marshal interpretation") {
		t.Fatalf("createRecord interpretation marshal error = %v, want marshal interpretation error", err)
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
		{name: "upsert interpretation", failAt: 4, mutate: func(rec *schema.MemoryRecord) {
			rec.Interpretation = &schema.Interpretation{Status: schema.InterpretationStatusResolved}
		}, wantErr: "upsert interpretations"},
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

func TestUpdateRecordDirectBranches(t *testing.T) {
	ctx := context.Background()

	if err := updateRecord(ctx, &failingExecQueryable{}, &schema.MemoryRecord{}); err == nil {
		t.Fatalf("updateRecord invalid record error = nil, want validation error")
	}

	rec := newSemanticRecord("update-direct-default-policy")
	rec.Tags = nil
	rec.Provenance.Sources = nil
	rec.Relations = nil
	rec.AuditLog = nil
	rec.Interpretation = nil
	rec.Lifecycle.DeletionPolicy = ""
	if err := updateRecord(ctx, &failingExecQueryable{failAt: 100, rowsAffected: 1}, rec); err != nil {
		t.Fatalf("updateRecord default policy: %v", err)
	}

	rec = newSemanticRecord("update-direct-bad-payload")
	rec.Tags = nil
	rec.Provenance.Sources = nil
	rec.Relations = nil
	rec.AuditLog = nil
	rec.Interpretation = nil
	rec.Payload = &schema.SemanticPayload{
		Kind:      "semantic",
		Subject:   "Go",
		Predicate: "cannot_marshal",
		Object:    make(chan int),
		Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
	}
	if err := updateRecord(ctx, &failingExecQueryable{failAt: 100, rowsAffected: 1}, rec); err == nil || !strings.Contains(err.Error(), "marshal payload") {
		t.Fatalf("updateRecord payload marshal error = %v, want marshal payload error", err)
	}

	rec = newSemanticRecord("update-direct-bad-interpretation")
	rec.Tags = nil
	rec.Provenance.Sources = nil
	rec.Relations = nil
	rec.AuditLog = nil
	rec.Interpretation = &schema.Interpretation{ExtractionConfidence: math.NaN()}
	if err := updateRecord(ctx, &failingExecQueryable{failAt: 100, rowsAffected: 1}, rec); err == nil || !strings.Contains(err.Error(), "marshal interpretation") {
		t.Fatalf("updateRecord interpretation marshal error = %v, want marshal interpretation error", err)
	}
}

func TestGetMissingRecord(t *testing.T) {
	ctx := context.Background()
	store, mock := newMockStore(t, EmbeddingConfig{})

	mock.ExpectQuery(`SELECT id, type, sensitivity, confidence, salience, scope, created_at, updated_at FROM memory_records WHERE id = \$1`).
		WithArgs("missing").
		WillReturnError(sql.ErrNoRows)

	record, err := store.Get(ctx, "missing")
	if !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("Get missing: err = %v, want ErrNotFound", err)
	}
	if record != nil {
		t.Fatalf("Get missing = %#v, want nil", record)
	}
}

func expectPostgresBaseRecord(mock sqlmock.Sqlmock, id string, now time.Time) {
	mock.ExpectQuery(`SELECT id, type, sensitivity, confidence, salience, scope, created_at, updated_at FROM memory_records WHERE id = \$1`).
		WithArgs(id).
		WillReturnRows(sqlmock.NewRows([]string{"id", "type", "sensitivity", "confidence", "salience", "scope", "created_at", "updated_at"}).
			AddRow(id, string(schema.MemoryTypeSemantic), string(schema.SensitivityLow), 0.9, 0.8, "project", now, now))
}

func expectPostgresDecayNoRows(mock sqlmock.Sqlmock, id string) {
	mock.ExpectQuery(`SELECT curve, half_life_seconds, min_salience, max_age_seconds, reinforcement_gain, last_reinforced_at, pinned, deletion_policy FROM decay_profiles WHERE record_id = \$1`).
		WithArgs(id).
		WillReturnError(sql.ErrNoRows)
}

func expectPostgresPayloadNoRows(mock sqlmock.Sqlmock, id string) {
	mock.ExpectQuery(`SELECT payload_json FROM payloads WHERE record_id = \$1`).
		WithArgs(id).
		WillReturnError(sql.ErrNoRows)
}

func expectPostgresInterpretationNoRows(mock sqlmock.Sqlmock, id string) {
	mock.ExpectQuery(`SELECT interpretation_json FROM interpretations WHERE record_id = \$1`).
		WithArgs(id).
		WillReturnError(sql.ErrNoRows)
}

func expectPostgresTagsEmpty(mock sqlmock.Sqlmock, id string) {
	mock.ExpectQuery(`SELECT tag FROM tags WHERE record_id = \$1`).
		WithArgs(id).
		WillReturnRows(sqlmock.NewRows([]string{"tag"}))
}

func expectPostgresProvenanceEmpty(mock sqlmock.Sqlmock, id string) {
	mock.ExpectQuery(`SELECT kind, ref, hash, created_by, timestamp FROM provenance_sources WHERE record_id = \$1 ORDER BY id`).
		WithArgs(id).
		WillReturnRows(sqlmock.NewRows([]string{"kind", "ref", "hash", "created_by", "timestamp"}))
}

func expectPostgresRelationsEmpty(mock sqlmock.Sqlmock, id string) {
	mock.ExpectQuery(`SELECT predicate, target_id, weight, created_at FROM relations WHERE source_id = \$1 ORDER BY id`).
		WithArgs(id).
		WillReturnRows(sqlmock.NewRows([]string{"predicate", "target_id", "weight", "created_at"}))
}

func TestGetRecordHydrationErrors(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 5, 1, 15, 0, 0, 0, time.UTC)

	t.Run("base query", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		mock.ExpectQuery(`SELECT id, type, sensitivity, confidence, salience, scope, created_at, updated_at FROM memory_records WHERE id = \$1`).
			WithArgs("rec-1").
			WillReturnError(errors.New("base failed"))
		if _, err := store.Get(ctx, "rec-1"); err == nil || !strings.Contains(err.Error(), "query memory_records") {
			t.Fatalf("Get base error = %v, want wrapped base query error", err)
		}
	})

	t.Run("decay query", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		expectPostgresBaseRecord(mock, "rec-1", now)
		mock.ExpectQuery(`SELECT curve, half_life_seconds, min_salience, max_age_seconds, reinforcement_gain, last_reinforced_at, pinned, deletion_policy FROM decay_profiles WHERE record_id = \$1`).
			WithArgs("rec-1").
			WillReturnError(errors.New("decay failed"))
		if _, err := store.Get(ctx, "rec-1"); err == nil || !strings.Contains(err.Error(), "query decay_profiles") {
			t.Fatalf("Get decay error = %v, want wrapped decay query error", err)
		}
	})

	t.Run("payload query", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		expectPostgresBaseRecord(mock, "rec-1", now)
		expectPostgresDecayNoRows(mock, "rec-1")
		mock.ExpectQuery(`SELECT payload_json FROM payloads WHERE record_id = \$1`).
			WithArgs("rec-1").
			WillReturnError(errors.New("payload failed"))
		if _, err := store.Get(ctx, "rec-1"); err == nil || !strings.Contains(err.Error(), "query payloads") {
			t.Fatalf("Get payload query error = %v, want wrapped payload query error", err)
		}
	})

	t.Run("payload unmarshal", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		expectPostgresBaseRecord(mock, "rec-1", now)
		expectPostgresDecayNoRows(mock, "rec-1")
		mock.ExpectQuery(`SELECT payload_json FROM payloads WHERE record_id = \$1`).
			WithArgs("rec-1").
			WillReturnRows(sqlmock.NewRows([]string{"payload_json"}).AddRow([]byte(`{"kind":"semantic"`)))
		if _, err := store.Get(ctx, "rec-1"); err == nil || !strings.Contains(err.Error(), "unmarshal payload") {
			t.Fatalf("Get payload unmarshal error = %v, want unmarshal payload error", err)
		}
	})

	t.Run("interpretation unmarshal", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		expectPostgresBaseRecord(mock, "rec-1", now)
		expectPostgresDecayNoRows(mock, "rec-1")
		expectPostgresPayloadNoRows(mock, "rec-1")
		mock.ExpectQuery(`SELECT interpretation_json FROM interpretations WHERE record_id = \$1`).
			WithArgs("rec-1").
			WillReturnRows(sqlmock.NewRows([]string{"interpretation_json"}).AddRow([]byte(`{"status":"resolved"`)))
		if _, err := store.Get(ctx, "rec-1"); err == nil || !strings.Contains(err.Error(), "unmarshal interpretation") {
			t.Fatalf("Get interpretation unmarshal error = %v, want unmarshal interpretation error", err)
		}
	})

	t.Run("interpretation query", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		expectPostgresBaseRecord(mock, "rec-1", now)
		expectPostgresDecayNoRows(mock, "rec-1")
		expectPostgresPayloadNoRows(mock, "rec-1")
		mock.ExpectQuery(`SELECT interpretation_json FROM interpretations WHERE record_id = \$1`).
			WithArgs("rec-1").
			WillReturnError(errors.New("interpretation failed"))
		if _, err := store.Get(ctx, "rec-1"); err == nil || !strings.Contains(err.Error(), "query interpretations") {
			t.Fatalf("Get interpretation query error = %v, want wrapped interpretation query error", err)
		}
	})

	t.Run("tag query", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		expectPostgresBaseRecord(mock, "rec-1", now)
		expectPostgresDecayNoRows(mock, "rec-1")
		expectPostgresPayloadNoRows(mock, "rec-1")
		expectPostgresInterpretationNoRows(mock, "rec-1")
		mock.ExpectQuery(`SELECT tag FROM tags WHERE record_id = \$1`).
			WithArgs("rec-1").
			WillReturnError(errors.New("tag failed"))
		if _, err := store.Get(ctx, "rec-1"); err == nil || !strings.Contains(err.Error(), "query tags") {
			t.Fatalf("Get tag query error = %v, want wrapped tag query error", err)
		}
	})

	t.Run("tag scan", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		expectPostgresBaseRecord(mock, "rec-1", now)
		expectPostgresDecayNoRows(mock, "rec-1")
		expectPostgresPayloadNoRows(mock, "rec-1")
		expectPostgresInterpretationNoRows(mock, "rec-1")
		mock.ExpectQuery(`SELECT tag FROM tags WHERE record_id = \$1`).
			WithArgs("rec-1").
			WillReturnRows(sqlmock.NewRows([]string{"tag", "extra"}).AddRow("tag", "extra"))
		if _, err := store.Get(ctx, "rec-1"); err == nil || !strings.Contains(err.Error(), "scan tag") {
			t.Fatalf("Get tag scan error = %v, want scan tag error", err)
		}
	})

	t.Run("tag iteration", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		expectPostgresBaseRecord(mock, "rec-1", now)
		expectPostgresDecayNoRows(mock, "rec-1")
		expectPostgresPayloadNoRows(mock, "rec-1")
		expectPostgresInterpretationNoRows(mock, "rec-1")
		mock.ExpectQuery(`SELECT tag FROM tags WHERE record_id = \$1`).
			WithArgs("rec-1").
			WillReturnRows(sqlmock.NewRows([]string{"tag"}).
				AddRow("tag").
				RowError(0, errors.New("tag rows failed")))
		if _, err := store.Get(ctx, "rec-1"); err == nil || !strings.Contains(err.Error(), "iterate tags") {
			t.Fatalf("Get tag iteration error = %v, want iterate tags error", err)
		}
	})

	t.Run("provenance query", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		expectPostgresBaseRecord(mock, "rec-1", now)
		expectPostgresDecayNoRows(mock, "rec-1")
		expectPostgresPayloadNoRows(mock, "rec-1")
		expectPostgresInterpretationNoRows(mock, "rec-1")
		expectPostgresTagsEmpty(mock, "rec-1")
		mock.ExpectQuery(`SELECT kind, ref, hash, created_by, timestamp FROM provenance_sources WHERE record_id = \$1 ORDER BY id`).
			WithArgs("rec-1").
			WillReturnError(errors.New("provenance failed"))
		if _, err := store.Get(ctx, "rec-1"); err == nil || !strings.Contains(err.Error(), "query provenance_sources") {
			t.Fatalf("Get provenance query error = %v, want wrapped provenance query error", err)
		}
	})

	t.Run("provenance scan", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		expectPostgresBaseRecord(mock, "rec-1", now)
		expectPostgresDecayNoRows(mock, "rec-1")
		expectPostgresPayloadNoRows(mock, "rec-1")
		expectPostgresInterpretationNoRows(mock, "rec-1")
		expectPostgresTagsEmpty(mock, "rec-1")
		mock.ExpectQuery(`SELECT kind, ref, hash, created_by, timestamp FROM provenance_sources WHERE record_id = \$1 ORDER BY id`).
			WithArgs("rec-1").
			WillReturnRows(sqlmock.NewRows([]string{"kind"}).AddRow("input"))
		if _, err := store.Get(ctx, "rec-1"); err == nil || !strings.Contains(err.Error(), "scan provenance_source") {
			t.Fatalf("Get provenance scan error = %v, want scan provenance_source error", err)
		}
	})

	t.Run("provenance iteration", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		expectPostgresBaseRecord(mock, "rec-1", now)
		expectPostgresDecayNoRows(mock, "rec-1")
		expectPostgresPayloadNoRows(mock, "rec-1")
		expectPostgresInterpretationNoRows(mock, "rec-1")
		expectPostgresTagsEmpty(mock, "rec-1")
		mock.ExpectQuery(`SELECT kind, ref, hash, created_by, timestamp FROM provenance_sources WHERE record_id = \$1 ORDER BY id`).
			WithArgs("rec-1").
			WillReturnRows(sqlmock.NewRows([]string{"kind", "ref", "hash", "created_by", "timestamp"}).
				AddRow("input", "ref", nil, nil, now).
				RowError(0, errors.New("provenance rows failed")))
		if _, err := store.Get(ctx, "rec-1"); err == nil || !strings.Contains(err.Error(), "iterate provenance_sources") {
			t.Fatalf("Get provenance iteration error = %v, want iterate provenance_sources error", err)
		}
	})

	t.Run("relations", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		expectPostgresBaseRecord(mock, "rec-1", now)
		expectPostgresDecayNoRows(mock, "rec-1")
		expectPostgresPayloadNoRows(mock, "rec-1")
		expectPostgresInterpretationNoRows(mock, "rec-1")
		expectPostgresTagsEmpty(mock, "rec-1")
		expectPostgresProvenanceEmpty(mock, "rec-1")
		mock.ExpectQuery(`SELECT predicate, target_id, weight, created_at FROM relations WHERE source_id = \$1 ORDER BY id`).
			WithArgs("rec-1").
			WillReturnError(errors.New("relations failed"))
		if _, err := store.Get(ctx, "rec-1"); err == nil || !strings.Contains(err.Error(), "query relations") {
			t.Fatalf("Get relations error = %v, want query relations error", err)
		}
	})

	t.Run("audit query", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		expectPostgresBaseRecord(mock, "rec-1", now)
		expectPostgresDecayNoRows(mock, "rec-1")
		expectPostgresPayloadNoRows(mock, "rec-1")
		expectPostgresInterpretationNoRows(mock, "rec-1")
		expectPostgresTagsEmpty(mock, "rec-1")
		expectPostgresProvenanceEmpty(mock, "rec-1")
		expectPostgresRelationsEmpty(mock, "rec-1")
		mock.ExpectQuery(`SELECT action, actor, timestamp, rationale FROM audit_log WHERE record_id = \$1 ORDER BY id`).
			WithArgs("rec-1").
			WillReturnError(errors.New("audit failed"))
		if _, err := store.Get(ctx, "rec-1"); err == nil || !strings.Contains(err.Error(), "query audit_log") {
			t.Fatalf("Get audit query error = %v, want wrapped audit query error", err)
		}
	})

	t.Run("audit scan", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		expectPostgresBaseRecord(mock, "rec-1", now)
		expectPostgresDecayNoRows(mock, "rec-1")
		expectPostgresPayloadNoRows(mock, "rec-1")
		expectPostgresInterpretationNoRows(mock, "rec-1")
		expectPostgresTagsEmpty(mock, "rec-1")
		expectPostgresProvenanceEmpty(mock, "rec-1")
		expectPostgresRelationsEmpty(mock, "rec-1")
		mock.ExpectQuery(`SELECT action, actor, timestamp, rationale FROM audit_log WHERE record_id = \$1 ORDER BY id`).
			WithArgs("rec-1").
			WillReturnRows(sqlmock.NewRows([]string{"action"}).AddRow("create"))
		if _, err := store.Get(ctx, "rec-1"); err == nil || !strings.Contains(err.Error(), "scan audit_log") {
			t.Fatalf("Get audit scan error = %v, want scan audit_log error", err)
		}
	})

	t.Run("audit iteration", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		expectPostgresBaseRecord(mock, "rec-1", now)
		expectPostgresDecayNoRows(mock, "rec-1")
		expectPostgresPayloadNoRows(mock, "rec-1")
		expectPostgresInterpretationNoRows(mock, "rec-1")
		expectPostgresTagsEmpty(mock, "rec-1")
		expectPostgresProvenanceEmpty(mock, "rec-1")
		expectPostgresRelationsEmpty(mock, "rec-1")
		mock.ExpectQuery(`SELECT action, actor, timestamp, rationale FROM audit_log WHERE record_id = \$1 ORDER BY id`).
			WithArgs("rec-1").
			WillReturnRows(sqlmock.NewRows([]string{"action", "actor", "timestamp", "rationale"}).
				AddRow("create", "tester", now, "created").
				RowError(0, errors.New("audit rows failed")))
		if _, err := store.Get(ctx, "rec-1"); err == nil || !strings.Contains(err.Error(), "iterate audit_log") {
			t.Fatalf("Get audit iteration error = %v, want iterate audit_log error", err)
		}
	})
}

func TestGetRecordSuccess(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 5, 1, 15, 0, 0, 0, time.UTC)
	store, mock := newMockStore(t, EmbeddingConfig{})
	payloadJSON := []byte(`{"kind":"semantic","subject":"Go","predicate":"is","object":"typed","validity":{"mode":"global"}}`)

	expectPostgresBaseRecord(mock, "rec-1", now)
	mock.ExpectQuery(`SELECT curve, half_life_seconds, min_salience, max_age_seconds, reinforcement_gain, last_reinforced_at, pinned, deletion_policy FROM decay_profiles WHERE record_id = \$1`).
		WithArgs("rec-1").
		WillReturnRows(sqlmock.NewRows([]string{"curve", "half_life_seconds", "min_salience", "max_age_seconds", "reinforcement_gain", "last_reinforced_at", "pinned", "deletion_policy"}).
			AddRow(string(schema.DecayCurveExponential), int64(3600), 0.1, int64(7200), 0.2, now, true, string(schema.DeletionPolicyManualOnly)))
	mock.ExpectQuery(`SELECT payload_json FROM payloads WHERE record_id = \$1`).
		WithArgs("rec-1").
		WillReturnRows(sqlmock.NewRows([]string{"payload_json"}).AddRow(payloadJSON))
	mock.ExpectQuery(`SELECT interpretation_json FROM interpretations WHERE record_id = \$1`).
		WithArgs("rec-1").
		WillReturnRows(sqlmock.NewRows([]string{"interpretation_json"}).AddRow([]byte(`{"status":"resolved","summary":"ok"}`)))
	mock.ExpectQuery(`SELECT tag FROM tags WHERE record_id = \$1`).
		WithArgs("rec-1").
		WillReturnRows(sqlmock.NewRows([]string{"tag"}).AddRow("tag-a"))
	mock.ExpectQuery(`SELECT kind, ref, hash, created_by, timestamp FROM provenance_sources WHERE record_id = \$1 ORDER BY id`).
		WithArgs("rec-1").
		WillReturnRows(sqlmock.NewRows([]string{"kind", "ref", "hash", "created_by", "timestamp"}).
			AddRow(string(schema.ProvenanceKindObservation), "obs-1", "hash", "tester", now))
	mock.ExpectQuery(`SELECT predicate, target_id, weight, created_at FROM relations WHERE source_id = \$1 ORDER BY id`).
		WithArgs("rec-1").
		WillReturnRows(sqlmock.NewRows([]string{"predicate", "target_id", "weight", "created_at"}).
			AddRow("supports", "rec-2", 0.5, now))
	mock.ExpectQuery(`SELECT action, actor, timestamp, rationale FROM audit_log WHERE record_id = \$1 ORDER BY id`).
		WithArgs("rec-1").
		WillReturnRows(sqlmock.NewRows([]string{"action", "actor", "timestamp", "rationale"}).
			AddRow(string(schema.AuditActionCreate), "tester", now, "created"))

	record, err := store.Get(ctx, "rec-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if record.ID != "rec-1" || record.Scope != "project" || len(record.Tags) != 1 || len(record.Relations) != 1 || len(record.AuditLog) != 1 {
		t.Fatalf("Get record = %+v, want hydrated record", record)
	}
	if record.Interpretation == nil || record.Interpretation.Status != schema.InterpretationStatusResolved {
		t.Fatalf("Interpretation = %+v, want resolved interpretation", record.Interpretation)
	}
	if _, ok := record.Payload.(*schema.SemanticPayload); !ok {
		t.Fatalf("Payload type = %T, want semantic", record.Payload)
	}
}

func TestGetRecordsBatchHydratesLargeBatch(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 5, 1, 16, 0, 0, 0, time.UTC)
	store, mock := newMockStore(t, EmbeddingConfig{})
	ids := []string{"batch-1", "batch-2", "batch-3", "batch-4"}
	payloadJSON := []byte(`{"kind":"semantic","subject":"Go","predicate":"is","object":"typed","validity":{"mode":"global"}}`)

	mock.ExpectQuery(`SELECT id, type, sensitivity, confidence, salience, scope, created_at, updated_at FROM memory_records WHERE id IN`).
		WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
		WillReturnRows(sqlmock.NewRows([]string{"id", "type", "sensitivity", "confidence", "salience", "scope", "created_at", "updated_at"}).
			AddRow("batch-1", string(schema.MemoryTypeSemantic), string(schema.SensitivityLow), 0.9, 0.8, "project", now, now).
			AddRow("batch-2", string(schema.MemoryTypeSemantic), string(schema.SensitivityLow), 0.8, 0.7, "project", now, now).
			AddRow("batch-3", string(schema.MemoryTypeSemantic), string(schema.SensitivityLow), 0.7, 0.6, "project", now, now).
			AddRow("batch-4", string(schema.MemoryTypeSemantic), string(schema.SensitivityLow), 0.6, 0.5, "project", now, now))
	mock.ExpectQuery(`SELECT record_id, curve, half_life_seconds, min_salience, max_age_seconds, reinforcement_gain, last_reinforced_at, pinned, deletion_policy FROM decay_profiles WHERE record_id IN`).
		WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
		WillReturnRows(sqlmock.NewRows([]string{"record_id", "curve", "half_life_seconds", "min_salience", "max_age_seconds", "reinforcement_gain", "last_reinforced_at", "pinned", "deletion_policy"}).
			AddRow("batch-1", string(schema.DecayCurveExponential), int64(3600), 0.1, nil, 0.2, now, false, string(schema.DeletionPolicyAutoPrune)).
			AddRow("batch-2", string(schema.DecayCurveExponential), int64(3600), 0.1, int64(7200), 0.2, now, true, string(schema.DeletionPolicyManualOnly)))
	mock.ExpectQuery(`SELECT record_id, payload_json FROM payloads WHERE record_id IN`).
		WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
		WillReturnRows(sqlmock.NewRows([]string{"record_id", "payload_json"}).
			AddRow("batch-1", payloadJSON).
			AddRow("batch-2", payloadJSON))
	mock.ExpectQuery(`SELECT record_id, interpretation_json FROM interpretations WHERE record_id IN`).
		WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
		WillReturnRows(sqlmock.NewRows([]string{"record_id", "interpretation_json"}).
			AddRow("batch-1", []byte(`{"status":"resolved","summary":"batch"}`)))
	mock.ExpectQuery(`SELECT record_id, tag FROM tags WHERE record_id IN`).
		WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
		WillReturnRows(sqlmock.NewRows([]string{"record_id", "tag"}).
			AddRow("batch-1", "tag-a").
			AddRow("batch-1", "tag-b").
			AddRow("batch-2", "tag-c"))
	mock.ExpectQuery(`SELECT record_id, kind, ref, hash, created_by, timestamp FROM provenance_sources WHERE record_id IN`).
		WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
		WillReturnRows(sqlmock.NewRows([]string{"record_id", "kind", "ref", "hash", "created_by", "timestamp"}).
			AddRow("batch-1", string(schema.ProvenanceKindObservation), "obs-1", nil, nil, now))
	mock.ExpectQuery(`SELECT source_id, predicate, target_id, weight, created_at FROM relations WHERE source_id IN`).
		WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
		WillReturnRows(sqlmock.NewRows([]string{"source_id", "predicate", "target_id", "weight", "created_at"}).
			AddRow("batch-1", "supports", "batch-2", 0.4, now))
	mock.ExpectQuery(`SELECT record_id, action, actor, timestamp, rationale FROM audit_log WHERE record_id IN`).
		WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
		WillReturnRows(sqlmock.NewRows([]string{"record_id", "action", "actor", "timestamp", "rationale"}).
			AddRow("batch-1", string(schema.AuditActionCreate), "tester", now, "created"))

	records, err := getRecordsBatch(ctx, store.db, ids)
	if err != nil {
		t.Fatalf("getRecordsBatch: %v", err)
	}
	if len(records) != len(ids) {
		t.Fatalf("getRecordsBatch len = %d, want %d", len(records), len(ids))
	}
	for i, id := range ids {
		if records[i].ID != id {
			t.Fatalf("records[%d].ID = %q, want %q", i, records[i].ID, id)
		}
	}
	if len(records[0].Tags) != 2 || len(records[0].Relations) != 1 || len(records[0].AuditLog) != 1 || records[0].Interpretation == nil {
		t.Fatalf("records[0] = %+v, want hydrated batch record", records[0])
	}
	if records[1].Lifecycle.Decay.MaxAgeSeconds != 7200 || !records[1].Lifecycle.Pinned {
		t.Fatalf("records[1] lifecycle = %+v, want max age and pinned", records[1].Lifecycle)
	}
}

func expectPostgresBatchBaseRows(mock sqlmock.Sqlmock, now time.Time) {
	mock.ExpectQuery(`SELECT id, type, sensitivity, confidence, salience, scope, created_at, updated_at FROM memory_records WHERE id IN`).
		WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
		WillReturnRows(sqlmock.NewRows([]string{"id", "type", "sensitivity", "confidence", "salience", "scope", "created_at", "updated_at"}).
			AddRow("batch-1", string(schema.MemoryTypeSemantic), string(schema.SensitivityLow), 0.9, 0.8, "project", now, now).
			AddRow("batch-2", string(schema.MemoryTypeSemantic), string(schema.SensitivityLow), 0.8, 0.7, "project", now, now).
			AddRow("batch-3", string(schema.MemoryTypeSemantic), string(schema.SensitivityLow), 0.7, 0.6, "project", now, now).
			AddRow("batch-4", string(schema.MemoryTypeSemantic), string(schema.SensitivityLow), 0.6, 0.5, "project", now, now))
}

func expectPostgresBatchEmptyDecayRows(mock sqlmock.Sqlmock) {
	mock.ExpectQuery(`SELECT record_id, curve, half_life_seconds, min_salience, max_age_seconds, reinforcement_gain, last_reinforced_at, pinned, deletion_policy FROM decay_profiles WHERE record_id IN`).
		WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
		WillReturnRows(sqlmock.NewRows([]string{"record_id", "curve", "half_life_seconds", "min_salience", "max_age_seconds", "reinforcement_gain", "last_reinforced_at", "pinned", "deletion_policy"}))
}

func expectPostgresBatchEmptyPayloadRows(mock sqlmock.Sqlmock) {
	mock.ExpectQuery(`SELECT record_id, payload_json FROM payloads WHERE record_id IN`).
		WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
		WillReturnRows(sqlmock.NewRows([]string{"record_id", "payload_json"}))
}

func expectPostgresBatchEmptyInterpretationRows(mock sqlmock.Sqlmock) {
	mock.ExpectQuery(`SELECT record_id, interpretation_json FROM interpretations WHERE record_id IN`).
		WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
		WillReturnRows(sqlmock.NewRows([]string{"record_id", "interpretation_json"}))
}

func expectPostgresBatchEmptyTagRows(mock sqlmock.Sqlmock) {
	mock.ExpectQuery(`SELECT record_id, tag FROM tags WHERE record_id IN`).
		WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
		WillReturnRows(sqlmock.NewRows([]string{"record_id", "tag"}))
}

func expectPostgresBatchEmptyProvenanceRows(mock sqlmock.Sqlmock) {
	mock.ExpectQuery(`SELECT record_id, kind, ref, hash, created_by, timestamp FROM provenance_sources WHERE record_id IN`).
		WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
		WillReturnRows(sqlmock.NewRows([]string{"record_id", "kind", "ref", "hash", "created_by", "timestamp"}))
}

func expectPostgresBatchEmptyRelationRows(mock sqlmock.Sqlmock) {
	mock.ExpectQuery(`SELECT source_id, predicate, target_id, weight, created_at FROM relations WHERE source_id IN`).
		WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
		WillReturnRows(sqlmock.NewRows([]string{"source_id", "predicate", "target_id", "weight", "created_at"}))
}

func TestGetRecordsBatchErrorBranches(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 5, 1, 16, 30, 0, 0, time.UTC)
	ids := []string{"batch-1", "batch-2", "batch-3", "batch-4"}

	t.Run("empty batch", func(t *testing.T) {
		store, _ := newMockStore(t, EmbeddingConfig{})
		records, err := getRecordsBatch(ctx, store.db, nil)
		if err != nil || len(records) != 0 {
			t.Fatalf("getRecordsBatch empty = %+v, %v; want empty nil", records, err)
		}
	})

	t.Run("small batch propagates get error", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		mock.ExpectQuery(`SELECT id, type, sensitivity, confidence, salience, scope, created_at, updated_at FROM memory_records WHERE id = \$1`).
			WithArgs("missing").
			WillReturnError(sql.ErrNoRows)

		if _, err := getRecordsBatch(ctx, store.db, []string{"missing"}); !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("getRecordsBatch small error = %v, want ErrNotFound", err)
		}
	})

	t.Run("small batch success", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		expectGetSemanticRecord(mock, "batch-small", now)
		records, err := getRecordsBatch(ctx, store.db, []string{"batch-small"})
		if err != nil {
			t.Fatalf("getRecordsBatch small success: %v", err)
		}
		if len(records) != 1 || records[0].ID != "batch-small" {
			t.Fatalf("getRecordsBatch small success = %+v, want batch-small", records)
		}
	})

	t.Run("base query error", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		mock.ExpectQuery(`SELECT id, type, sensitivity, confidence, salience, scope, created_at, updated_at FROM memory_records WHERE id IN`).
			WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
			WillReturnError(errors.New("base failed"))

		if _, err := getRecordsBatch(ctx, store.db, ids); err == nil || !strings.Contains(err.Error(), "batch query memory_records") {
			t.Fatalf("getRecordsBatch base query error = %v, want wrapped base query error", err)
		}
	})

	t.Run("base scan error", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		mock.ExpectQuery(`SELECT id, type, sensitivity, confidence, salience, scope, created_at, updated_at FROM memory_records WHERE id IN`).
			WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
			WillReturnRows(sqlmock.NewRows([]string{"id", "type", "sensitivity", "confidence", "salience", "scope", "created_at", "updated_at"}).
				AddRow("batch-1", string(schema.MemoryTypeSemantic), string(schema.SensitivityLow), "bad-confidence", 0.8, "project", now, now))

		if _, err := getRecordsBatch(ctx, store.db, ids); err == nil || !strings.Contains(err.Error(), "batch scan memory_records") {
			t.Fatalf("getRecordsBatch base scan error = %v, want wrapped base scan error", err)
		}
	})

	t.Run("base iterate error", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		mock.ExpectQuery(`SELECT id, type, sensitivity, confidence, salience, scope, created_at, updated_at FROM memory_records WHERE id IN`).
			WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
			WillReturnRows(sqlmock.NewRows([]string{"id", "type", "sensitivity", "confidence", "salience", "scope", "created_at", "updated_at"}).
				AddRow("batch-1", string(schema.MemoryTypeSemantic), string(schema.SensitivityLow), 0.9, 0.8, "project", now, now).
				RowError(0, errors.New("base rows failed")))

		if _, err := getRecordsBatch(ctx, store.db, ids); err == nil || !strings.Contains(err.Error(), "batch iterate memory_records") {
			t.Fatalf("getRecordsBatch base iterate error = %v, want wrapped base iteration error", err)
		}
	})

	t.Run("decay query error", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		expectPostgresBatchBaseRows(mock, now)
		mock.ExpectQuery(`SELECT record_id, curve, half_life_seconds, min_salience, max_age_seconds, reinforcement_gain, last_reinforced_at, pinned, deletion_policy FROM decay_profiles WHERE record_id IN`).
			WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
			WillReturnError(errors.New("decay failed"))

		if _, err := getRecordsBatch(ctx, store.db, ids); err == nil || !strings.Contains(err.Error(), "batch query decay_profiles") {
			t.Fatalf("getRecordsBatch decay query error = %v, want wrapped decay query error", err)
		}
	})

	t.Run("decay scan error", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		expectPostgresBatchBaseRows(mock, now)
		mock.ExpectQuery(`SELECT record_id, curve, half_life_seconds, min_salience, max_age_seconds, reinforcement_gain, last_reinforced_at, pinned, deletion_policy FROM decay_profiles WHERE record_id IN`).
			WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
			WillReturnRows(sqlmock.NewRows([]string{"record_id"}).AddRow("batch-1"))

		if _, err := getRecordsBatch(ctx, store.db, ids); err == nil || !strings.Contains(err.Error(), "batch scan decay_profiles") {
			t.Fatalf("getRecordsBatch decay scan error = %v, want wrapped decay scan error", err)
		}
	})

	t.Run("decay iterate error", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		expectPostgresBatchBaseRows(mock, now)
		mock.ExpectQuery(`SELECT record_id, curve, half_life_seconds, min_salience, max_age_seconds, reinforcement_gain, last_reinforced_at, pinned, deletion_policy FROM decay_profiles WHERE record_id IN`).
			WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
			WillReturnRows(sqlmock.NewRows([]string{"record_id", "curve", "half_life_seconds", "min_salience", "max_age_seconds", "reinforcement_gain", "last_reinforced_at", "pinned", "deletion_policy"}).
				AddRow("batch-1", string(schema.DecayCurveExponential), int64(3600), 0.1, int64(7200), 0.2, now, true, string(schema.DeletionPolicyAutoPrune)).
				RowError(0, errors.New("decay rows failed")))

		if _, err := getRecordsBatch(ctx, store.db, ids); err == nil || !strings.Contains(err.Error(), "batch iterate decay_profiles") {
			t.Fatalf("getRecordsBatch decay iterate error = %v, want wrapped decay iterate error", err)
		}
	})

	t.Run("payload query error", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		expectPostgresBatchBaseRows(mock, now)
		expectPostgresBatchEmptyDecayRows(mock)
		mock.ExpectQuery(`SELECT record_id, payload_json FROM payloads WHERE record_id IN`).
			WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
			WillReturnError(errors.New("payload failed"))

		if _, err := getRecordsBatch(ctx, store.db, ids); err == nil || !strings.Contains(err.Error(), "batch query payloads") {
			t.Fatalf("getRecordsBatch payload query error = %v, want wrapped payload query error", err)
		}
	})

	t.Run("payload scan error", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		expectPostgresBatchBaseRows(mock, now)
		expectPostgresBatchEmptyDecayRows(mock)
		mock.ExpectQuery(`SELECT record_id, payload_json FROM payloads WHERE record_id IN`).
			WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
			WillReturnRows(sqlmock.NewRows([]string{"record_id"}).AddRow("batch-1"))

		if _, err := getRecordsBatch(ctx, store.db, ids); err == nil || !strings.Contains(err.Error(), "batch scan payloads") {
			t.Fatalf("getRecordsBatch payload scan error = %v, want wrapped payload scan error", err)
		}
	})

	t.Run("payload iterate error", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		expectPostgresBatchBaseRows(mock, now)
		expectPostgresBatchEmptyDecayRows(mock)
		mock.ExpectQuery(`SELECT record_id, payload_json FROM payloads WHERE record_id IN`).
			WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
			WillReturnRows(sqlmock.NewRows([]string{"record_id", "payload_json"}).
				AddRow("batch-1", []byte{}).
				RowError(0, errors.New("payload rows failed")))

		if _, err := getRecordsBatch(ctx, store.db, ids); err == nil || !strings.Contains(err.Error(), "batch iterate payloads") {
			t.Fatalf("getRecordsBatch payload iterate error = %v, want wrapped payload iterate error", err)
		}
	})

	t.Run("payload unmarshal error", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		expectPostgresBatchBaseRows(mock, now)
		expectPostgresBatchEmptyDecayRows(mock)
		mock.ExpectQuery(`SELECT record_id, payload_json FROM payloads WHERE record_id IN`).
			WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
			WillReturnRows(sqlmock.NewRows([]string{"record_id", "payload_json"}).
				AddRow("batch-1", []byte(`{"kind":"semantic"`)))

		if _, err := getRecordsBatch(ctx, store.db, ids); err == nil || !strings.Contains(err.Error(), "unmarshal payload for batch-1") {
			t.Fatalf("getRecordsBatch payload unmarshal error = %v, want wrapped unmarshal error", err)
		}
	})

	t.Run("interpretation query error", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		expectPostgresBatchBaseRows(mock, now)
		expectPostgresBatchEmptyDecayRows(mock)
		expectPostgresBatchEmptyPayloadRows(mock)
		mock.ExpectQuery(`SELECT record_id, interpretation_json FROM interpretations WHERE record_id IN`).
			WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
			WillReturnError(errors.New("interpretation failed"))

		if _, err := getRecordsBatch(ctx, store.db, ids); err == nil || !strings.Contains(err.Error(), "batch query interpretations") {
			t.Fatalf("getRecordsBatch interpretation query error = %v, want wrapped interpretation query error", err)
		}
	})

	t.Run("interpretation scan error", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		expectPostgresBatchBaseRows(mock, now)
		expectPostgresBatchEmptyDecayRows(mock)
		expectPostgresBatchEmptyPayloadRows(mock)
		mock.ExpectQuery(`SELECT record_id, interpretation_json FROM interpretations WHERE record_id IN`).
			WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
			WillReturnRows(sqlmock.NewRows([]string{"record_id"}).AddRow("batch-1"))

		if _, err := getRecordsBatch(ctx, store.db, ids); err == nil || !strings.Contains(err.Error(), "batch scan interpretations") {
			t.Fatalf("getRecordsBatch interpretation scan error = %v, want wrapped interpretation scan error", err)
		}
	})

	t.Run("interpretation unmarshal error", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		expectPostgresBatchBaseRows(mock, now)
		expectPostgresBatchEmptyDecayRows(mock)
		expectPostgresBatchEmptyPayloadRows(mock)
		mock.ExpectQuery(`SELECT record_id, interpretation_json FROM interpretations WHERE record_id IN`).
			WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
			WillReturnRows(sqlmock.NewRows([]string{"record_id", "interpretation_json"}).
				AddRow("batch-1", []byte(`{"status":"resolved"`)))

		if _, err := getRecordsBatch(ctx, store.db, ids); err == nil || !strings.Contains(err.Error(), "unmarshal interpretation for batch-1") {
			t.Fatalf("getRecordsBatch interpretation unmarshal error = %v, want wrapped interpretation unmarshal error", err)
		}
	})

	t.Run("interpretation iterate error", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		expectPostgresBatchBaseRows(mock, now)
		expectPostgresBatchEmptyDecayRows(mock)
		expectPostgresBatchEmptyPayloadRows(mock)
		mock.ExpectQuery(`SELECT record_id, interpretation_json FROM interpretations WHERE record_id IN`).
			WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
			WillReturnRows(sqlmock.NewRows([]string{"record_id", "interpretation_json"}).
				AddRow("batch-1", []byte{}).
				RowError(0, errors.New("interpretation rows failed")))

		if _, err := getRecordsBatch(ctx, store.db, ids); err == nil || !strings.Contains(err.Error(), "batch iterate interpretations") {
			t.Fatalf("getRecordsBatch interpretation iterate error = %v, want wrapped interpretation iterate error", err)
		}
	})

	t.Run("tag query error", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		expectPostgresBatchBaseRows(mock, now)
		expectPostgresBatchEmptyDecayRows(mock)
		expectPostgresBatchEmptyPayloadRows(mock)
		expectPostgresBatchEmptyInterpretationRows(mock)
		mock.ExpectQuery(`SELECT record_id, tag FROM tags WHERE record_id IN`).
			WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
			WillReturnError(errors.New("tag failed"))

		if _, err := getRecordsBatch(ctx, store.db, ids); err == nil || !strings.Contains(err.Error(), "batch query tags") {
			t.Fatalf("getRecordsBatch tag query error = %v, want wrapped tag query error", err)
		}
	})

	t.Run("tag scan error", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		expectPostgresBatchBaseRows(mock, now)
		expectPostgresBatchEmptyDecayRows(mock)
		expectPostgresBatchEmptyPayloadRows(mock)
		expectPostgresBatchEmptyInterpretationRows(mock)
		mock.ExpectQuery(`SELECT record_id, tag FROM tags WHERE record_id IN`).
			WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
			WillReturnRows(sqlmock.NewRows([]string{"record_id"}).AddRow("batch-1"))

		if _, err := getRecordsBatch(ctx, store.db, ids); err == nil || !strings.Contains(err.Error(), "batch scan tags") {
			t.Fatalf("getRecordsBatch tag scan error = %v, want wrapped tag scan error", err)
		}
	})

	t.Run("tag iterate error", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		expectPostgresBatchBaseRows(mock, now)
		expectPostgresBatchEmptyDecayRows(mock)
		expectPostgresBatchEmptyPayloadRows(mock)
		expectPostgresBatchEmptyInterpretationRows(mock)
		mock.ExpectQuery(`SELECT record_id, tag FROM tags WHERE record_id IN`).
			WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
			WillReturnRows(sqlmock.NewRows([]string{"record_id", "tag"}).
				AddRow("batch-1", "tag").
				RowError(0, errors.New("tag rows failed")))

		if _, err := getRecordsBatch(ctx, store.db, ids); err == nil || !strings.Contains(err.Error(), "batch iterate tags") {
			t.Fatalf("getRecordsBatch tag iterate error = %v, want wrapped tag iterate error", err)
		}
	})

	t.Run("provenance query error", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		expectPostgresBatchBaseRows(mock, now)
		expectPostgresBatchEmptyDecayRows(mock)
		expectPostgresBatchEmptyPayloadRows(mock)
		expectPostgresBatchEmptyInterpretationRows(mock)
		expectPostgresBatchEmptyTagRows(mock)
		mock.ExpectQuery(`SELECT record_id, kind, ref, hash, created_by, timestamp FROM provenance_sources WHERE record_id IN`).
			WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
			WillReturnError(errors.New("provenance failed"))

		if _, err := getRecordsBatch(ctx, store.db, ids); err == nil || !strings.Contains(err.Error(), "batch query provenance_sources") {
			t.Fatalf("getRecordsBatch provenance query error = %v, want wrapped provenance query error", err)
		}
	})

	t.Run("provenance scan error", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		expectPostgresBatchBaseRows(mock, now)
		expectPostgresBatchEmptyDecayRows(mock)
		expectPostgresBatchEmptyPayloadRows(mock)
		expectPostgresBatchEmptyInterpretationRows(mock)
		expectPostgresBatchEmptyTagRows(mock)
		mock.ExpectQuery(`SELECT record_id, kind, ref, hash, created_by, timestamp FROM provenance_sources WHERE record_id IN`).
			WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
			WillReturnRows(sqlmock.NewRows([]string{"record_id"}).AddRow("batch-1"))

		if _, err := getRecordsBatch(ctx, store.db, ids); err == nil || !strings.Contains(err.Error(), "batch scan provenance_sources") {
			t.Fatalf("getRecordsBatch provenance scan error = %v, want wrapped provenance scan error", err)
		}
	})

	t.Run("provenance iterate error", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		expectPostgresBatchBaseRows(mock, now)
		expectPostgresBatchEmptyDecayRows(mock)
		expectPostgresBatchEmptyPayloadRows(mock)
		expectPostgresBatchEmptyInterpretationRows(mock)
		expectPostgresBatchEmptyTagRows(mock)
		mock.ExpectQuery(`SELECT record_id, kind, ref, hash, created_by, timestamp FROM provenance_sources WHERE record_id IN`).
			WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
			WillReturnRows(sqlmock.NewRows([]string{"record_id", "kind", "ref", "hash", "created_by", "timestamp"}).
				AddRow("batch-1", string(schema.ProvenanceKindObservation), "ref", nil, nil, now).
				RowError(0, errors.New("provenance rows failed")))

		if _, err := getRecordsBatch(ctx, store.db, ids); err == nil || !strings.Contains(err.Error(), "batch iterate provenance_sources") {
			t.Fatalf("getRecordsBatch provenance iterate error = %v, want wrapped provenance iterate error", err)
		}
	})

	t.Run("relation query error", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		expectPostgresBatchBaseRows(mock, now)
		expectPostgresBatchEmptyDecayRows(mock)
		expectPostgresBatchEmptyPayloadRows(mock)
		expectPostgresBatchEmptyInterpretationRows(mock)
		expectPostgresBatchEmptyTagRows(mock)
		expectPostgresBatchEmptyProvenanceRows(mock)
		mock.ExpectQuery(`SELECT source_id, predicate, target_id, weight, created_at FROM relations WHERE source_id IN`).
			WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
			WillReturnError(errors.New("relation failed"))

		if _, err := getRecordsBatch(ctx, store.db, ids); err == nil || !strings.Contains(err.Error(), "batch query relations") {
			t.Fatalf("getRecordsBatch relation query error = %v, want wrapped relation query error", err)
		}
	})

	t.Run("relation scan error", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		expectPostgresBatchBaseRows(mock, now)
		expectPostgresBatchEmptyDecayRows(mock)
		expectPostgresBatchEmptyPayloadRows(mock)
		expectPostgresBatchEmptyInterpretationRows(mock)
		expectPostgresBatchEmptyTagRows(mock)
		expectPostgresBatchEmptyProvenanceRows(mock)
		mock.ExpectQuery(`SELECT source_id, predicate, target_id, weight, created_at FROM relations WHERE source_id IN`).
			WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
			WillReturnRows(sqlmock.NewRows([]string{"source_id"}).AddRow("batch-1"))

		if _, err := getRecordsBatch(ctx, store.db, ids); err == nil || !strings.Contains(err.Error(), "batch scan relations") {
			t.Fatalf("getRecordsBatch relation scan error = %v, want wrapped relation scan error", err)
		}
	})

	t.Run("relation iterate error", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		expectPostgresBatchBaseRows(mock, now)
		expectPostgresBatchEmptyDecayRows(mock)
		expectPostgresBatchEmptyPayloadRows(mock)
		expectPostgresBatchEmptyInterpretationRows(mock)
		expectPostgresBatchEmptyTagRows(mock)
		expectPostgresBatchEmptyProvenanceRows(mock)
		mock.ExpectQuery(`SELECT source_id, predicate, target_id, weight, created_at FROM relations WHERE source_id IN`).
			WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
			WillReturnRows(sqlmock.NewRows([]string{"source_id", "predicate", "target_id", "weight", "created_at"}).
				AddRow("batch-1", "relates_to", "batch-2", 0.4, now).
				RowError(0, errors.New("relation rows failed")))

		if _, err := getRecordsBatch(ctx, store.db, ids); err == nil || !strings.Contains(err.Error(), "batch iterate relations") {
			t.Fatalf("getRecordsBatch relation iterate error = %v, want wrapped relation iterate error", err)
		}
	})

	t.Run("audit query error", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		expectPostgresBatchBaseRows(mock, now)
		expectPostgresBatchEmptyDecayRows(mock)
		expectPostgresBatchEmptyPayloadRows(mock)
		expectPostgresBatchEmptyInterpretationRows(mock)
		expectPostgresBatchEmptyTagRows(mock)
		expectPostgresBatchEmptyProvenanceRows(mock)
		expectPostgresBatchEmptyRelationRows(mock)
		mock.ExpectQuery(`SELECT record_id, action, actor, timestamp, rationale FROM audit_log WHERE record_id IN`).
			WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
			WillReturnError(errors.New("audit failed"))

		if _, err := getRecordsBatch(ctx, store.db, ids); err == nil || !strings.Contains(err.Error(), "batch query audit_log") {
			t.Fatalf("getRecordsBatch audit query error = %v, want wrapped audit query error", err)
		}
	})

	t.Run("audit scan error", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		expectPostgresBatchBaseRows(mock, now)
		expectPostgresBatchEmptyDecayRows(mock)
		expectPostgresBatchEmptyPayloadRows(mock)
		expectPostgresBatchEmptyInterpretationRows(mock)
		expectPostgresBatchEmptyTagRows(mock)
		expectPostgresBatchEmptyProvenanceRows(mock)
		expectPostgresBatchEmptyRelationRows(mock)
		mock.ExpectQuery(`SELECT record_id, action, actor, timestamp, rationale FROM audit_log WHERE record_id IN`).
			WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
			WillReturnRows(sqlmock.NewRows([]string{"record_id", "action", "actor", "timestamp", "rationale"}).
				AddRow("batch-1", string(schema.AuditActionCreate), "tester", "bad-time", "created"))

		if _, err := getRecordsBatch(ctx, store.db, ids); err == nil || !strings.Contains(err.Error(), "batch scan audit_log") {
			t.Fatalf("getRecordsBatch audit scan error = %v, want wrapped audit scan error", err)
		}
	})

	t.Run("audit iterate error", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		expectPostgresBatchBaseRows(mock, now)
		expectPostgresBatchEmptyDecayRows(mock)
		expectPostgresBatchEmptyPayloadRows(mock)
		expectPostgresBatchEmptyInterpretationRows(mock)
		expectPostgresBatchEmptyTagRows(mock)
		expectPostgresBatchEmptyProvenanceRows(mock)
		expectPostgresBatchEmptyRelationRows(mock)
		mock.ExpectQuery(`SELECT record_id, action, actor, timestamp, rationale FROM audit_log WHERE record_id IN`).
			WithArgs("batch-1", "batch-2", "batch-3", "batch-4").
			WillReturnRows(sqlmock.NewRows([]string{"record_id", "action", "actor", "timestamp", "rationale"}).
				AddRow("batch-1", string(schema.AuditActionCreate), "tester", now, "ok").
				RowError(0, errors.New("audit rows failed")))

		if _, err := getRecordsBatch(ctx, store.db, ids); err == nil || !strings.Contains(err.Error(), "batch iterate audit_log") {
			t.Fatalf("getRecordsBatch audit iterate error = %v, want wrapped audit iterate error", err)
		}
	})
}

func TestUpdateSemanticRecord(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 5, 1, 14, 0, 0, 0, time.UTC)
	store, mock := newMockStore(t, EmbeddingConfig{})
	rec := newSemanticRecord("semantic-1")
	rec.CreatedAt = now
	rec.UpdatedAt = now
	rec.Lifecycle.LastReinforcedAt = now
	rec.Tags = []string{"semantic-tag"}
	rec.Provenance.Sources = []schema.ProvenanceSource{{
		Kind:      schema.ProvenanceKindObservation,
		Ref:       "obs-1",
		Timestamp: now,
	}}
	rec.Relations = []schema.Relation{{Predicate: "supports", TargetID: "semantic-2", Weight: 0.4, CreatedAt: now}}
	rec.AuditLog = nil
	rec.Interpretation = nil

	mock.ExpectBegin()
	mock.ExpectExec(`UPDATE memory_records`).
		WithArgs(string(rec.Type), string(rec.Sensitivity), rec.Confidence, rec.Salience, rec.Scope, rec.UpdatedAt.UTC(), rec.ID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO decay_profiles`).
		WithArgs(rec.ID, string(rec.Lifecycle.Decay.Curve), rec.Lifecycle.Decay.HalfLifeSeconds, rec.Lifecycle.Decay.MinSalience, rec.Lifecycle.Decay.MaxAgeSeconds, rec.Lifecycle.Decay.ReinforcementGain, rec.Lifecycle.LastReinforcedAt.UTC(), rec.Lifecycle.Pinned, string(rec.Lifecycle.DeletionPolicy)).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO payloads`).
		WithArgs(rec.ID, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`DELETE FROM interpretations WHERE record_id = \$1`).
		WithArgs(rec.ID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`DELETE FROM tags WHERE record_id = \$1`).
		WithArgs(rec.ID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO tags`).
		WithArgs(rec.ID, "semantic-tag").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`DELETE FROM provenance_sources WHERE record_id = \$1`).
		WithArgs(rec.ID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO provenance_sources`).
		WithArgs(rec.ID, string(schema.ProvenanceKindObservation), "obs-1", nil, nil, now.UTC()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`DELETE FROM relations WHERE source_id = \$1`).
		WithArgs(rec.ID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO relations`).
		WithArgs(rec.ID, "supports", "semantic-2", 0.4, now.UTC()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`DELETE FROM entity_terms WHERE record_id = \$1`).
		WithArgs(rec.ID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`DELETE FROM entity_types WHERE record_id = \$1`).
		WithArgs(rec.ID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`DELETE FROM entity_identifiers WHERE record_id = \$1`).
		WithArgs(rec.ID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	if err := store.Update(ctx, rec); err != nil {
		t.Fatalf("Update semantic: %v", err)
	}
}

func TestUpdateSemanticRecordUpsertsInterpretationAndDefaultRelationValues(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 5, 1, 14, 30, 0, 0, time.UTC)
	store, mock := newMockStore(t, EmbeddingConfig{})
	rec := newSemanticRecord("semantic-interpreted")
	rec.CreatedAt = now
	rec.UpdatedAt = now
	rec.Lifecycle.LastReinforcedAt = now
	rec.Tags = nil
	rec.Provenance.Sources = nil
	rec.Interpretation = &schema.Interpretation{Status: schema.InterpretationStatusResolved, Summary: "fresh"}
	rec.Relations = []schema.Relation{{Predicate: "supports", TargetID: "semantic-2"}}

	mock.ExpectBegin()
	mock.ExpectExec(`UPDATE memory_records`).
		WithArgs(string(rec.Type), string(rec.Sensitivity), rec.Confidence, rec.Salience, rec.Scope, rec.UpdatedAt.UTC(), rec.ID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO decay_profiles`).
		WithArgs(rec.ID, string(rec.Lifecycle.Decay.Curve), rec.Lifecycle.Decay.HalfLifeSeconds, rec.Lifecycle.Decay.MinSalience, rec.Lifecycle.Decay.MaxAgeSeconds, rec.Lifecycle.Decay.ReinforcementGain, rec.Lifecycle.LastReinforcedAt.UTC(), rec.Lifecycle.Pinned, string(rec.Lifecycle.DeletionPolicy)).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO payloads`).
		WithArgs(rec.ID, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO interpretations`).
		WithArgs(rec.ID, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`DELETE FROM tags WHERE record_id = \$1`).
		WithArgs(rec.ID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`DELETE FROM provenance_sources WHERE record_id = \$1`).
		WithArgs(rec.ID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`DELETE FROM relations WHERE source_id = \$1`).
		WithArgs(rec.ID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO relations`).
		WithArgs(rec.ID, "supports", "semantic-2", 1.0, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`DELETE FROM entity_terms WHERE record_id = \$1`).
		WithArgs(rec.ID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`DELETE FROM entity_types WHERE record_id = \$1`).
		WithArgs(rec.ID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`DELETE FROM entity_identifiers WHERE record_id = \$1`).
		WithArgs(rec.ID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	if err := store.Update(ctx, rec); err != nil {
		t.Fatalf("Update semantic with interpretation: %v", err)
	}
}

func TestFirstNonEmptyString(t *testing.T) {
	if got := firstNonEmptyString("", " fallback ", "next"); got != "fallback" {
		t.Fatalf("firstNonEmptyString = %q, want fallback", got)
	}
	if got := firstNonEmptyString("", " "); got != "" {
		t.Fatalf("firstNonEmptyString empty = %q, want empty", got)
	}
}

func TestGetRelations(t *testing.T) {
	ctx := context.Background()
	ts := time.Date(2026, 5, 1, 13, 0, 0, 0, time.UTC)
	store, mock := newMockStore(t, EmbeddingConfig{})

	mock.ExpectQuery(`SELECT predicate, target_id, weight, created_at FROM relations`).
		WithArgs("src").
		WillReturnRows(sqlmock.NewRows([]string{"predicate", "target_id", "weight", "created_at"}).
			AddRow("supports", "target", 0.75, ts))

	relations, err := store.GetRelations(ctx, "src")
	if err != nil {
		t.Fatalf("GetRelations: %v", err)
	}
	if len(relations) != 1 {
		t.Fatalf("GetRelations len = %d, want 1", len(relations))
	}
	if relations[0].Predicate != "supports" || relations[0].TargetID != "target" || relations[0].Weight != 0.75 {
		t.Fatalf("GetRelations[0] = %+v, want supports relation", relations[0])
	}

	mock.ExpectQuery(`SELECT predicate, target_id, weight, created_at FROM relations`).
		WithArgs("empty").
		WillReturnRows(sqlmock.NewRows([]string{"predicate", "target_id", "weight", "created_at"}))
	mock.ExpectQuery(`SELECT 1 FROM memory_records WHERE id = \$1`).
		WithArgs("empty").
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(1))
	relations, err = store.GetRelations(ctx, "empty")
	if err != nil {
		t.Fatalf("GetRelations empty: %v", err)
	}
	if len(relations) != 0 {
		t.Fatalf("GetRelations empty = %#v, want empty", relations)
	}

	mock.ExpectQuery(`SELECT predicate, target_id, weight, created_at FROM relations`).
		WithArgs("missing").
		WillReturnRows(sqlmock.NewRows([]string{"predicate", "target_id", "weight", "created_at"}))
	mock.ExpectQuery(`SELECT 1 FROM memory_records WHERE id = \$1`).
		WithArgs("missing").
		WillReturnError(sql.ErrNoRows)
	if _, err := store.GetRelations(ctx, "missing"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("GetRelations missing err = %v, want ErrNotFound", err)
	}
}

func TestGetRelationsErrorBranches(t *testing.T) {
	ctx := context.Background()
	ts := time.Date(2026, 5, 1, 13, 30, 0, 0, time.UTC)

	t.Run("query error", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		mock.ExpectQuery(`SELECT predicate, target_id, weight, created_at FROM relations`).
			WithArgs("src").
			WillReturnError(errors.New("query failed"))

		if _, err := store.GetRelations(ctx, "src"); err == nil || !strings.Contains(err.Error(), "query relations") {
			t.Fatalf("GetRelations query error = %v, want wrapped query error", err)
		}
	})

	t.Run("scan error", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		mock.ExpectQuery(`SELECT predicate, target_id, weight, created_at FROM relations`).
			WithArgs("src").
			WillReturnRows(sqlmock.NewRows([]string{"predicate", "target_id", "weight", "created_at"}).
				AddRow("supports", "target", "bad-weight", ts))

		if _, err := store.GetRelations(ctx, "src"); err == nil || !strings.Contains(err.Error(), "scan relation") {
			t.Fatalf("GetRelations scan error = %v, want wrapped scan error", err)
		}
	})

	t.Run("iterate error", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		mock.ExpectQuery(`SELECT predicate, target_id, weight, created_at FROM relations`).
			WithArgs("src").
			WillReturnRows(sqlmock.NewRows([]string{"predicate", "target_id", "weight", "created_at"}).
				AddRow("supports", "target", 0.5, ts).
				RowError(0, errors.New("rows failed")))

		if _, err := store.GetRelations(ctx, "src"); err == nil || !strings.Contains(err.Error(), "iterate relations") {
			t.Fatalf("GetRelations iteration error = %v, want wrapped iteration error", err)
		}
	})

	t.Run("empty without source check", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		mock.ExpectQuery(`SELECT predicate, target_id, weight, created_at FROM relations`).
			WithArgs("src").
			WillReturnRows(sqlmock.NewRows([]string{"predicate", "target_id", "weight", "created_at"}))

		rels, err := getRelations(ctx, store.db, "src", false)
		if err != nil {
			t.Fatalf("getRelations empty without source check: %v", err)
		}
		if len(rels) != 0 {
			t.Fatalf("getRelations empty without source check = %#v, want empty", rels)
		}
	})

	t.Run("source existence error", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		mock.ExpectQuery(`SELECT predicate, target_id, weight, created_at FROM relations`).
			WithArgs("src").
			WillReturnRows(sqlmock.NewRows([]string{"predicate", "target_id", "weight", "created_at"}))
		mock.ExpectQuery(`SELECT 1 FROM memory_records WHERE id = \$1`).
			WithArgs("src").
			WillReturnError(errors.New("existence failed"))

		if _, err := store.GetRelations(ctx, "src"); err == nil || !strings.Contains(err.Error(), "check record existence") {
			t.Fatalf("GetRelations existence error = %v, want wrapped existence error", err)
		}
	})
}

func TestExtractionMaintenance(t *testing.T) {
	ctx := context.Background()
	store, mock := newMockStore(t, EmbeddingConfig{})

	mock.ExpectQuery(`INSERT INTO episodic_extraction_log`).
		WithArgs(50).
		WillReturnRows(sqlmock.NewRows([]string{"record_id"}).AddRow("episode-1").AddRow("episode-2"))
	claimed, err := store.ClaimUnextractedEpisodics(ctx, 0)
	if err != nil {
		t.Fatalf("ClaimUnextractedEpisodics: %v", err)
	}
	if len(claimed) != 2 || claimed[0] != "episode-1" || claimed[1] != "episode-2" {
		t.Fatalf("ClaimUnextractedEpisodics = %#v, want [episode-1 episode-2]", claimed)
	}

	mock.ExpectExec(`UPDATE episodic_extraction_log`).
		WithArgs("episode-1", 3).
		WillReturnResult(sqlmock.NewResult(0, 1))
	if err := store.MarkEpisodicExtracted(ctx, "episode-1", 3); err != nil {
		t.Fatalf("MarkEpisodicExtracted: %v", err)
	}

	mock.ExpectExec(`DELETE FROM episodic_extraction_log`).
		WithArgs("episode-2").
		WillReturnResult(sqlmock.NewResult(0, 1))
	if err := store.ReleaseEpisodicClaim(ctx, "episode-2"); err != nil {
		t.Fatalf("ReleaseEpisodicClaim: %v", err)
	}

	mock.ExpectExec(`DELETE FROM episodic_extraction_log`).
		WithArgs(int64(3600)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	if err := store.CleanStaleExtractionClaims(ctx, 0); err != nil {
		t.Fatalf("CleanStaleExtractionClaims: %v", err)
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

func TestExtractionMaintenanceErrors(t *testing.T) {
	ctx := context.Background()

	t.Run("claim query error", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		mock.ExpectQuery(`INSERT INTO episodic_extraction_log`).
			WithArgs(3).
			WillReturnError(errors.New("claim failed"))

		if _, err := store.ClaimUnextractedEpisodics(ctx, 3); err == nil || !strings.Contains(err.Error(), "claim episodic extraction records") {
			t.Fatalf("ClaimUnextractedEpisodics query error = %v, want wrapped query error", err)
		}
	})

	t.Run("claim row error", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		mock.ExpectQuery(`INSERT INTO episodic_extraction_log`).
			WithArgs(2).
			WillReturnRows(sqlmock.NewRows([]string{"record_id"}).
				AddRow("episode-1").
				RowError(0, errors.New("claim rows failed")))

		if _, err := store.ClaimUnextractedEpisodics(ctx, 2); err == nil || !strings.Contains(err.Error(), "iterate claimed episodic extraction records") {
			t.Fatalf("ClaimUnextractedEpisodics row error = %v, want iterate error", err)
		}
	})

	t.Run("claim scan error", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		mock.ExpectQuery(`INSERT INTO episodic_extraction_log`).
			WithArgs(2).
			WillReturnRows(sqlmock.NewRows([]string{"record_id", "extra"}).
				AddRow("episode-1", "extra"))

		if _, err := store.ClaimUnextractedEpisodics(ctx, 2); err == nil || !strings.Contains(err.Error(), "scan claimed episodic extraction record") {
			t.Fatalf("ClaimUnextractedEpisodics scan error = %v, want scan error", err)
		}
	})

	t.Run("mark error", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		mock.ExpectExec(`UPDATE episodic_extraction_log`).
			WithArgs("episode-1", 1).
			WillReturnError(errors.New("mark failed"))

		if err := store.MarkEpisodicExtracted(ctx, "episode-1", 1); err == nil || !strings.Contains(err.Error(), "mark episodic extracted") {
			t.Fatalf("MarkEpisodicExtracted error = %v, want wrapped mark error", err)
		}
	})

	t.Run("release error", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		mock.ExpectExec(`DELETE FROM episodic_extraction_log`).
			WithArgs("episode-1").
			WillReturnError(errors.New("release failed"))

		if err := store.ReleaseEpisodicClaim(ctx, "episode-1"); err == nil || !strings.Contains(err.Error(), "release episodic claim") {
			t.Fatalf("ReleaseEpisodicClaim error = %v, want wrapped release error", err)
		}
	})

	t.Run("clean error", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		mock.ExpectExec(`DELETE FROM episodic_extraction_log`).
			WithArgs(int64(120)).
			WillReturnError(errors.New("clean failed"))

		if err := store.CleanStaleExtractionClaims(ctx, 2*time.Minute); err == nil || !strings.Contains(err.Error(), "clean stale extraction claims") {
			t.Fatalf("CleanStaleExtractionClaims error = %v, want wrapped clean error", err)
		}
	})

	t.Run("reset error", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		mock.ExpectExec(`TRUNCATE TABLE`).
			WillReturnError(errors.New("reset failed"))

		if err := store.Reset(ctx); err == nil || !strings.Contains(err.Error(), "reset postgres store") {
			t.Fatalf("Reset error = %v, want wrapped reset error", err)
		}
	})
}

func TestFindSemanticExactNoMatch(t *testing.T) {
	ctx := context.Background()
	store, mock := newMockStore(t, EmbeddingConfig{})

	mock.ExpectQuery(`SELECT mr.id FROM memory_records mr`).
		WithArgs("subject", "predicate", "object").
		WillReturnError(sql.ErrNoRows)
	record, err := store.FindSemanticExact(ctx, "subject", "predicate", "object")
	if err != nil {
		t.Fatalf("FindSemanticExact no match: %v", err)
	}
	if record != nil {
		t.Fatalf("FindSemanticExact no match = %#v, want nil", record)
	}
}

func TestFindSemanticExactSuccessAndQueryError(t *testing.T) {
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		now := time.Date(2026, 5, 1, 16, 30, 0, 0, time.UTC)
		mock.ExpectQuery(`SELECT mr.id FROM memory_records mr`).
			WithArgs("subject", "predicate", "object").
			WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("semantic-found"))
		expectGetSemanticRecord(mock, "semantic-found", now)

		record, err := store.FindSemanticExact(ctx, "subject", "predicate", "object")
		if err != nil {
			t.Fatalf("FindSemanticExact: %v", err)
		}
		if record == nil || record.ID != "semantic-found" {
			t.Fatalf("FindSemanticExact = %+v, want semantic-found", record)
		}
	})

	t.Run("query error", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		mock.ExpectQuery(`SELECT mr.id FROM memory_records mr`).
			WithArgs("subject", "predicate", "object").
			WillReturnError(errors.New("database unavailable"))
		record, err := store.FindSemanticExact(ctx, "subject", "predicate", "object")
		if err == nil || !strings.Contains(err.Error(), "find semantic exact") {
			t.Fatalf("FindSemanticExact error = %v, want wrapped query error", err)
		}
		if record != nil {
			t.Fatalf("FindSemanticExact error record = %+v, want nil", record)
		}
	})
}

func TestRecordsFromRowsHydratesRecordsAndPropagatesErrors(t *testing.T) {
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		now := time.Date(2026, 5, 1, 17, 0, 0, 0, time.UTC)
		mock.ExpectQuery(`SELECT record_id`).
			WillReturnRows(sqlmock.NewRows([]string{"record_id"}).AddRow("entity-1"))
		expectGetSemanticRecord(mock, "entity-1", now)

		rows, err := store.db.QueryContext(ctx, `SELECT record_id`)
		if err != nil {
			t.Fatalf("QueryContext: %v", err)
		}
		records, err := store.recordsFromRows(ctx, rows)
		if err != nil {
			t.Fatalf("recordsFromRows: %v", err)
		}
		if len(records) != 1 || records[0].ID != "entity-1" {
			t.Fatalf("recordsFromRows = %+v, want entity-1", records)
		}
	})

	t.Run("scan error", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		mock.ExpectQuery(`SELECT record_id`).
			WillReturnRows(sqlmock.NewRows([]string{"record_id", "extra"}).AddRow("entity-1", "unexpected"))

		rows, err := store.db.QueryContext(ctx, `SELECT record_id`)
		if err != nil {
			t.Fatalf("QueryContext: %v", err)
		}
		if _, err := store.recordsFromRows(ctx, rows); err == nil || !strings.Contains(err.Error(), "scan entity id") {
			t.Fatalf("recordsFromRows scan error = %v, want scan entity id error", err)
		}
		_ = rows.Close()
	})

	t.Run("get error", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		mock.ExpectQuery(`SELECT record_id`).
			WillReturnRows(sqlmock.NewRows([]string{"record_id"}).AddRow("missing"))
		mock.ExpectQuery(`SELECT id, type, sensitivity, confidence, salience, scope, created_at, updated_at FROM memory_records WHERE id = \$1`).
			WithArgs("missing").
			WillReturnError(sql.ErrNoRows)

		rows, err := store.db.QueryContext(ctx, `SELECT record_id`)
		if err != nil {
			t.Fatalf("QueryContext: %v", err)
		}
		if _, err := store.recordsFromRows(ctx, rows); !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("recordsFromRows get error = %v, want ErrNotFound", err)
		}
		_ = rows.Close()
	})
}

func TestReset(t *testing.T) {
	ctx := context.Background()
	store, mock := newMockStore(t, EmbeddingConfig{})

	mock.ExpectExec(`TRUNCATE TABLE`).
		WillReturnResult(sqlmock.NewResult(0, 1))
	if err := store.Reset(ctx); err != nil {
		t.Fatalf("Reset: %v", err)
	}
}

func TestBeginCommitRollback(t *testing.T) {
	ctx := context.Background()

	t.Run("commit", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		mock.ExpectBegin()
		mock.ExpectCommit()

		tx, err := store.Begin(ctx)
		if err != nil {
			t.Fatalf("Begin: %v", err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("Commit: %v", err)
		}
	})

	t.Run("rollback", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		mock.ExpectBegin()
		mock.ExpectRollback()

		tx, err := store.Begin(ctx)
		if err != nil {
			t.Fatalf("Begin: %v", err)
		}
		if err := tx.Rollback(); err != nil {
			t.Fatalf("Rollback: %v", err)
		}
	})

	t.Run("begin error", func(t *testing.T) {
		store, mock := newMockStore(t, EmbeddingConfig{})
		mock.ExpectBegin().WillReturnError(errors.New("boom"))

		tx, err := store.Begin(ctx)
		if err == nil {
			t.Fatalf("Begin error = nil, want error")
		}
		if tx != nil {
			t.Fatalf("Begin tx = %#v, want nil", tx)
		}
	})
}

func TestPostgresTransactionOpenOperationsDelegate(t *testing.T) {
	ctx := context.Background()
	ts := time.Date(2026, 5, 1, 19, 0, 0, 0, time.UTC)
	store, mock := newMockStore(t, EmbeddingConfig{})

	mock.ExpectBegin()
	rawTx, err := store.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	tx := &postgresTx{tx: rawTx}

	mock.ExpectExec(`DELETE FROM memory_records WHERE id = \$1`).
		WithArgs("rec-delete").
		WillReturnResult(sqlmock.NewResult(0, 1))
	if err := tx.Delete(ctx, "rec-delete"); err != nil {
		t.Fatalf("tx.Delete: %v", err)
	}

	expectGetSemanticRecord(mock, "rec-get", ts)
	got, err := tx.Get(ctx, "rec-get")
	if err != nil {
		t.Fatalf("tx.Get: %v", err)
	}
	if got.ID != "rec-get" {
		t.Fatalf("tx.Get ID = %q, want rec-get", got.ID)
	}

	mock.ExpectQuery(`SELECT id FROM memory_records`).
		WillReturnRows(sqlmock.NewRows([]string{"id"}))
	records, err := tx.List(ctx, storage.ListOptions{})
	if err != nil {
		t.Fatalf("tx.List: %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("tx.List = %#v, want empty", records)
	}

	mock.ExpectQuery(`SELECT id FROM memory_records`).
		WithArgs(string(schema.MemoryTypeSemantic)).
		WillReturnRows(sqlmock.NewRows([]string{"id"}))
	records, err = tx.ListByType(ctx, schema.MemoryTypeSemantic)
	if err != nil {
		t.Fatalf("tx.ListByType: %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("tx.ListByType = %#v, want empty", records)
	}

	mock.ExpectExec(`UPDATE memory_records SET salience = \$1`).
		WithArgs(0.42, sqlmock.AnyArg(), "rec-salience").
		WillReturnResult(sqlmock.NewResult(0, 1))
	if err := tx.UpdateSalience(ctx, "rec-salience", 0.42); err != nil {
		t.Fatalf("tx.UpdateSalience: %v", err)
	}

	entry := schema.AuditEntry{
		Action:    schema.AuditActionRevise,
		Actor:     "tester",
		Timestamp: ts,
		Rationale: "transaction delegate",
	}
	mock.ExpectQuery(`SELECT 1 FROM memory_records WHERE id = \$1`).
		WithArgs("rec-audit").
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(1))
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs("rec-audit", string(entry.Action), entry.Actor, entry.Timestamp.UTC(), entry.Rationale, nil).
		WillReturnResult(sqlmock.NewResult(1, 1))
	if err := tx.AddAuditEntry(ctx, "rec-audit", entry); err != nil {
		t.Fatalf("tx.AddAuditEntry: %v", err)
	}

	rel := schema.Relation{Predicate: "supports", TargetID: "target", Weight: 0.25, CreatedAt: ts}
	mock.ExpectQuery(`SELECT 1 FROM memory_records WHERE id = \$1`).
		WithArgs("rec-rel").
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(1))
	mock.ExpectExec(`INSERT INTO relations`).
		WithArgs("rec-rel", rel.Predicate, rel.TargetID, rel.Weight, rel.CreatedAt.UTC()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	if err := tx.AddRelation(ctx, "rec-rel", rel); err != nil {
		t.Fatalf("tx.AddRelation: %v", err)
	}

	mock.ExpectQuery(`SELECT predicate, target_id, weight, created_at FROM relations`).
		WithArgs("rec-rel").
		WillReturnRows(sqlmock.NewRows([]string{"predicate", "target_id", "weight", "created_at"}))
	mock.ExpectQuery(`SELECT 1 FROM memory_records WHERE id = \$1`).
		WithArgs("rec-rel").
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(1))
	rels, err := tx.GetRelations(ctx, "rec-rel")
	if err != nil {
		t.Fatalf("tx.GetRelations: %v", err)
	}
	if len(rels) != 0 {
		t.Fatalf("tx.GetRelations = %#v, want empty", rels)
	}

	mock.ExpectCommit()
	if err := tx.Commit(); err != nil {
		t.Fatalf("tx.Commit: %v", err)
	}
}

func TestPostgresTransactionClosedGuards(t *testing.T) {
	ctx := context.Background()
	tx := &postgresTx{closed: true}
	rec := newSemanticRecord("closed-rec")
	entry := schema.AuditEntry{Action: schema.AuditActionCreate, Actor: "tester", Timestamp: time.Now().UTC()}
	rel := schema.Relation{Predicate: "related_to", TargetID: "target"}

	if err := tx.Create(ctx, rec); !errors.Is(err, storage.ErrTxClosed) {
		t.Fatalf("Create closed: err = %v, want ErrTxClosed", err)
	}
	if _, err := tx.Get(ctx, rec.ID); !errors.Is(err, storage.ErrTxClosed) {
		t.Fatalf("Get closed: err = %v, want ErrTxClosed", err)
	}
	if err := tx.Update(ctx, rec); !errors.Is(err, storage.ErrTxClosed) {
		t.Fatalf("Update closed: err = %v, want ErrTxClosed", err)
	}
	if err := tx.Delete(ctx, rec.ID); !errors.Is(err, storage.ErrTxClosed) {
		t.Fatalf("Delete closed: err = %v, want ErrTxClosed", err)
	}
	if _, err := tx.List(ctx, storage.ListOptions{}); !errors.Is(err, storage.ErrTxClosed) {
		t.Fatalf("List closed: err = %v, want ErrTxClosed", err)
	}
	if _, err := tx.ListByType(ctx, schema.MemoryTypeSemantic); !errors.Is(err, storage.ErrTxClosed) {
		t.Fatalf("ListByType closed: err = %v, want ErrTxClosed", err)
	}
	if err := tx.UpdateSalience(ctx, rec.ID, 0.5); !errors.Is(err, storage.ErrTxClosed) {
		t.Fatalf("UpdateSalience closed: err = %v, want ErrTxClosed", err)
	}
	if err := tx.AddAuditEntry(ctx, rec.ID, entry); !errors.Is(err, storage.ErrTxClosed) {
		t.Fatalf("AddAuditEntry closed: err = %v, want ErrTxClosed", err)
	}
	if err := tx.AddRelation(ctx, rec.ID, rel); !errors.Is(err, storage.ErrTxClosed) {
		t.Fatalf("AddRelation closed: err = %v, want ErrTxClosed", err)
	}
	if _, err := tx.GetRelations(ctx, rec.ID); !errors.Is(err, storage.ErrTxClosed) {
		t.Fatalf("GetRelations closed: err = %v, want ErrTxClosed", err)
	}
	if err := tx.Commit(); !errors.Is(err, storage.ErrTxClosed) {
		t.Fatalf("Commit closed: err = %v, want ErrTxClosed", err)
	}
	if err := tx.Rollback(); !errors.Is(err, storage.ErrTxClosed) {
		t.Fatalf("Rollback closed: err = %v, want ErrTxClosed", err)
	}
}

func expectGetSemanticRecord(mock sqlmock.Sqlmock, id string, now time.Time) {
	payloadJSON := []byte(`{"kind":"semantic","subject":"Go","predicate":"is","object":"typed","validity":{"mode":"global"}}`)
	mock.ExpectQuery(`SELECT id, type, sensitivity, confidence, salience, scope, created_at, updated_at FROM memory_records WHERE id = \$1`).
		WithArgs(id).
		WillReturnRows(sqlmock.NewRows([]string{"id", "type", "sensitivity", "confidence", "salience", "scope", "created_at", "updated_at"}).
			AddRow(id, string(schema.MemoryTypeSemantic), string(schema.SensitivityLow), 0.9, 0.8, "project", now, now))
	mock.ExpectQuery(`SELECT curve, half_life_seconds, min_salience, max_age_seconds, reinforcement_gain, last_reinforced_at, pinned, deletion_policy FROM decay_profiles WHERE record_id = \$1`).
		WithArgs(id).
		WillReturnRows(sqlmock.NewRows([]string{"curve", "half_life_seconds", "min_salience", "max_age_seconds", "reinforcement_gain", "last_reinforced_at", "pinned", "deletion_policy"}).
			AddRow(string(schema.DecayCurveExponential), int64(3600), 0.1, nil, 0.2, now, false, string(schema.DeletionPolicyAutoPrune)))
	mock.ExpectQuery(`SELECT payload_json FROM payloads WHERE record_id = \$1`).
		WithArgs(id).
		WillReturnRows(sqlmock.NewRows([]string{"payload_json"}).AddRow(payloadJSON))
	mock.ExpectQuery(`SELECT interpretation_json FROM interpretations WHERE record_id = \$1`).
		WithArgs(id).
		WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery(`SELECT tag FROM tags WHERE record_id = \$1`).
		WithArgs(id).
		WillReturnRows(sqlmock.NewRows([]string{"tag"}))
	mock.ExpectQuery(`SELECT kind, ref, hash, created_by, timestamp FROM provenance_sources WHERE record_id = \$1 ORDER BY id`).
		WithArgs(id).
		WillReturnRows(sqlmock.NewRows([]string{"kind", "ref", "hash", "created_by", "timestamp"}))
	mock.ExpectQuery(`SELECT predicate, target_id, weight, created_at FROM relations WHERE source_id = \$1 ORDER BY id`).
		WithArgs(id).
		WillReturnRows(sqlmock.NewRows([]string{"predicate", "target_id", "weight", "created_at"}))
	mock.ExpectQuery(`SELECT action, actor, timestamp, rationale FROM audit_log WHERE record_id = \$1 ORDER BY id`).
		WithArgs(id).
		WillReturnRows(sqlmock.NewRows([]string{"action", "actor", "timestamp", "rationale"}))
}
