// Package sqlite implements the storage.Store interface using SQLite via
// github.com/mattn/go-sqlite3.
package sqlite

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"github.com/GustyCube/membrane/pkg/schema"
	"github.com/GustyCube/membrane/pkg/storage"
)

//go:embed schema.sql
var ddl string

// SQLiteStore implements storage.Store backed by a SQLite database.
type SQLiteStore struct {
	db *sql.DB
}

// Open creates a new SQLiteStore at the given DSN (file path or ":memory:").
// It initializes the database schema on first use.
func Open(dsn string) (*SQLiteStore, error) {
	db, err := sql.Open("sqlite3", dsn+"?_foreign_keys=on&_journal_mode=WAL")
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}

	// Apply schema.
	if _, err := db.Exec(ddl); err != nil {
		db.Close()
		return nil, fmt.Errorf("apply schema: %w", err)
	}

	return &SQLiteStore{db: db}, nil
}

// Close closes the underlying database connection.
func (s *SQLiteStore) Close() error {
	return s.db.Close()
}

// ----------------------------------------------------------------------------
// queryable abstracts *sql.DB and *sql.Tx so CRUD helpers work for both.
// ----------------------------------------------------------------------------

type queryable interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// ----------------------------------------------------------------------------
// Create
// ----------------------------------------------------------------------------

func createRecord(ctx context.Context, q queryable, rec *schema.MemoryRecord) error {
	if err := rec.Validate(); err != nil {
		return err
	}

	now := rec.UpdatedAt.UTC().Format(time.RFC3339Nano)
	createdAt := rec.CreatedAt.UTC().Format(time.RFC3339Nano)

	// Insert base record.
	_, err := q.ExecContext(ctx,
		`INSERT INTO memory_records (id, type, sensitivity, confidence, salience, scope, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		rec.ID, string(rec.Type), string(rec.Sensitivity),
		rec.Confidence, rec.Salience, nullableString(rec.Scope),
		createdAt, now,
	)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return storage.ErrAlreadyExists
		}
		return fmt.Errorf("insert memory_records: %w", err)
	}

	// Decay profile.
	pinnedInt := 0
	if rec.Lifecycle.Pinned {
		pinnedInt = 1
	}
	dp := rec.Lifecycle.Decay
	delPolicy := string(rec.Lifecycle.DeletionPolicy)
	if delPolicy == "" {
		delPolicy = string(schema.DeletionPolicyAutoPrune)
	}
	_, err = q.ExecContext(ctx,
		`INSERT INTO decay_profiles (record_id, curve, half_life_seconds, min_salience, max_age_seconds, reinforcement_gain, last_reinforced_at, pinned, deletion_policy)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		rec.ID, string(dp.Curve), dp.HalfLifeSeconds, dp.MinSalience,
		nullableInt64(dp.MaxAgeSeconds),
		dp.ReinforcementGain,
		rec.Lifecycle.LastReinforcedAt.UTC().Format(time.RFC3339Nano),
		pinnedInt, delPolicy,
	)
	if err != nil {
		return fmt.Errorf("insert decay_profiles: %w", err)
	}

	// Payload as JSON.
	payloadJSON, err := json.Marshal(rec.Payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}
	if _, err := q.ExecContext(ctx,
		`INSERT INTO payloads (record_id, payload_json) VALUES (?, ?)`,
		rec.ID, string(payloadJSON),
	); err != nil {
		return fmt.Errorf("insert payloads: %w", err)
	}

	// Tags.
	for _, tag := range rec.Tags {
		if _, err := q.ExecContext(ctx,
			`INSERT INTO tags (record_id, tag) VALUES (?, ?)`,
			rec.ID, tag,
		); err != nil {
			return fmt.Errorf("insert tag: %w", err)
		}
	}

	// Provenance sources.
	for _, src := range rec.Provenance.Sources {
		if _, err := q.ExecContext(ctx,
			`INSERT INTO provenance_sources (record_id, kind, ref, hash, created_by, timestamp)
			 VALUES (?, ?, ?, ?, ?, ?)`,
			rec.ID, string(src.Kind), src.Ref,
			nullableString(src.Hash), nullableString(src.CreatedBy),
			src.Timestamp.UTC().Format(time.RFC3339Nano),
		); err != nil {
			return fmt.Errorf("insert provenance_sources: %w", err)
		}
	}

	// Relations.
	for _, rel := range rec.Relations {
		w := rel.Weight
		if w == 0 {
			w = 1.0
		}
		if _, err := q.ExecContext(ctx,
			`INSERT INTO relations (source_id, predicate, target_id, weight, created_at)
			 VALUES (?, ?, ?, ?, ?)`,
			rec.ID, rel.Predicate, rel.TargetID, w,
			rel.CreatedAt.UTC().Format(time.RFC3339Nano),
		); err != nil {
			return fmt.Errorf("insert relations: %w", err)
		}
	}

	// Audit log entries.
	for _, entry := range rec.AuditLog {
		if _, err := q.ExecContext(ctx,
			`INSERT INTO audit_log (record_id, action, actor, timestamp, rationale, previous_state_json)
			 VALUES (?, ?, ?, ?, ?, ?)`,
			rec.ID, string(entry.Action), entry.Actor,
			entry.Timestamp.UTC().Format(time.RFC3339Nano),
			entry.Rationale, nil,
		); err != nil {
			return fmt.Errorf("insert audit_log: %w", err)
		}
	}

	// Competence stats (if applicable).
	if cp, ok := rec.Payload.(*schema.CompetencePayload); ok && cp.Performance != nil {
		if _, err := q.ExecContext(ctx,
			`INSERT INTO competence_stats (record_id, success_count, failure_count) VALUES (?, ?, ?)`,
			rec.ID, cp.Performance.SuccessCount, cp.Performance.FailureCount,
		); err != nil {
			return fmt.Errorf("insert competence_stats: %w", err)
		}
	}

	return nil
}

func (s *SQLiteStore) Create(ctx context.Context, rec *schema.MemoryRecord) error {
	return createRecord(ctx, s.db, rec)
}

// ----------------------------------------------------------------------------
// Get
// ----------------------------------------------------------------------------

func getRecord(ctx context.Context, q queryable, id string) (*schema.MemoryRecord, error) {
	rec := &schema.MemoryRecord{}

	// Base record.
	var scope sql.NullString
	var createdAt, updatedAt string
	err := q.QueryRowContext(ctx,
		`SELECT id, type, sensitivity, confidence, salience, scope, created_at, updated_at
		 FROM memory_records WHERE id = ?`, id,
	).Scan(&rec.ID, &rec.Type, &rec.Sensitivity, &rec.Confidence, &rec.Salience,
		&scope, &createdAt, &updatedAt)
	if err == sql.ErrNoRows {
		return nil, storage.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("query memory_records: %w", err)
	}
	rec.Scope = scope.String
	rec.CreatedAt, _ = time.Parse(time.RFC3339Nano, createdAt)
	rec.UpdatedAt, _ = time.Parse(time.RFC3339Nano, updatedAt)

	// Decay profile.
	var lastReinforced string
	var pinnedInt int
	var maxAge sql.NullInt64
	err = q.QueryRowContext(ctx,
		`SELECT curve, half_life_seconds, min_salience, max_age_seconds, reinforcement_gain,
		        last_reinforced_at, pinned, deletion_policy
		 FROM decay_profiles WHERE record_id = ?`, id,
	).Scan(&rec.Lifecycle.Decay.Curve, &rec.Lifecycle.Decay.HalfLifeSeconds,
		&rec.Lifecycle.Decay.MinSalience, &maxAge,
		&rec.Lifecycle.Decay.ReinforcementGain, &lastReinforced,
		&pinnedInt, &rec.Lifecycle.DeletionPolicy)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("query decay_profiles: %w", err)
	}
	rec.Lifecycle.LastReinforcedAt, _ = time.Parse(time.RFC3339Nano, lastReinforced)
	rec.Lifecycle.Pinned = pinnedInt != 0
	if maxAge.Valid {
		rec.Lifecycle.Decay.MaxAgeSeconds = maxAge.Int64
	}

	// Payload.
	var payloadJSON string
	err = q.QueryRowContext(ctx,
		`SELECT payload_json FROM payloads WHERE record_id = ?`, id,
	).Scan(&payloadJSON)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("query payloads: %w", err)
	}
	if payloadJSON != "" {
		var wrapper schema.PayloadWrapper
		if err := wrapper.UnmarshalJSON([]byte(payloadJSON)); err != nil {
			return nil, fmt.Errorf("unmarshal payload: %w", err)
		}
		rec.Payload = wrapper.Payload
	}

	// Tags.
	tagRows, err := q.QueryContext(ctx,
		`SELECT tag FROM tags WHERE record_id = ?`, id)
	if err != nil {
		return nil, fmt.Errorf("query tags: %w", err)
	}
	defer tagRows.Close()
	for tagRows.Next() {
		var tag string
		if err := tagRows.Scan(&tag); err != nil {
			return nil, fmt.Errorf("scan tag: %w", err)
		}
		rec.Tags = append(rec.Tags, tag)
	}

	// Provenance sources.
	provRows, err := q.QueryContext(ctx,
		`SELECT kind, ref, hash, created_by, timestamp
		 FROM provenance_sources WHERE record_id = ? ORDER BY id`, id)
	if err != nil {
		return nil, fmt.Errorf("query provenance_sources: %w", err)
	}
	defer provRows.Close()
	rec.Provenance.Sources = []schema.ProvenanceSource{}
	for provRows.Next() {
		var src schema.ProvenanceSource
		var hash, createdBy sql.NullString
		var ts string
		if err := provRows.Scan(&src.Kind, &src.Ref, &hash, &createdBy, &ts); err != nil {
			return nil, fmt.Errorf("scan provenance_source: %w", err)
		}
		src.Hash = hash.String
		src.CreatedBy = createdBy.String
		src.Timestamp, _ = time.Parse(time.RFC3339Nano, ts)
		rec.Provenance.Sources = append(rec.Provenance.Sources, src)
	}

	// Relations.
	rec.Relations, err = getRelations(ctx, q, id)
	if err != nil {
		return nil, err
	}

	// Audit log.
	auditRows, err := q.QueryContext(ctx,
		`SELECT action, actor, timestamp, rationale
		 FROM audit_log WHERE record_id = ? ORDER BY id`, id)
	if err != nil {
		return nil, fmt.Errorf("query audit_log: %w", err)
	}
	defer auditRows.Close()
	rec.AuditLog = []schema.AuditEntry{}
	for auditRows.Next() {
		var entry schema.AuditEntry
		var ts string
		if err := auditRows.Scan(&entry.Action, &entry.Actor, &ts, &entry.Rationale); err != nil {
			return nil, fmt.Errorf("scan audit_log: %w", err)
		}
		entry.Timestamp, _ = time.Parse(time.RFC3339Nano, ts)
		rec.AuditLog = append(rec.AuditLog, entry)
	}

	return rec, nil
}

func (s *SQLiteStore) Get(ctx context.Context, id string) (*schema.MemoryRecord, error) {
	return getRecord(ctx, s.db, id)
}

// ----------------------------------------------------------------------------
// Update
// ----------------------------------------------------------------------------

func updateRecord(ctx context.Context, q queryable, rec *schema.MemoryRecord) error {
	if err := rec.Validate(); err != nil {
		return err
	}

	now := rec.UpdatedAt.UTC().Format(time.RFC3339Nano)

	res, err := q.ExecContext(ctx,
		`UPDATE memory_records SET type=?, sensitivity=?, confidence=?, salience=?, scope=?, updated_at=?
		 WHERE id=?`,
		string(rec.Type), string(rec.Sensitivity), rec.Confidence, rec.Salience,
		nullableString(rec.Scope), now, rec.ID,
	)
	if err != nil {
		return fmt.Errorf("update memory_records: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return storage.ErrNotFound
	}

	// Update decay profile.
	pinnedInt := 0
	if rec.Lifecycle.Pinned {
		pinnedInt = 1
	}
	dp := rec.Lifecycle.Decay
	delPolicy := string(rec.Lifecycle.DeletionPolicy)
	if delPolicy == "" {
		delPolicy = string(schema.DeletionPolicyAutoPrune)
	}
	if _, err := q.ExecContext(ctx,
		`INSERT OR REPLACE INTO decay_profiles
		 (record_id, curve, half_life_seconds, min_salience, max_age_seconds, reinforcement_gain, last_reinforced_at, pinned, deletion_policy)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		rec.ID, string(dp.Curve), dp.HalfLifeSeconds, dp.MinSalience,
		nullableInt64(dp.MaxAgeSeconds), dp.ReinforcementGain,
		rec.Lifecycle.LastReinforcedAt.UTC().Format(time.RFC3339Nano),
		pinnedInt, delPolicy,
	); err != nil {
		return fmt.Errorf("upsert decay_profiles: %w", err)
	}

	// Update payload.
	payloadJSON, err := json.Marshal(rec.Payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}
	if _, err := q.ExecContext(ctx,
		`INSERT OR REPLACE INTO payloads (record_id, payload_json) VALUES (?, ?)`,
		rec.ID, string(payloadJSON),
	); err != nil {
		return fmt.Errorf("upsert payloads: %w", err)
	}

	// Replace tags.
	if _, err := q.ExecContext(ctx, `DELETE FROM tags WHERE record_id = ?`, rec.ID); err != nil {
		return fmt.Errorf("delete tags: %w", err)
	}
	for _, tag := range rec.Tags {
		if _, err := q.ExecContext(ctx,
			`INSERT INTO tags (record_id, tag) VALUES (?, ?)`,
			rec.ID, tag,
		); err != nil {
			return fmt.Errorf("insert tag: %w", err)
		}
	}

	// Replace provenance sources.
	if _, err := q.ExecContext(ctx, `DELETE FROM provenance_sources WHERE record_id = ?`, rec.ID); err != nil {
		return fmt.Errorf("delete provenance_sources: %w", err)
	}
	for _, src := range rec.Provenance.Sources {
		if _, err := q.ExecContext(ctx,
			`INSERT INTO provenance_sources (record_id, kind, ref, hash, created_by, timestamp)
			 VALUES (?, ?, ?, ?, ?, ?)`,
			rec.ID, string(src.Kind), src.Ref,
			nullableString(src.Hash), nullableString(src.CreatedBy),
			src.Timestamp.UTC().Format(time.RFC3339Nano),
		); err != nil {
			return fmt.Errorf("insert provenance_sources: %w", err)
		}
	}

	return nil
}

func (s *SQLiteStore) Update(ctx context.Context, rec *schema.MemoryRecord) error {
	return updateRecord(ctx, s.db, rec)
}

// ----------------------------------------------------------------------------
// Delete
// ----------------------------------------------------------------------------

func deleteRecord(ctx context.Context, q queryable, id string) error {
	res, err := q.ExecContext(ctx, `DELETE FROM memory_records WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("delete memory_records: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return storage.ErrNotFound
	}
	return nil
}

func (s *SQLiteStore) Delete(ctx context.Context, id string) error {
	return deleteRecord(ctx, s.db, id)
}

// ----------------------------------------------------------------------------
// List / ListByType
// ----------------------------------------------------------------------------

func listRecords(ctx context.Context, q queryable, opts storage.ListOptions) ([]*schema.MemoryRecord, error) {
	query := `SELECT id FROM memory_records WHERE 1=1`
	args := []any{}

	if opts.Type != "" {
		query += ` AND type = ?`
		args = append(args, string(opts.Type))
	}
	if opts.Scope != "" {
		query += ` AND scope = ?`
		args = append(args, opts.Scope)
	}
	if opts.Sensitivity != "" {
		query += ` AND sensitivity = ?`
		args = append(args, string(opts.Sensitivity))
	}
	if opts.MinSalience > 0 {
		query += ` AND salience >= ?`
		args = append(args, opts.MinSalience)
	}
	if opts.MaxSalience > 0 {
		query += ` AND salience <= ?`
		args = append(args, opts.MaxSalience)
	}

	// Tag filtering: record must have ALL specified tags.
	for i, tag := range opts.Tags {
		alias := fmt.Sprintf("t%d", i)
		query += fmt.Sprintf(` AND EXISTS (SELECT 1 FROM tags %s WHERE %s.record_id = memory_records.id AND %s.tag = ?)`, alias, alias, alias)
		args = append(args, tag)
	}

	query += ` ORDER BY salience DESC, created_at DESC`

	if opts.Limit > 0 {
		query += ` LIMIT ?`
		args = append(args, opts.Limit)
	}
	if opts.Offset > 0 {
		query += ` OFFSET ?`
		args = append(args, opts.Offset)
	}

	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list query: %w", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan id: %w", err)
		}
		ids = append(ids, id)
	}

	records := make([]*schema.MemoryRecord, 0, len(ids))
	for _, id := range ids {
		rec, err := getRecord(ctx, q, id)
		if err != nil {
			return nil, err
		}
		records = append(records, rec)
	}

	return records, nil
}

func (s *SQLiteStore) List(ctx context.Context, opts storage.ListOptions) ([]*schema.MemoryRecord, error) {
	return listRecords(ctx, s.db, opts)
}

func (s *SQLiteStore) ListByType(ctx context.Context, memType schema.MemoryType) ([]*schema.MemoryRecord, error) {
	return s.List(ctx, storage.ListOptions{Type: memType})
}

// ----------------------------------------------------------------------------
// UpdateSalience
// ----------------------------------------------------------------------------

func updateSalience(ctx context.Context, q queryable, id string, salience float64) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	res, err := q.ExecContext(ctx,
		`UPDATE memory_records SET salience = ?, updated_at = ? WHERE id = ?`,
		salience, now, id,
	)
	if err != nil {
		return fmt.Errorf("update salience: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return storage.ErrNotFound
	}
	return nil
}

func (s *SQLiteStore) UpdateSalience(ctx context.Context, id string, salience float64) error {
	return updateSalience(ctx, s.db, id, salience)
}

// ----------------------------------------------------------------------------
// AddAuditEntry
// ----------------------------------------------------------------------------

func addAuditEntry(ctx context.Context, q queryable, id string, entry schema.AuditEntry) error {
	// Verify record exists.
	var exists int
	err := q.QueryRowContext(ctx, `SELECT 1 FROM memory_records WHERE id = ?`, id).Scan(&exists)
	if err == sql.ErrNoRows {
		return storage.ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("check record existence: %w", err)
	}

	_, err = q.ExecContext(ctx,
		`INSERT INTO audit_log (record_id, action, actor, timestamp, rationale, previous_state_json)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		id, string(entry.Action), entry.Actor,
		entry.Timestamp.UTC().Format(time.RFC3339Nano),
		entry.Rationale, nil,
	)
	if err != nil {
		return fmt.Errorf("insert audit_log: %w", err)
	}
	return nil
}

func (s *SQLiteStore) AddAuditEntry(ctx context.Context, id string, entry schema.AuditEntry) error {
	return addAuditEntry(ctx, s.db, id, entry)
}

// ----------------------------------------------------------------------------
// AddRelation / GetRelations
// ----------------------------------------------------------------------------

func addRelation(ctx context.Context, q queryable, sourceID string, rel schema.Relation) error {
	// Verify source record exists.
	var exists int
	err := q.QueryRowContext(ctx, `SELECT 1 FROM memory_records WHERE id = ?`, sourceID).Scan(&exists)
	if err == sql.ErrNoRows {
		return storage.ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("check record existence: %w", err)
	}

	w := rel.Weight
	if w == 0 {
		w = 1.0
	}
	ca := rel.CreatedAt
	if ca.IsZero() {
		ca = time.Now().UTC()
	}

	_, err = q.ExecContext(ctx,
		`INSERT INTO relations (source_id, predicate, target_id, weight, created_at)
		 VALUES (?, ?, ?, ?, ?)`,
		sourceID, rel.Predicate, rel.TargetID, w,
		ca.UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		return fmt.Errorf("insert relations: %w", err)
	}
	return nil
}

func getRelations(ctx context.Context, q queryable, id string) ([]schema.Relation, error) {
	rows, err := q.QueryContext(ctx,
		`SELECT predicate, target_id, weight, created_at
		 FROM relations WHERE source_id = ? ORDER BY id`, id)
	if err != nil {
		return nil, fmt.Errorf("query relations: %w", err)
	}
	defer rows.Close()

	var rels []schema.Relation
	for rows.Next() {
		var rel schema.Relation
		var w sql.NullFloat64
		var ca string
		if err := rows.Scan(&rel.Predicate, &rel.TargetID, &w, &ca); err != nil {
			return nil, fmt.Errorf("scan relation: %w", err)
		}
		if w.Valid {
			rel.Weight = w.Float64
		}
		rel.CreatedAt, _ = time.Parse(time.RFC3339Nano, ca)
		rels = append(rels, rel)
	}
	if rels == nil {
		rels = []schema.Relation{}
	}
	return rels, nil
}

func (s *SQLiteStore) AddRelation(ctx context.Context, sourceID string, rel schema.Relation) error {
	return addRelation(ctx, s.db, sourceID, rel)
}

func (s *SQLiteStore) GetRelations(ctx context.Context, id string) ([]schema.Relation, error) {
	return getRelations(ctx, s.db, id)
}

// ----------------------------------------------------------------------------
// Transactions
// ----------------------------------------------------------------------------

// Begin starts a new database transaction.
func (s *SQLiteStore) Begin(ctx context.Context) (storage.Transaction, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	return &sqliteTx{tx: tx}, nil
}

// sqliteTx implements storage.Transaction.
type sqliteTx struct {
	tx     *sql.Tx
	closed bool
}

func (t *sqliteTx) checkClosed() error {
	if t.closed {
		return storage.ErrTxClosed
	}
	return nil
}

func (t *sqliteTx) Create(ctx context.Context, rec *schema.MemoryRecord) error {
	if err := t.checkClosed(); err != nil {
		return err
	}
	return createRecord(ctx, t.tx, rec)
}

func (t *sqliteTx) Get(ctx context.Context, id string) (*schema.MemoryRecord, error) {
	if err := t.checkClosed(); err != nil {
		return nil, err
	}
	return getRecord(ctx, t.tx, id)
}

func (t *sqliteTx) Update(ctx context.Context, rec *schema.MemoryRecord) error {
	if err := t.checkClosed(); err != nil {
		return err
	}
	return updateRecord(ctx, t.tx, rec)
}

func (t *sqliteTx) Delete(ctx context.Context, id string) error {
	if err := t.checkClosed(); err != nil {
		return err
	}
	return deleteRecord(ctx, t.tx, id)
}

func (t *sqliteTx) List(ctx context.Context, opts storage.ListOptions) ([]*schema.MemoryRecord, error) {
	if err := t.checkClosed(); err != nil {
		return nil, err
	}
	return listRecords(ctx, t.tx, opts)
}

func (t *sqliteTx) ListByType(ctx context.Context, memType schema.MemoryType) ([]*schema.MemoryRecord, error) {
	if err := t.checkClosed(); err != nil {
		return nil, err
	}
	return listRecords(ctx, t.tx, storage.ListOptions{Type: memType})
}

func (t *sqliteTx) UpdateSalience(ctx context.Context, id string, salience float64) error {
	if err := t.checkClosed(); err != nil {
		return err
	}
	return updateSalience(ctx, t.tx, id, salience)
}

func (t *sqliteTx) AddAuditEntry(ctx context.Context, id string, entry schema.AuditEntry) error {
	if err := t.checkClosed(); err != nil {
		return err
	}
	return addAuditEntry(ctx, t.tx, id, entry)
}

func (t *sqliteTx) AddRelation(ctx context.Context, sourceID string, rel schema.Relation) error {
	if err := t.checkClosed(); err != nil {
		return err
	}
	return addRelation(ctx, t.tx, sourceID, rel)
}

func (t *sqliteTx) GetRelations(ctx context.Context, id string) ([]schema.Relation, error) {
	if err := t.checkClosed(); err != nil {
		return nil, err
	}
	return getRelations(ctx, t.tx, id)
}

func (t *sqliteTx) Commit() error {
	if err := t.checkClosed(); err != nil {
		return err
	}
	t.closed = true
	return t.tx.Commit()
}

func (t *sqliteTx) Rollback() error {
	if err := t.checkClosed(); err != nil {
		return err
	}
	t.closed = true
	return t.tx.Rollback()
}

// ----------------------------------------------------------------------------
// Helpers
// ----------------------------------------------------------------------------

func nullableString(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func nullableInt64(v int64) any {
	if v == 0 {
		return nil
	}
	return v
}

// Compile-time interface checks.
var (
	_ storage.Store       = (*SQLiteStore)(nil)
	_ storage.Transaction = (*sqliteTx)(nil)
)
