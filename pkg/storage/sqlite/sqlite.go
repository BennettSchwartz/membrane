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

	_ "github.com/mutecomm/go-sqlcipher/v4"

	"github.com/BennettSchwartz/membrane/pkg/schema"
	"github.com/BennettSchwartz/membrane/pkg/storage"
)

//go:embed schema.sql
var ddl string

var sqlOpen = sql.Open

// SQLiteStore implements storage.Store backed by a SQLite database.
type SQLiteStore struct {
	db *sql.DB
}

// Open creates a new SQLiteStore at the given DSN (file path or ":memory:").
// If encryptionKey is non-empty, the database is encrypted using SQLCipher.
// It initializes the database schema on first use.
func Open(dsn string, encryptionKey string) (*SQLiteStore, error) {
	db, err := sqlOpen("sqlite3", dsnWithOptions(dsn))
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}

	if encryptionKey != "" {
		escapedKey := strings.ReplaceAll(encryptionKey, "'", "''")
		if _, err := db.Exec("PRAGMA key = '" + escapedKey + "'"); err != nil {
			db.Close()
			return nil, fmt.Errorf("set encryption key: %w", err)
		}
	}

	if encryptionKey != "" {
		// Verify encryption key works.
		if _, err := db.Exec("SELECT count(*) FROM sqlite_master"); err != nil {
			db.Close()
			return nil, fmt.Errorf("encryption key verification failed: %w", err)
		}
	}

	// Apply schema.
	if _, err := db.Exec(ddl); err != nil {
		db.Close()
		return nil, fmt.Errorf("apply schema: %w", err)
	}

	return &SQLiteStore{db: db}, nil
}

func dsnWithOptions(dsn string) string {
	separator := "?"
	if strings.Contains(dsn, "?") {
		separator = "&"
	}
	return dsn + separator + "_foreign_keys=on&_journal_mode=WAL&_busy_timeout=5000"
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

	if rec.Interpretation != nil {
		interpretationJSON, err := json.Marshal(rec.Interpretation)
		if err != nil {
			return fmt.Errorf("marshal interpretation: %w", err)
		}
		if _, err := q.ExecContext(ctx,
			`INSERT INTO interpretations (record_id, interpretation_json) VALUES (?, ?)`,
			rec.ID, string(interpretationJSON),
		); err != nil {
			return fmt.Errorf("insert interpretations: %w", err)
		}
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
			 VALUES (?, ?, ?, ?, ?)
			 ON CONFLICT(source_id, predicate, target_id) DO UPDATE SET
			 weight = excluded.weight,
			 created_at = excluded.created_at`,
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

	if err := replaceEntityIndexes(ctx, q, rec); err != nil {
		return err
	}

	return nil
}

func (s *SQLiteStore) Create(ctx context.Context, rec *schema.MemoryRecord) error {
	if err := rec.Validate(); err != nil {
		return err
	}
	return storage.WithTransaction(ctx, s, func(tx storage.Transaction) error {
		return tx.Create(ctx, rec)
	})
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

	var interpretationJSON string
	err = q.QueryRowContext(ctx,
		`SELECT interpretation_json FROM interpretations WHERE record_id = ?`, id,
	).Scan(&interpretationJSON)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("query interpretations: %w", err)
	}
	if interpretationJSON != "" {
		var interpretation schema.Interpretation
		if err := json.Unmarshal([]byte(interpretationJSON), &interpretation); err != nil {
			return nil, fmt.Errorf("unmarshal interpretation: %w", err)
		}
		rec.Interpretation = &interpretation
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
	if err := tagRows.Err(); err != nil {
		return nil, fmt.Errorf("iterate tags: %w", err)
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
	if err := provRows.Err(); err != nil {
		return nil, fmt.Errorf("iterate provenance_sources: %w", err)
	}

	// Relations.
	rec.Relations, err = getRelations(ctx, q, id, false)
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
	if err := auditRows.Err(); err != nil {
		return nil, fmt.Errorf("iterate audit_log: %w", err)
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

	if _, err := q.ExecContext(ctx, `DELETE FROM interpretations WHERE record_id = ?`, rec.ID); err != nil {
		return fmt.Errorf("delete interpretations: %w", err)
	}
	if rec.Interpretation != nil {
		interpretationJSON, err := json.Marshal(rec.Interpretation)
		if err != nil {
			return fmt.Errorf("marshal interpretation: %w", err)
		}
		if _, err := q.ExecContext(ctx,
			`INSERT INTO interpretations (record_id, interpretation_json) VALUES (?, ?)`,
			rec.ID, string(interpretationJSON),
		); err != nil {
			return fmt.Errorf("insert interpretations: %w", err)
		}
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

	// Replace relations (prevents orphaned relations on update).
	if _, err := q.ExecContext(ctx, `DELETE FROM relations WHERE source_id = ?`, rec.ID); err != nil {
		return fmt.Errorf("delete relations: %w", err)
	}
	for _, rel := range rec.Relations {
		w := rel.Weight
		if w == 0 {
			w = 1.0
		}
		ca := rel.CreatedAt
		if ca.IsZero() {
			ca = time.Now().UTC()
		}
		if _, err := q.ExecContext(ctx,
			`INSERT INTO relations (source_id, predicate, target_id, weight, created_at)
			 VALUES (?, ?, ?, ?, ?)
			 ON CONFLICT(source_id, predicate, target_id) DO UPDATE SET
			 weight = excluded.weight,
			 created_at = excluded.created_at`,
			rec.ID, rel.Predicate, rel.TargetID, w,
			ca.UTC().Format(time.RFC3339Nano),
		); err != nil {
			return fmt.Errorf("insert relations: %w", err)
		}
	}

	if err := replaceEntityIndexes(ctx, q, rec); err != nil {
		return err
	}

	return nil
}

func (s *SQLiteStore) Update(ctx context.Context, rec *schema.MemoryRecord) error {
	if err := rec.Validate(); err != nil {
		return err
	}
	return storage.WithTransaction(ctx, s, func(tx storage.Transaction) error {
		return tx.Update(ctx, rec)
	})
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
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate ids: %w", err)
	}

	return getRecordsBatch(ctx, q, ids)
}

// getRecordsBatch fetches multiple records by ID in a single pass per table,
// avoiding the N+1 query problem that occurs when calling getRecord per ID.
func getRecordsBatch(ctx context.Context, q queryable, ids []string) ([]*schema.MemoryRecord, error) {
	if len(ids) == 0 {
		return []*schema.MemoryRecord{}, nil
	}

	// For small batches, fall back to individual fetches to keep things simple.
	if len(ids) <= 3 {
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

	// Build placeholder string and args for IN clause.
	placeholders := strings.Repeat("?,", len(ids))
	placeholders = placeholders[:len(placeholders)-1] // trim trailing comma
	idArgs := make([]any, len(ids))
	for i, id := range ids {
		idArgs[i] = id
	}

	// Index records by ID for ordered output.
	recMap := make(map[string]*schema.MemoryRecord, len(ids))

	// 1. Fetch base records.
	baseQuery := fmt.Sprintf(
		`SELECT id, type, sensitivity, confidence, salience, scope, created_at, updated_at
		 FROM memory_records WHERE id IN (%s)`, placeholders)
	baseRows, err := q.QueryContext(ctx, baseQuery, idArgs...)
	if err != nil {
		return nil, fmt.Errorf("batch query memory_records: %w", err)
	}
	defer baseRows.Close()
	for baseRows.Next() {
		rec := &schema.MemoryRecord{}
		var scope sql.NullString
		var createdAt, updatedAt string
		if err := baseRows.Scan(&rec.ID, &rec.Type, &rec.Sensitivity, &rec.Confidence, &rec.Salience,
			&scope, &createdAt, &updatedAt); err != nil {
			return nil, fmt.Errorf("batch scan memory_records: %w", err)
		}
		rec.Scope = scope.String
		rec.CreatedAt, _ = time.Parse(time.RFC3339Nano, createdAt)
		rec.UpdatedAt, _ = time.Parse(time.RFC3339Nano, updatedAt)
		rec.Provenance.Sources = []schema.ProvenanceSource{}
		rec.Relations = []schema.Relation{}
		rec.AuditLog = []schema.AuditEntry{}
		recMap[rec.ID] = rec
	}
	if err := baseRows.Err(); err != nil {
		return nil, fmt.Errorf("batch iterate memory_records: %w", err)
	}

	// 2. Fetch decay profiles.
	dpQuery := fmt.Sprintf(
		`SELECT record_id, curve, half_life_seconds, min_salience, max_age_seconds, reinforcement_gain,
		        last_reinforced_at, pinned, deletion_policy
		 FROM decay_profiles WHERE record_id IN (%s)`, placeholders)
	dpRows, err := q.QueryContext(ctx, dpQuery, idArgs...)
	if err != nil {
		return nil, fmt.Errorf("batch query decay_profiles: %w", err)
	}
	defer dpRows.Close()
	for dpRows.Next() {
		var recordID, lastReinforced string
		var pinnedInt int
		var maxAge sql.NullInt64
		var curve schema.DecayCurve
		var halfLife int64
		var minSalience, gain float64
		var delPolicy schema.DeletionPolicy
		if err := dpRows.Scan(&recordID, &curve, &halfLife, &minSalience, &maxAge, &gain,
			&lastReinforced, &pinnedInt, &delPolicy); err != nil {
			return nil, fmt.Errorf("batch scan decay_profiles: %w", err)
		}
		if rec, ok := recMap[recordID]; ok {
			rec.Lifecycle.Decay.Curve = curve
			rec.Lifecycle.Decay.HalfLifeSeconds = halfLife
			rec.Lifecycle.Decay.MinSalience = minSalience
			rec.Lifecycle.Decay.ReinforcementGain = gain
			rec.Lifecycle.LastReinforcedAt, _ = time.Parse(time.RFC3339Nano, lastReinforced)
			rec.Lifecycle.Pinned = pinnedInt != 0
			rec.Lifecycle.DeletionPolicy = delPolicy
			if maxAge.Valid {
				rec.Lifecycle.Decay.MaxAgeSeconds = maxAge.Int64
			}
		}
	}
	if err := dpRows.Err(); err != nil {
		return nil, fmt.Errorf("batch iterate decay_profiles: %w", err)
	}

	// 3. Fetch payloads.
	plQuery := fmt.Sprintf(
		`SELECT record_id, payload_json FROM payloads WHERE record_id IN (%s)`, placeholders)
	plRows, err := q.QueryContext(ctx, plQuery, idArgs...)
	if err != nil {
		return nil, fmt.Errorf("batch query payloads: %w", err)
	}
	defer plRows.Close()
	for plRows.Next() {
		var recordID, payloadJSON string
		if err := plRows.Scan(&recordID, &payloadJSON); err != nil {
			return nil, fmt.Errorf("batch scan payloads: %w", err)
		}
		if rec, ok := recMap[recordID]; ok && payloadJSON != "" {
			var wrapper schema.PayloadWrapper
			if err := wrapper.UnmarshalJSON([]byte(payloadJSON)); err != nil {
				return nil, fmt.Errorf("unmarshal payload for %s: %w", recordID, err)
			}
			rec.Payload = wrapper.Payload
		}
	}
	if err := plRows.Err(); err != nil {
		return nil, fmt.Errorf("batch iterate payloads: %w", err)
	}

	// 4. Fetch tags.
	intQuery := fmt.Sprintf(
		`SELECT record_id, interpretation_json FROM interpretations WHERE record_id IN (%s)`, placeholders)
	intRows, err := q.QueryContext(ctx, intQuery, idArgs...)
	if err != nil {
		return nil, fmt.Errorf("batch query interpretations: %w", err)
	}
	defer intRows.Close()
	for intRows.Next() {
		var recordID, interpretationJSON string
		if err := intRows.Scan(&recordID, &interpretationJSON); err != nil {
			return nil, fmt.Errorf("batch scan interpretations: %w", err)
		}
		if rec, ok := recMap[recordID]; ok && interpretationJSON != "" {
			var interpretation schema.Interpretation
			if err := json.Unmarshal([]byte(interpretationJSON), &interpretation); err != nil {
				return nil, fmt.Errorf("unmarshal interpretation for %s: %w", recordID, err)
			}
			rec.Interpretation = &interpretation
		}
	}
	if err := intRows.Err(); err != nil {
		return nil, fmt.Errorf("batch iterate interpretations: %w", err)
	}

	// 4. Fetch tags.
	tagQuery := fmt.Sprintf(
		`SELECT record_id, tag FROM tags WHERE record_id IN (%s)`, placeholders)
	tagRows, err := q.QueryContext(ctx, tagQuery, idArgs...)
	if err != nil {
		return nil, fmt.Errorf("batch query tags: %w", err)
	}
	defer tagRows.Close()
	for tagRows.Next() {
		var recordID, tag string
		if err := tagRows.Scan(&recordID, &tag); err != nil {
			return nil, fmt.Errorf("batch scan tags: %w", err)
		}
		if rec, ok := recMap[recordID]; ok {
			rec.Tags = append(rec.Tags, tag)
		}
	}
	if err := tagRows.Err(); err != nil {
		return nil, fmt.Errorf("batch iterate tags: %w", err)
	}

	// 5. Fetch provenance sources.
	provQuery := fmt.Sprintf(
		`SELECT record_id, kind, ref, hash, created_by, timestamp
		 FROM provenance_sources WHERE record_id IN (%s) ORDER BY id`, placeholders)
	provRows, err := q.QueryContext(ctx, provQuery, idArgs...)
	if err != nil {
		return nil, fmt.Errorf("batch query provenance_sources: %w", err)
	}
	defer provRows.Close()
	for provRows.Next() {
		var recordID string
		var src schema.ProvenanceSource
		var hash, createdBy sql.NullString
		var ts string
		if err := provRows.Scan(&recordID, &src.Kind, &src.Ref, &hash, &createdBy, &ts); err != nil {
			return nil, fmt.Errorf("batch scan provenance_sources: %w", err)
		}
		src.Hash = hash.String
		src.CreatedBy = createdBy.String
		src.Timestamp, _ = time.Parse(time.RFC3339Nano, ts)
		if rec, ok := recMap[recordID]; ok {
			rec.Provenance.Sources = append(rec.Provenance.Sources, src)
		}
	}
	if err := provRows.Err(); err != nil {
		return nil, fmt.Errorf("batch iterate provenance_sources: %w", err)
	}

	// 6. Fetch relations.
	relQuery := fmt.Sprintf(
		`SELECT source_id, predicate, target_id, weight, created_at
		 FROM relations WHERE source_id IN (%s) ORDER BY id`, placeholders)
	relRows, err := q.QueryContext(ctx, relQuery, idArgs...)
	if err != nil {
		return nil, fmt.Errorf("batch query relations: %w", err)
	}
	defer relRows.Close()
	for relRows.Next() {
		var recordID string
		var rel schema.Relation
		var w sql.NullFloat64
		var ca string
		if err := relRows.Scan(&recordID, &rel.Predicate, &rel.TargetID, &w, &ca); err != nil {
			return nil, fmt.Errorf("batch scan relations: %w", err)
		}
		if w.Valid {
			rel.Weight = w.Float64
		}
		rel.CreatedAt, _ = time.Parse(time.RFC3339Nano, ca)
		if rec, ok := recMap[recordID]; ok {
			rec.Relations = append(rec.Relations, rel)
		}
	}
	if err := relRows.Err(); err != nil {
		return nil, fmt.Errorf("batch iterate relations: %w", err)
	}

	// 7. Fetch audit log.
	auditQuery := fmt.Sprintf(
		`SELECT record_id, action, actor, timestamp, rationale
		 FROM audit_log WHERE record_id IN (%s) ORDER BY id`, placeholders)
	auditRows, err := q.QueryContext(ctx, auditQuery, idArgs...)
	if err != nil {
		return nil, fmt.Errorf("batch query audit_log: %w", err)
	}
	defer auditRows.Close()
	for auditRows.Next() {
		var recordID string
		var entry schema.AuditEntry
		var ts string
		if err := auditRows.Scan(&recordID, &entry.Action, &entry.Actor, &ts, &entry.Rationale); err != nil {
			return nil, fmt.Errorf("batch scan audit_log: %w", err)
		}
		entry.Timestamp, _ = time.Parse(time.RFC3339Nano, ts)
		if rec, ok := recMap[recordID]; ok {
			rec.AuditLog = append(rec.AuditLog, entry)
		}
	}
	if err := auditRows.Err(); err != nil {
		return nil, fmt.Errorf("batch iterate audit_log: %w", err)
	}

	// Return records in the original ID order.
	records := make([]*schema.MemoryRecord, 0, len(ids))
	for _, id := range ids {
		if rec, ok := recMap[id]; ok {
			records = append(records, rec)
		}
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
		 VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT(source_id, predicate, target_id) DO UPDATE SET
		 weight = excluded.weight,
		 created_at = excluded.created_at`,
		sourceID, rel.Predicate, rel.TargetID, w,
		ca.UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		return fmt.Errorf("insert relations: %w", err)
	}
	return nil
}

func getRelations(ctx context.Context, q queryable, id string, requireSource bool) ([]schema.Relation, error) {
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
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate relations: %w", err)
	}
	if rels == nil {
		if !requireSource {
			return []schema.Relation{}, nil
		}
		var exists int
		err := q.QueryRowContext(ctx, `SELECT 1 FROM memory_records WHERE id = ?`, id).Scan(&exists)
		if err == sql.ErrNoRows {
			return nil, storage.ErrNotFound
		}
		if err != nil {
			return nil, fmt.Errorf("check record existence: %w", err)
		}
		rels = []schema.Relation{}
	}
	return rels, nil
}

func (s *SQLiteStore) AddRelation(ctx context.Context, sourceID string, rel schema.Relation) error {
	return addRelation(ctx, s.db, sourceID, rel)
}

func (s *SQLiteStore) GetRelations(ctx context.Context, id string) ([]schema.Relation, error) {
	return getRelations(ctx, s.db, id, true)
}

// ----------------------------------------------------------------------------
// Entity lookup indexes
// ----------------------------------------------------------------------------

func replaceEntityIndexes(ctx context.Context, q queryable, rec *schema.MemoryRecord) error {
	if _, err := q.ExecContext(ctx, `DELETE FROM entity_terms WHERE record_id = ?`, rec.ID); err != nil {
		return fmt.Errorf("delete entity_terms: %w", err)
	}
	if _, err := q.ExecContext(ctx, `DELETE FROM entity_types WHERE record_id = ?`, rec.ID); err != nil {
		return fmt.Errorf("delete entity_types: %w", err)
	}
	if _, err := q.ExecContext(ctx, `DELETE FROM entity_identifiers WHERE record_id = ?`, rec.ID); err != nil {
		return fmt.Errorf("delete entity_identifiers: %w", err)
	}
	if rec.Type != schema.MemoryTypeEntity {
		return nil
	}
	entity, ok := rec.Payload.(*schema.EntityPayload)
	if !ok || entity == nil {
		return nil
	}
	scope := rec.Scope
	if term := schema.NormalizeEntityTerm(entity.CanonicalName); term != "" {
		if _, err := q.ExecContext(ctx,
			`INSERT OR IGNORE INTO entity_terms (record_id, normalized_term, term_kind, scope)
			 VALUES (?, ?, ?, ?)`,
			rec.ID, term, schema.EntityTermKindCanonical, scope,
		); err != nil {
			return fmt.Errorf("insert entity canonical term: %w", err)
		}
	}
	for _, alias := range entity.Aliases {
		if term := schema.NormalizeEntityTerm(alias.Value); term != "" {
			if _, err := q.ExecContext(ctx,
				`INSERT OR IGNORE INTO entity_terms (record_id, normalized_term, term_kind, scope)
				 VALUES (?, ?, ?, ?)`,
				rec.ID, term, firstNonEmpty(alias.Kind, schema.EntityTermKindAlias), scope,
			); err != nil {
				return fmt.Errorf("insert entity alias term: %w", err)
			}
		}
	}
	for _, entityType := range schema.EntityTypes(entity) {
		if _, err := q.ExecContext(ctx,
			`INSERT OR IGNORE INTO entity_types (record_id, entity_type) VALUES (?, ?)`,
			rec.ID, entityType,
		); err != nil {
			return fmt.Errorf("insert entity type: %w", err)
		}
	}
	for _, identifier := range entity.Identifiers {
		namespace := strings.TrimSpace(identifier.Namespace)
		value := strings.TrimSpace(identifier.Value)
		if namespace == "" || value == "" {
			continue
		}
		if _, err := q.ExecContext(ctx,
			`INSERT OR IGNORE INTO entity_identifiers (record_id, namespace, value, scope)
			 VALUES (?, ?, ?, ?)`,
			rec.ID, namespace, value, scope,
		); err != nil {
			return fmt.Errorf("insert entity identifier: %w", err)
		}
	}
	return nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func (s *SQLiteStore) FindEntitiesByTerm(ctx context.Context, term, scope string, limit int) ([]*schema.MemoryRecord, error) {
	normalized := schema.NormalizeEntityTerm(term)
	if normalized == "" {
		return []*schema.MemoryRecord{}, nil
	}
	if limit <= 0 {
		limit = 10
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT DISTINCT record_id FROM entity_terms
		 WHERE normalized_term = ? AND (scope = ? OR scope = '')
		 ORDER BY CASE WHEN scope = ? THEN 0 ELSE 1 END, record_id
		 LIMIT ?`,
		normalized, scope, scope, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("query entity terms: %w", err)
	}
	defer rows.Close()
	return s.recordsFromRows(ctx, rows)
}

func (s *SQLiteStore) FindEntityByIdentifier(ctx context.Context, namespace, value, scope string) (*schema.MemoryRecord, error) {
	namespace = strings.TrimSpace(namespace)
	value = strings.TrimSpace(value)
	if namespace == "" || value == "" {
		return nil, storage.ErrNotFound
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT record_id FROM entity_identifiers
		 WHERE namespace = ? AND value = ? AND (scope = ? OR scope = '')
		 ORDER BY CASE WHEN scope = ? THEN 0 ELSE 1 END, record_id
		 LIMIT 1`,
		namespace, value, scope, scope,
	)
	if err != nil {
		return nil, fmt.Errorf("query entity identifiers: %w", err)
	}
	defer rows.Close()
	records, err := s.recordsFromRows(ctx, rows)
	if err != nil {
		return nil, err
	}
	if len(records) == 0 {
		return nil, storage.ErrNotFound
	}
	return records[0], nil
}

func (s *SQLiteStore) recordsFromRows(ctx context.Context, rows *sql.Rows) ([]*schema.MemoryRecord, error) {
	ids := make([]string, 0)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan entity id: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate entity ids: %w", err)
	}
	records := make([]*schema.MemoryRecord, 0, len(ids))
	for _, id := range ids {
		rec, err := s.Get(ctx, id)
		if err != nil {
			return nil, err
		}
		records = append(records, rec)
	}
	return records, nil
}

// ----------------------------------------------------------------------------
// Transactions
// ----------------------------------------------------------------------------

// Begin starts a new database transaction.
func (s *SQLiteStore) Begin(ctx context.Context) (storage.Transaction, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
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
	return getRelations(ctx, t.tx, id, true)
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
