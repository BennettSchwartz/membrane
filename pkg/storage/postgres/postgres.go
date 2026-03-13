package postgres

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/GustyCube/membrane/pkg/schema"
	"github.com/GustyCube/membrane/pkg/storage"
)

//go:embed schema.sql
var ddl string

// EmbeddingConfig controls the vector schema created for Postgres-backed stores.
type EmbeddingConfig struct {
	Dimensions int
	Model      string
}

// PostgresStore implements storage.Store backed by PostgreSQL plus pgvector.
type PostgresStore struct {
	db              *sql.DB
	embeddingConfig EmbeddingConfig
}

// Open creates a new PostgresStore and ensures the schema exists.
func Open(dsn string, cfg EmbeddingConfig) (*PostgresStore, error) {
	if dsn == "" {
		return nil, fmt.Errorf("open postgres: dsn is required")
	}
	if cfg.Dimensions <= 0 {
		cfg.Dimensions = 1536
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("open postgres: %w", err)
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	schemaDDL := strings.ReplaceAll(ddl, "{{EMBEDDING_DIMENSIONS}}", strconv.Itoa(cfg.Dimensions))
	if _, err := db.Exec(schemaDDL); err != nil {
		db.Close()
		return nil, fmt.Errorf("apply schema: %w", err)
	}

	store := &PostgresStore{db: db, embeddingConfig: cfg}
	if err := store.ensureEmbeddingMetadata(context.Background()); err != nil {
		db.Close()
		return nil, err
	}

	return store, nil
}

// Close closes the underlying database connection.
func (s *PostgresStore) Close() error {
	return s.db.Close()
}

type queryable interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func (s *PostgresStore) ensureEmbeddingMetadata(ctx context.Context) error {
	if err := s.ensureMetadataKey(ctx, "dimensions", strconv.Itoa(s.embeddingConfig.Dimensions)); err != nil {
		return err
	}
	if s.embeddingConfig.Model != "" {
		if err := s.ensureMetadataKey(ctx, "model", s.embeddingConfig.Model); err != nil {
			return err
		}
	}
	return nil
}

func (s *PostgresStore) ensureMetadataKey(ctx context.Context, key, value string) error {
	var existing string
	err := s.db.QueryRowContext(ctx, `SELECT value FROM embedding_metadata WHERE key = $1`, key).Scan(&existing)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		_, err = s.db.ExecContext(ctx,
			`INSERT INTO embedding_metadata (key, value) VALUES ($1, $2)`,
			key, value,
		)
		if err != nil {
			return fmt.Errorf("insert embedding metadata %s: %w", key, err)
		}
		return nil
	case err != nil:
		return fmt.Errorf("read embedding metadata %s: %w", key, err)
	case existing != value:
		return fmt.Errorf("embedding metadata mismatch for %s: configured %q, stored %q", key, value, existing)
	default:
		return nil
	}
}

func createRecord(ctx context.Context, q queryable, rec *schema.MemoryRecord) error {
	if err := rec.Validate(); err != nil {
		return err
	}

	_, err := q.ExecContext(ctx,
		`INSERT INTO memory_records (id, type, sensitivity, confidence, salience, scope, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		rec.ID, string(rec.Type), string(rec.Sensitivity),
		rec.Confidence, rec.Salience, nullableString(rec.Scope),
		rec.CreatedAt.UTC(), rec.UpdatedAt.UTC(),
	)
	if err != nil {
		if isDuplicateError(err) {
			return storage.ErrAlreadyExists
		}
		return fmt.Errorf("insert memory_records: %w", err)
	}

	pinned := rec.Lifecycle.Pinned
	dp := rec.Lifecycle.Decay
	delPolicy := string(rec.Lifecycle.DeletionPolicy)
	if delPolicy == "" {
		delPolicy = string(schema.DeletionPolicyAutoPrune)
	}
	_, err = q.ExecContext(ctx,
		`INSERT INTO decay_profiles
		 (record_id, curve, half_life_seconds, min_salience, max_age_seconds, reinforcement_gain, last_reinforced_at, pinned, deletion_policy)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		rec.ID, string(dp.Curve), dp.HalfLifeSeconds, dp.MinSalience,
		nullableInt64(dp.MaxAgeSeconds), dp.ReinforcementGain,
		rec.Lifecycle.LastReinforcedAt.UTC(), pinned, delPolicy,
	)
	if err != nil {
		return fmt.Errorf("insert decay_profiles: %w", err)
	}

	payloadJSON, err := json.Marshal(rec.Payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}
	if _, err := q.ExecContext(ctx,
		`INSERT INTO payloads (record_id, payload_json) VALUES ($1, $2)`,
		rec.ID, payloadJSON,
	); err != nil {
		return fmt.Errorf("insert payloads: %w", err)
	}

	for _, tag := range rec.Tags {
		if _, err := q.ExecContext(ctx,
			`INSERT INTO tags (record_id, tag) VALUES ($1, $2)`,
			rec.ID, tag,
		); err != nil {
			return fmt.Errorf("insert tag: %w", err)
		}
	}

	for _, src := range rec.Provenance.Sources {
		if _, err := q.ExecContext(ctx,
			`INSERT INTO provenance_sources (record_id, kind, ref, hash, created_by, timestamp)
			 VALUES ($1, $2, $3, $4, $5, $6)`,
			rec.ID, string(src.Kind), src.Ref,
			nullableString(src.Hash), nullableString(src.CreatedBy),
			src.Timestamp.UTC(),
		); err != nil {
			return fmt.Errorf("insert provenance_sources: %w", err)
		}
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
			 VALUES ($1, $2, $3, $4, $5)`,
			rec.ID, rel.Predicate, rel.TargetID, w, ca.UTC(),
		); err != nil {
			return fmt.Errorf("insert relations: %w", err)
		}
	}

	for _, entry := range rec.AuditLog {
		if _, err := q.ExecContext(ctx,
			`INSERT INTO audit_log (record_id, action, actor, timestamp, rationale, previous_state_json)
			 VALUES ($1, $2, $3, $4, $5, $6)`,
			rec.ID, string(entry.Action), entry.Actor,
			entry.Timestamp.UTC(), entry.Rationale, nil,
		); err != nil {
			return fmt.Errorf("insert audit_log: %w", err)
		}
	}

	if cp, ok := rec.Payload.(*schema.CompetencePayload); ok && cp.Performance != nil {
		if _, err := q.ExecContext(ctx,
			`INSERT INTO competence_stats (record_id, success_count, failure_count) VALUES ($1, $2, $3)`,
			rec.ID, cp.Performance.SuccessCount, cp.Performance.FailureCount,
		); err != nil {
			return fmt.Errorf("insert competence_stats: %w", err)
		}
	}

	return nil
}

func (s *PostgresStore) Create(ctx context.Context, rec *schema.MemoryRecord) error {
	return createRecord(ctx, s.db, rec)
}

func getRecord(ctx context.Context, q queryable, id string) (*schema.MemoryRecord, error) {
	rec := &schema.MemoryRecord{}

	var scope sql.NullString
	err := q.QueryRowContext(ctx,
		`SELECT id, type, sensitivity, confidence, salience, scope, created_at, updated_at
		 FROM memory_records WHERE id = $1`,
		id,
	).Scan(&rec.ID, &rec.Type, &rec.Sensitivity, &rec.Confidence, &rec.Salience,
		&scope, &rec.CreatedAt, &rec.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, storage.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("query memory_records: %w", err)
	}
	rec.Scope = scope.String

	var (
		lastReinforced time.Time
		pinned         bool
		maxAge         sql.NullInt64
	)
	err = q.QueryRowContext(ctx,
		`SELECT curve, half_life_seconds, min_salience, max_age_seconds, reinforcement_gain,
		        last_reinforced_at, pinned, deletion_policy
		 FROM decay_profiles WHERE record_id = $1`,
		id,
	).Scan(&rec.Lifecycle.Decay.Curve, &rec.Lifecycle.Decay.HalfLifeSeconds,
		&rec.Lifecycle.Decay.MinSalience, &maxAge,
		&rec.Lifecycle.Decay.ReinforcementGain, &lastReinforced,
		&pinned, &rec.Lifecycle.DeletionPolicy)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("query decay_profiles: %w", err)
	}
	rec.Lifecycle.LastReinforcedAt = lastReinforced
	rec.Lifecycle.Pinned = pinned
	if maxAge.Valid {
		rec.Lifecycle.Decay.MaxAgeSeconds = maxAge.Int64
	}

	var payloadJSON []byte
	err = q.QueryRowContext(ctx,
		`SELECT payload_json FROM payloads WHERE record_id = $1`,
		id,
	).Scan(&payloadJSON)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("query payloads: %w", err)
	}
	if len(payloadJSON) > 0 {
		var wrapper schema.PayloadWrapper
		if err := wrapper.UnmarshalJSON(payloadJSON); err != nil {
			return nil, fmt.Errorf("unmarshal payload: %w", err)
		}
		rec.Payload = wrapper.Payload
	}

	tagRows, err := q.QueryContext(ctx, `SELECT tag FROM tags WHERE record_id = $1`, id)
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

	provRows, err := q.QueryContext(ctx,
		`SELECT kind, ref, hash, created_by, timestamp
		 FROM provenance_sources WHERE record_id = $1 ORDER BY id`,
		id,
	)
	if err != nil {
		return nil, fmt.Errorf("query provenance_sources: %w", err)
	}
	defer provRows.Close()
	rec.Provenance.Sources = []schema.ProvenanceSource{}
	for provRows.Next() {
		var src schema.ProvenanceSource
		var hash, createdBy sql.NullString
		if err := provRows.Scan(&src.Kind, &src.Ref, &hash, &createdBy, &src.Timestamp); err != nil {
			return nil, fmt.Errorf("scan provenance_source: %w", err)
		}
		src.Hash = hash.String
		src.CreatedBy = createdBy.String
		rec.Provenance.Sources = append(rec.Provenance.Sources, src)
	}

	rec.Relations, err = getRelations(ctx, q, id)
	if err != nil {
		return nil, err
	}

	auditRows, err := q.QueryContext(ctx,
		`SELECT action, actor, timestamp, rationale
		 FROM audit_log WHERE record_id = $1 ORDER BY id`,
		id,
	)
	if err != nil {
		return nil, fmt.Errorf("query audit_log: %w", err)
	}
	defer auditRows.Close()
	rec.AuditLog = []schema.AuditEntry{}
	for auditRows.Next() {
		var entry schema.AuditEntry
		if err := auditRows.Scan(&entry.Action, &entry.Actor, &entry.Timestamp, &entry.Rationale); err != nil {
			return nil, fmt.Errorf("scan audit_log: %w", err)
		}
		rec.AuditLog = append(rec.AuditLog, entry)
	}

	return rec, nil
}

func (s *PostgresStore) Get(ctx context.Context, id string) (*schema.MemoryRecord, error) {
	return getRecord(ctx, s.db, id)
}

func updateRecord(ctx context.Context, q queryable, rec *schema.MemoryRecord) error {
	if err := rec.Validate(); err != nil {
		return err
	}

	res, err := q.ExecContext(ctx,
		`UPDATE memory_records
		 SET type = $1, sensitivity = $2, confidence = $3, salience = $4, scope = $5, updated_at = $6
		 WHERE id = $7`,
		string(rec.Type), string(rec.Sensitivity), rec.Confidence, rec.Salience,
		nullableString(rec.Scope), rec.UpdatedAt.UTC(), rec.ID,
	)
	if err != nil {
		return fmt.Errorf("update memory_records: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return storage.ErrNotFound
	}

	dp := rec.Lifecycle.Decay
	delPolicy := string(rec.Lifecycle.DeletionPolicy)
	if delPolicy == "" {
		delPolicy = string(schema.DeletionPolicyAutoPrune)
	}
	if _, err := q.ExecContext(ctx,
		`INSERT INTO decay_profiles
		 (record_id, curve, half_life_seconds, min_salience, max_age_seconds, reinforcement_gain, last_reinforced_at, pinned, deletion_policy)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		 ON CONFLICT (record_id) DO UPDATE
		 SET curve = EXCLUDED.curve,
		     half_life_seconds = EXCLUDED.half_life_seconds,
		     min_salience = EXCLUDED.min_salience,
		     max_age_seconds = EXCLUDED.max_age_seconds,
		     reinforcement_gain = EXCLUDED.reinforcement_gain,
		     last_reinforced_at = EXCLUDED.last_reinforced_at,
		     pinned = EXCLUDED.pinned,
		     deletion_policy = EXCLUDED.deletion_policy`,
		rec.ID, string(dp.Curve), dp.HalfLifeSeconds, dp.MinSalience,
		nullableInt64(dp.MaxAgeSeconds), dp.ReinforcementGain,
		rec.Lifecycle.LastReinforcedAt.UTC(), rec.Lifecycle.Pinned, delPolicy,
	); err != nil {
		return fmt.Errorf("upsert decay_profiles: %w", err)
	}

	payloadJSON, err := json.Marshal(rec.Payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}
	if _, err := q.ExecContext(ctx,
		`INSERT INTO payloads (record_id, payload_json) VALUES ($1, $2)
		 ON CONFLICT (record_id) DO UPDATE SET payload_json = EXCLUDED.payload_json`,
		rec.ID, payloadJSON,
	); err != nil {
		return fmt.Errorf("upsert payloads: %w", err)
	}

	if _, err := q.ExecContext(ctx, `DELETE FROM tags WHERE record_id = $1`, rec.ID); err != nil {
		return fmt.Errorf("delete tags: %w", err)
	}
	for _, tag := range rec.Tags {
		if _, err := q.ExecContext(ctx,
			`INSERT INTO tags (record_id, tag) VALUES ($1, $2)`,
			rec.ID, tag,
		); err != nil {
			return fmt.Errorf("insert tag: %w", err)
		}
	}

	if _, err := q.ExecContext(ctx, `DELETE FROM provenance_sources WHERE record_id = $1`, rec.ID); err != nil {
		return fmt.Errorf("delete provenance_sources: %w", err)
	}
	for _, src := range rec.Provenance.Sources {
		if _, err := q.ExecContext(ctx,
			`INSERT INTO provenance_sources (record_id, kind, ref, hash, created_by, timestamp)
			 VALUES ($1, $2, $3, $4, $5, $6)`,
			rec.ID, string(src.Kind), src.Ref,
			nullableString(src.Hash), nullableString(src.CreatedBy), src.Timestamp.UTC(),
		); err != nil {
			return fmt.Errorf("insert provenance_sources: %w", err)
		}
	}

	if _, err := q.ExecContext(ctx, `DELETE FROM relations WHERE source_id = $1`, rec.ID); err != nil {
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
			 VALUES ($1, $2, $3, $4, $5)`,
			rec.ID, rel.Predicate, rel.TargetID, w, ca.UTC(),
		); err != nil {
			return fmt.Errorf("insert relations: %w", err)
		}
	}

	return nil
}

func (s *PostgresStore) Update(ctx context.Context, rec *schema.MemoryRecord) error {
	return updateRecord(ctx, s.db, rec)
}

func deleteRecord(ctx context.Context, q queryable, id string) error {
	res, err := q.ExecContext(ctx, `DELETE FROM memory_records WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete memory_records: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return storage.ErrNotFound
	}
	return nil
}

func (s *PostgresStore) Delete(ctx context.Context, id string) error {
	return deleteRecord(ctx, s.db, id)
}

func listRecords(ctx context.Context, q queryable, opts storage.ListOptions) ([]*schema.MemoryRecord, error) {
	query := `SELECT id FROM memory_records WHERE 1=1`
	args := []any{}

	addArg := func(v any) string {
		args = append(args, v)
		return fmt.Sprintf("$%d", len(args))
	}

	if opts.Type != "" {
		query += ` AND type = ` + addArg(string(opts.Type))
	}
	if opts.Scope != "" {
		query += ` AND scope = ` + addArg(opts.Scope)
	}
	if opts.Sensitivity != "" {
		query += ` AND sensitivity = ` + addArg(string(opts.Sensitivity))
	}
	if opts.MinSalience > 0 {
		query += ` AND salience >= ` + addArg(opts.MinSalience)
	}
	if opts.MaxSalience > 0 {
		query += ` AND salience <= ` + addArg(opts.MaxSalience)
	}
	for i, tag := range opts.Tags {
		alias := fmt.Sprintf("t%d", i)
		tagPlaceholder := addArg(tag)
		query += fmt.Sprintf(` AND EXISTS (SELECT 1 FROM tags %s WHERE %s.record_id = memory_records.id AND %s.tag = %s)`,
			alias, alias, alias, tagPlaceholder)
	}

	query += ` ORDER BY salience DESC, created_at DESC`
	if opts.Limit > 0 {
		query += ` LIMIT ` + addArg(opts.Limit)
	}
	if opts.Offset > 0 {
		query += ` OFFSET ` + addArg(opts.Offset)
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

	return getRecordsBatch(ctx, q, ids)
}

func getRecordsBatch(ctx context.Context, q queryable, ids []string) ([]*schema.MemoryRecord, error) {
	if len(ids) == 0 {
		return []*schema.MemoryRecord{}, nil
	}
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

	placeholders, idArgs := buildIDPlaceholders(ids, 1)
	recMap := make(map[string]*schema.MemoryRecord, len(ids))

	baseRows, err := q.QueryContext(ctx, fmt.Sprintf(
		`SELECT id, type, sensitivity, confidence, salience, scope, created_at, updated_at
		 FROM memory_records WHERE id IN (%s)`, placeholders), idArgs...)
	if err != nil {
		return nil, fmt.Errorf("batch query memory_records: %w", err)
	}
	defer baseRows.Close()
	for baseRows.Next() {
		rec := &schema.MemoryRecord{}
		var scope sql.NullString
		if err := baseRows.Scan(&rec.ID, &rec.Type, &rec.Sensitivity, &rec.Confidence, &rec.Salience,
			&scope, &rec.CreatedAt, &rec.UpdatedAt); err != nil {
			return nil, fmt.Errorf("batch scan memory_records: %w", err)
		}
		rec.Scope = scope.String
		rec.Provenance.Sources = []schema.ProvenanceSource{}
		rec.Relations = []schema.Relation{}
		rec.AuditLog = []schema.AuditEntry{}
		recMap[rec.ID] = rec
	}

	dpRows, err := q.QueryContext(ctx, fmt.Sprintf(
		`SELECT record_id, curve, half_life_seconds, min_salience, max_age_seconds, reinforcement_gain,
		        last_reinforced_at, pinned, deletion_policy
		 FROM decay_profiles WHERE record_id IN (%s)`, placeholders), idArgs...)
	if err != nil {
		return nil, fmt.Errorf("batch query decay_profiles: %w", err)
	}
	defer dpRows.Close()
	for dpRows.Next() {
		var (
			recordID       string
			curve          schema.DecayCurve
			halfLife       int64
			minSalience    float64
			maxAge         sql.NullInt64
			gain           float64
			lastReinforced time.Time
			pinned         bool
			delPolicy      schema.DeletionPolicy
		)
		if err := dpRows.Scan(&recordID, &curve, &halfLife, &minSalience, &maxAge, &gain,
			&lastReinforced, &pinned, &delPolicy); err != nil {
			return nil, fmt.Errorf("batch scan decay_profiles: %w", err)
		}
		if rec, ok := recMap[recordID]; ok {
			rec.Lifecycle.Decay.Curve = curve
			rec.Lifecycle.Decay.HalfLifeSeconds = halfLife
			rec.Lifecycle.Decay.MinSalience = minSalience
			rec.Lifecycle.Decay.ReinforcementGain = gain
			rec.Lifecycle.LastReinforcedAt = lastReinforced
			rec.Lifecycle.Pinned = pinned
			rec.Lifecycle.DeletionPolicy = delPolicy
			if maxAge.Valid {
				rec.Lifecycle.Decay.MaxAgeSeconds = maxAge.Int64
			}
		}
	}

	plRows, err := q.QueryContext(ctx, fmt.Sprintf(
		`SELECT record_id, payload_json FROM payloads WHERE record_id IN (%s)`, placeholders), idArgs...)
	if err != nil {
		return nil, fmt.Errorf("batch query payloads: %w", err)
	}
	defer plRows.Close()
	for plRows.Next() {
		var (
			recordID    string
			payloadJSON []byte
		)
		if err := plRows.Scan(&recordID, &payloadJSON); err != nil {
			return nil, fmt.Errorf("batch scan payloads: %w", err)
		}
		if rec, ok := recMap[recordID]; ok && len(payloadJSON) > 0 {
			var wrapper schema.PayloadWrapper
			if err := wrapper.UnmarshalJSON(payloadJSON); err != nil {
				return nil, fmt.Errorf("unmarshal payload for %s: %w", recordID, err)
			}
			rec.Payload = wrapper.Payload
		}
	}

	tagRows, err := q.QueryContext(ctx, fmt.Sprintf(
		`SELECT record_id, tag FROM tags WHERE record_id IN (%s)`, placeholders), idArgs...)
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

	provRows, err := q.QueryContext(ctx, fmt.Sprintf(
		`SELECT record_id, kind, ref, hash, created_by, timestamp
		 FROM provenance_sources WHERE record_id IN (%s) ORDER BY id`, placeholders), idArgs...)
	if err != nil {
		return nil, fmt.Errorf("batch query provenance_sources: %w", err)
	}
	defer provRows.Close()
	for provRows.Next() {
		var (
			recordID string
			src      schema.ProvenanceSource
			hash     sql.NullString
			created  sql.NullString
		)
		if err := provRows.Scan(&recordID, &src.Kind, &src.Ref, &hash, &created, &src.Timestamp); err != nil {
			return nil, fmt.Errorf("batch scan provenance_sources: %w", err)
		}
		src.Hash = hash.String
		src.CreatedBy = created.String
		if rec, ok := recMap[recordID]; ok {
			rec.Provenance.Sources = append(rec.Provenance.Sources, src)
		}
	}

	relRows, err := q.QueryContext(ctx, fmt.Sprintf(
		`SELECT source_id, predicate, target_id, weight, created_at
		 FROM relations WHERE source_id IN (%s) ORDER BY id`, placeholders), idArgs...)
	if err != nil {
		return nil, fmt.Errorf("batch query relations: %w", err)
	}
	defer relRows.Close()
	for relRows.Next() {
		var (
			recordID string
			rel      schema.Relation
			weight   sql.NullFloat64
		)
		if err := relRows.Scan(&recordID, &rel.Predicate, &rel.TargetID, &weight, &rel.CreatedAt); err != nil {
			return nil, fmt.Errorf("batch scan relations: %w", err)
		}
		if weight.Valid {
			rel.Weight = weight.Float64
		}
		if rec, ok := recMap[recordID]; ok {
			rec.Relations = append(rec.Relations, rel)
		}
	}

	auditRows, err := q.QueryContext(ctx, fmt.Sprintf(
		`SELECT record_id, action, actor, timestamp, rationale
		 FROM audit_log WHERE record_id IN (%s) ORDER BY id`, placeholders), idArgs...)
	if err != nil {
		return nil, fmt.Errorf("batch query audit_log: %w", err)
	}
	defer auditRows.Close()
	for auditRows.Next() {
		var (
			recordID string
			entry    schema.AuditEntry
		)
		if err := auditRows.Scan(&recordID, &entry.Action, &entry.Actor, &entry.Timestamp, &entry.Rationale); err != nil {
			return nil, fmt.Errorf("batch scan audit_log: %w", err)
		}
		if rec, ok := recMap[recordID]; ok {
			rec.AuditLog = append(rec.AuditLog, entry)
		}
	}

	records := make([]*schema.MemoryRecord, 0, len(ids))
	for _, id := range ids {
		if rec, ok := recMap[id]; ok {
			records = append(records, rec)
		}
	}
	return records, nil
}

func (s *PostgresStore) List(ctx context.Context, opts storage.ListOptions) ([]*schema.MemoryRecord, error) {
	return listRecords(ctx, s.db, opts)
}

func (s *PostgresStore) ListByType(ctx context.Context, memType schema.MemoryType) ([]*schema.MemoryRecord, error) {
	return s.List(ctx, storage.ListOptions{Type: memType})
}

func updateSalience(ctx context.Context, q queryable, id string, salience float64) error {
	res, err := q.ExecContext(ctx,
		`UPDATE memory_records SET salience = $1, updated_at = $2 WHERE id = $3`,
		salience, time.Now().UTC(), id,
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

func (s *PostgresStore) UpdateSalience(ctx context.Context, id string, salience float64) error {
	return updateSalience(ctx, s.db, id, salience)
}

func addAuditEntry(ctx context.Context, q queryable, id string, entry schema.AuditEntry) error {
	var exists int
	err := q.QueryRowContext(ctx, `SELECT 1 FROM memory_records WHERE id = $1`, id).Scan(&exists)
	if errors.Is(err, sql.ErrNoRows) {
		return storage.ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("check record existence: %w", err)
	}

	_, err = q.ExecContext(ctx,
		`INSERT INTO audit_log (record_id, action, actor, timestamp, rationale, previous_state_json)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		id, string(entry.Action), entry.Actor,
		entry.Timestamp.UTC(), entry.Rationale, nil,
	)
	if err != nil {
		return fmt.Errorf("insert audit_log: %w", err)
	}
	return nil
}

func (s *PostgresStore) AddAuditEntry(ctx context.Context, id string, entry schema.AuditEntry) error {
	return addAuditEntry(ctx, s.db, id, entry)
}

func addRelation(ctx context.Context, q queryable, sourceID string, rel schema.Relation) error {
	var exists int
	err := q.QueryRowContext(ctx, `SELECT 1 FROM memory_records WHERE id = $1`, sourceID).Scan(&exists)
	if errors.Is(err, sql.ErrNoRows) {
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
		 VALUES ($1, $2, $3, $4, $5)`,
		sourceID, rel.Predicate, rel.TargetID, w, ca.UTC(),
	)
	if err != nil {
		return fmt.Errorf("insert relations: %w", err)
	}
	return nil
}

func getRelations(ctx context.Context, q queryable, id string) ([]schema.Relation, error) {
	rows, err := q.QueryContext(ctx,
		`SELECT predicate, target_id, weight, created_at
		 FROM relations WHERE source_id = $1 ORDER BY id`,
		id,
	)
	if err != nil {
		return nil, fmt.Errorf("query relations: %w", err)
	}
	defer rows.Close()

	var rels []schema.Relation
	for rows.Next() {
		var rel schema.Relation
		var weight sql.NullFloat64
		if err := rows.Scan(&rel.Predicate, &rel.TargetID, &weight, &rel.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan relation: %w", err)
		}
		if weight.Valid {
			rel.Weight = weight.Float64
		}
		rels = append(rels, rel)
	}
	if rels == nil {
		rels = []schema.Relation{}
	}
	return rels, nil
}

func (s *PostgresStore) AddRelation(ctx context.Context, sourceID string, rel schema.Relation) error {
	return addRelation(ctx, s.db, sourceID, rel)
}

func (s *PostgresStore) GetRelations(ctx context.Context, id string) ([]schema.Relation, error) {
	return getRelations(ctx, s.db, id)
}

// StoreTriggerEmbedding stores or updates a trigger embedding for a record.
func (s *PostgresStore) StoreTriggerEmbedding(ctx context.Context, recordID string, embedding []float32, model string) error {
	if len(embedding) == 0 {
		return fmt.Errorf("store trigger embedding: embedding is empty")
	}
	if s.embeddingConfig.Dimensions > 0 && len(embedding) != s.embeddingConfig.Dimensions {
		return fmt.Errorf("store trigger embedding: embedding dimension %d does not match configured dimension %d", len(embedding), s.embeddingConfig.Dimensions)
	}
	if model == "" {
		model = s.embeddingConfig.Model
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO trigger_embeddings (record_id, embedding, model, created_at)
		 VALUES ($1, $2::vector, $3, $4)
		 ON CONFLICT (record_id) DO UPDATE
		 SET embedding = EXCLUDED.embedding,
		     model = EXCLUDED.model,
		     created_at = EXCLUDED.created_at`,
		recordID, vectorLiteral(embedding), model, time.Now().UTC(),
	)
	if err != nil {
		return fmt.Errorf("store trigger embedding: %w", err)
	}
	return nil
}

// GetTriggerEmbedding retrieves the stored embedding for a record.
func (s *PostgresStore) GetTriggerEmbedding(ctx context.Context, recordID string) ([]float32, error) {
	var raw string
	err := s.db.QueryRowContext(ctx,
		`SELECT embedding::text FROM trigger_embeddings WHERE record_id = $1`,
		recordID,
	).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get trigger embedding: %w", err)
	}
	return parseVectorLiteral(raw)
}

// SearchByEmbedding returns record IDs ordered by cosine distance ascending.
func (s *PostgresStore) SearchByEmbedding(ctx context.Context, query []float32, limit int) ([]string, error) {
	if len(query) == 0 {
		return nil, nil
	}
	if limit <= 0 {
		limit = 10
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT record_id
		 FROM trigger_embeddings
		 ORDER BY embedding <=> $1::vector
		 LIMIT $2`,
		vectorLiteral(query), limit,
	)
	if err != nil {
		return nil, fmt.Errorf("search trigger embeddings: %w", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan trigger embedding result: %w", err)
		}
		ids = append(ids, id)
	}
	return ids, nil
}

// Reset deletes all stored records and embeddings. Intended for tests and local evaluation flows.
func (s *PostgresStore) Reset(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `
		TRUNCATE TABLE
			trigger_embeddings,
			competence_stats,
			audit_log,
			relations,
			provenance_sources,
			tags,
			payloads,
			decay_profiles,
			memory_records
		RESTART IDENTITY CASCADE`)
	if err != nil {
		return fmt.Errorf("reset postgres store: %w", err)
	}
	return nil
}

func (s *PostgresStore) Begin(ctx context.Context) (storage.Transaction, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	return &postgresTx{tx: tx}, nil
}

type postgresTx struct {
	tx     *sql.Tx
	closed bool
}

func (t *postgresTx) checkClosed() error {
	if t.closed {
		return storage.ErrTxClosed
	}
	return nil
}

func (t *postgresTx) Create(ctx context.Context, rec *schema.MemoryRecord) error {
	if err := t.checkClosed(); err != nil {
		return err
	}
	return createRecord(ctx, t.tx, rec)
}

func (t *postgresTx) Get(ctx context.Context, id string) (*schema.MemoryRecord, error) {
	if err := t.checkClosed(); err != nil {
		return nil, err
	}
	return getRecord(ctx, t.tx, id)
}

func (t *postgresTx) Update(ctx context.Context, rec *schema.MemoryRecord) error {
	if err := t.checkClosed(); err != nil {
		return err
	}
	return updateRecord(ctx, t.tx, rec)
}

func (t *postgresTx) Delete(ctx context.Context, id string) error {
	if err := t.checkClosed(); err != nil {
		return err
	}
	return deleteRecord(ctx, t.tx, id)
}

func (t *postgresTx) List(ctx context.Context, opts storage.ListOptions) ([]*schema.MemoryRecord, error) {
	if err := t.checkClosed(); err != nil {
		return nil, err
	}
	return listRecords(ctx, t.tx, opts)
}

func (t *postgresTx) ListByType(ctx context.Context, memType schema.MemoryType) ([]*schema.MemoryRecord, error) {
	if err := t.checkClosed(); err != nil {
		return nil, err
	}
	return listRecords(ctx, t.tx, storage.ListOptions{Type: memType})
}

func (t *postgresTx) UpdateSalience(ctx context.Context, id string, salience float64) error {
	if err := t.checkClosed(); err != nil {
		return err
	}
	return updateSalience(ctx, t.tx, id, salience)
}

func (t *postgresTx) AddAuditEntry(ctx context.Context, id string, entry schema.AuditEntry) error {
	if err := t.checkClosed(); err != nil {
		return err
	}
	return addAuditEntry(ctx, t.tx, id, entry)
}

func (t *postgresTx) AddRelation(ctx context.Context, sourceID string, rel schema.Relation) error {
	if err := t.checkClosed(); err != nil {
		return err
	}
	return addRelation(ctx, t.tx, sourceID, rel)
}

func (t *postgresTx) GetRelations(ctx context.Context, id string) ([]schema.Relation, error) {
	if err := t.checkClosed(); err != nil {
		return nil, err
	}
	return getRelations(ctx, t.tx, id)
}

func (t *postgresTx) Commit() error {
	if err := t.checkClosed(); err != nil {
		return err
	}
	t.closed = true
	return t.tx.Commit()
}

func (t *postgresTx) Rollback() error {
	if err := t.checkClosed(); err != nil {
		return err
	}
	t.closed = true
	return t.tx.Rollback()
}

func buildIDPlaceholders(ids []string, start int) (string, []any) {
	parts := make([]string, len(ids))
	args := make([]any, len(ids))
	for i, id := range ids {
		parts[i] = fmt.Sprintf("$%d", start+i)
		args[i] = id
	}
	return strings.Join(parts, ","), args
}

func vectorLiteral(values []float32) string {
	parts := make([]string, len(values))
	for i, v := range values {
		parts[i] = strconv.FormatFloat(float64(v), 'f', -1, 32)
	}
	return "[" + strings.Join(parts, ",") + "]"
}

func parseVectorLiteral(raw string) ([]float32, error) {
	trimmed := strings.TrimSpace(raw)
	trimmed = strings.TrimPrefix(trimmed, "[")
	trimmed = strings.TrimSuffix(trimmed, "]")
	if trimmed == "" {
		return []float32{}, nil
	}
	parts := strings.Split(trimmed, ",")
	values := make([]float32, 0, len(parts))
	for _, part := range parts {
		f, err := strconv.ParseFloat(strings.TrimSpace(part), 32)
		if err != nil {
			return nil, fmt.Errorf("parse vector literal: %w", err)
		}
		values = append(values, float32(f))
	}
	return values, nil
}

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

func isDuplicateError(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

var (
	_ storage.Store       = (*PostgresStore)(nil)
	_ storage.Transaction = (*postgresTx)(nil)
)
