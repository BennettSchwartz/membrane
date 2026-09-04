package postgres

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/BennettSchwartz/membrane/pkg/schema"
	"github.com/BennettSchwartz/membrane/pkg/storage"
)

//go:embed schema.sql
var ddl string

var sqlOpen = sql.Open

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

	db, err := sqlOpen("pgx", dsn)
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
	if s == nil || s.db == nil {
		return nil
	}
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
		if err := s.setMetadataKey(ctx, "model", s.embeddingConfig.Model); err != nil {
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

func (s *PostgresStore) setMetadataKey(ctx context.Context, key, value string) error {
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
		_, err = s.db.ExecContext(ctx,
			`UPDATE embedding_metadata SET value = $2 WHERE key = $1`,
			key, value,
		)
		if err != nil {
			return fmt.Errorf("update embedding metadata %s: %w", key, err)
		}
		return nil
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

	payloadJSON, err := json.Marshal(normalizePayloadForStorage(rec.Payload))
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}
	if _, err := q.ExecContext(ctx,
		`INSERT INTO payloads (record_id, payload_json) VALUES ($1, $2)`,
		rec.ID, payloadJSON,
	); err != nil {
		return fmt.Errorf("insert payloads: %w", err)
	}

	if rec.Interpretation != nil {
		interpretationJSON, err := json.Marshal(rec.Interpretation)
		if err != nil {
			return fmt.Errorf("marshal interpretation: %w", err)
		}
		if _, err := q.ExecContext(ctx,
			`INSERT INTO interpretations (record_id, interpretation_json) VALUES ($1, $2)`,
			rec.ID, interpretationJSON,
		); err != nil {
			return fmt.Errorf("insert interpretations: %w", err)
		}
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
		rel = normalizeRelationForStorage(rel)
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
			 VALUES ($1, $2, $3, $4, $5)
			 ON CONFLICT (source_id, predicate, target_id) DO UPDATE SET
			 weight = EXCLUDED.weight,
			 created_at = EXCLUDED.created_at`,
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

	if err := replaceEntityIndexes(ctx, q, rec); err != nil {
		return err
	}

	return nil
}

func (s *PostgresStore) Create(ctx context.Context, rec *schema.MemoryRecord) error {
	if err := rec.Validate(); err != nil {
		return err
	}
	return storage.WithTransaction(ctx, s, func(tx storage.Transaction) error {
		return tx.Create(ctx, rec)
	})
}

type recordReadOptions struct {
	omitRelations bool
	omitHistory   bool
}

func getRecord(ctx context.Context, q queryable, id string) (*schema.MemoryRecord, error) {
	return getRecordWithOptions(ctx, q, id, recordReadOptions{})
}

func getRecordWithOptions(ctx context.Context, q queryable, id string, opts recordReadOptions) (*schema.MemoryRecord, error) {
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

	var interpretationJSON []byte
	err = q.QueryRowContext(ctx,
		`SELECT interpretation_json FROM interpretations WHERE record_id = $1`,
		id,
	).Scan(&interpretationJSON)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("query interpretations: %w", err)
	}
	if len(interpretationJSON) > 0 {
		var interpretation schema.Interpretation
		if err := json.Unmarshal(interpretationJSON, &interpretation); err != nil {
			return nil, fmt.Errorf("unmarshal interpretation: %w", err)
		}
		rec.Interpretation = &interpretation
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
	if err := tagRows.Err(); err != nil {
		return nil, fmt.Errorf("iterate tags: %w", err)
	}

	if opts.omitHistory {
		rec.Provenance.Sources = nil
	} else {
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
		if err := provRows.Err(); err != nil {
			return nil, fmt.Errorf("iterate provenance_sources: %w", err)
		}
	}

	if opts.omitRelations {
		rec.Relations = nil
	} else {
		rec.Relations, err = getRelations(ctx, q, id, false)
		if err != nil {
			return nil, err
		}
	}

	if opts.omitHistory {
		rec.AuditLog = nil
	} else {
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
		if err := auditRows.Err(); err != nil {
			return nil, fmt.Errorf("iterate audit_log: %w", err)
		}
	}

	return rec, nil
}

func (s *PostgresStore) Get(ctx context.Context, id string) (*schema.MemoryRecord, error) {
	return getRecord(ctx, s.db, id)
}

func (s *PostgresStore) GetGraphRecord(ctx context.Context, id string) (*schema.MemoryRecord, error) {
	return getRecordWithOptions(ctx, s.db, id, recordReadOptions{omitRelations: true, omitHistory: true})
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

	payloadJSON, err := json.Marshal(normalizePayloadForStorage(rec.Payload))
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

	if rec.Interpretation != nil {
		interpretationJSON, err := json.Marshal(rec.Interpretation)
		if err != nil {
			return fmt.Errorf("marshal interpretation: %w", err)
		}
		if _, err := q.ExecContext(ctx,
			`INSERT INTO interpretations (record_id, interpretation_json) VALUES ($1, $2)
			 ON CONFLICT (record_id) DO UPDATE SET interpretation_json = EXCLUDED.interpretation_json`,
			rec.ID, interpretationJSON,
		); err != nil {
			return fmt.Errorf("upsert interpretations: %w", err)
		}
	} else {
		if _, err := q.ExecContext(ctx, `DELETE FROM interpretations WHERE record_id = $1`, rec.ID); err != nil {
			return fmt.Errorf("delete interpretations: %w", err)
		}
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
		rel = normalizeRelationForStorage(rel)
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
			 VALUES ($1, $2, $3, $4, $5)
			 ON CONFLICT (source_id, predicate, target_id) DO UPDATE SET
			 weight = EXCLUDED.weight,
			 created_at = EXCLUDED.created_at`,
			rec.ID, rel.Predicate, rel.TargetID, w, ca.UTC(),
		); err != nil {
			return fmt.Errorf("insert relations: %w", err)
		}
	}

	if err := replaceEntityIndexes(ctx, q, rec); err != nil {
		return err
	}

	return nil
}

func (s *PostgresStore) Update(ctx context.Context, rec *schema.MemoryRecord) error {
	if err := rec.Validate(); err != nil {
		return err
	}
	return storage.WithTransaction(ctx, s, func(tx storage.Transaction) error {
		return tx.Update(ctx, rec)
	})
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
	result, err := listRecordsBounded(ctx, q, opts)
	return result.Records, err
}

func listRecordsBounded(ctx context.Context, q queryable, opts storage.ListOptions) (storage.BoundedListResult, error) {
	budgeted := opts.MaxHydratedBytes > 0
	if budgeted {
		if opts.MaxHydratedBytes > storage.MaxBoundedHydrationBytes {
			opts.MaxHydratedBytes = storage.MaxBoundedHydrationBytes
		}
		if opts.Limit <= 0 || opts.Limit > storage.MaxBoundedLookupLimit {
			opts.Limit = storage.MaxBoundedLookupLimit
		}
		opts.Limit = budgetedCandidateLimit(opts.Limit, opts.MaxHydratedBytes)
	}
	query := `SELECT id`
	if budgeted {
		query += `, salience, created_at`
	}
	query += ` FROM memory_records WHERE 1=1`
	args := []any{}

	addArg := func(v any) string {
		args = append(args, v)
		return fmt.Sprintf("$%d", len(args))
	}

	if opts.ID != "" {
		query += ` AND id = ` + addArg(opts.ID)
	}
	if opts.Type != "" {
		query += ` AND type = ` + addArg(string(opts.Type))
	} else if len(opts.Types) > 0 {
		values := uniqueMemoryTypes(opts.Types)
		if len(values) == 0 {
			query += ` AND FALSE`
		} else {
			placeholders := make([]string, 0, len(values))
			for _, value := range values {
				placeholders = append(placeholders, addArg(string(value)))
			}
			query += ` AND type IN (` + strings.Join(placeholders, ", ") + `)`
		}
	}
	if opts.Scope != "" {
		query += ` AND scope = ` + addArg(opts.Scope)
	} else if len(opts.Scopes) > 0 && !containsString(opts.Scopes, "*") {
		values := uniqueNonEmptyStrings(opts.Scopes)
		if len(values) == 0 {
			if opts.IncludeUnscoped {
				query += ` AND (scope IS NULL OR scope = '')`
			} else {
				query += ` AND FALSE`
			}
		} else {
			placeholders := make([]string, 0, len(values))
			for _, value := range values {
				placeholders = append(placeholders, addArg(value))
			}
			inClause := `scope IN (` + strings.Join(placeholders, ", ") + `)`
			if opts.IncludeUnscoped {
				query += ` AND (scope IS NULL OR scope = '' OR ` + inClause + `)`
			} else {
				query += ` AND ` + inClause
			}
		}
	}
	if opts.Sensitivity != "" {
		query += ` AND sensitivity = ` + addArg(string(opts.Sensitivity))
	} else if opts.MaxSensitivity != "" {
		values := sensitivitiesAtOrBelow(opts.MaxSensitivity)
		if len(values) == 0 {
			query += ` AND FALSE`
		} else {
			placeholders := make([]string, 0, len(values))
			for _, value := range values {
				placeholders = append(placeholders, addArg(string(value)))
			}
			query += ` AND sensitivity IN (` + strings.Join(placeholders, ", ") + `)`
		}
	}
	if opts.MinSalience > 0 {
		query += ` AND salience >= ` + addArg(opts.MinSalience)
	}
	if opts.MaxSalience > 0 {
		query += ` AND salience <= ` + addArg(opts.MaxSalience)
	}
	if budgeted {
		query += fmt.Sprintf(` AND octet_length(id) <= %d AND octet_length(COALESCE(scope, '')) <= %d`, maxProjectedBaseFieldBytes, maxProjectedBaseFieldBytes)
	}
	for i, tag := range opts.Tags {
		alias := fmt.Sprintf("t%d", i)
		tagPlaceholder := addArg(tag)
		query += fmt.Sprintf(` AND EXISTS (SELECT 1 FROM tags %s WHERE %s.record_id = memory_records.id AND %s.tag = %s)`,
			alias, alias, alias, tagPlaceholder)
	}

	query += ` ORDER BY salience DESC, created_at DESC, id`
	if opts.Limit > 0 {
		query += ` LIMIT ` + addArg(opts.Limit)
	}
	if opts.Offset > 0 {
		query += ` OFFSET ` + addArg(opts.Offset)
	}
	if budgeted {
		query = `WITH bounded_candidates AS MATERIALIZED (` + query + `)
			SELECT id,
			       COALESCE((SELECT octet_length(payload_json::text) FROM payloads WHERE record_id = bounded_candidates.id), 0) + ` + projectedRecordVariableBytesSQL("bounded_candidates.id") + `,
			       COALESCE((SELECT octet_length(interpretation_json::text) FROM interpretations WHERE record_id = bounded_candidates.id), 0)
			FROM bounded_candidates
			ORDER BY salience DESC, created_at DESC, id`
	}

	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return storage.BoundedListResult{}, fmt.Errorf("list query: %w", err)
	}
	defer rows.Close()

	idCapacity := opts.Limit
	if idCapacity < 0 {
		idCapacity = 0
	}
	if idCapacity > storage.MaxBoundedLookupLimit {
		idCapacity = storage.MaxBoundedLookupLimit
	}
	ids := make([]string, 0, idCapacity)
	remainingBytes := opts.MaxHydratedBytes
	truncatedByBytes := false
	for rows.Next() {
		var id string
		if budgeted {
			var payloadBytes, interpretationBytes int64
			if err := rows.Scan(&id, &payloadBytes, &interpretationBytes); err != nil {
				return storage.BoundedListResult{}, fmt.Errorf("scan bounded id: %w", err)
			}
			projectedBytes := storage.ProjectedRecordOverheadBytes + payloadBytes + interpretationBytes
			if projectedBytes > remainingBytes {
				truncatedByBytes = true
				break
			}
			remainingBytes -= projectedBytes
		} else if err := rows.Scan(&id); err != nil {
			return storage.BoundedListResult{}, fmt.Errorf("scan id: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return storage.BoundedListResult{}, fmt.Errorf("iterate ids: %w", err)
	}
	if err := rows.Close(); err != nil {
		return storage.BoundedListResult{}, fmt.Errorf("close bounded ids: %w", err)
	}

	records, err := getRecordsBatchWithOptions(ctx, q, ids, recordReadOptions{
		omitRelations: opts.OmitRelations,
		omitHistory:   opts.OmitHistory,
	})
	if err != nil {
		return storage.BoundedListResult{}, err
	}
	return storage.BoundedListResult{
		Records:                 records,
		ProjectedBytes:          opts.MaxHydratedBytes - remainingBytes,
		HydrationBytesTruncated: truncatedByBytes,
	}, nil
}

func uniqueMemoryTypes(values []schema.MemoryType) []schema.MemoryType {
	out := make([]schema.MemoryType, 0, len(values))
	seen := make(map[schema.MemoryType]struct{}, len(values))
	for _, value := range values {
		if !schema.IsValidMemoryType(value) {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func sensitivitiesAtOrBelow(max schema.Sensitivity) []schema.Sensitivity {
	ordered := []schema.Sensitivity{
		schema.SensitivityPublic,
		schema.SensitivityLow,
		schema.SensitivityMedium,
		schema.SensitivityHigh,
		schema.SensitivityHyper,
	}
	for i, value := range ordered {
		if value == max {
			return ordered[:i+1]
		}
	}
	return nil
}

func getRecordsBatch(ctx context.Context, q queryable, ids []string) ([]*schema.MemoryRecord, error) {
	return getRecordsBatchWithOptions(ctx, q, ids, recordReadOptions{})
}

func getRecordsBatchWithOptions(ctx context.Context, q queryable, ids []string, opts recordReadOptions) ([]*schema.MemoryRecord, error) {
	if len(ids) == 0 {
		return []*schema.MemoryRecord{}, nil
	}
	if len(ids) <= 3 {
		records := make([]*schema.MemoryRecord, 0, len(ids))
		for _, id := range ids {
			rec, err := getRecordWithOptions(ctx, q, id, opts)
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
		if !opts.omitHistory {
			rec.Provenance.Sources = []schema.ProvenanceSource{}
		}
		if !opts.omitRelations {
			rec.Relations = []schema.Relation{}
		}
		if !opts.omitHistory {
			rec.AuditLog = []schema.AuditEntry{}
		}
		recMap[rec.ID] = rec
	}
	if err := baseRows.Err(); err != nil {
		return nil, fmt.Errorf("batch iterate memory_records: %w", err)
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
	if err := dpRows.Err(); err != nil {
		return nil, fmt.Errorf("batch iterate decay_profiles: %w", err)
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
	if err := plRows.Err(); err != nil {
		return nil, fmt.Errorf("batch iterate payloads: %w", err)
	}

	intRows, err := q.QueryContext(ctx, fmt.Sprintf(
		`SELECT record_id, interpretation_json FROM interpretations WHERE record_id IN (%s)`, placeholders), idArgs...)
	if err != nil {
		return nil, fmt.Errorf("batch query interpretations: %w", err)
	}
	defer intRows.Close()
	for intRows.Next() {
		var (
			recordID           string
			interpretationJSON []byte
		)
		if err := intRows.Scan(&recordID, &interpretationJSON); err != nil {
			return nil, fmt.Errorf("batch scan interpretations: %w", err)
		}
		if rec, ok := recMap[recordID]; ok && len(interpretationJSON) > 0 {
			var interpretation schema.Interpretation
			if err := json.Unmarshal(interpretationJSON, &interpretation); err != nil {
				return nil, fmt.Errorf("unmarshal interpretation for %s: %w", recordID, err)
			}
			rec.Interpretation = &interpretation
		}
	}
	if err := intRows.Err(); err != nil {
		return nil, fmt.Errorf("batch iterate interpretations: %w", err)
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
	if err := tagRows.Err(); err != nil {
		return nil, fmt.Errorf("batch iterate tags: %w", err)
	}

	if !opts.omitHistory {
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
		if err := provRows.Err(); err != nil {
			return nil, fmt.Errorf("batch iterate provenance_sources: %w", err)
		}
	}

	if !opts.omitRelations {
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
		if err := relRows.Err(); err != nil {
			return nil, fmt.Errorf("batch iterate relations: %w", err)
		}
	}

	if !opts.omitHistory {
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
		if err := auditRows.Err(); err != nil {
			return nil, fmt.Errorf("batch iterate audit_log: %w", err)
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

func getRecordsBatchWithHydrationBudget(ctx context.Context, q queryable, ids []string, opts recordReadOptions, maxHydratedBytes int64) ([]*schema.MemoryRecord, int64, bool, error) {
	if len(ids) == 0 {
		return []*schema.MemoryRecord{}, 0, false, nil
	}
	if maxHydratedBytes <= 0 {
		records, err := getRecordsBatchWithOptions(ctx, q, ids, opts)
		return records, 0, false, err
	}
	if maxHydratedBytes > storage.MaxBoundedHydrationBytes {
		maxHydratedBytes = storage.MaxBoundedHydrationBytes
	}
	candidateLimit := budgetedCandidateLimit(len(ids), maxHydratedBytes)
	idsTruncated := len(ids) > candidateLimit
	ids = ids[:candidateLimit]
	values := make([]string, len(ids))
	args := make([]any, len(ids))
	for i, id := range ids {
		values[i] = fmt.Sprintf("($%d, %d)", i+1, i)
		args[i] = id
	}
	rows, err := q.QueryContext(ctx, `WITH bounded_candidates(id, ord) AS (VALUES `+strings.Join(values, ",")+`)
		SELECT id,
		       COALESCE((SELECT octet_length(payload_json::text) FROM payloads WHERE record_id = bounded_candidates.id), 0) + `+projectedRecordVariableBytesSQL("bounded_candidates.id")+`,
		       COALESCE((SELECT octet_length(interpretation_json::text) FROM interpretations WHERE record_id = bounded_candidates.id), 0)
		FROM bounded_candidates
		ORDER BY ord`, args...)
	if err != nil {
		return nil, 0, false, fmt.Errorf("query bounded hydration sizes: %w", err)
	}
	defer rows.Close()
	boundedIDs := make([]string, 0, len(ids))
	remaining := maxHydratedBytes
	projectedBytes := int64(0)
	truncated := idsTruncated
	for rows.Next() {
		var id string
		var payloadBytes, interpretationBytes int64
		if err := rows.Scan(&id, &payloadBytes, &interpretationBytes); err != nil {
			return nil, 0, false, fmt.Errorf("scan bounded hydration size: %w", err)
		}
		recordBytes := storage.ProjectedRecordOverheadBytes + payloadBytes + interpretationBytes
		if recordBytes > remaining {
			truncated = true
			break
		}
		remaining -= recordBytes
		projectedBytes += recordBytes
		boundedIDs = append(boundedIDs, id)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, false, fmt.Errorf("iterate bounded hydration sizes: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, 0, false, fmt.Errorf("close bounded hydration sizes: %w", err)
	}
	records, err := getRecordsBatchWithOptions(ctx, q, boundedIDs, opts)
	if err != nil {
		return nil, 0, false, err
	}
	return records, projectedBytes, truncated, nil
}

func (s *PostgresStore) List(ctx context.Context, opts storage.ListOptions) ([]*schema.MemoryRecord, error) {
	return listRecords(ctx, s.db, opts)
}

func (s *PostgresStore) ListBounded(ctx context.Context, opts storage.ListOptions) (storage.BoundedListResult, error) {
	if opts.MaxHydratedBytes <= 0 || opts.MaxHydratedBytes > storage.MaxBoundedHydrationBytes {
		opts.MaxHydratedBytes = storage.MaxBoundedHydrationBytes
	}
	if opts.Limit <= 0 || opts.Limit > storage.MaxBoundedLookupLimit {
		opts.Limit = storage.MaxBoundedLookupLimit
	}
	return listRecordsBounded(ctx, s.db, opts)
}

func (s *PostgresStore) GetAuthorizationMetadata(ctx context.Context, ids []string) ([]storage.RecordAuthorizationMetadata, error) {
	return getAuthorizationMetadata(ctx, s.db, ids, false)
}

func getAuthorizationMetadata(ctx context.Context, q queryable, ids []string, lockRows bool) ([]storage.RecordAuthorizationMetadata, error) {
	if len(ids) > storage.MaxAuthorizationMetadataIDs {
		return nil, storage.ErrAuthorizationMetadataLimit
	}
	if len(ids) == 0 {
		return []storage.RecordAuthorizationMetadata{}, nil
	}

	unique := make([]string, 0, len(ids))
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		unique = append(unique, id)
	}
	if len(unique) == 0 {
		return []storage.RecordAuthorizationMetadata{}, nil
	}

	placeholders, args := buildIDPlaceholders(unique, 1)
	query := fmt.Sprintf(
		`SELECT id, COALESCE(scope, ''), sensitivity FROM memory_records WHERE id IN (%s) ORDER BY id`, placeholders,
	)
	if lockRows {
		query += ` FOR SHARE`
	}
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query authorization metadata: %w", err)
	}
	defer rows.Close()

	metadata := make([]storage.RecordAuthorizationMetadata, 0, len(unique))
	for rows.Next() {
		var record storage.RecordAuthorizationMetadata
		if err := rows.Scan(&record.ID, &record.Scope, &record.Sensitivity); err != nil {
			return nil, fmt.Errorf("scan authorization metadata: %w", err)
		}
		metadata = append(metadata, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate authorization metadata: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close authorization metadata: %w", err)
	}
	return metadata, nil
}

func (s *PostgresStore) ListByType(ctx context.Context, memType schema.MemoryType) ([]*schema.MemoryRecord, error) {
	return s.List(ctx, storage.ListOptions{Type: memType})
}

func updateSalience(ctx context.Context, q queryable, id string, salience float64) error {
	if err := storage.ValidateSalience(salience); err != nil {
		return err
	}
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
	if err := ensureRecordExists(ctx, q, id); err != nil {
		return err
	}

	_, err := q.ExecContext(ctx,
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
	if strings.TrimSpace(sourceID) == "" {
		return &schema.ValidationError{Field: "source_id", Message: "source_id is required for relations"}
	}
	if err := rel.Validate(); err != nil {
		return err
	}
	if err := ensureRecordExists(ctx, q, sourceID); err != nil {
		return err
	}
	if err := ensureRecordExists(ctx, q, rel.TargetID); err != nil {
		return err
	}
	rel = normalizeRelationForStorage(rel)

	w := rel.Weight
	if w == 0 {
		w = 1.0
	}
	ca := rel.CreatedAt
	if ca.IsZero() {
		ca = time.Now().UTC()
	}

	_, err := q.ExecContext(ctx,
		`INSERT INTO relations (source_id, predicate, target_id, weight, created_at)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (source_id, predicate, target_id) DO UPDATE SET
		 weight = EXCLUDED.weight,
		 created_at = EXCLUDED.created_at`,
		sourceID, rel.Predicate, rel.TargetID, w, ca.UTC(),
	)
	if err != nil {
		return fmt.Errorf("insert relations: %w", err)
	}
	return nil
}

func normalizeRelationForStorage(rel schema.Relation) schema.Relation {
	rel.Predicate = schema.NormalizeGraphPredicate(rel.Predicate)
	return rel
}

func normalizePayloadForStorage(payload schema.Payload) schema.Payload {
	if semantic, ok := payload.(*schema.SemanticPayload); ok && semantic != nil {
		normalized := *semantic
		normalized.Predicate = schema.NormalizeSemanticPredicate(normalized.Predicate)
		return &normalized
	}
	return payload
}

func ensureRecordExists(ctx context.Context, q queryable, id string) error {
	var exists int
	err := q.QueryRowContext(ctx, `SELECT 1 FROM memory_records WHERE id = $1`, id).Scan(&exists)
	if errors.Is(err, sql.ErrNoRows) {
		return storage.ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("check record existence: %w", err)
	}
	return nil
}

func getRelations(ctx context.Context, q queryable, id string, requireSource bool) ([]schema.Relation, error) {
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
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate relations: %w", err)
	}
	if rels == nil {
		if !requireSource {
			return []schema.Relation{}, nil
		}
		var exists int
		err := q.QueryRowContext(ctx, `SELECT 1 FROM memory_records WHERE id = $1`, id).Scan(&exists)
		if errors.Is(err, sql.ErrNoRows) {
			return nil, storage.ErrNotFound
		}
		if err != nil {
			return nil, fmt.Errorf("check record existence: %w", err)
		}
		rels = []schema.Relation{}
	}
	return rels, nil
}

func (s *PostgresStore) AddRelation(ctx context.Context, sourceID string, rel schema.Relation) error {
	return addRelation(ctx, s.db, sourceID, rel)
}

func (s *PostgresStore) GetRelations(ctx context.Context, id string) ([]schema.Relation, error) {
	return getRelations(ctx, s.db, id, true)
}

func (s *PostgresStore) GetRelationsLimited(ctx context.Context, id string, limit int) ([]schema.Relation, error) {
	if limit <= 0 {
		if err := ensureRecordExists(ctx, s.db, id); err != nil {
			return nil, err
		}
		return []schema.Relation{}, nil
	}
	limit = capBoundedLookupLimit(limit)
	rows, err := s.db.QueryContext(ctx,
		`SELECT predicate, target_id, weight, created_at
		 FROM relations
		 WHERE source_id = $1
		 ORDER BY weight DESC NULLS LAST, created_at DESC, predicate, target_id
		 LIMIT $2`,
		id, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("query bounded relations: %w", err)
	}
	defer rows.Close()

	relations := make([]schema.Relation, 0, limit)
	for rows.Next() {
		var rel schema.Relation
		var weight sql.NullFloat64
		if err := rows.Scan(&rel.Predicate, &rel.TargetID, &weight, &rel.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan bounded relation: %w", err)
		}
		if weight.Valid {
			rel.Weight = weight.Float64
		}
		rel.Predicate = schema.NormalizeGraphPredicate(rel.Predicate)
		relations = append(relations, rel)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate bounded relations: %w", err)
	}
	if len(relations) == 0 {
		if err := ensureRecordExists(ctx, s.db, id); err != nil {
			return nil, err
		}
	}
	return relations, nil
}

func (s *PostgresStore) GetRelationsBounded(ctx context.Context, id string, limit int, maxHydratedBytes int64) (storage.BoundedRelationResult, error) {
	limit = capBoundedLookupLimit(limit)
	if maxHydratedBytes > storage.MaxBoundedHydrationBytes {
		maxHydratedBytes = storage.MaxBoundedHydrationBytes
	}
	if limit <= 0 {
		if err := ensureRecordExists(ctx, s.db, id); err != nil {
			return storage.BoundedRelationResult{}, err
		}
		return storage.BoundedRelationResult{Relations: []schema.Relation{}}, nil
	}
	if maxHydratedBytes <= 0 {
		return storage.BoundedRelationResult{Relations: []schema.Relation{}, HydrationBytesTruncated: true}, nil
	}

	candidateLimit := budgetedRelationCandidateLimit(limit, maxHydratedBytes)
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(
		`SELECT id, %d + octet_length(predicate) + octet_length(target_id) AS projected_bytes
		 FROM relations
		 WHERE source_id = $1
		 ORDER BY weight DESC NULLS LAST, created_at DESC, predicate, target_id
		 LIMIT $2`, storage.ProjectedRelationOverheadBytes), id, candidateLimit)
	if err != nil {
		return storage.BoundedRelationResult{}, fmt.Errorf("query bounded relation sizes: %w", err)
	}

	ids := make([]int64, 0, candidateLimit)
	remaining := maxHydratedBytes
	projectedBytes := int64(0)
	truncated := false
	for rows.Next() {
		var relationID, rowBytes int64
		if err := rows.Scan(&relationID, &rowBytes); err != nil {
			rows.Close()
			return storage.BoundedRelationResult{}, fmt.Errorf("scan bounded relation size: %w", err)
		}
		if rowBytes < storage.ProjectedRelationOverheadBytes || rowBytes > remaining {
			truncated = true
			break
		}
		remaining -= rowBytes
		projectedBytes += rowBytes
		ids = append(ids, relationID)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return storage.BoundedRelationResult{}, fmt.Errorf("iterate bounded relation sizes: %w", err)
	}
	if err := rows.Close(); err != nil {
		return storage.BoundedRelationResult{}, fmt.Errorf("close bounded relation sizes: %w", err)
	}
	if len(ids) == 0 && !truncated {
		if err := ensureRecordExists(ctx, s.db, id); err != nil {
			return storage.BoundedRelationResult{}, err
		}
	}
	relations, err := getRelationsByIDs(ctx, s.db, ids)
	if err != nil {
		return storage.BoundedRelationResult{}, err
	}
	return storage.BoundedRelationResult{
		Relations:               relations,
		ProjectedBytes:          projectedBytes,
		HydrationBytesTruncated: truncated,
	}, nil
}

func getRelationsByIDs(ctx context.Context, q queryable, ids []int64) ([]schema.Relation, error) {
	if len(ids) == 0 {
		return []schema.Relation{}, nil
	}
	values, args := orderedInt64Values(ids)
	rows, err := q.QueryContext(ctx,
		`WITH selected(id, ord) AS (VALUES `+values+`)
		 SELECT relations.predicate, relations.target_id, relations.weight, relations.created_at
		 FROM relations
		 JOIN selected ON selected.id = relations.id
		 ORDER BY selected.ord`, args...)
	if err != nil {
		return nil, fmt.Errorf("hydrate bounded relations: %w", err)
	}
	defer rows.Close()
	relations := make([]schema.Relation, 0, len(ids))
	for rows.Next() {
		var rel schema.Relation
		var weight sql.NullFloat64
		if err := rows.Scan(&rel.Predicate, &rel.TargetID, &weight, &rel.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan bounded relation: %w", err)
		}
		if weight.Valid {
			rel.Weight = weight.Float64
		}
		rel.Predicate = schema.NormalizeGraphPredicate(rel.Predicate)
		relations = append(relations, rel)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate bounded relations: %w", err)
	}
	return relations, nil
}

func (s *PostgresStore) GetIncomingRelations(ctx context.Context, targetID string) ([]schema.GraphEdge, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT source_id, predicate, target_id, weight, created_at
		 FROM relations WHERE target_id = $1 ORDER BY id`,
		targetID,
	)
	if err != nil {
		return nil, fmt.Errorf("query incoming relations: %w", err)
	}
	defer rows.Close()

	edges := make([]schema.GraphEdge, 0)
	for rows.Next() {
		var edge schema.GraphEdge
		var weight sql.NullFloat64
		if err := rows.Scan(&edge.SourceID, &edge.Predicate, &edge.TargetID, &weight, &edge.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan incoming relation: %w", err)
		}
		if weight.Valid {
			edge.Weight = weight.Float64
		}
		edge.Predicate = schema.NormalizeGraphPredicate(edge.Predicate)
		edges = append(edges, edge)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate incoming relations: %w", err)
	}
	if len(edges) == 0 {
		if err := ensureRecordExists(ctx, s.db, targetID); err != nil {
			return nil, err
		}
	}
	return edges, nil
}

func (s *PostgresStore) GetIncomingRelationsLimited(ctx context.Context, targetID string, limit int) ([]schema.GraphEdge, error) {
	if limit <= 0 {
		if err := ensureRecordExists(ctx, s.db, targetID); err != nil {
			return nil, err
		}
		return []schema.GraphEdge{}, nil
	}
	limit = capBoundedLookupLimit(limit)
	rows, err := s.db.QueryContext(ctx,
		`SELECT source_id, predicate, target_id, weight, created_at
		 FROM relations
		 WHERE target_id = $1
		 ORDER BY weight DESC NULLS LAST, created_at DESC, predicate, source_id
		 LIMIT $2`,
		targetID, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("query bounded incoming relations: %w", err)
	}
	defer rows.Close()

	edges := make([]schema.GraphEdge, 0, limit)
	for rows.Next() {
		var edge schema.GraphEdge
		var weight sql.NullFloat64
		if err := rows.Scan(&edge.SourceID, &edge.Predicate, &edge.TargetID, &weight, &edge.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan bounded incoming relation: %w", err)
		}
		if weight.Valid {
			edge.Weight = weight.Float64
		}
		edge.Predicate = schema.NormalizeGraphPredicate(edge.Predicate)
		edges = append(edges, edge)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate bounded incoming relations: %w", err)
	}
	if len(edges) == 0 {
		if err := ensureRecordExists(ctx, s.db, targetID); err != nil {
			return nil, err
		}
	}
	return edges, nil
}

func (s *PostgresStore) GetIncomingRelationsBounded(ctx context.Context, targetID string, limit int, maxHydratedBytes int64) (storage.BoundedIncomingRelationResult, error) {
	limit = capBoundedLookupLimit(limit)
	if maxHydratedBytes > storage.MaxBoundedHydrationBytes {
		maxHydratedBytes = storage.MaxBoundedHydrationBytes
	}
	if limit <= 0 {
		if err := ensureRecordExists(ctx, s.db, targetID); err != nil {
			return storage.BoundedIncomingRelationResult{}, err
		}
		return storage.BoundedIncomingRelationResult{Edges: []schema.GraphEdge{}}, nil
	}
	if maxHydratedBytes <= 0 {
		return storage.BoundedIncomingRelationResult{Edges: []schema.GraphEdge{}, HydrationBytesTruncated: true}, nil
	}

	candidateLimit := budgetedRelationCandidateLimit(limit, maxHydratedBytes)
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(
		`SELECT id, %d + octet_length(source_id) + octet_length(predicate) + octet_length(target_id) AS projected_bytes
		 FROM relations
		 WHERE target_id = $1
		 ORDER BY weight DESC NULLS LAST, created_at DESC, predicate, source_id
		 LIMIT $2`, storage.ProjectedRelationOverheadBytes), targetID, candidateLimit)
	if err != nil {
		return storage.BoundedIncomingRelationResult{}, fmt.Errorf("query bounded incoming relation sizes: %w", err)
	}

	ids := make([]int64, 0, candidateLimit)
	remaining := maxHydratedBytes
	projectedBytes := int64(0)
	truncated := false
	for rows.Next() {
		var relationID, rowBytes int64
		if err := rows.Scan(&relationID, &rowBytes); err != nil {
			rows.Close()
			return storage.BoundedIncomingRelationResult{}, fmt.Errorf("scan bounded incoming relation size: %w", err)
		}
		if rowBytes < storage.ProjectedRelationOverheadBytes || rowBytes > remaining {
			truncated = true
			break
		}
		remaining -= rowBytes
		projectedBytes += rowBytes
		ids = append(ids, relationID)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return storage.BoundedIncomingRelationResult{}, fmt.Errorf("iterate bounded incoming relation sizes: %w", err)
	}
	if err := rows.Close(); err != nil {
		return storage.BoundedIncomingRelationResult{}, fmt.Errorf("close bounded incoming relation sizes: %w", err)
	}
	if len(ids) == 0 && !truncated {
		if err := ensureRecordExists(ctx, s.db, targetID); err != nil {
			return storage.BoundedIncomingRelationResult{}, err
		}
	}
	edges, err := getIncomingRelationsByIDs(ctx, s.db, ids)
	if err != nil {
		return storage.BoundedIncomingRelationResult{}, err
	}
	return storage.BoundedIncomingRelationResult{
		Edges:                   edges,
		ProjectedBytes:          projectedBytes,
		HydrationBytesTruncated: truncated,
	}, nil
}

func getIncomingRelationsByIDs(ctx context.Context, q queryable, ids []int64) ([]schema.GraphEdge, error) {
	if len(ids) == 0 {
		return []schema.GraphEdge{}, nil
	}
	values, args := orderedInt64Values(ids)
	rows, err := q.QueryContext(ctx,
		`WITH selected(id, ord) AS (VALUES `+values+`)
		 SELECT relations.source_id, relations.predicate, relations.target_id, relations.weight, relations.created_at
		 FROM relations
		 JOIN selected ON selected.id = relations.id
		 ORDER BY selected.ord`, args...)
	if err != nil {
		return nil, fmt.Errorf("hydrate bounded incoming relations: %w", err)
	}
	defer rows.Close()
	edges := make([]schema.GraphEdge, 0, len(ids))
	for rows.Next() {
		var edge schema.GraphEdge
		var weight sql.NullFloat64
		if err := rows.Scan(&edge.SourceID, &edge.Predicate, &edge.TargetID, &weight, &edge.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan bounded incoming relation: %w", err)
		}
		if weight.Valid {
			edge.Weight = weight.Float64
		}
		edge.Predicate = schema.NormalizeGraphPredicate(edge.Predicate)
		edges = append(edges, edge)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate bounded incoming relations: %w", err)
	}
	return edges, nil
}

func replaceEntityIndexes(ctx context.Context, q queryable, rec *schema.MemoryRecord) error {
	if _, err := q.ExecContext(ctx, `DELETE FROM entity_terms WHERE record_id = $1`, rec.ID); err != nil {
		return fmt.Errorf("delete entity_terms: %w", err)
	}
	if _, err := q.ExecContext(ctx, `DELETE FROM entity_types WHERE record_id = $1`, rec.ID); err != nil {
		return fmt.Errorf("delete entity_types: %w", err)
	}
	if _, err := q.ExecContext(ctx, `DELETE FROM entity_identifiers WHERE record_id = $1`, rec.ID); err != nil {
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
			`INSERT INTO entity_terms (record_id, normalized_term, term_kind, scope)
			 VALUES ($1, $2, $3, $4)
			 ON CONFLICT DO NOTHING`,
			rec.ID, term, schema.EntityTermKindCanonical, scope,
		); err != nil {
			return fmt.Errorf("insert entity canonical term: %w", err)
		}
	}
	for _, alias := range entity.Aliases {
		if term := schema.NormalizeEntityTerm(alias.Value); term != "" {
			if _, err := q.ExecContext(ctx,
				`INSERT INTO entity_terms (record_id, normalized_term, term_kind, scope)
				 VALUES ($1, $2, $3, $4)
				 ON CONFLICT DO NOTHING`,
				rec.ID, term, firstNonEmptyString(alias.Kind, schema.EntityTermKindAlias), scope,
			); err != nil {
				return fmt.Errorf("insert entity alias term: %w", err)
			}
		}
	}
	for _, entityType := range schema.EntityTypes(entity) {
		if _, err := q.ExecContext(ctx,
			`INSERT INTO entity_types (record_id, entity_type) VALUES ($1, $2)
			 ON CONFLICT DO NOTHING`,
			rec.ID, entityType,
		); err != nil {
			return fmt.Errorf("insert entity type: %w", err)
		}
	}
	for _, identifier := range entity.Identifiers {
		namespace := schema.NormalizeEntityIdentifierNamespace(identifier.Namespace)
		value := strings.TrimSpace(identifier.Value)
		if namespace == "" || value == "" {
			continue
		}
		if _, err := q.ExecContext(ctx,
			`INSERT INTO entity_identifiers (record_id, namespace, value, scope)
			 VALUES ($1, $2, $3, $4)
			 ON CONFLICT DO NOTHING`,
			rec.ID, namespace, value, scope,
		); err != nil {
			return fmt.Errorf("insert entity identifier: %w", err)
		}
		for _, term := range []string{value, namespace + ":" + value} {
			normalizedTerm := schema.NormalizeEntityTerm(term)
			if normalizedTerm == "" {
				continue
			}
			if _, err := q.ExecContext(ctx,
				`INSERT INTO entity_terms (record_id, normalized_term, term_kind, scope)
				 VALUES ($1, $2, $3, $4)
				 ON CONFLICT DO NOTHING`,
				rec.ID, normalizedTerm, schema.EntityTermKindIdentifier, scope,
			); err != nil {
				return fmt.Errorf("insert entity identifier term: %w", err)
			}
		}
	}
	return nil
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func (s *PostgresStore) FindEntitiesByTerm(ctx context.Context, term, scope string, limit int) ([]*schema.MemoryRecord, error) {
	result, err := s.findEntitiesByTerm(ctx, term, scope, limit, false, 0)
	return result.Records, err
}

func (s *PostgresStore) FindGraphEntitiesByTerm(ctx context.Context, term, scope string, limit int) ([]*schema.MemoryRecord, error) {
	result, err := s.findEntitiesByTerm(ctx, term, scope, limit, true, storage.MaxBoundedHydrationBytes)
	return result.Records, err
}

func (s *PostgresStore) FindGraphEntitiesByTermBounded(ctx context.Context, term, scope string, limit int, maxHydratedBytes int64) (storage.BoundedGraphEntityResult, error) {
	return s.findEntitiesByTerm(ctx, term, scope, limit, true, maxHydratedBytes)
}

func (s *PostgresStore) findEntitiesByTerm(ctx context.Context, term, scope string, limit int, omitRelations bool, maxHydratedBytes int64) (storage.BoundedGraphEntityResult, error) {
	normalized := schema.NormalizeEntityTerm(term)
	if normalized == "" {
		return storage.BoundedGraphEntityResult{Records: []*schema.MemoryRecord{}}, nil
	}
	if limit <= 0 {
		limit = 10
	}
	limit = capBoundedLookupLimit(limit)
	candidateLimit := entityCandidateLimit(limit)
	if omitRelations && maxHydratedBytes > 0 {
		candidateLimit = budgetedCandidateLimit(candidateLimit, maxHydratedBytes)
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT record_id, normalized_term
		 FROM entity_terms
		 WHERE (scope = $1 OR scope = '')
		   AND octet_length(record_id) <= 100000
		   AND octet_length(normalized_term) <= 100000
		   AND (normalized_term = $2 OR POSITION(normalized_term IN $2) > 0 OR POSITION($2 IN normalized_term) > 0)
		 ORDER BY CASE WHEN scope = $1 THEN 0 ELSE 1 END,
		          CASE
		            WHEN normalized_term = $2 THEN 0
		            WHEN POSITION(normalized_term IN $2) > 0 THEN 1
		            WHEN POSITION($2 IN normalized_term) > 0 THEN 2
		            ELSE 3
		          END,
		          CASE
		            WHEN POSITION(normalized_term IN $2) > 0 AND normalized_term <> $2 THEN -length(normalized_term)
		            WHEN POSITION($2 IN normalized_term) > 0 AND normalized_term <> $2 THEN length(normalized_term)
		            ELSE 0
		          END,
		          record_id
		 LIMIT $3`,
		scope, normalized, candidateLimit,
	)
	if err != nil {
		return storage.BoundedGraphEntityResult{}, fmt.Errorf("query entity terms: %w", err)
	}
	defer rows.Close()
	type entityTermCandidate struct {
		id          string
		indexedTerm string
		rank        int
		specificity int
	}
	candidates := make([]entityTermCandidate, 0, limit)
	seen := make(map[string]struct{}, limit)
	for rows.Next() {
		var id, indexedTerm string
		if err := rows.Scan(&id, &indexedTerm); err != nil {
			return storage.BoundedGraphEntityResult{}, fmt.Errorf("scan entity term: %w", err)
		}
		if !schema.EntityTermMatchesQuery(indexedTerm, normalized) {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		candidates = append(candidates, entityTermCandidate{
			id:          id,
			indexedTerm: indexedTerm,
			rank:        schema.EntityTermMatchRank(indexedTerm, normalized),
			specificity: schema.EntityTermMatchSpecificity(indexedTerm, normalized),
		})
	}
	if err := rows.Err(); err != nil {
		return storage.BoundedGraphEntityResult{}, fmt.Errorf("iterate entity terms: %w", err)
	}
	if err := rows.Close(); err != nil {
		return storage.BoundedGraphEntityResult{}, fmt.Errorf("close entity terms: %w", err)
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].rank != candidates[j].rank {
			return candidates[i].rank < candidates[j].rank
		}
		if candidates[i].specificity != candidates[j].specificity {
			return candidates[i].specificity > candidates[j].specificity
		}
		if candidates[i].indexedTerm != candidates[j].indexedTerm {
			return candidates[i].indexedTerm < candidates[j].indexedTerm
		}
		return candidates[i].id < candidates[j].id
	})
	if len(candidates) > limit {
		candidates = candidates[:limit]
	}
	ids := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		ids = append(ids, candidate.id)
	}
	records, projectedBytes, truncated, err := getRecordsBatchWithHydrationBudget(ctx, s.db, ids, recordReadOptions{omitRelations: omitRelations, omitHistory: omitRelations}, maxHydratedBytes)
	if err != nil {
		return storage.BoundedGraphEntityResult{}, fmt.Errorf("query memory_records: %w", err)
	}
	sort.SliceStable(records, func(i, j int) bool {
		return entityScopeRank(records[i].Scope, scope) < entityScopeRank(records[j].Scope, scope)
	})
	return storage.BoundedGraphEntityResult{Records: records, ProjectedBytes: projectedBytes, HydrationBytesTruncated: truncated}, nil
}

func (s *PostgresStore) FindEntitiesByTermAllScopes(ctx context.Context, term string, limit int) ([]*schema.MemoryRecord, error) {
	result, err := s.findEntitiesByTermAllScopes(ctx, term, limit, false, 0)
	return result.Records, err
}

func (s *PostgresStore) FindGraphEntitiesByTermAllScopes(ctx context.Context, term string, limit int) ([]*schema.MemoryRecord, error) {
	result, err := s.findEntitiesByTermAllScopes(ctx, term, limit, true, storage.MaxBoundedHydrationBytes)
	return result.Records, err
}

func (s *PostgresStore) FindGraphEntitiesByTermAllScopesBounded(ctx context.Context, term string, limit int, maxHydratedBytes int64) (storage.BoundedGraphEntityResult, error) {
	return s.findEntitiesByTermAllScopes(ctx, term, limit, true, maxHydratedBytes)
}

func (s *PostgresStore) findEntitiesByTermAllScopes(ctx context.Context, term string, limit int, omitRelations bool, maxHydratedBytes int64) (storage.BoundedGraphEntityResult, error) {
	normalized := schema.NormalizeEntityTerm(term)
	if normalized == "" {
		return storage.BoundedGraphEntityResult{Records: []*schema.MemoryRecord{}}, nil
	}
	if limit <= 0 {
		limit = 10
	}
	limit = capBoundedLookupLimit(limit)
	candidateLimit := entityCandidateLimit(limit)
	if omitRelations && maxHydratedBytes > 0 {
		candidateLimit = budgetedCandidateLimit(candidateLimit, maxHydratedBytes)
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT record_id, normalized_term
		 FROM entity_terms
		 WHERE octet_length(record_id) <= 100000
		   AND octet_length(normalized_term) <= 100000
		   AND (normalized_term = $1 OR POSITION(normalized_term IN $1) > 0 OR POSITION($1 IN normalized_term) > 0)
		 ORDER BY CASE
		            WHEN normalized_term = $1 THEN 0
		            WHEN POSITION(normalized_term IN $1) > 0 THEN 1
		            WHEN POSITION($1 IN normalized_term) > 0 THEN 2
		            ELSE 3
		          END,
		          CASE
		            WHEN POSITION(normalized_term IN $1) > 0 AND normalized_term <> $1 THEN -length(normalized_term)
		            WHEN POSITION($1 IN normalized_term) > 0 AND normalized_term <> $1 THEN length(normalized_term)
		            ELSE 0
		          END,
		          record_id
		 LIMIT $2`,
		normalized, candidateLimit,
	)
	if err != nil {
		return storage.BoundedGraphEntityResult{}, fmt.Errorf("query entity terms: %w", err)
	}
	defer rows.Close()
	type entityTermCandidate struct {
		id          string
		indexedTerm string
		rank        int
		specificity int
	}
	candidates := make([]entityTermCandidate, 0, limit)
	seen := make(map[string]struct{}, limit)
	for rows.Next() {
		var id, indexedTerm string
		if err := rows.Scan(&id, &indexedTerm); err != nil {
			return storage.BoundedGraphEntityResult{}, fmt.Errorf("scan entity term: %w", err)
		}
		if !schema.EntityTermMatchesQuery(indexedTerm, normalized) {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		candidates = append(candidates, entityTermCandidate{
			id:          id,
			indexedTerm: indexedTerm,
			rank:        schema.EntityTermMatchRank(indexedTerm, normalized),
			specificity: schema.EntityTermMatchSpecificity(indexedTerm, normalized),
		})
	}
	if err := rows.Err(); err != nil {
		return storage.BoundedGraphEntityResult{}, fmt.Errorf("iterate entity terms: %w", err)
	}
	if err := rows.Close(); err != nil {
		return storage.BoundedGraphEntityResult{}, fmt.Errorf("close entity terms: %w", err)
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].rank != candidates[j].rank {
			return candidates[i].rank < candidates[j].rank
		}
		if candidates[i].specificity != candidates[j].specificity {
			return candidates[i].specificity > candidates[j].specificity
		}
		if candidates[i].indexedTerm != candidates[j].indexedTerm {
			return candidates[i].indexedTerm < candidates[j].indexedTerm
		}
		return candidates[i].id < candidates[j].id
	})
	if len(candidates) > limit {
		candidates = candidates[:limit]
	}
	ids := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		ids = append(ids, candidate.id)
	}
	records, projectedBytes, truncated, err := getRecordsBatchWithHydrationBudget(ctx, s.db, ids, recordReadOptions{omitRelations: omitRelations, omitHistory: omitRelations}, maxHydratedBytes)
	if err != nil {
		return storage.BoundedGraphEntityResult{}, fmt.Errorf("query memory_records: %w", err)
	}
	return storage.BoundedGraphEntityResult{Records: records, ProjectedBytes: projectedBytes, HydrationBytesTruncated: truncated}, nil
}

func entityCandidateLimit(limit int) int {
	if limit <= 0 {
		limit = 10
	}
	limit = capBoundedLookupLimit(limit)
	candidateLimit := limit * 10
	if candidateLimit < 50 {
		return 50
	}
	if candidateLimit > maxRetrievalEntityTermRows {
		return maxRetrievalEntityTermRows
	}
	return candidateLimit
}

const maxRetrievalEntityTermRows = 10_000

func capBoundedLookupLimit(limit int) int {
	if limit > storage.MaxBoundedLookupLimit {
		return storage.MaxBoundedLookupLimit
	}
	return limit
}

func budgetedCandidateLimit(limit int, maxHydratedBytes int64) int {
	return candidateLimitForBudget(limit, maxHydratedBytes, storage.ProjectedRecordOverheadBytes)
}

const (
	maxProjectedBaseFieldBytes = 100_000
	maxProjectedTags           = 100
)

func projectedRecordVariableBytesSQL(recordID string) string {
	// The tag subquery deliberately reads at most maxProjectedTags+1 index
	// entries. A record beyond that cap is made larger than the whole budget so
	// it cannot reach batch hydration. octet_length obtains varlena size without
	// materializing text into the Go process.
	return fmt.Sprintf(`octet_length(%[1]s)
		+ COALESCE((SELECT octet_length(COALESCE(scope, '')) FROM memory_records WHERE id = %[1]s), 0)
		+ COALESCE((SELECT octet_length(curve) + octet_length(deletion_policy) FROM decay_profiles WHERE record_id = %[1]s), 0)
		+ COALESCE((
			SELECT CASE WHEN count(*) > %[2]d THEN %[3]d ELSE COALESCE(sum(tag_bytes), 0) END
			FROM (
				SELECT octet_length(tag) AS tag_bytes
				FROM tags
				WHERE record_id = %[1]s
				ORDER BY tag
				LIMIT %[4]d
			) AS bounded_tags
		), 0)`, recordID, maxProjectedTags, storage.MaxBoundedHydrationBytes+1, maxProjectedTags+1)
}

func budgetedRelationCandidateLimit(limit int, maxHydratedBytes int64) int {
	return candidateLimitForBudget(limit, maxHydratedBytes, storage.ProjectedRelationOverheadBytes)
}

func candidateLimitForBudget(limit int, maxHydratedBytes, rowOverhead int64) int {
	limit = capBoundedLookupLimit(limit)
	if limit <= 0 || maxHydratedBytes <= 0 || rowOverhead <= 0 {
		return 0
	}
	if maxHydratedBytes > storage.MaxBoundedHydrationBytes {
		maxHydratedBytes = storage.MaxBoundedHydrationBytes
	}
	// One extra candidate is enough to prove deterministic prefix truncation:
	// every row consumes at least rowOverhead bytes.
	byBudget := maxHydratedBytes/rowOverhead + 1
	if byBudget < 1 {
		byBudget = 1
	}
	if byBudget < int64(limit) {
		return int(byBudget)
	}
	return limit
}

func entityScopeRank(recordScope, requested string) int {
	if recordScope == requested {
		return 0
	}
	return 1
}

func (s *PostgresStore) FindEntityByIdentifier(ctx context.Context, namespace, value, scope string) (*schema.MemoryRecord, error) {
	result, err := s.findEntityByIdentifier(ctx, namespace, value, scope, false, 0)
	if err != nil || len(result.Records) == 0 {
		return nil, err
	}
	return result.Records[0], nil
}

func (s *PostgresStore) FindGraphEntityByIdentifier(ctx context.Context, namespace, value, scope string) (*schema.MemoryRecord, error) {
	result, err := s.findEntityByIdentifier(ctx, namespace, value, scope, true, storage.MaxBoundedHydrationBytes)
	if err != nil || len(result.Records) == 0 {
		return nil, err
	}
	return result.Records[0], nil
}

func (s *PostgresStore) FindGraphEntityByIdentifierBounded(ctx context.Context, namespace, value, scope string, maxHydratedBytes int64) (storage.BoundedGraphEntityResult, error) {
	return s.findEntityByIdentifier(ctx, namespace, value, scope, true, maxHydratedBytes)
}

func (s *PostgresStore) findEntityByIdentifier(ctx context.Context, namespace, value, scope string, omitRelations bool, maxHydratedBytes int64) (storage.BoundedGraphEntityResult, error) {
	namespace = schema.NormalizeEntityIdentifierNamespace(namespace)
	value = strings.TrimSpace(value)
	if namespace == "" || value == "" {
		return storage.BoundedGraphEntityResult{}, storage.ErrNotFound
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT record_id FROM entity_identifiers
		 WHERE namespace = $1 AND value = $2 AND (scope = $3 OR scope = '')
		   AND octet_length(record_id) <= 100000
		 ORDER BY CASE WHEN scope = $3 THEN 0 ELSE 1 END, record_id
		 LIMIT 1`,
		namespace, value, scope,
	)
	if err != nil {
		return storage.BoundedGraphEntityResult{}, fmt.Errorf("query entity identifiers: %w", err)
	}
	defer rows.Close()
	var id string
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return storage.BoundedGraphEntityResult{}, fmt.Errorf("iterate entity ids: %w", err)
		}
		return storage.BoundedGraphEntityResult{}, storage.ErrNotFound
	}
	if err := rows.Scan(&id); err != nil {
		return storage.BoundedGraphEntityResult{}, fmt.Errorf("scan entity id: %w", err)
	}
	if err := rows.Close(); err != nil {
		return storage.BoundedGraphEntityResult{}, fmt.Errorf("close entity ids: %w", err)
	}
	records, projectedBytes, truncated, err := getRecordsBatchWithHydrationBudget(ctx, s.db, []string{id}, recordReadOptions{omitRelations: omitRelations, omitHistory: omitRelations}, maxHydratedBytes)
	if err != nil {
		return storage.BoundedGraphEntityResult{}, err
	}
	return storage.BoundedGraphEntityResult{Records: records, ProjectedBytes: projectedBytes, HydrationBytesTruncated: truncated}, nil
}

func (s *PostgresStore) FindEntityByIdentifierAllScopes(ctx context.Context, namespace, value string) (*schema.MemoryRecord, error) {
	result, err := s.findEntityByIdentifierAllScopes(ctx, namespace, value, false, 0)
	if err != nil || len(result.Records) == 0 {
		return nil, err
	}
	return result.Records[0], nil
}

func (s *PostgresStore) FindGraphEntityByIdentifierAllScopes(ctx context.Context, namespace, value string) (*schema.MemoryRecord, error) {
	result, err := s.findEntityByIdentifierAllScopes(ctx, namespace, value, true, storage.MaxBoundedHydrationBytes)
	if err != nil || len(result.Records) == 0 {
		return nil, err
	}
	return result.Records[0], nil
}

func (s *PostgresStore) FindGraphEntityByIdentifierAllScopesBounded(ctx context.Context, namespace, value string, maxHydratedBytes int64) (storage.BoundedGraphEntityResult, error) {
	return s.findEntityByIdentifierAllScopes(ctx, namespace, value, true, maxHydratedBytes)
}

func (s *PostgresStore) findEntityByIdentifierAllScopes(ctx context.Context, namespace, value string, omitRelations bool, maxHydratedBytes int64) (storage.BoundedGraphEntityResult, error) {
	namespace = schema.NormalizeEntityIdentifierNamespace(namespace)
	value = strings.TrimSpace(value)
	if namespace == "" || value == "" {
		return storage.BoundedGraphEntityResult{}, storage.ErrNotFound
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT record_id FROM entity_identifiers
		 WHERE namespace = $1 AND value = $2
		   AND octet_length(record_id) <= 100000
		 ORDER BY record_id
		 LIMIT 1`,
		namespace, value,
	)
	if err != nil {
		return storage.BoundedGraphEntityResult{}, fmt.Errorf("query entity identifiers: %w", err)
	}
	defer rows.Close()
	var id string
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return storage.BoundedGraphEntityResult{}, fmt.Errorf("iterate entity ids: %w", err)
		}
		return storage.BoundedGraphEntityResult{}, storage.ErrNotFound
	}
	if err := rows.Scan(&id); err != nil {
		return storage.BoundedGraphEntityResult{}, fmt.Errorf("scan entity id: %w", err)
	}
	if err := rows.Close(); err != nil {
		return storage.BoundedGraphEntityResult{}, fmt.Errorf("close entity ids: %w", err)
	}
	records, projectedBytes, truncated, err := getRecordsBatchWithHydrationBudget(ctx, s.db, []string{id}, recordReadOptions{omitRelations: omitRelations, omitHistory: omitRelations}, maxHydratedBytes)
	if err != nil {
		return storage.BoundedGraphEntityResult{}, err
	}
	return storage.BoundedGraphEntityResult{Records: records, ProjectedBytes: projectedBytes, HydrationBytesTruncated: truncated}, nil
}

func (s *PostgresStore) recordsFromRows(ctx context.Context, rows *sql.Rows) ([]*schema.MemoryRecord, error) {
	return s.recordsFromRowsWithOptions(ctx, rows, recordReadOptions{})
}

func (s *PostgresStore) recordsFromRowsWithOptions(ctx context.Context, rows *sql.Rows, opts recordReadOptions) ([]*schema.MemoryRecord, error) {
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
		rec, err := getRecordWithOptions(ctx, s.db, id, opts)
		if err != nil {
			return nil, err
		}
		records = append(records, rec)
	}
	return records, nil
}

// StoreTriggerEmbedding stores or updates a trigger embedding for a record.
func (s *PostgresStore) StoreTriggerEmbedding(ctx context.Context, recordID string, embedding []float32, model string) error {
	if strings.TrimSpace(recordID) == "" {
		return fmt.Errorf("store trigger embedding: record id is required")
	}
	if err := validateEmbeddingVector("store trigger embedding", embedding, s.embeddingConfig.Dimensions); err != nil {
		return err
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
	var err error
	if s.embeddingConfig.Model != "" {
		err = s.db.QueryRowContext(ctx,
			`SELECT embedding::text FROM trigger_embeddings WHERE record_id = $1 AND model = $2`,
			recordID, s.embeddingConfig.Model,
		).Scan(&raw)
	} else {
		err = s.db.QueryRowContext(ctx,
			`SELECT embedding::text FROM trigger_embeddings WHERE record_id = $1`,
			recordID,
		).Scan(&raw)
	}
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
	if err := validateEmbeddingVector("search trigger embeddings", query, s.embeddingConfig.Dimensions); err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 10
	}
	vectorArg := vectorLiteral(query)
	var (
		rows *sql.Rows
		err  error
	)
	if s.embeddingConfig.Model != "" {
		rows, err = s.db.QueryContext(ctx,
			`SELECT record_id
			 FROM trigger_embeddings
			 WHERE embedding IS NOT NULL AND model = $2
			 ORDER BY embedding <=> $1::vector, record_id
			 LIMIT $3`,
			vectorArg, s.embeddingConfig.Model, limit,
		)
	} else {
		rows, err = s.db.QueryContext(ctx,
			`SELECT record_id
			 FROM trigger_embeddings
			 WHERE embedding IS NOT NULL
			 ORDER BY embedding <=> $1::vector, record_id
			 LIMIT $2`,
			vectorArg, limit,
		)
	}
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
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate trigger embedding results: %w", err)
	}
	return ids, nil
}

// SearchByEmbeddingCandidates returns candidate record IDs ordered by cosine
// distance ascending. The candidate filter is applied inside Postgres so
// unrelated embeddings cannot consume the vector search window.
func (s *PostgresStore) SearchByEmbeddingCandidates(ctx context.Context, query []float32, recordIDs []string, limit int) ([]string, error) {
	if len(query) == 0 || len(recordIDs) == 0 {
		return nil, nil
	}
	if err := validateEmbeddingVector("search candidate trigger embeddings", query, s.embeddingConfig.Dimensions); err != nil {
		return nil, err
	}
	candidateIDs := boundedEmbeddingCandidateIDs(recordIDs)
	if len(candidateIDs) == 0 {
		return nil, nil
	}
	if limit <= 0 || limit > len(candidateIDs) {
		limit = len(candidateIDs)
	}

	args := make([]any, 0, len(candidateIDs)+3)
	args = append(args, vectorLiteral(query))
	values := make([]string, 0, len(candidateIDs))
	for _, id := range candidateIDs {
		args = append(args, id)
		values = append(values, fmt.Sprintf("($%d)", len(args)))
	}

	var modelClause string
	if s.embeddingConfig.Model != "" {
		args = append(args, s.embeddingConfig.Model)
		modelClause = fmt.Sprintf(" AND e.model = $%d", len(args))
	}
	args = append(args, limit)
	querySQL := fmt.Sprintf(
		`WITH candidates(record_id) AS (VALUES %s)
		 SELECT e.record_id
		 FROM trigger_embeddings e
		 JOIN candidates c ON c.record_id = e.record_id
		 WHERE e.embedding IS NOT NULL%s
		 ORDER BY e.embedding <=> $1::vector, e.record_id
		 LIMIT $%d`,
		strings.Join(values, ", "),
		modelClause,
		len(args),
	)

	rows, err := s.db.QueryContext(ctx, querySQL, args...)
	if err != nil {
		return nil, fmt.Errorf("search candidate trigger embeddings: %w", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan candidate trigger embedding result: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate candidate trigger embedding results: %w", err)
	}
	return ids, nil
}

// EmbeddingStats reports how many records have a stored embedding for the
// currently configured model. When no model is configured, all stored
// embeddings are counted because search also runs without a model filter.
func (s *PostgresStore) EmbeddingStats(ctx context.Context) (storage.EmbeddingStats, error) {
	stats := storage.EmbeddingStats{Model: s.embeddingConfig.Model}
	var err error
	if s.embeddingConfig.Model != "" {
		err = s.db.QueryRowContext(ctx,
			`SELECT COUNT(mr.id), COUNT(te.record_id)
			 FROM memory_records mr
			 LEFT JOIN trigger_embeddings te
			   ON te.record_id = mr.id
			  AND te.embedding IS NOT NULL
			  AND te.model = $1`,
			s.embeddingConfig.Model,
		).Scan(&stats.TotalRecords, &stats.EmbeddedRecords)
	} else {
		err = s.db.QueryRowContext(ctx,
			`SELECT COUNT(mr.id), COUNT(te.record_id)
			 FROM memory_records mr
			 LEFT JOIN trigger_embeddings te
			   ON te.record_id = mr.id
			  AND te.embedding IS NOT NULL`,
		).Scan(&stats.TotalRecords, &stats.EmbeddedRecords)
	}
	if err != nil {
		return storage.EmbeddingStats{}, fmt.Errorf("embedding stats: %w", err)
	}
	return stats, nil
}

// AggregateMetrics computes a policy-filtered metrics snapshot with a fixed
// number of aggregate queries. It never hydrates individual memory records.
func (s *PostgresStore) AggregateMetrics(ctx context.Context, filter storage.MetricsFilter) (storage.MetricsAggregate, error) {
	aggregate := storage.MetricsAggregate{
		RecordsByType:        make(map[string]int, 6),
		SalienceDistribution: make(map[string]int, 5),
		EmbeddingModel:       s.embeddingConfig.Model,
	}
	where, filterArgs := metricsFilterClause(filter, "mr", 1)
	baseArgs := append([]any(nil), filterArgs...)
	modelClause := ""
	if s.embeddingConfig.Model != "" {
		baseArgs = append(baseArgs, s.embeddingConfig.Model)
		modelClause = fmt.Sprintf(" AND te.model = $%d", len(baseArgs))
	}

	var recentRecords int
	err := s.db.QueryRowContext(ctx, fmt.Sprintf(
		`SELECT COUNT(*),
		        COALESCE(AVG(mr.salience), 0),
		        COALESCE(AVG(mr.confidence), 0),
		        COUNT(*) FILTER (WHERE mr.salience > 0),
		        COUNT(*) FILTER (WHERE dp.pinned IS TRUE),
		        COUNT(*) FILTER (WHERE mr.created_at >= NOW() - INTERVAL '24 hours'),
		        COUNT(*) FILTER (WHERE mr.salience < 0.2),
		        COUNT(*) FILTER (WHERE mr.salience >= 0.2 AND mr.salience < 0.4),
		        COUNT(*) FILTER (WHERE mr.salience >= 0.4 AND mr.salience < 0.6),
		        COUNT(*) FILTER (WHERE mr.salience >= 0.6 AND mr.salience < 0.8),
		        COUNT(*) FILTER (WHERE mr.salience >= 0.8),
		        COUNT(te.record_id)
		 FROM memory_records mr
		 LEFT JOIN decay_profiles dp ON dp.record_id = mr.id
		 LEFT JOIN trigger_embeddings te
		   ON te.record_id = mr.id
		  AND te.embedding IS NOT NULL%s
		 WHERE %s`, modelClause, where), baseArgs...).Scan(
		&aggregate.TotalRecords,
		&aggregate.AvgSalience,
		&aggregate.AvgConfidence,
		&aggregate.ActiveRecords,
		&aggregate.PinnedRecords,
		&recentRecords,
		newMapIntScanner(aggregate.SalienceDistribution, "0.0-0.2"),
		newMapIntScanner(aggregate.SalienceDistribution, "0.2-0.4"),
		newMapIntScanner(aggregate.SalienceDistribution, "0.4-0.6"),
		newMapIntScanner(aggregate.SalienceDistribution, "0.6-0.8"),
		newMapIntScanner(aggregate.SalienceDistribution, "0.8-1.0"),
		&aggregate.EmbeddedRecords,
	)
	if err != nil {
		return storage.MetricsAggregate{}, fmt.Errorf("aggregate metrics base: %w", err)
	}
	if aggregate.TotalRecords > 0 {
		aggregate.MemoryGrowthRate = float64(recentRecords) / float64(aggregate.TotalRecords)
		aggregate.EmbeddingCoverage = float64(aggregate.EmbeddedRecords) / float64(aggregate.TotalRecords)
	}

	typeRows, err := s.db.QueryContext(ctx, fmt.Sprintf(
		`SELECT mr.type, COUNT(*)
		 FROM memory_records mr
		 WHERE %s
		 GROUP BY mr.type`, where), filterArgs...)
	if err != nil {
		return storage.MetricsAggregate{}, fmt.Errorf("aggregate metrics by type: %w", err)
	}
	for typeRows.Next() {
		var memoryType string
		var count int
		if err := typeRows.Scan(&memoryType, &count); err != nil {
			typeRows.Close()
			return storage.MetricsAggregate{}, fmt.Errorf("scan aggregate metrics by type: %w", err)
		}
		aggregate.RecordsByType[memoryType] = count
	}
	if err := typeRows.Err(); err != nil {
		typeRows.Close()
		return storage.MetricsAggregate{}, fmt.Errorf("iterate aggregate metrics by type: %w", err)
	}
	if err := typeRows.Close(); err != nil {
		return storage.MetricsAggregate{}, fmt.Errorf("close aggregate metrics by type: %w", err)
	}

	var reinforceCount, revisionCount int
	err = s.db.QueryRowContext(ctx, fmt.Sprintf(
		`SELECT COUNT(*),
		        COUNT(*) FILTER (WHERE a.action = 'reinforce'),
		        COUNT(*) FILTER (WHERE a.action IN ('revise', 'fork', 'merge'))
		 FROM audit_log a
		 JOIN memory_records mr ON mr.id = a.record_id
		 WHERE %s`, where), filterArgs...).Scan(
		&aggregate.TotalAuditEntries,
		&reinforceCount,
		&revisionCount,
	)
	if err != nil {
		return storage.MetricsAggregate{}, fmt.Errorf("aggregate metrics audit: %w", err)
	}
	if aggregate.TotalAuditEntries > 0 {
		aggregate.RetrievalUsefulness = float64(reinforceCount) / float64(aggregate.TotalAuditEntries)
		aggregate.RevisionRate = float64(revisionCount) / float64(aggregate.TotalAuditEntries)
	}

	err = s.db.QueryRowContext(ctx, fmt.Sprintf(
		`SELECT COALESCE(AVG(CASE
		          WHEN mr.type = 'competence' AND p.payload_json ? 'performance'
		          THEN COALESCE((p.payload_json->'performance'->>'success_rate')::double precision, 0)
		        END), 0),
		        COALESCE(AVG(CASE
		          WHEN mr.type = 'plan_graph' AND p.payload_json ? 'metrics'
		          THEN COALESCE((p.payload_json->'metrics'->>'execution_count')::double precision, 0)
		        END), 0)
		 FROM memory_records mr
		 JOIN payloads p ON p.record_id = mr.id
		 WHERE %s`, where), filterArgs...).Scan(
		&aggregate.CompetenceSuccessRate,
		&aggregate.PlanReuseFrequency,
	)
	if err != nil {
		return storage.MetricsAggregate{}, fmt.Errorf("aggregate metrics payloads: %w", err)
	}

	return aggregate, nil
}

type mapIntScanner struct {
	target map[string]int
	key    string
}

func newMapIntScanner(target map[string]int, key string) *mapIntScanner {
	return &mapIntScanner{target: target, key: key}
}

func (s *mapIntScanner) Scan(src any) error {
	var value int64
	switch typed := src.(type) {
	case int64:
		value = typed
	case int32:
		value = int64(typed)
	case int:
		value = int64(typed)
	case nil:
		value = 0
	default:
		return fmt.Errorf("unsupported aggregate integer type %T", src)
	}
	s.target[s.key] = int(value)
	return nil
}

func metricsFilterClause(filter storage.MetricsFilter, alias string, start int) (string, []any) {
	args := make([]any, 0, len(filter.Scopes)+5)
	addArg := func(value any) string {
		args = append(args, value)
		return fmt.Sprintf("$%d", start+len(args)-1)
	}
	parts := make([]string, 0, 2)
	if len(filter.Scopes) > 0 && !containsString(filter.Scopes, "*") {
		scopes := uniqueNonEmptyStrings(filter.Scopes)
		if len(scopes) == 0 {
			if filter.IncludeUnscoped {
				parts = append(parts, "("+alias+".scope IS NULL OR "+alias+".scope = '')")
			} else {
				parts = append(parts, "FALSE")
			}
		} else {
			placeholders := make([]string, 0, len(scopes))
			for _, scope := range scopes {
				placeholders = append(placeholders, addArg(scope))
			}
			inClause := alias + ".scope IN (" + strings.Join(placeholders, ", ") + ")"
			if filter.IncludeUnscoped {
				parts = append(parts, "("+alias+".scope IS NULL OR "+alias+".scope = '' OR "+inClause+")")
			} else {
				parts = append(parts, inClause)
			}
		}
	}
	if filter.MaxSensitivity != "" {
		sensitivities := sensitivitiesAtOrBelow(filter.MaxSensitivity)
		if len(sensitivities) == 0 {
			parts = append(parts, "FALSE")
		} else {
			placeholders := make([]string, 0, len(sensitivities))
			for _, sensitivity := range sensitivities {
				placeholders = append(placeholders, addArg(string(sensitivity)))
			}
			parts = append(parts, alias+".sensitivity IN ("+strings.Join(placeholders, ", ")+")")
		}
	}
	if len(parts) == 0 {
		return "TRUE", args
	}
	return strings.Join(parts, " AND "), args
}

func uniqueNonEmptyStrings(values []string) []string {
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

const maxEmbeddingCandidateIDs = 10_000

func boundedEmbeddingCandidateIDs(values []string) []string {
	unique := uniqueNonEmptyStrings(values)
	if len(unique) > maxEmbeddingCandidateIDs {
		return unique[:maxEmbeddingCandidateIDs]
	}
	return unique
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if strings.TrimSpace(value) == want {
			return true
		}
	}
	return false
}

func validateEmbeddingVector(operation string, values []float32, dimensions int) error {
	if len(values) == 0 {
		return fmt.Errorf("%s: embedding is empty", operation)
	}
	if dimensions > 0 && len(values) != dimensions {
		return fmt.Errorf("%s: embedding dimension %d does not match configured dimension %d", operation, len(values), dimensions)
	}
	nonZero := false
	for i, v := range values {
		if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
			return fmt.Errorf("%s: embedding contains non-finite value at index %d", operation, i)
		}
		if v != 0 {
			nonZero = true
		}
	}
	if !nonZero {
		return fmt.Errorf("%s: embedding is all zeros", operation)
	}
	return nil
}

// ClaimUnextractedEpisodics atomically claims up to limit episodic records
// for semantic extraction and returns their record IDs.
func (s *PostgresStore) ClaimUnextractedEpisodics(ctx context.Context, limit int) ([]string, error) {
	if limit <= 0 {
		limit = 50
	}

	rows, err := s.db.QueryContext(ctx,
		`INSERT INTO episodic_extraction_log (record_id, extracted_at, triple_count)
		 SELECT mr.id, NOW(), -1
		 FROM memory_records mr
		 LEFT JOIN episodic_extraction_log eel ON mr.id = eel.record_id
		 WHERE mr.type = 'episodic'
		   AND eel.record_id IS NULL
		 ORDER BY mr.created_at ASC
		 LIMIT $1
		 ON CONFLICT (record_id) DO NOTHING
		 RETURNING record_id`,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("claim episodic extraction records: %w", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan claimed episodic extraction record: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate claimed episodic extraction records: %w", err)
	}
	return ids, nil
}

// MarkEpisodicExtracted marks a claimed episodic record as fully processed.
func (s *PostgresStore) MarkEpisodicExtracted(ctx context.Context, recordID string, tripleCount int) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE episodic_extraction_log
		 SET triple_count = $2, extracted_at = NOW()
		 WHERE record_id = $1`,
		recordID, tripleCount,
	)
	if err != nil {
		return fmt.Errorf("mark episodic extracted: %w", err)
	}
	return nil
}

// ReleaseEpisodicClaim clears an in-flight extraction claim so the episode can be retried.
func (s *PostgresStore) ReleaseEpisodicClaim(ctx context.Context, recordID string) error {
	_, err := s.db.ExecContext(ctx,
		`DELETE FROM episodic_extraction_log
		 WHERE record_id = $1
		   AND triple_count = -1`,
		recordID,
	)
	if err != nil {
		return fmt.Errorf("release episodic claim: %w", err)
	}
	return nil
}

// CleanStaleExtractionClaims deletes in-flight claims older than olderThan.
func (s *PostgresStore) CleanStaleExtractionClaims(ctx context.Context, olderThan time.Duration) error {
	if olderThan <= 0 {
		olderThan = time.Hour
	}
	_, err := s.db.ExecContext(ctx,
		`DELETE FROM episodic_extraction_log
		 WHERE triple_count = -1
		   AND extracted_at < NOW() - ($1 * INTERVAL '1 second')`,
		int64(olderThan.Seconds()),
	)
	if err != nil {
		return fmt.Errorf("clean stale extraction claims: %w", err)
	}
	return nil
}

func findSemanticExact(ctx context.Context, q queryable, subject, predicate, object string) (*schema.MemoryRecord, error) {
	return findSemanticExactInScope(ctx, q, subject, predicate, object, nil)
}

func findSemanticExactInScope(ctx context.Context, q queryable, subject, predicate, object string, scope *string) (*schema.MemoryRecord, error) {
	var id string
	predicate = schema.NormalizeSemanticPredicate(predicate)
	objectJSON := semanticObjectJSONArgument(object)
	query := `SELECT mr.id
	 FROM memory_records mr
	 JOIN payloads p ON mr.id = p.record_id
	 WHERE mr.type = 'semantic'
	   AND p.payload_json->>'subject' = $1
	   AND p.payload_json->>'predicate' = $2
	   AND (
	       p.payload_json->>'object' = $3
	       OR p.payload_json->'object' = $4::jsonb
	   )`
	args := []any{subject, predicate, object, objectJSON}
	if scope != nil {
		query += ` AND COALESCE(mr.scope, '') = $5`
		args = append(args, *scope)
	}
	query += `
	 ORDER BY mr.updated_at DESC, mr.id
	 LIMIT 1`
	err := q.QueryRowContext(ctx, query, args...).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("find semantic exact: %w", err)
	}
	return getRecord(ctx, q, id)
}

func semanticObjectJSONArgument(object string) string {
	if json.Valid([]byte(object)) {
		return object
	}
	encoded, err := json.Marshal(object)
	if err != nil {
		return `""`
	}
	return string(encoded)
}

// FindSemanticExact retrieves a semantic record by exact subject-predicate-object match.
func (s *PostgresStore) FindSemanticExact(ctx context.Context, subject, predicate, object string) (*schema.MemoryRecord, error) {
	return findSemanticExact(ctx, s.db, subject, predicate, object)
}

// FindSemanticExactInScope retrieves a semantic record by exact subject-predicate-object match in one scope.
func (s *PostgresStore) FindSemanticExactInScope(ctx context.Context, subject, predicate, object, scope string) (*schema.MemoryRecord, error) {
	return findSemanticExactInScope(ctx, s.db, subject, predicate, object, &scope)
}

// Reset deletes all stored records and embeddings. Intended for tests and local evaluation flows.
func (s *PostgresStore) Reset(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `
		TRUNCATE TABLE
			episodic_extraction_log,
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

func (t *postgresTx) GetAuthorizationMetadata(ctx context.Context, ids []string) ([]storage.RecordAuthorizationMetadata, error) {
	if err := t.checkClosed(); err != nil {
		return nil, err
	}
	return getAuthorizationMetadata(ctx, t.tx, ids, true)
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
	return getRelations(ctx, t.tx, id, true)
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

func orderedInt64Values(ids []int64) (string, []any) {
	parts := make([]string, len(ids))
	args := make([]any, len(ids))
	for i, id := range ids {
		// VALUES otherwise resolves untyped parameters as text before the JOIN
		// can constrain them to the bigint relation primary key.
		parts[i] = fmt.Sprintf("($%d::bigint, %d)", i+1, i)
		args[i] = id
	}
	return strings.Join(parts, ", "), args
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
	_ storage.Store                             = (*PostgresStore)(nil)
	_ storage.BoundedListStore                  = (*PostgresStore)(nil)
	_ storage.AuthorizationMetadataStore        = (*PostgresStore)(nil)
	_ storage.IncomingRelationLookup            = (*PostgresStore)(nil)
	_ storage.BoundedRelationLookup             = (*PostgresStore)(nil)
	_ storage.BoundedIncomingRelationLookup     = (*PostgresStore)(nil)
	_ storage.ByteBoundedRelationLookup         = (*PostgresStore)(nil)
	_ storage.ByteBoundedIncomingRelationLookup = (*PostgresStore)(nil)
	_ storage.GraphRecordLookup                 = (*PostgresStore)(nil)
	_ storage.EntityLookup                      = (*PostgresStore)(nil)
	_ storage.EntityLookupAllScopes             = (*PostgresStore)(nil)
	_ storage.GraphEntityLookup                 = (*PostgresStore)(nil)
	_ storage.GraphEntityLookupAllScopes        = (*PostgresStore)(nil)
	_ storage.SemanticLookup                    = (*PostgresStore)(nil)
	_ storage.SemanticLookupInScope             = (*PostgresStore)(nil)
	_ storage.EmbeddingStatsProvider            = (*PostgresStore)(nil)
	_ storage.Transaction                       = (*postgresTx)(nil)
)
