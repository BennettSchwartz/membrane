package consolidation

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/GustyCube/membrane/pkg/schema"
	"github.com/GustyCube/membrane/pkg/storage"
)

const (
	defaultExtractionBatchSize = 50
	defaultExtractionStaleAge  = time.Hour
)

// Reinforcer updates salience and audit state for an existing record.
type Reinforcer interface {
	Reinforce(ctx context.Context, id, actor, rationale string) error
}

// ExtractionStore provides the Postgres-only extractor queue operations.
type ExtractionStore interface {
	ClaimUnextractedEpisodics(ctx context.Context, limit int) ([]string, error)
	MarkEpisodicExtracted(ctx context.Context, recordID string, tripleCount int) error
	ReleaseEpisodicClaim(ctx context.Context, recordID string) error
	CleanStaleExtractionClaims(ctx context.Context, olderThan time.Duration) error
	FindSemanticExact(ctx context.Context, subject, predicate, object string) (*schema.MemoryRecord, error)
}

// SemanticExtractor pulls structured semantic facts from episodic records via an LLM.
type SemanticExtractor struct {
	store           storage.Store
	extractionStore ExtractionStore
	reinforcer      Reinforcer
	llm             LLMClient
	batchSize       int
	staleAge        time.Duration
}

// NewSemanticExtractor creates a semantic extractor with default queue settings.
func NewSemanticExtractor(
	store storage.Store,
	extractionStore ExtractionStore,
	reinforcer Reinforcer,
	llm LLMClient,
) *SemanticExtractor {
	return &SemanticExtractor{
		store:           store,
		extractionStore: extractionStore,
		reinforcer:      reinforcer,
		llm:             llm,
		batchSize:       defaultExtractionBatchSize,
		staleAge:        defaultExtractionStaleAge,
	}
}

// Extract runs one batch of semantic extraction work.
func (e *SemanticExtractor) Extract(ctx context.Context) (int, int, error) {
	if e.llm == nil || e.extractionStore == nil || e.store == nil {
		return 0, 0, nil
	}

	if err := e.extractionStore.CleanStaleExtractionClaims(ctx, e.staleAge); err != nil {
		return 0, 0, err
	}

	ids, err := e.extractionStore.ClaimUnextractedEpisodics(ctx, e.batchSize)
	if err != nil {
		return 0, 0, err
	}

	totalCreated := 0
	recordsSkipped := 0

	for _, id := range ids {
		created, completed, err := e.processClaimedRecord(ctx, id)
		if err != nil {
			log.Printf("semantic extractor: record %s: %v", id, err)
		}
		totalCreated += created
		if !completed {
			recordsSkipped++
		}
	}

	return totalCreated, recordsSkipped, nil
}

func (e *SemanticExtractor) processClaimedRecord(ctx context.Context, id string) (int, bool, error) {
	rec, err := e.store.Get(ctx, id)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return 0, true, nil
		}
		_ = e.releaseClaim(ctx, id)
		return 0, false, fmt.Errorf("get claimed episodic record: %w", err)
	}

	content := formatEpisodicContent(rec)
	if content == "" {
		if err := e.extractionStore.MarkEpisodicExtracted(ctx, id, 0); err != nil {
			return 0, false, fmt.Errorf("mark empty episodic record extracted: %w", err)
		}
		return 0, true, nil
	}

	triples, err := e.llm.Extract(ctx, content)
	if err != nil {
		_ = e.releaseClaim(ctx, id)
		return 0, false, fmt.Errorf("extract triples: %w", err)
	}

	created := 0
	hadProcessingError := false
	for _, triple := range dedupeTriples(triples) {
		inserted, err := e.upsertTriple(ctx, rec, triple)
		if err != nil {
			log.Printf("semantic extractor: record %s triple (%s, %s, %s): %v", id, triple.Subject, triple.Predicate, triple.Object, err)
			hadProcessingError = true
			continue
		}
		if inserted {
			created++
		}
	}

	if err := e.extractionStore.MarkEpisodicExtracted(ctx, id, created); err != nil {
		return created, false, fmt.Errorf("mark episodic extracted: %w", err)
	}

	return created, !hadProcessingError, nil
}

func (e *SemanticExtractor) upsertTriple(ctx context.Context, source *schema.MemoryRecord, triple Triple) (bool, error) {
	existing, err := e.extractionStore.FindSemanticExact(ctx, triple.Subject, triple.Predicate, triple.Object)
	if err != nil {
		return false, fmt.Errorf("find existing semantic record: %w", err)
	}
	if existing != nil {
		if e.reinforcer == nil {
			return false, nil
		}
		if err := e.reinforcer.Reinforce(
			ctx,
			existing.ID,
			"consolidation/semantic_extractor",
			fmt.Sprintf("Reinforced exact semantic fact extracted from episodic record %s", source.ID),
		); err != nil {
			return false, fmt.Errorf("reinforce existing semantic record: %w", err)
		}
		return false, nil
	}

	now := time.Now().UTC()
	newRec := newExtractedSemanticRecord(source, triple, now)
	if err := storage.WithTransaction(ctx, e.store, func(tx storage.Transaction) error {
		if err := tx.Create(ctx, newRec); err != nil {
			return err
		}
		return tx.AddRelation(ctx, newRec.ID, schema.Relation{
			Predicate: "derived_from",
			TargetID:  source.ID,
			Weight:    1.0,
			CreatedAt: now,
		})
	}); err != nil {
		return false, fmt.Errorf("create semantic record: %w", err)
	}

	return true, nil
}

func (e *SemanticExtractor) releaseClaim(ctx context.Context, id string) error {
	if err := e.extractionStore.ReleaseEpisodicClaim(ctx, id); err != nil {
		log.Printf("semantic extractor: release claim for %s: %v", id, err)
		return err
	}
	return nil
}

func formatEpisodicContent(rec *schema.MemoryRecord) string {
	payload, ok := rec.Payload.(*schema.EpisodicPayload)
	if !ok {
		return ""
	}

	lines := make([]string, 0, len(payload.Timeline)+len(payload.ToolGraph)+1)
	for _, evt := range payload.Timeline {
		if strings.TrimSpace(evt.Summary) == "" {
			continue
		}
		lines = append(lines, fmt.Sprintf("%s: %s", evt.EventKind, evt.Summary))
	}
	for _, node := range payload.ToolGraph {
		line := fmt.Sprintf("tool %s", node.Tool)
		if result := strings.TrimSpace(fmt.Sprint(node.Result)); result != "" && result != "<nil>" {
			line += fmt.Sprintf(" -> %s", result)
		}
		lines = append(lines, line)
	}
	if payload.Outcome != "" {
		lines = append(lines, fmt.Sprintf("outcome: %s", payload.Outcome))
	}
	return strings.TrimSpace(strings.Join(lines, "\n"))
}

func dedupeTriples(triples []Triple) []Triple {
	seen := make(map[string]struct{}, len(triples))
	deduped := make([]Triple, 0, len(triples))
	for _, triple := range triples {
		normalized := Triple{
			Subject:   strings.TrimSpace(triple.Subject),
			Predicate: strings.TrimSpace(triple.Predicate),
			Object:    strings.TrimSpace(triple.Object),
		}
		if normalized.Subject == "" || normalized.Predicate == "" || normalized.Object == "" {
			continue
		}
		key := normalized.Subject + "\x00" + normalized.Predicate + "\x00" + normalized.Object
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		deduped = append(deduped, normalized)
	}
	return deduped
}

func newExtractedSemanticRecord(source *schema.MemoryRecord, triple Triple, now time.Time) *schema.MemoryRecord {
	payload := &schema.SemanticPayload{
		Kind:      "semantic",
		Subject:   triple.Subject,
		Predicate: triple.Predicate,
		Object:    triple.Object,
		Validity: schema.Validity{
			Mode: schema.ValidityModeGlobal,
		},
		Evidence: []schema.ProvenanceRef{
			{
				SourceType: "episodic",
				SourceID:   source.ID,
				Timestamp:  now,
			},
		},
		RevisionPolicy: "replace",
	}

	rec := schema.NewMemoryRecord(
		uuid.New().String(),
		schema.MemoryTypeSemantic,
		source.Sensitivity,
		payload,
	)
	rec.Confidence = source.Confidence
	rec.Scope = source.Scope
	rec.Tags = deriveTags(source)
	rec.Provenance = schema.Provenance{
		Sources: []schema.ProvenanceSource{
			{
				Kind:      schema.ProvenanceKindObservation,
				Ref:       source.ID,
				CreatedBy: "consolidation/semantic_extractor",
				Timestamp: now,
			},
		},
		CreatedBy: "consolidation/semantic_extractor",
	}
	rec.AuditLog = []schema.AuditEntry{
		{
			Action:    schema.AuditActionCreate,
			Actor:     "consolidation/semantic_extractor",
			Timestamp: now,
			Rationale: fmt.Sprintf("Extracted semantic fact from episodic record %s", source.ID),
		},
	}
	return rec
}
