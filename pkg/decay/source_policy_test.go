package decay

import (
	"context"
	"errors"
	"testing"

	"github.com/BennettSchwartz/membrane/internal/teststore"
	"github.com/BennettSchwartz/membrane/pkg/schema"
	"github.com/BennettSchwartz/membrane/pkg/storage"
)

type sourcePolicyStore struct {
	*teststore.MemoryStore
	unsupported bool
	beforeBegin func()
}

func (s *sourcePolicyStore) Begin(ctx context.Context) (storage.Transaction, error) {
	if s.beforeBegin != nil {
		fn := s.beforeBegin
		s.beforeBegin = nil
		fn()
	}
	tx, err := s.MemoryStore.Begin(ctx)
	if err != nil || !s.unsupported {
		return tx, err
	}
	return &struct{ storage.Transaction }{tx}, nil
}

func TestReinforceFromSourceEnforcesAuthoritativeClassification(t *testing.T) {
	ctx := context.Background()
	for _, sensitivity := range []schema.Sensitivity{schema.SensitivityHigh, schema.SensitivityHyper} {
		t.Run(string(sensitivity), func(t *testing.T) {
			store := teststore.NewMemoryStore()
			record := schema.NewMemoryRecord("fact", schema.MemoryTypeSemantic, schema.SensitivityLow, &schema.SemanticPayload{Kind: "semantic", Subject: "deploy", Predicate: "is", Object: "done", Validity: schema.Validity{Mode: schema.ValidityModeGlobal}})
			source := schema.NewMemoryRecord("source", schema.MemoryTypeEpisodic, sensitivity, &schema.EpisodicPayload{Kind: "episodic", Outcome: schema.OutcomeStatusSuccess})
			record.Salience = 0.2
			record.Lifecycle.Decay.ReinforcementGain = 0.1
			for _, rec := range []*schema.MemoryRecord{record, source} {
				if err := store.Create(ctx, rec); err != nil {
					t.Fatal(err)
				}
			}
			svc := NewService(store)
			if err := svc.ReinforceFromSource(ctx, record.ID, "episodic", source.ID, "test", "source evidence"); err != nil {
				t.Fatal(err)
			}
			after, _ := store.Get(ctx, record.ID)
			if after.Sensitivity != sensitivity || !semanticPayloadHasSource(after.Payload.(*schema.SemanticPayload), source.ID) {
				t.Fatalf("reinforced record = %+v", after)
			}
			beforeSalience := after.Salience
			after.Sensitivity = schema.SensitivityLow
			if err := store.Update(ctx, after); err != nil {
				t.Fatal(err)
			}
			if err := svc.ReinforceFromSource(ctx, record.ID, "episodic", source.ID, "test", "source retry"); err != nil {
				t.Fatal(err)
			}
			after, _ = store.Get(ctx, record.ID)
			if after.Sensitivity != sensitivity || after.Salience != beforeSalience || len(after.Payload.(*schema.SemanticPayload).Evidence) != 1 {
				t.Fatalf("retry repair = %+v", after)
			}
		})
	}
}

func TestReinforceFromSourceRejectsMissingIncompatibleAndUnsupportedPolicy(t *testing.T) {
	ctx := context.Background()
	for _, scenario := range []string{"missing", "scope", "unsupported", "empty"} {
		t.Run(scenario, func(t *testing.T) {
			base := teststore.NewMemoryStore()
			store := &sourcePolicyStore{MemoryStore: base, unsupported: scenario == "unsupported"}
			record := schema.NewMemoryRecord("fact", schema.MemoryTypeSemantic, schema.SensitivityLow, &schema.SemanticPayload{Kind: "semantic", Subject: "deploy", Predicate: "is", Object: "done", Validity: schema.Validity{Mode: schema.ValidityModeGlobal}})
			if err := base.Create(ctx, record); err != nil {
				t.Fatal(err)
			}
			sourceID := "source"
			if scenario == "empty" {
				sourceID = ""
			}
			if scenario != "missing" && scenario != "empty" {
				source := schema.NewMemoryRecord(sourceID, schema.MemoryTypeEpisodic, schema.SensitivityHigh, &schema.EpisodicPayload{Kind: "episodic", Outcome: schema.OutcomeStatusSuccess})
				if scenario == "scope" {
					source.Scope = "private"
				}
				if err := base.Create(ctx, source); err != nil {
					t.Fatal(err)
				}
			}
			err := NewService(store).ReinforceFromSource(ctx, record.ID, "episodic", sourceID, "test", "source evidence")
			if err == nil {
				t.Fatal("unsafe reinforcement succeeded")
			}
			if scenario == "unsupported" && !errors.Is(err, storage.ErrAuthorizationMetadataUnsupported) {
				t.Fatalf("error = %v", err)
			}
			after, _ := base.Get(ctx, record.ID)
			if after.Sensitivity != record.Sensitivity || after.Salience != record.Salience || len(after.AuditLog) != len(record.AuditLog) || len(after.Provenance.Sources) != 0 {
				t.Fatalf("rejected reinforcement mutated destination: %+v", after)
			}
		})
	}
}

func TestReinforceFromSourceDoesNotGrantLowerSourceInfluence(t *testing.T) {
	ctx := context.Background()
	for _, known := range []bool{false, true} {
		t.Run(map[bool]string{false: "new", true: "known"}[known], func(t *testing.T) {
			store := teststore.NewMemoryStore()
			record := schema.NewMemoryRecord("fact", schema.MemoryTypeSemantic, schema.SensitivityHigh, &schema.SemanticPayload{Kind: "semantic", Subject: "deploy", Predicate: "is", Object: "done", Validity: schema.Validity{Mode: schema.ValidityModeGlobal}})
			record.Salience = 0.2
			record.Lifecycle.Decay.ReinforcementGain = 0.1
			source := schema.NewMemoryRecord("source", schema.MemoryTypeEpisodic, schema.SensitivityLow, &schema.EpisodicPayload{Kind: "episodic", Outcome: schema.OutcomeStatusSuccess})
			if known {
				appendSemanticEvidenceSource(record, "episodic", source.ID, "test", record.CreatedAt)
			}
			for _, rec := range []*schema.MemoryRecord{source, record} {
				if err := store.Create(ctx, rec); err != nil {
					t.Fatal(err)
				}
			}
			if err := NewService(store).ReinforceFromSource(ctx, record.ID, "episodic", source.ID, "test", "lower evidence"); err != nil {
				t.Fatal(err)
			}
			after, _ := store.Get(ctx, record.ID)
			if after.Salience != record.Salience || len(after.Provenance.Sources) != len(record.Provenance.Sources) || len(after.Payload.(*schema.SemanticPayload).Evidence) != len(record.Payload.(*schema.SemanticPayload).Evidence) || len(after.AuditLog) != len(record.AuditLog) {
				t.Fatal("lower evidence influenced protected destination")
			}
		})
	}
}
