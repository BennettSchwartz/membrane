package consolidation

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/BennettSchwartz/membrane/pkg/schema"
	"github.com/BennettSchwartz/membrane/pkg/storage"
)

type typedListConsolidationStore struct {
	storage.Store
	byType          map[schema.MemoryType][]*schema.MemoryRecord
	relations       map[string][]schema.Relation
	entityMatches   map[string][]*schema.MemoryRecord
	calls           int
	listErrAt       int
	getRelationsErr error
	tx              *consolidationTx
}

func (s *typedListConsolidationStore) ListByType(_ context.Context, memType schema.MemoryType) ([]*schema.MemoryRecord, error) {
	s.calls++
	if s.listErrAt > 0 && s.calls == s.listErrAt {
		return nil, errors.New("list failed")
	}
	return append([]*schema.MemoryRecord(nil), s.byType[memType]...), nil
}

func (s *typedListConsolidationStore) GetRelations(_ context.Context, sourceID string) ([]schema.Relation, error) {
	if s.getRelationsErr != nil {
		return nil, s.getRelationsErr
	}
	return append([]schema.Relation(nil), s.relations[sourceID]...), nil
}

func (s *typedListConsolidationStore) FindEntitiesByTerm(_ context.Context, term, _ string, _ int) ([]*schema.MemoryRecord, error) {
	return append([]*schema.MemoryRecord(nil), s.entityMatches[term]...), nil
}

func (s *typedListConsolidationStore) FindEntityByIdentifier(context.Context, string, string, string) (*schema.MemoryRecord, error) {
	return nil, storage.ErrNotFound
}

func (s *typedListConsolidationStore) Begin(context.Context) (storage.Transaction, error) {
	if s.tx == nil {
		s.tx = &consolidationTx{}
	}
	return s.tx, nil
}

type consolidationTx struct {
	storage.Transaction
	updateSalienceErr error
	auditErr          error
	createErr         error
	relationErr       error
	relationErrAt     int
	relationCalls     int
}

func (tx *consolidationTx) Create(context.Context, *schema.MemoryRecord) error {
	return tx.createErr
}

func (tx *consolidationTx) UpdateSalience(context.Context, string, float64) error {
	return tx.updateSalienceErr
}

func (tx *consolidationTx) AddAuditEntry(context.Context, string, schema.AuditEntry) error {
	return tx.auditErr
}

func (tx *consolidationTx) AddRelation(context.Context, string, schema.Relation) error {
	tx.relationCalls++
	if tx.relationErrAt > 0 {
		if tx.relationCalls == tx.relationErrAt {
			return tx.relationErr
		}
		return nil
	}
	return tx.relationErr
}

func (tx *consolidationTx) Commit() error {
	return nil
}

func (tx *consolidationTx) Rollback() error {
	return nil
}

func TestSemanticConsolidatorPropagatesListAndTransactionErrors(t *testing.T) {
	ctx := context.Background()
	source := semanticSourceEpisode("semantic-error-source", "deploy", "deployment completed", schema.OutcomeStatusSuccess)
	existing := schema.NewMemoryRecord("semantic-error-existing", schema.MemoryTypeSemantic, schema.SensitivityLow, &schema.SemanticPayload{
		Kind:      "semantic",
		Subject:   "deploy",
		Predicate: "observed_in",
		Object:    "deployment completed",
		Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
	})

	for _, tc := range []struct {
		name      string
		store     *typedListConsolidationStore
		wantError string
	}{
		{
			name: "episodic list",
			store: &typedListConsolidationStore{
				byType:    map[schema.MemoryType][]*schema.MemoryRecord{},
				listErrAt: 1,
			},
			wantError: "list failed",
		},
		{
			name: "semantic list",
			store: &typedListConsolidationStore{
				byType: map[schema.MemoryType][]*schema.MemoryRecord{
					schema.MemoryTypeEpisodic: {source},
				},
				listErrAt: 2,
			},
			wantError: "list failed",
		},
		{
			name: "reinforce update",
			store: &typedListConsolidationStore{
				byType: map[schema.MemoryType][]*schema.MemoryRecord{
					schema.MemoryTypeEpisodic: {source},
					schema.MemoryTypeSemantic: {existing, schema.NewMemoryRecord("bad-semantic-payload", schema.MemoryTypeSemantic, schema.SensitivityLow, &schema.WorkingPayload{
						Kind:     "working",
						ThreadID: "thread",
						State:    schema.TaskStatePlanning,
					})},
				},
				tx: &consolidationTx{updateSalienceErr: errors.New("update salience failed")},
			},
			wantError: "update salience failed",
		},
		{
			name: "create",
			store: &typedListConsolidationStore{
				byType: map[schema.MemoryType][]*schema.MemoryRecord{
					schema.MemoryTypeEpisodic: {source},
				},
				tx: &consolidationTx{createErr: errors.New("create failed")},
			},
			wantError: "create failed",
		},
		{
			name: "derived relation",
			store: &typedListConsolidationStore{
				byType: map[schema.MemoryType][]*schema.MemoryRecord{
					schema.MemoryTypeEpisodic: {source},
				},
				tx: &consolidationTx{relationErr: errors.New("relation failed")},
			},
			wantError: "relation failed",
		},
		{
			name: "entity inverse relation",
			store: &typedListConsolidationStore{
				byType: map[schema.MemoryType][]*schema.MemoryRecord{
					schema.MemoryTypeEpisodic: {source},
				},
				entityMatches: map[string][]*schema.MemoryRecord{
					"deploy": {schema.NewMemoryRecord("entity-deploy", schema.MemoryTypeEntity, schema.SensitivityLow, &schema.EntityPayload{
						Kind:          "entity",
						CanonicalName: "deploy",
					})},
				},
				tx: &consolidationTx{relationErr: errors.New("inverse relation failed"), relationErrAt: 2},
			},
			wantError: "inverse relation failed",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := NewSemanticConsolidator(tc.store).Consolidate(ctx)
			if err == nil || !strings.Contains(err.Error(), tc.wantError) {
				t.Fatalf("Consolidate error = %v, want %q", err, tc.wantError)
			}
		})
	}
}

func TestEpisodicConsolidatorPropagatesTransactionErrors(t *testing.T) {
	ctx := context.Background()
	source := episodicRecordForCompression("episodic-error-source", time.Now().UTC().Add(-48*time.Hour), 0.8)

	for _, tc := range []struct {
		name      string
		tx        *consolidationTx
		wantError string
	}{
		{
			name:      "update salience",
			tx:        &consolidationTx{updateSalienceErr: errors.New("update salience failed")},
			wantError: "update salience failed",
		},
		{
			name:      "audit",
			tx:        &consolidationTx{auditErr: errors.New("audit failed")},
			wantError: "audit failed",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &typedListConsolidationStore{
				byType: map[schema.MemoryType][]*schema.MemoryRecord{
					schema.MemoryTypeEpisodic: {source},
				},
				tx: tc.tx,
			}
			count, err := NewEpisodicConsolidator(store).Consolidate(ctx)
			if err == nil || !strings.Contains(err.Error(), tc.wantError) {
				t.Fatalf("Consolidate error = %v, want %q", err, tc.wantError)
			}
			if count != 0 {
				t.Fatalf("compressed = %d, want 0 after transaction failure", count)
			}
		})
	}
}

func TestSemanticConsolidatorSkipsNonEpisodicPayload(t *testing.T) {
	ctx := context.Background()
	store := &typedListConsolidationStore{
		byType: map[schema.MemoryType][]*schema.MemoryRecord{
			schema.MemoryTypeEpisodic: {
				schema.NewMemoryRecord("wrong-episodic-payload", schema.MemoryTypeEpisodic, schema.SensitivityLow, &schema.WorkingPayload{
					Kind:     "working",
					ThreadID: "thread",
					State:    schema.TaskStatePlanning,
				}),
			},
		},
	}

	created, reinforced, err := NewSemanticConsolidator(store).Consolidate(ctx)
	if err != nil {
		t.Fatalf("Consolidate: %v", err)
	}
	if created != 0 || reinforced != 0 {
		t.Fatalf("created/reinforced = %d/%d, want 0/0", created, reinforced)
	}
}

func TestCompetenceConsolidatorPropagatesListAndTransactionErrors(t *testing.T) {
	ctx := context.Background()
	tools := []schema.ToolNode{{ID: "tool-1", Tool: "rg"}}
	sourceA := episodicToolRecord("competence-error-a", tools)
	sourceB := episodicToolRecord("competence-error-b", tools)
	existing := schema.NewMemoryRecord("competence-error-existing", schema.MemoryTypeCompetence, schema.SensitivityLow, &schema.CompetencePayload{
		Kind:      "competence",
		SkillName: "skill:rg",
		Triggers:  []schema.Trigger{{Signal: "rg"}},
		Recipe:    []schema.RecipeStep{{Step: "run rg", Tool: "rg"}},
	})

	for _, tc := range []struct {
		name      string
		store     *typedListConsolidationStore
		wantError string
	}{
		{
			name: "episodic list",
			store: &typedListConsolidationStore{
				byType:    map[schema.MemoryType][]*schema.MemoryRecord{},
				listErrAt: 1,
			},
			wantError: "list failed",
		},
		{
			name: "competence list",
			store: &typedListConsolidationStore{
				byType: map[schema.MemoryType][]*schema.MemoryRecord{
					schema.MemoryTypeEpisodic: {sourceA, sourceB},
				},
				listErrAt: 2,
			},
			wantError: "list failed",
		},
		{
			name: "reinforce update",
			store: &typedListConsolidationStore{
				byType: map[schema.MemoryType][]*schema.MemoryRecord{
					schema.MemoryTypeEpisodic:   {sourceA, sourceB},
					schema.MemoryTypeCompetence: {existing},
				},
				tx: &consolidationTx{updateSalienceErr: errors.New("update salience failed")},
			},
			wantError: "update salience failed",
		},
		{
			name: "reinforce audit",
			store: &typedListConsolidationStore{
				byType: map[schema.MemoryType][]*schema.MemoryRecord{
					schema.MemoryTypeEpisodic: {sourceA, sourceB},
					schema.MemoryTypeCompetence: {existing, schema.NewMemoryRecord("bad-competence-payload", schema.MemoryTypeCompetence, schema.SensitivityLow, &schema.WorkingPayload{
						Kind:     "working",
						ThreadID: "thread",
						State:    schema.TaskStatePlanning,
					})},
				},
				tx: &consolidationTx{auditErr: errors.New("audit failed")},
			},
			wantError: "audit failed",
		},
		{
			name: "create",
			store: &typedListConsolidationStore{
				byType: map[schema.MemoryType][]*schema.MemoryRecord{
					schema.MemoryTypeEpisodic: {sourceA, sourceB},
				},
				tx: &consolidationTx{createErr: errors.New("create failed")},
			},
			wantError: "create failed",
		},
		{
			name: "derived relation",
			store: &typedListConsolidationStore{
				byType: map[schema.MemoryType][]*schema.MemoryRecord{
					schema.MemoryTypeEpisodic: {sourceA, sourceB},
				},
				tx: &consolidationTx{relationErr: errors.New("relation failed")},
			},
			wantError: "relation failed",
		},
		{
			name: "entity inverse relation",
			store: &typedListConsolidationStore{
				byType: map[schema.MemoryType][]*schema.MemoryRecord{
					schema.MemoryTypeEpisodic: {sourceA, sourceB},
				},
				entityMatches: map[string][]*schema.MemoryRecord{
					"rg": {schema.NewMemoryRecord("entity-rg", schema.MemoryTypeEntity, schema.SensitivityLow, &schema.EntityPayload{
						Kind:          "entity",
						CanonicalName: "rg",
					})},
				},
				tx: &consolidationTx{relationErr: errors.New("inverse relation failed"), relationErrAt: 3},
			},
			wantError: "inverse relation failed",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := NewCompetenceConsolidator(tc.store).Consolidate(ctx)
			if err == nil || !strings.Contains(err.Error(), tc.wantError) {
				t.Fatalf("Consolidate error = %v, want %q", err, tc.wantError)
			}
		})
	}
}

func TestCompetenceConsolidatorSkipsIneligiblePatterns(t *testing.T) {
	ctx := context.Background()
	store := &typedListConsolidationStore{
		byType: map[schema.MemoryType][]*schema.MemoryRecord{
			schema.MemoryTypeEpisodic: {
				schema.NewMemoryRecord("wrong-payload", schema.MemoryTypeEpisodic, schema.SensitivityLow, &schema.WorkingPayload{
					Kind:     "working",
					ThreadID: "thread",
					State:    schema.TaskStatePlanning,
				}),
				schema.NewMemoryRecord("no-toolgraph", schema.MemoryTypeEpisodic, schema.SensitivityLow, &schema.EpisodicPayload{
					Kind:    "episodic",
					Outcome: schema.OutcomeStatusSuccess,
				}),
				episodicToolRecord("single-occurrence", []schema.ToolNode{{ID: "tool-1", Tool: "single"}}),
			},
		},
	}

	created, reinforced, err := NewCompetenceConsolidator(store).Consolidate(ctx)
	if err != nil {
		t.Fatalf("Consolidate: %v", err)
	}
	if created != 0 || reinforced != 0 {
		t.Fatalf("created/reinforced = %d/%d, want 0/0", created, reinforced)
	}
}

func TestPlanGraphConsolidatorPropagatesListAndTransactionErrors(t *testing.T) {
	ctx := context.Background()
	source := episodicToolRecord("plan-error-source", []schema.ToolNode{
		{ID: "n1", Tool: "checkout"},
		{ID: "n2", Tool: "test", DependsOn: []string{"n1"}},
		{ID: "n3", Tool: "deploy", DependsOn: []string{"n2"}},
	})
	derivedPlan := schema.NewMemoryRecord("derived-plan", schema.MemoryTypePlanGraph, schema.SensitivityLow, &schema.PlanGraphPayload{
		Kind:   "plan_graph",
		PlanID: "derived-plan",
	})

	for _, tc := range []struct {
		name      string
		store     *typedListConsolidationStore
		wantError string
		wantCount int
	}{
		{
			name: "episodic list",
			store: &typedListConsolidationStore{
				byType:    map[schema.MemoryType][]*schema.MemoryRecord{},
				listErrAt: 1,
			},
			wantError: "list failed",
		},
		{
			name: "plan graph list",
			store: &typedListConsolidationStore{
				byType: map[schema.MemoryType][]*schema.MemoryRecord{
					schema.MemoryTypeEpisodic: {source},
				},
				listErrAt: 2,
			},
			wantError: "list failed",
		},
		{
			name: "existing relation lookup failure is skipped",
			store: &typedListConsolidationStore{
				byType: map[schema.MemoryType][]*schema.MemoryRecord{
					schema.MemoryTypeEpisodic:  {source},
					schema.MemoryTypePlanGraph: {derivedPlan},
				},
				getRelationsErr: errors.New("relations unavailable"),
			},
			wantCount: 1,
		},
		{
			name: "non episodic payload",
			store: &typedListConsolidationStore{
				byType: map[schema.MemoryType][]*schema.MemoryRecord{
					schema.MemoryTypeEpisodic: {
						schema.NewMemoryRecord("wrong-payload", schema.MemoryTypeEpisodic, schema.SensitivityLow, &schema.WorkingPayload{
							Kind:     "working",
							ThreadID: "thread",
							State:    schema.TaskStatePlanning,
						}),
					},
				},
			},
			wantCount: 0,
		},
		{
			name: "create",
			store: &typedListConsolidationStore{
				byType: map[schema.MemoryType][]*schema.MemoryRecord{
					schema.MemoryTypeEpisodic: {source},
				},
				tx: &consolidationTx{createErr: errors.New("create failed")},
			},
			wantError: "create failed",
		},
		{
			name: "derived relation",
			store: &typedListConsolidationStore{
				byType: map[schema.MemoryType][]*schema.MemoryRecord{
					schema.MemoryTypeEpisodic: {source},
				},
				tx: &consolidationTx{relationErr: errors.New("relation failed")},
			},
			wantError: "relation failed",
		},
		{
			name: "entity inverse relation",
			store: &typedListConsolidationStore{
				byType: map[schema.MemoryType][]*schema.MemoryRecord{
					schema.MemoryTypeEpisodic: {source},
				},
				entityMatches: map[string][]*schema.MemoryRecord{
					"deploy": {schema.NewMemoryRecord("entity-deploy", schema.MemoryTypeEntity, schema.SensitivityLow, &schema.EntityPayload{
						Kind:          "entity",
						CanonicalName: "deploy",
					})},
				},
				tx: &consolidationTx{relationErr: errors.New("inverse relation failed"), relationErrAt: 2},
			},
			wantError: "inverse relation failed",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			count, err := NewPlanGraphConsolidator(tc.store).Consolidate(ctx)
			if tc.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantError) {
					t.Fatalf("Consolidate error = %v, want %q", err, tc.wantError)
				}
				return
			}
			if err != nil {
				t.Fatalf("Consolidate: %v", err)
			}
			if count != tc.wantCount {
				t.Fatalf("created = %d, want %d", count, tc.wantCount)
			}
		})
	}
}
