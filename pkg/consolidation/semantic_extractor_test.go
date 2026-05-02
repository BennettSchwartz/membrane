package consolidation

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/BennettSchwartz/membrane/pkg/schema"
	"github.com/BennettSchwartz/membrane/pkg/storage"
)

func TestSemanticExtractorExtract(t *testing.T) {
	t.Run("creates semantic records from claimed episodics", func(t *testing.T) {
		store := newFakeExtractionStore()
		store.records["episode-1"] = newTestEpisodic("episode-1", "user_input", "Bennett prefers terse explanations")
		store.claimQueue = []string{"episode-1"}

		llm := &fakeLLMClient{
			triplesByRecord: map[string][]Triple{
				"user_input: Bennett prefers terse explanations\noutcome: success": {
					{Subject: "bennett", Predicate: "prefers", Object: "terse explanations"},
					{Subject: "bennett", Predicate: "values", Object: "clear structure"},
				},
			},
		}

		extractor := NewSemanticExtractor(store, store, &fakeReinforcer{}, llm)
		created, skipped, err := extractor.Extract(context.Background())
		if err != nil {
			t.Fatalf("Extract: %v", err)
		}
		if created != 2 {
			t.Fatalf("created = %d, want 2", created)
		}
		if skipped != 0 {
			t.Fatalf("skipped = %d, want 0", skipped)
		}
		if got := store.marked["episode-1"]; got != 2 {
			t.Fatalf("marked triple count = %d, want 2", got)
		}
		if llm.calls != 1 {
			t.Fatalf("llm calls = %d, want 1", llm.calls)
		}
	})

	t.Run("releases claim on llm error", func(t *testing.T) {
		store := newFakeExtractionStore()
		store.records["episode-1"] = newTestEpisodic("episode-1", "user_input", "Bennett likes Rust")
		store.claimQueue = []string{"episode-1"}

		llm := &fakeLLMClient{err: errors.New("timeout")}
		extractor := NewSemanticExtractor(store, store, &fakeReinforcer{}, llm)

		created, skipped, err := extractor.Extract(context.Background())
		if err != nil {
			t.Fatalf("Extract: %v", err)
		}
		if created != 0 {
			t.Fatalf("created = %d, want 0", created)
		}
		if skipped != 1 {
			t.Fatalf("skipped = %d, want 1", skipped)
		}
		if _, ok := store.marked["episode-1"]; ok {
			t.Fatalf("episode should not be marked complete on llm error")
		}
		if !store.released["episode-1"] {
			t.Fatalf("episode claim was not released")
		}
	})

	t.Run("marks empty episodics complete without calling llm", func(t *testing.T) {
		store := newFakeExtractionStore()
		store.records["episode-1"] = &schema.MemoryRecord{
			ID:          "episode-1",
			Type:        schema.MemoryTypeEpisodic,
			Sensitivity: schema.SensitivityLow,
			Confidence:  0.6,
			Salience:    0.5,
			CreatedAt:   time.Now().UTC(),
			UpdatedAt:   time.Now().UTC(),
			Lifecycle: schema.Lifecycle{
				Decay: schema.DecayProfile{
					Curve:           schema.DecayCurveExponential,
					HalfLifeSeconds: 86400,
				},
				LastReinforcedAt: time.Now().UTC(),
				DeletionPolicy:   schema.DeletionPolicyAutoPrune,
			},
			Payload: &schema.EpisodicPayload{Kind: "episodic"},
		}
		store.claimQueue = []string{"episode-1"}

		llm := &fakeLLMClient{}
		extractor := NewSemanticExtractor(store, store, &fakeReinforcer{}, llm)

		created, skipped, err := extractor.Extract(context.Background())
		if err != nil {
			t.Fatalf("Extract: %v", err)
		}
		if created != 0 || skipped != 0 {
			t.Fatalf("created=%d skipped=%d, want 0 0", created, skipped)
		}
		if llm.calls != 0 {
			t.Fatalf("llm calls = %d, want 0", llm.calls)
		}
		if got := store.marked["episode-1"]; got != 0 {
			t.Fatalf("marked triple count = %d, want 0", got)
		}
	})

	t.Run("reinforces exact matches instead of inserting duplicates", func(t *testing.T) {
		store := newFakeExtractionStore()
		store.records["episode-1"] = newTestEpisodic("episode-1", "user_input", "Bennett prefers Go")
		store.claimQueue = []string{"episode-1"}
		store.existingSemantic["bennett\x00prefers\x00go"] = &schema.MemoryRecord{ID: "semantic-1"}

		llm := &fakeLLMClient{
			triplesByRecord: map[string][]Triple{
				"user_input: Bennett prefers Go\noutcome: success": {
					{Subject: "bennett", Predicate: "prefers", Object: "go"},
				},
			},
		}
		reinforcer := &fakeReinforcer{}
		extractor := NewSemanticExtractor(store, store, reinforcer, llm)

		created, skipped, err := extractor.Extract(context.Background())
		if err != nil {
			t.Fatalf("Extract: %v", err)
		}
		if created != 0 || skipped != 0 {
			t.Fatalf("created=%d skipped=%d, want 0 0", created, skipped)
		}
		if len(reinforcer.calls) != 1 || reinforcer.calls[0] != "semantic-1" {
			t.Fatalf("reinforce calls = %v, want [semantic-1]", reinforcer.calls)
		}
	})

	t.Run("links extracted semantic records back to canonical entities", func(t *testing.T) {
		store := &fakeEntityExtractionStore{fakeExtractionStore: newFakeExtractionStore()}
		store.records["episode-1"] = newTestEpisodic("episode-1", "user_input", "Bennett prefers Go")
		store.claimQueue = []string{"episode-1"}
		bennett := newEntityRecord("entity-bennett", "bennett")
		goEntity := newEntityRecord("entity-go", "go")
		store.records[bennett.ID] = bennett
		store.records[goEntity.ID] = goEntity
		store.entityMatches = map[string][]*schema.MemoryRecord{
			"bennett": {bennett},
			"go":      {goEntity},
		}
		llm := &fakeLLMClient{triplesByRecord: map[string][]Triple{
			"user_input: Bennett prefers Go\noutcome: success": {{Subject: "bennett", Predicate: "prefers", Object: "go"}},
		}}
		extractor := NewSemanticExtractor(store, store, &fakeReinforcer{}, llm)

		created, skipped, err := extractor.Extract(context.Background())
		if err != nil {
			t.Fatalf("Extract: %v", err)
		}
		if created != 1 || skipped != 0 {
			t.Fatalf("created=%d skipped=%d, want 1/0", created, skipped)
		}
		var semantic *schema.MemoryRecord
		for _, rec := range store.records {
			if rec.Type == schema.MemoryTypeSemantic {
				semantic = rec
				break
			}
		}
		if semantic == nil {
			t.Fatalf("semantic record was not created")
		}
		payload := semantic.Payload.(*schema.SemanticPayload)
		if payload.Subject != "entity-bennett" || payload.Object != "entity-go" {
			t.Fatalf("semantic payload = %+v, want canonical entity IDs", payload)
		}
		if !hasRelation(store.records["entity-bennett"].Relations, "fact_subject_of", semantic.ID) {
			t.Fatalf("subject entity relations = %+v, want inverse semantic link", store.records["entity-bennett"].Relations)
		}
		if !hasRelation(store.records["entity-go"].Relations, "fact_object_of", semantic.ID) {
			t.Fatalf("object entity relations = %+v, want inverse semantic link", store.records["entity-go"].Relations)
		}
	})

	t.Run("claim failure returns an error", func(t *testing.T) {
		store := newFakeExtractionStore()
		store.claimErr = errors.New("db down")
		extractor := NewSemanticExtractor(store, store, &fakeReinforcer{}, &fakeLLMClient{})

		_, _, err := extractor.Extract(context.Background())
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})
}

func TestSemanticExtractorCleansStaleClaims(t *testing.T) {
	store := newFakeExtractionStore()
	extractor := NewSemanticExtractor(store, store, &fakeReinforcer{}, &fakeLLMClient{})

	if _, _, err := extractor.Extract(context.Background()); err != nil {
		t.Fatalf("Extract: %v", err)
	}
	if store.cleanCalls != 1 {
		t.Fatalf("clean calls = %d, want 1", store.cleanCalls)
	}
}

func TestDedupeTriplesTrimsSkipsAndDedupes(t *testing.T) {
	got := dedupeTriples([]Triple{
		{Subject: " Bennett ", Predicate: " prefers ", Object: " Go "},
		{Subject: "Bennett", Predicate: "prefers", Object: "Go"},
		{Subject: "", Predicate: "prefers", Object: "Go"},
		{Subject: "Bennett", Predicate: "", Object: "Go"},
		{Subject: "Bennett", Predicate: "prefers", Object: ""},
		{Subject: "Bennett", Predicate: "uses", Object: "Membrane"},
	})
	want := []Triple{
		{Subject: "Bennett", Predicate: "prefers", Object: "Go"},
		{Subject: "Bennett", Predicate: "uses", Object: "Membrane"},
	}
	if len(got) != len(want) {
		t.Fatalf("dedupeTriples len = %d, want %d: %#v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("dedupeTriples[%d] = %#v, want %#v", i, got[i], want[i])
		}
	}
}

func TestSemanticExtractorFailurePaths(t *testing.T) {
	t.Run("missing dependencies are a no-op", func(t *testing.T) {
		store := newFakeExtractionStore()
		for _, extractor := range []*SemanticExtractor{
			NewSemanticExtractor(store, store, &fakeReinforcer{}, nil),
			NewSemanticExtractor(store, nil, &fakeReinforcer{}, &fakeLLMClient{}),
			NewSemanticExtractor(nil, store, &fakeReinforcer{}, &fakeLLMClient{}),
		} {
			created, skipped, err := extractor.Extract(context.Background())
			if err != nil || created != 0 || skipped != 0 {
				t.Fatalf("Extract with missing dependency = created:%d skipped:%d err:%v, want no-op", created, skipped, err)
			}
		}
	})

	t.Run("clean stale claims error", func(t *testing.T) {
		store := newFakeExtractionStore()
		store.cleanErr = errors.New("clean failed")
		extractor := NewSemanticExtractor(store, store, &fakeReinforcer{}, &fakeLLMClient{})

		_, _, err := extractor.Extract(context.Background())
		if err == nil || !errors.Is(err, store.cleanErr) {
			t.Fatalf("Extract clean error = %v, want %v", err, store.cleanErr)
		}
	})

	t.Run("get claimed record error releases claim", func(t *testing.T) {
		store := newFakeExtractionStore()
		store.claimQueue = []string{"episode-1"}
		store.getErr = errors.New("get failed")
		extractor := NewSemanticExtractor(store, store, &fakeReinforcer{}, &fakeLLMClient{})

		created, skipped, err := extractor.Extract(context.Background())
		if err != nil {
			t.Fatalf("Extract: %v", err)
		}
		if created != 0 || skipped != 1 {
			t.Fatalf("created=%d skipped=%d, want 0/1", created, skipped)
		}
		if !store.released["episode-1"] {
			t.Fatalf("claim was not released after get error")
		}
	})

	t.Run("missing claimed record is completed without release", func(t *testing.T) {
		store := newFakeExtractionStore()
		store.claimQueue = []string{"missing"}
		extractor := NewSemanticExtractor(store, store, &fakeReinforcer{}, &fakeLLMClient{})

		created, skipped, err := extractor.Extract(context.Background())
		if err != nil {
			t.Fatalf("Extract: %v", err)
		}
		if created != 0 || skipped != 0 {
			t.Fatalf("created=%d skipped=%d, want 0/0", created, skipped)
		}
		if store.released["missing"] {
			t.Fatalf("missing record claim should not be released")
		}
	})

	t.Run("mark empty episodic error keeps skipped", func(t *testing.T) {
		store := newFakeExtractionStore()
		store.records["episode-1"] = &schema.MemoryRecord{
			ID:      "episode-1",
			Type:    schema.MemoryTypeEpisodic,
			Payload: &schema.EpisodicPayload{Kind: "episodic"},
		}
		store.claimQueue = []string{"episode-1"}
		store.markErr = errors.New("mark failed")
		extractor := NewSemanticExtractor(store, store, &fakeReinforcer{}, &fakeLLMClient{})

		created, skipped, err := extractor.Extract(context.Background())
		if err != nil {
			t.Fatalf("Extract: %v", err)
		}
		if created != 0 || skipped != 1 {
			t.Fatalf("created=%d skipped=%d, want 0/1", created, skipped)
		}
	})

	t.Run("find exact error marks processing error", func(t *testing.T) {
		store := newFakeExtractionStore()
		store.records["episode-1"] = newTestEpisodic("episode-1", "user_input", "Bennett prefers Go")
		store.claimQueue = []string{"episode-1"}
		store.findErr = errors.New("lookup failed")
		llm := &fakeLLMClient{triplesByRecord: map[string][]Triple{
			"user_input: Bennett prefers Go\noutcome: success": {{Subject: "bennett", Predicate: "prefers", Object: "go"}},
		}}
		extractor := NewSemanticExtractor(store, store, &fakeReinforcer{}, llm)

		created, skipped, err := extractor.Extract(context.Background())
		if err != nil {
			t.Fatalf("Extract: %v", err)
		}
		if created != 0 || skipped != 1 {
			t.Fatalf("created=%d skipped=%d, want 0/1", created, skipped)
		}
		if got := store.marked["episode-1"]; got != 0 {
			t.Fatalf("marked triple count = %d, want 0", got)
		}
	})

	t.Run("reinforce error marks processing error", func(t *testing.T) {
		store := newFakeExtractionStore()
		store.records["episode-1"] = newTestEpisodic("episode-1", "user_input", "Bennett prefers Go")
		store.claimQueue = []string{"episode-1"}
		store.existingSemantic["bennett\x00prefers\x00go"] = &schema.MemoryRecord{ID: "semantic-1"}
		llm := &fakeLLMClient{triplesByRecord: map[string][]Triple{
			"user_input: Bennett prefers Go\noutcome: success": {{Subject: "bennett", Predicate: "prefers", Object: "go"}},
		}}
		extractor := NewSemanticExtractor(store, store, &fakeReinforcer{err: errors.New("reinforce failed")}, llm)

		created, skipped, err := extractor.Extract(context.Background())
		if err != nil {
			t.Fatalf("Extract: %v", err)
		}
		if created != 0 || skipped != 1 {
			t.Fatalf("created=%d skipped=%d, want 0/1", created, skipped)
		}
	})

	t.Run("existing exact match without reinforcer completes", func(t *testing.T) {
		store := newFakeExtractionStore()
		store.records["episode-1"] = newTestEpisodic("episode-1", "user_input", "Bennett prefers Go")
		store.claimQueue = []string{"episode-1"}
		store.existingSemantic["bennett\x00prefers\x00go"] = &schema.MemoryRecord{ID: "semantic-1"}
		llm := &fakeLLMClient{triplesByRecord: map[string][]Triple{
			"user_input: Bennett prefers Go\noutcome: success": {{Subject: "bennett", Predicate: "prefers", Object: "go"}},
		}}
		extractor := NewSemanticExtractor(store, store, nil, llm)

		created, skipped, err := extractor.Extract(context.Background())
		if err != nil {
			t.Fatalf("Extract: %v", err)
		}
		if created != 0 || skipped != 0 {
			t.Fatalf("created=%d skipped=%d, want 0/0", created, skipped)
		}
	})

	t.Run("transaction create error marks processing error", func(t *testing.T) {
		store := newFakeExtractionStore()
		store.records["episode-1"] = newTestEpisodic("episode-1", "user_input", "Bennett prefers Go")
		store.claimQueue = []string{"episode-1"}
		store.createErr = errors.New("create failed")
		llm := &fakeLLMClient{triplesByRecord: map[string][]Triple{
			"user_input: Bennett prefers Go\noutcome: success": {{Subject: "bennett", Predicate: "prefers", Object: "go"}},
		}}
		extractor := NewSemanticExtractor(store, store, &fakeReinforcer{}, llm)

		created, skipped, err := extractor.Extract(context.Background())
		if err != nil {
			t.Fatalf("Extract: %v", err)
		}
		if created != 0 || skipped != 1 {
			t.Fatalf("created=%d skipped=%d, want 0/1", created, skipped)
		}
	})

	t.Run("transaction relation error marks processing error", func(t *testing.T) {
		store := newFakeExtractionStore()
		store.records["episode-1"] = newTestEpisodic("episode-1", "user_input", "Bennett prefers Go")
		store.claimQueue = []string{"episode-1"}
		store.addRelationErr = errors.New("relation failed")
		llm := &fakeLLMClient{triplesByRecord: map[string][]Triple{
			"user_input: Bennett prefers Go\noutcome: success": {{Subject: "bennett", Predicate: "prefers", Object: "go"}},
		}}
		extractor := NewSemanticExtractor(store, store, &fakeReinforcer{}, llm)

		created, skipped, err := extractor.Extract(context.Background())
		if err != nil {
			t.Fatalf("Extract: %v", err)
		}
		if created != 0 || skipped != 1 {
			t.Fatalf("created=%d skipped=%d, want 0/1", created, skipped)
		}
	})

	t.Run("entity inverse relation error marks processing error", func(t *testing.T) {
		store := &fakeEntityExtractionStore{fakeExtractionStore: newFakeExtractionStore()}
		store.records["episode-1"] = newTestEpisodic("episode-1", "user_input", "Bennett prefers Go")
		store.claimQueue = []string{"episode-1"}
		bennett := newEntityRecord("entity-bennett", "bennett")
		store.records[bennett.ID] = bennett
		store.entityMatches = map[string][]*schema.MemoryRecord{
			"bennett": {bennett},
		}
		store.addRelationErr = errors.New("inverse relation failed")
		store.addRelationErrAt = 2
		llm := &fakeLLMClient{triplesByRecord: map[string][]Triple{
			"user_input: Bennett prefers Go\noutcome: success": {{Subject: "bennett", Predicate: "prefers", Object: "go"}},
		}}
		extractor := NewSemanticExtractor(store, store, &fakeReinforcer{}, llm)

		created, skipped, err := extractor.Extract(context.Background())
		if err != nil {
			t.Fatalf("Extract: %v", err)
		}
		if created != 0 || skipped != 1 {
			t.Fatalf("created=%d skipped=%d, want 0/1", created, skipped)
		}
	})

	t.Run("mark created episodic error keeps skipped", func(t *testing.T) {
		store := newFakeExtractionStore()
		store.records["episode-1"] = newTestEpisodic("episode-1", "user_input", "Bennett prefers Go")
		store.claimQueue = []string{"episode-1"}
		store.markErr = errors.New("mark failed")
		llm := &fakeLLMClient{triplesByRecord: map[string][]Triple{
			"user_input: Bennett prefers Go\noutcome: success": {{Subject: "bennett", Predicate: "prefers", Object: "go"}},
		}}
		extractor := NewSemanticExtractor(store, store, &fakeReinforcer{}, llm)

		created, skipped, err := extractor.Extract(context.Background())
		if err != nil {
			t.Fatalf("Extract: %v", err)
		}
		if created != 1 || skipped != 1 {
			t.Fatalf("created=%d skipped=%d, want 1/1", created, skipped)
		}
	})

	t.Run("release claim returns error", func(t *testing.T) {
		store := newFakeExtractionStore()
		store.releaseErr = errors.New("release failed")
		extractor := NewSemanticExtractor(store, store, &fakeReinforcer{}, &fakeLLMClient{})

		err := extractor.releaseClaim(context.Background(), "episode-1")
		if err == nil || !errors.Is(err, store.releaseErr) {
			t.Fatalf("releaseClaim error = %v, want %v", err, store.releaseErr)
		}
	})
}

type fakeLLMClient struct {
	err             error
	calls           int
	triplesByRecord map[string][]Triple
}

func (f *fakeLLMClient) Extract(_ context.Context, content string) ([]Triple, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	if triples, ok := f.triplesByRecord[content]; ok {
		return triples, nil
	}
	return []Triple{}, nil
}

type fakeReinforcer struct {
	calls []string
	err   error
}

func (f *fakeReinforcer) Reinforce(_ context.Context, id, _, _ string) error {
	if f.err != nil {
		return f.err
	}
	f.calls = append(f.calls, id)
	return nil
}

type fakeExtractionStore struct {
	records          map[string]*schema.MemoryRecord
	existingSemantic map[string]*schema.MemoryRecord
	entityMatches    map[string][]*schema.MemoryRecord
	claimQueue       []string
	marked           map[string]int
	released         map[string]bool
	cleanCalls       int
	addRelationCalls int
	claimErr         error
	cleanErr         error
	getErr           error
	findErr          error
	markErr          error
	releaseErr       error
	createErr        error
	addRelationErr   error
	addRelationErrAt int
}

func newFakeExtractionStore() *fakeExtractionStore {
	return &fakeExtractionStore{
		records:          map[string]*schema.MemoryRecord{},
		existingSemantic: map[string]*schema.MemoryRecord{},
		marked:           map[string]int{},
		released:         map[string]bool{},
	}
}

func (f *fakeExtractionStore) ClaimUnextractedEpisodics(_ context.Context, _ int) ([]string, error) {
	if f.claimErr != nil {
		return nil, f.claimErr
	}
	claimed := append([]string(nil), f.claimQueue...)
	f.claimQueue = nil
	return claimed, nil
}

func (f *fakeExtractionStore) MarkEpisodicExtracted(_ context.Context, recordID string, tripleCount int) error {
	if f.markErr != nil {
		return f.markErr
	}
	f.marked[recordID] = tripleCount
	return nil
}

func (f *fakeExtractionStore) ReleaseEpisodicClaim(_ context.Context, recordID string) error {
	if f.releaseErr != nil {
		return f.releaseErr
	}
	f.released[recordID] = true
	return nil
}

func (f *fakeExtractionStore) CleanStaleExtractionClaims(_ context.Context, _ time.Duration) error {
	if f.cleanErr != nil {
		return f.cleanErr
	}
	f.cleanCalls++
	return nil
}

func (f *fakeExtractionStore) FindSemanticExact(_ context.Context, subject, predicate, object string) (*schema.MemoryRecord, error) {
	if f.findErr != nil {
		return nil, f.findErr
	}
	return f.existingSemantic[subject+"\x00"+predicate+"\x00"+object], nil
}

func (f *fakeExtractionStore) Create(_ context.Context, record *schema.MemoryRecord) error {
	if f.createErr != nil {
		return f.createErr
	}
	f.records[record.ID] = record
	return nil
}

type fakeEntityExtractionStore struct {
	*fakeExtractionStore
}

func (f *fakeEntityExtractionStore) FindEntitiesByTerm(_ context.Context, term, _ string, _ int) ([]*schema.MemoryRecord, error) {
	return append([]*schema.MemoryRecord(nil), f.entityMatches[term]...), nil
}

func (f *fakeEntityExtractionStore) FindEntityByIdentifier(context.Context, string, string, string) (*schema.MemoryRecord, error) {
	return nil, storage.ErrNotFound
}

func (f *fakeExtractionStore) Get(_ context.Context, id string) (*schema.MemoryRecord, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	record, ok := f.records[id]
	if !ok {
		return nil, storage.ErrNotFound
	}
	return record, nil
}

func (f *fakeExtractionStore) Update(_ context.Context, record *schema.MemoryRecord) error {
	f.records[record.ID] = record
	return nil
}

func (f *fakeExtractionStore) Delete(_ context.Context, id string) error {
	delete(f.records, id)
	return nil
}

func (f *fakeExtractionStore) List(_ context.Context, _ storage.ListOptions) ([]*schema.MemoryRecord, error) {
	return nil, nil
}

func (f *fakeExtractionStore) ListByType(_ context.Context, _ schema.MemoryType) ([]*schema.MemoryRecord, error) {
	return nil, nil
}

func (f *fakeExtractionStore) UpdateSalience(_ context.Context, _ string, _ float64) error {
	return nil
}

func (f *fakeExtractionStore) AddAuditEntry(_ context.Context, _ string, _ schema.AuditEntry) error {
	return nil
}

func (f *fakeExtractionStore) AddRelation(_ context.Context, sourceID string, rel schema.Relation) error {
	f.addRelationCalls++
	if f.addRelationErrAt > 0 {
		if f.addRelationCalls == f.addRelationErrAt {
			return f.addRelationErr
		}
	} else if f.addRelationErr != nil {
		return f.addRelationErr
	}
	record, ok := f.records[sourceID]
	if !ok {
		return storage.ErrNotFound
	}
	record.Relations = append(record.Relations, rel)
	return nil
}

func (f *fakeExtractionStore) GetRelations(_ context.Context, id string) ([]schema.Relation, error) {
	record, ok := f.records[id]
	if !ok {
		return nil, storage.ErrNotFound
	}
	return append([]schema.Relation(nil), record.Relations...), nil
}

func (f *fakeExtractionStore) Begin(_ context.Context) (storage.Transaction, error) {
	return &fakeTx{store: f}, nil
}

func (f *fakeExtractionStore) Close() error {
	return nil
}

type fakeTx struct {
	store  *fakeExtractionStore
	closed bool
}

func (t *fakeTx) Create(ctx context.Context, record *schema.MemoryRecord) error {
	if t.closed {
		return storage.ErrTxClosed
	}
	return t.store.Create(ctx, record)
}

func (t *fakeTx) Get(ctx context.Context, id string) (*schema.MemoryRecord, error) {
	if t.closed {
		return nil, storage.ErrTxClosed
	}
	return t.store.Get(ctx, id)
}

func (t *fakeTx) Update(ctx context.Context, record *schema.MemoryRecord) error {
	if t.closed {
		return storage.ErrTxClosed
	}
	return t.store.Update(ctx, record)
}

func (t *fakeTx) Delete(ctx context.Context, id string) error {
	if t.closed {
		return storage.ErrTxClosed
	}
	return t.store.Delete(ctx, id)
}

func (t *fakeTx) List(ctx context.Context, opts storage.ListOptions) ([]*schema.MemoryRecord, error) {
	if t.closed {
		return nil, storage.ErrTxClosed
	}
	return t.store.List(ctx, opts)
}

func (t *fakeTx) ListByType(ctx context.Context, memType schema.MemoryType) ([]*schema.MemoryRecord, error) {
	if t.closed {
		return nil, storage.ErrTxClosed
	}
	return t.store.ListByType(ctx, memType)
}

func (t *fakeTx) UpdateSalience(ctx context.Context, id string, salience float64) error {
	if t.closed {
		return storage.ErrTxClosed
	}
	return t.store.UpdateSalience(ctx, id, salience)
}

func (t *fakeTx) AddAuditEntry(ctx context.Context, id string, entry schema.AuditEntry) error {
	if t.closed {
		return storage.ErrTxClosed
	}
	return t.store.AddAuditEntry(ctx, id, entry)
}

func (t *fakeTx) AddRelation(ctx context.Context, sourceID string, rel schema.Relation) error {
	if t.closed {
		return storage.ErrTxClosed
	}
	return t.store.AddRelation(ctx, sourceID, rel)
}

func (t *fakeTx) GetRelations(ctx context.Context, id string) ([]schema.Relation, error) {
	if t.closed {
		return nil, storage.ErrTxClosed
	}
	return t.store.GetRelations(ctx, id)
}

func (t *fakeTx) Commit() error {
	t.closed = true
	return nil
}

func (t *fakeTx) Rollback() error {
	t.closed = true
	return nil
}

func newTestEpisodic(id, kind, summary string) *schema.MemoryRecord {
	now := time.Now().UTC()
	return &schema.MemoryRecord{
		ID:          id,
		Type:        schema.MemoryTypeEpisodic,
		Sensitivity: schema.SensitivityLow,
		Confidence:  0.8,
		Salience:    0.7,
		Scope:       "project",
		Tags:        []string{"episode"},
		CreatedAt:   now,
		UpdatedAt:   now,
		Lifecycle: schema.Lifecycle{
			Decay: schema.DecayProfile{
				Curve:             schema.DecayCurveExponential,
				HalfLifeSeconds:   86400,
				ReinforcementGain: 0.1,
			},
			LastReinforcedAt: now,
			DeletionPolicy:   schema.DeletionPolicyAutoPrune,
		},
		Payload: &schema.EpisodicPayload{
			Kind: "episodic",
			Timeline: []schema.TimelineEvent{
				{
					T:         now,
					EventKind: kind,
					Ref:       "evt",
					Summary:   summary,
				},
			},
			Outcome: schema.OutcomeStatusSuccess,
		},
	}
}
