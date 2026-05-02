package revision

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/BennettSchwartz/membrane/pkg/schema"
	"github.com/BennettSchwartz/membrane/pkg/storage"
)

func TestSupersedeRetractsOldRecordAndLinksReplacement(t *testing.T) {
	ctx := context.Background()
	store := newRevisionTestStore(t)
	svc := NewService(store)

	old := semanticRevisionRecord("old-runtime", "runtime", "go1.24")
	replacement := semanticRevisionRecord("", "runtime", "go1.25")
	if err := store.Create(ctx, old); err != nil {
		t.Fatalf("Create old: %v", err)
	}

	got, err := svc.Supersede(ctx, old.ID, replacement, "tester", "runtime upgrade")
	if err != nil {
		t.Fatalf("Supersede: %v", err)
	}
	if got.ID == "" {
		t.Fatalf("Supersede replacement ID = empty, want generated ID")
	}
	if len(got.Relations) != 1 || got.Relations[0].Predicate != "supersedes" || got.Relations[0].TargetID != old.ID {
		t.Fatalf("replacement relations = %+v, want supersedes old record", got.Relations)
	}
	payload, ok := got.Payload.(*schema.SemanticPayload)
	if !ok || payload.Revision == nil || payload.Revision.Supersedes != old.ID || payload.Revision.Status != schema.RevisionStatusActive {
		t.Fatalf("replacement payload = %+v, want active supersedes revision", got.Payload)
	}

	retracted, err := store.Get(ctx, old.ID)
	if err != nil {
		t.Fatalf("Get old: %v", err)
	}
	oldPayload := retracted.Payload.(*schema.SemanticPayload)
	if retracted.Salience != 0 || oldPayload.Revision == nil || oldPayload.Revision.Status != schema.RevisionStatusRetracted || oldPayload.Revision.SupersededBy != got.ID {
		t.Fatalf("old record = %+v payload=%+v, want retracted and superseded_by replacement", retracted, oldPayload)
	}
	if !hasAuditAction(retracted.AuditLog, schema.AuditActionRevise) {
		t.Fatalf("old audit log = %+v, want revision audit entry", retracted.AuditLog)
	}
}

func TestForkCreatesDerivedRecordWithoutRetractingSource(t *testing.T) {
	ctx := context.Background()
	store := newRevisionTestStore(t)
	svc := NewService(store)

	source := semanticRevisionRecord("source-db", "database", "postgres")
	forked := semanticRevisionRecord("", "database", "sqlite")
	if err := store.Create(ctx, source); err != nil {
		t.Fatalf("Create source: %v", err)
	}

	got, err := svc.Fork(ctx, source.ID, forked, "tester", "local backend")
	if err != nil {
		t.Fatalf("Fork: %v", err)
	}
	if got.ID == "" {
		t.Fatalf("Fork ID = empty, want generated ID")
	}
	if len(got.Relations) != 1 || got.Relations[0].Predicate != "derived_from" || got.Relations[0].TargetID != source.ID {
		t.Fatalf("fork relations = %+v, want derived_from source", got.Relations)
	}

	storedSource, err := store.Get(ctx, source.ID)
	if err != nil {
		t.Fatalf("Get source: %v", err)
	}
	if storedSource.Salience == 0 {
		t.Fatalf("source salience = 0, want fork to leave source active")
	}
	if !hasAuditAction(storedSource.AuditLog, schema.AuditActionFork) {
		t.Fatalf("source audit log = %+v, want fork audit entry", storedSource.AuditLog)
	}
}

func TestMergeRetractsSourcesAndCreatesMergedRecord(t *testing.T) {
	ctx := context.Background()
	store := newRevisionTestStore(t)
	svc := NewService(store)

	first := semanticRevisionRecord("merge-first", "editor", "vim")
	second := semanticRevisionRecord("merge-second", "editor", "neovim")
	for _, rec := range []*schema.MemoryRecord{first, second} {
		if err := store.Create(ctx, rec); err != nil {
			t.Fatalf("Create %s: %v", rec.ID, err)
		}
	}

	merged := semanticRevisionRecord("", "editor", "neovim")
	got, err := svc.Merge(ctx, []string{first.ID, second.ID}, merged, "tester", "combine editor facts")
	if err != nil {
		t.Fatalf("Merge: %v", err)
	}
	if got.ID == "" {
		t.Fatalf("Merge ID = empty, want generated ID")
	}
	if len(got.Relations) != 2 {
		t.Fatalf("merged relations = %+v, want derived_from both sources", got.Relations)
	}
	for _, id := range []string{first.ID, second.ID} {
		rec, err := store.Get(ctx, id)
		if err != nil {
			t.Fatalf("Get %s: %v", id, err)
		}
		payload := rec.Payload.(*schema.SemanticPayload)
		if rec.Salience != 0 || payload.Revision == nil || payload.Revision.Status != schema.RevisionStatusRetracted {
			t.Fatalf("source %s = %+v payload=%+v, want retracted", id, rec, payload)
		}
		if !hasAuditAction(rec.AuditLog, schema.AuditActionMerge) {
			t.Fatalf("source %s audit log = %+v, want merge audit entry", id, rec.AuditLog)
		}
	}
}

func TestRetractAndContestUpdateSemanticRevisionState(t *testing.T) {
	ctx := context.Background()
	store := newRevisionTestStore(t)
	svc := NewService(store)

	retractable := semanticRevisionRecord("retract-me", "cache", "redis")
	contested := semanticRevisionRecord("contest-me", "queue", "sqs")
	contesting := semanticRevisionRecord("contesting-ref", "queue", "kafka")
	for _, rec := range []*schema.MemoryRecord{retractable, contested, contesting} {
		if err := store.Create(ctx, rec); err != nil {
			t.Fatalf("Create %s: %v", rec.ID, err)
		}
	}

	if err := svc.Retract(ctx, retractable.ID, "tester", "obsolete"); err != nil {
		t.Fatalf("Retract: %v", err)
	}
	retracted, err := store.Get(ctx, retractable.ID)
	if err != nil {
		t.Fatalf("Get retracted: %v", err)
	}
	retractedPayload := retracted.Payload.(*schema.SemanticPayload)
	if retracted.Salience != 0 || retractedPayload.Revision == nil || retractedPayload.Revision.Status != schema.RevisionStatusRetracted {
		t.Fatalf("retracted record = %+v payload=%+v, want retracted", retracted, retractedPayload)
	}
	if !hasAuditAction(retracted.AuditLog, schema.AuditActionDelete) {
		t.Fatalf("retracted audit log = %+v, want delete audit entry", retracted.AuditLog)
	}

	if err := svc.Contest(ctx, contested.ID, contesting.ID, "tester", "conflicting source"); err != nil {
		t.Fatalf("Contest: %v", err)
	}
	gotContested, err := store.Get(ctx, contested.ID)
	if err != nil {
		t.Fatalf("Get contested: %v", err)
	}
	contestedPayload := gotContested.Payload.(*schema.SemanticPayload)
	if contestedPayload.Revision == nil || contestedPayload.Revision.Status != schema.RevisionStatusContested {
		t.Fatalf("contested payload = %+v, want contested status", contestedPayload)
	}
	relations, err := store.GetRelations(ctx, contested.ID)
	if err != nil {
		t.Fatalf("GetRelations contested: %v", err)
	}
	if len(relations) != 1 || relations[0].Predicate != "contested_by" || relations[0].TargetID != contesting.ID {
		t.Fatalf("contest relations = %+v, want contested_by relation", relations)
	}
}

func TestRevisionRejectsInvalidInputs(t *testing.T) {
	ctx := context.Background()
	store := newRevisionTestStore(t)
	svc := NewService(store)

	episodic := schema.NewMemoryRecord("episode", schema.MemoryTypeEpisodic, schema.SensitivityLow, &schema.EpisodicPayload{
		Kind:     "episodic",
		Timeline: []schema.TimelineEvent{{EventKind: "event", Ref: "evt-1"}},
	})
	semantic := semanticRevisionRecord("semantic", "runtime", "go1.24")
	for _, rec := range []*schema.MemoryRecord{episodic, semantic} {
		if err := store.Create(ctx, rec); err != nil {
			t.Fatalf("Create %s: %v", rec.ID, err)
		}
	}

	replacementWithoutEvidence := schema.NewMemoryRecord("replacement-no-evidence", schema.MemoryTypeSemantic, schema.SensitivityLow, &schema.SemanticPayload{
		Kind:      "semantic",
		Subject:   "runtime",
		Predicate: "is",
		Object:    "go1.25",
		Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
	})
	if _, err := svc.Supersede(ctx, semantic.ID, replacementWithoutEvidence, "tester", "missing evidence"); err == nil {
		t.Fatalf("Supersede missing evidence error = nil")
	}
	if _, err := svc.Supersede(ctx, semantic.ID, nil, "tester", "nil replacement"); err == nil {
		t.Fatalf("Supersede nil replacement error = nil")
	}
	if _, err := svc.Fork(ctx, semantic.ID, replacementWithoutEvidence, "tester", "missing evidence"); err == nil {
		t.Fatalf("Fork missing evidence error = nil")
	}
	if _, err := svc.Fork(ctx, semantic.ID, nil, "tester", "nil fork"); err == nil {
		t.Fatalf("Fork nil record error = nil")
	}
	if _, err := svc.Merge(ctx, nil, semanticRevisionRecord("unused", "runtime", "go1.26"), "tester", "empty"); err == nil {
		t.Fatalf("Merge empty source IDs error = nil")
	}
	if _, err := svc.Merge(ctx, []string{semantic.ID}, nil, "tester", "nil merge"); err == nil {
		t.Fatalf("Merge nil record error = nil")
	}

	if _, err := svc.Supersede(ctx, episodic.ID, semanticRevisionRecord("replacement", "runtime", "go1.25"), "tester", "immutable"); !errors.Is(err, ErrEpisodicImmutable) {
		t.Fatalf("Supersede episodic err = %v, want ErrEpisodicImmutable", err)
	}
	if _, err := svc.Fork(ctx, episodic.ID, semanticRevisionRecord("fork", "runtime", "go1.25"), "tester", "immutable"); !errors.Is(err, ErrEpisodicImmutable) {
		t.Fatalf("Fork episodic err = %v, want ErrEpisodicImmutable", err)
	}
	if err := svc.Retract(ctx, episodic.ID, "tester", "immutable"); !errors.Is(err, ErrEpisodicImmutable) {
		t.Fatalf("Retract episodic err = %v, want ErrEpisodicImmutable", err)
	}
	if err := svc.Contest(ctx, episodic.ID, "", "tester", "immutable"); !errors.Is(err, ErrEpisodicImmutable) {
		t.Fatalf("Contest episodic err = %v, want ErrEpisodicImmutable", err)
	}

	if _, err := svc.Supersede(ctx, "missing", semanticRevisionRecord("replacement-2", "runtime", "go1.25"), "tester", "missing"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("Supersede missing err = %v, want ErrNotFound", err)
	}
}

func TestRevisionValidationHelpers(t *testing.T) {
	if err := ensureRevisable(nil); err == nil {
		t.Fatalf("ensureRevisable nil error = nil")
	}
	semantic := semanticRevisionRecord("helper-semantic", "runtime", "go1.24")
	if err := ensureRevisable(semantic); err != nil {
		t.Fatalf("ensureRevisable semantic: %v", err)
	}
	if err := ensureEvidence(nil); err == nil {
		t.Fatalf("ensureEvidence nil error = nil")
	}
	if err := ensureEvidence(schema.NewMemoryRecord("working", schema.MemoryTypeWorking, schema.SensitivityLow, &schema.WorkingPayload{
		Kind:     "working",
		ThreadID: "thread",
		State:    schema.TaskStatePlanning,
	})); err != nil {
		t.Fatalf("ensureEvidence non-semantic: %v", err)
	}
	if err := ensureEvidence(schema.NewMemoryRecord("semantic-opaque", schema.MemoryTypeSemantic, schema.SensitivityLow, nil)); err != nil {
		t.Fatalf("ensureEvidence semantic without typed payload: %v", err)
	}
}

func TestContestWithoutContestingReferenceSkipsRelation(t *testing.T) {
	ctx := context.Background()
	store := newRevisionTestStore(t)
	svc := NewService(store)
	rec := semanticRevisionRecord("contest-no-ref", "queue", "sqs")
	if err := store.Create(ctx, rec); err != nil {
		t.Fatalf("Create record: %v", err)
	}

	if err := svc.Contest(ctx, rec.ID, "", "tester", "reviewed"); err != nil {
		t.Fatalf("Contest: %v", err)
	}
	relations, err := store.GetRelations(ctx, rec.ID)
	if err != nil {
		t.Fatalf("GetRelations: %v", err)
	}
	if len(relations) != 0 {
		t.Fatalf("relations = %+v, want none when contesting ref is empty", relations)
	}
}

type revisionErrorStore struct {
	beginErr error
	tx       *revisionErrorTx
}

func (s *revisionErrorStore) Create(context.Context, *schema.MemoryRecord) error {
	return errors.New("unexpected create")
}

func (s *revisionErrorStore) Get(context.Context, string) (*schema.MemoryRecord, error) {
	return nil, errors.New("unexpected get")
}

func (s *revisionErrorStore) Update(context.Context, *schema.MemoryRecord) error {
	return errors.New("unexpected update")
}

func (s *revisionErrorStore) Delete(context.Context, string) error {
	return errors.New("unexpected delete")
}

func (s *revisionErrorStore) List(context.Context, storage.ListOptions) ([]*schema.MemoryRecord, error) {
	return nil, errors.New("unexpected list")
}

func (s *revisionErrorStore) ListByType(context.Context, schema.MemoryType) ([]*schema.MemoryRecord, error) {
	return nil, errors.New("unexpected list by type")
}

func (s *revisionErrorStore) UpdateSalience(context.Context, string, float64) error {
	return errors.New("unexpected update salience")
}

func (s *revisionErrorStore) AddAuditEntry(context.Context, string, schema.AuditEntry) error {
	return errors.New("unexpected audit")
}

func (s *revisionErrorStore) AddRelation(context.Context, string, schema.Relation) error {
	return errors.New("unexpected relation")
}

func (s *revisionErrorStore) GetRelations(context.Context, string) ([]schema.Relation, error) {
	return nil, errors.New("unexpected get relations")
}

func (s *revisionErrorStore) Begin(context.Context) (storage.Transaction, error) {
	if s.beginErr != nil {
		return nil, s.beginErr
	}
	return s.tx, nil
}

func (s *revisionErrorStore) Close() error {
	return nil
}

type revisionErrorTx struct {
	records        map[string]*schema.MemoryRecord
	getErr         error
	updateErr      error
	createErr      error
	addAuditErr    error
	addRelationErr error
}

func newRevisionErrorTx(records ...*schema.MemoryRecord) *revisionErrorTx {
	tx := &revisionErrorTx{records: map[string]*schema.MemoryRecord{}}
	for _, rec := range records {
		tx.records[rec.ID] = rec
	}
	return tx
}

func (tx *revisionErrorTx) Create(_ context.Context, rec *schema.MemoryRecord) error {
	if tx.createErr != nil {
		return tx.createErr
	}
	tx.records[rec.ID] = rec
	return nil
}

func (tx *revisionErrorTx) Get(_ context.Context, id string) (*schema.MemoryRecord, error) {
	if tx.getErr != nil {
		return nil, tx.getErr
	}
	rec, ok := tx.records[id]
	if !ok {
		return nil, storage.ErrNotFound
	}
	return rec, nil
}

func (tx *revisionErrorTx) Update(_ context.Context, rec *schema.MemoryRecord) error {
	if tx.updateErr != nil {
		return tx.updateErr
	}
	tx.records[rec.ID] = rec
	return nil
}

func (tx *revisionErrorTx) Delete(context.Context, string) error {
	return errors.New("unexpected delete")
}

func (tx *revisionErrorTx) List(context.Context, storage.ListOptions) ([]*schema.MemoryRecord, error) {
	return nil, errors.New("unexpected list")
}

func (tx *revisionErrorTx) ListByType(context.Context, schema.MemoryType) ([]*schema.MemoryRecord, error) {
	return nil, errors.New("unexpected list by type")
}

func (tx *revisionErrorTx) UpdateSalience(context.Context, string, float64) error {
	return errors.New("unexpected update salience")
}

func (tx *revisionErrorTx) AddAuditEntry(context.Context, string, schema.AuditEntry) error {
	return tx.addAuditErr
}

func (tx *revisionErrorTx) AddRelation(context.Context, string, schema.Relation) error {
	return tx.addRelationErr
}

func (tx *revisionErrorTx) GetRelations(context.Context, string) ([]schema.Relation, error) {
	return nil, errors.New("unexpected get relations")
}

func (tx *revisionErrorTx) Commit() error {
	return nil
}

func (tx *revisionErrorTx) Rollback() error {
	return nil
}

func TestRevisionMutationErrorBranches(t *testing.T) {
	ctx := context.Background()

	t.Run("contest get error", func(t *testing.T) {
		tx := newRevisionErrorTx()
		tx.getErr = errors.New("get failed")
		svc := NewService(&revisionErrorStore{tx: tx})

		if err := svc.Contest(ctx, "contest-missing", "", "tester", "conflict"); err == nil || !strings.Contains(err.Error(), "contest: get record") {
			t.Fatalf("Contest get error = %v, want wrapped get error", err)
		}
	})

	t.Run("contest update error", func(t *testing.T) {
		tx := newRevisionErrorTx(semanticRevisionRecord("contest-update", "queue", "sqs"))
		tx.updateErr = errors.New("update failed")
		svc := NewService(&revisionErrorStore{tx: tx})

		if err := svc.Contest(ctx, "contest-update", "", "tester", "conflict"); err == nil || !strings.Contains(err.Error(), "contest: update record") {
			t.Fatalf("Contest update error = %v, want wrapped update error", err)
		}
	})

	t.Run("retract update error", func(t *testing.T) {
		tx := newRevisionErrorTx(semanticRevisionRecord("retract-update", "runtime", "go1.24"))
		tx.updateErr = errors.New("update failed")
		svc := NewService(&revisionErrorStore{tx: tx})

		if err := svc.Retract(ctx, "retract-update", "tester", "obsolete"); err == nil || !strings.Contains(err.Error(), "update record retract-update") {
			t.Fatalf("Retract update error = %v, want wrapped update error", err)
		}
	})

	t.Run("retract get error", func(t *testing.T) {
		tx := newRevisionErrorTx()
		tx.getErr = errors.New("get failed")
		svc := NewService(&revisionErrorStore{tx: tx})

		if err := svc.Retract(ctx, "retract-missing", "tester", "obsolete"); err == nil || !strings.Contains(err.Error(), "get record retract-missing") {
			t.Fatalf("Retract get error = %v, want wrapped get error", err)
		}
	})

	t.Run("retract audit error", func(t *testing.T) {
		tx := newRevisionErrorTx(semanticRevisionRecord("retract-audit", "runtime", "go1.24"))
		tx.addAuditErr = errors.New("audit failed")
		svc := NewService(&revisionErrorStore{tx: tx})

		if err := svc.Retract(ctx, "retract-audit", "tester", "obsolete"); err == nil || !strings.Contains(err.Error(), "add audit entry") {
			t.Fatalf("Retract audit error = %v, want wrapped audit error", err)
		}
	})

	t.Run("contest relation error", func(t *testing.T) {
		tx := newRevisionErrorTx(semanticRevisionRecord("contest-relation", "queue", "sqs"))
		tx.addRelationErr = errors.New("relation failed")
		svc := NewService(&revisionErrorStore{tx: tx})

		if err := svc.Contest(ctx, "contest-relation", "contesting-ref", "tester", "conflict"); err == nil || !strings.Contains(err.Error(), "contest: add relation") {
			t.Fatalf("Contest relation error = %v, want wrapped relation error", err)
		}
	})

	t.Run("contest audit error", func(t *testing.T) {
		tx := newRevisionErrorTx(semanticRevisionRecord("contest-audit", "queue", "sqs"))
		tx.addAuditErr = errors.New("audit failed")
		svc := NewService(&revisionErrorStore{tx: tx})

		if err := svc.Contest(ctx, "contest-audit", "", "tester", "conflict"); err == nil || !strings.Contains(err.Error(), "audit failed") {
			t.Fatalf("Contest audit error = %v, want audit error", err)
		}
	})

	t.Run("fork get error", func(t *testing.T) {
		tx := newRevisionErrorTx()
		tx.getErr = errors.New("get failed")
		svc := NewService(&revisionErrorStore{tx: tx})

		if _, err := svc.Fork(ctx, "fork-missing", semanticRevisionRecord("fork-target", "database", "sqlite"), "tester", "variant"); err == nil || !strings.Contains(err.Error(), "get source record fork-missing") {
			t.Fatalf("Fork get error = %v, want wrapped get error", err)
		}
	})

	t.Run("fork audit error", func(t *testing.T) {
		tx := newRevisionErrorTx(semanticRevisionRecord("fork-source", "database", "postgres"))
		tx.addAuditErr = errors.New("audit failed")
		svc := NewService(&revisionErrorStore{tx: tx})

		if _, err := svc.Fork(ctx, "fork-source", semanticRevisionRecord("fork-target", "database", "sqlite"), "tester", "variant"); err == nil || !strings.Contains(err.Error(), "add audit entry to source record") {
			t.Fatalf("Fork audit error = %v, want wrapped audit error", err)
		}
	})

	t.Run("fork create error", func(t *testing.T) {
		tx := newRevisionErrorTx(semanticRevisionRecord("fork-create-source", "database", "postgres"))
		tx.createErr = errors.New("create failed")
		svc := NewService(&revisionErrorStore{tx: tx})

		if _, err := svc.Fork(ctx, "fork-create-source", semanticRevisionRecord("fork-create-target", "database", "sqlite"), "tester", "variant"); err == nil || !strings.Contains(err.Error(), "create forked record") {
			t.Fatalf("Fork create error = %v, want wrapped create error", err)
		}
	})

	t.Run("merge get error", func(t *testing.T) {
		tx := newRevisionErrorTx()
		svc := NewService(&revisionErrorStore{tx: tx})

		if _, err := svc.Merge(ctx, []string{"missing"}, semanticRevisionRecord("merge-get-target", "editor", "neovim"), "tester", "merge"); err == nil || !strings.Contains(err.Error(), "get source record missing") {
			t.Fatalf("Merge get error = %v, want wrapped get source error", err)
		}
	})

	t.Run("merge rejects episodic source", func(t *testing.T) {
		episodic := schema.NewMemoryRecord("merge-episode", schema.MemoryTypeEpisodic, schema.SensitivityLow, &schema.EpisodicPayload{
			Kind:     "episodic",
			Timeline: []schema.TimelineEvent{{EventKind: "event", Ref: "evt-1"}},
		})
		tx := newRevisionErrorTx(episodic)
		svc := NewService(&revisionErrorStore{tx: tx})

		if _, err := svc.Merge(ctx, []string{episodic.ID}, semanticRevisionRecord("merge-episodic-target", "editor", "neovim"), "tester", "merge"); !errors.Is(err, ErrEpisodicImmutable) {
			t.Fatalf("Merge episodic source error = %v, want ErrEpisodicImmutable", err)
		}
	})

	t.Run("merge update error", func(t *testing.T) {
		tx := newRevisionErrorTx(
			semanticRevisionRecord("merge-update-a", "editor", "vim"),
			semanticRevisionRecord("merge-update-b", "editor", "neovim"),
		)
		tx.updateErr = errors.New("update failed")
		svc := NewService(&revisionErrorStore{tx: tx})

		if _, err := svc.Merge(ctx, []string{"merge-update-a", "merge-update-b"}, semanticRevisionRecord("merge-update-target", "editor", "neovim"), "tester", "merge"); err == nil || !strings.Contains(err.Error(), "update source record") {
			t.Fatalf("Merge update error = %v, want wrapped update error", err)
		}
	})

	t.Run("merge audit error", func(t *testing.T) {
		tx := newRevisionErrorTx(
			semanticRevisionRecord("merge-audit-a", "editor", "vim"),
			semanticRevisionRecord("merge-audit-b", "editor", "neovim"),
		)
		tx.addAuditErr = errors.New("audit failed")
		svc := NewService(&revisionErrorStore{tx: tx})

		if _, err := svc.Merge(ctx, []string{"merge-audit-a", "merge-audit-b"}, semanticRevisionRecord("merge-audit-target", "editor", "neovim"), "tester", "merge"); err == nil || !strings.Contains(err.Error(), "add audit entry to source record") {
			t.Fatalf("Merge audit error = %v, want wrapped audit error", err)
		}
	})

	t.Run("merge create error", func(t *testing.T) {
		tx := newRevisionErrorTx(
			semanticRevisionRecord("merge-create-a", "editor", "vim"),
			semanticRevisionRecord("merge-create-b", "editor", "neovim"),
		)
		tx.createErr = errors.New("create failed")
		svc := NewService(&revisionErrorStore{tx: tx})

		if _, err := svc.Merge(ctx, []string{"merge-create-a", "merge-create-b"}, semanticRevisionRecord("merge-create-target", "editor", "neovim"), "tester", "merge"); err == nil || !strings.Contains(err.Error(), "create merged record") {
			t.Fatalf("Merge create error = %v, want wrapped create error", err)
		}
	})

	t.Run("supersede update error", func(t *testing.T) {
		tx := newRevisionErrorTx(semanticRevisionRecord("supersede-update-old", "runtime", "go1.24"))
		tx.updateErr = errors.New("update failed")
		svc := NewService(&revisionErrorStore{tx: tx})

		if _, err := svc.Supersede(ctx, "supersede-update-old", semanticRevisionRecord("supersede-update-new", "runtime", "go1.25"), "tester", "upgrade"); err == nil || !strings.Contains(err.Error(), "update old record supersede-update-old") {
			t.Fatalf("Supersede update error = %v, want wrapped update error", err)
		}
	})

	t.Run("supersede audit error", func(t *testing.T) {
		tx := newRevisionErrorTx(semanticRevisionRecord("supersede-audit-old", "runtime", "go1.24"))
		tx.addAuditErr = errors.New("audit failed")
		svc := NewService(&revisionErrorStore{tx: tx})

		if _, err := svc.Supersede(ctx, "supersede-audit-old", semanticRevisionRecord("supersede-audit-new", "runtime", "go1.25"), "tester", "upgrade"); err == nil || !strings.Contains(err.Error(), "add audit entry to old record") {
			t.Fatalf("Supersede audit error = %v, want wrapped audit error", err)
		}
	})

	t.Run("supersede create error", func(t *testing.T) {
		tx := newRevisionErrorTx(semanticRevisionRecord("supersede-create-old", "runtime", "go1.24"))
		tx.createErr = errors.New("create failed")
		svc := NewService(&revisionErrorStore{tx: tx})

		if _, err := svc.Supersede(ctx, "supersede-create-old", semanticRevisionRecord("supersede-create-new", "runtime", "go1.25"), "tester", "upgrade"); err == nil || !strings.Contains(err.Error(), "create new record") {
			t.Fatalf("Supersede create error = %v, want wrapped create error", err)
		}
	})
}

func hasAuditAction(entries []schema.AuditEntry, action schema.AuditAction) bool {
	for _, entry := range entries {
		if entry.Action == action {
			return true
		}
	}
	return false
}
