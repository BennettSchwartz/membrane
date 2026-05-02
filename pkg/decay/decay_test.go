package decay_test

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/BennettSchwartz/membrane/pkg/decay"
	"github.com/BennettSchwartz/membrane/pkg/schema"
	"github.com/BennettSchwartz/membrane/pkg/storage"
	"github.com/BennettSchwartz/membrane/pkg/storage/sqlite"
)

func newTestStore(t *testing.T) *sqlite.SQLiteStore {
	t.Helper()

	store, err := sqlite.Open(":memory:", "")
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close sqlite store: %v", err)
		}
	})

	return store
}

func newRecord(id string, now time.Time) *schema.MemoryRecord {
	return &schema.MemoryRecord{
		ID:          id,
		Type:        schema.MemoryTypeSemantic,
		Sensitivity: schema.SensitivityLow,
		Confidence:  0.9,
		Salience:    0.8,
		CreatedAt:   now,
		UpdatedAt:   now,
		Lifecycle: schema.Lifecycle{
			Decay: schema.DecayProfile{
				Curve:             schema.DecayCurveExponential,
				HalfLifeSeconds:   int64(time.Hour / time.Second),
				MinSalience:       0.1,
				ReinforcementGain: 0.3,
			},
			LastReinforcedAt: now,
			DeletionPolicy:   schema.DeletionPolicyAutoPrune,
		},
		Provenance: schema.Provenance{},
		Relations:  []schema.Relation{},
		Payload: &schema.SemanticPayload{
			Kind:      "semantic",
			Subject:   id,
			Predicate: "is",
			Object:    "test record",
			Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
		},
		AuditLog: []schema.AuditEntry{
			{
				Action:    schema.AuditActionCreate,
				Actor:     "test",
				Timestamp: now,
				Rationale: "fixture",
			},
		},
	}
}

func mustCreate(t *testing.T, ctx context.Context, store storage.Store, rec *schema.MemoryRecord) {
	t.Helper()
	if err := store.Create(ctx, rec); err != nil {
		t.Fatalf("create %s: %v", rec.ID, err)
	}
}

func mustGet(t *testing.T, ctx context.Context, store storage.Store, id string) *schema.MemoryRecord {
	t.Helper()
	rec, err := store.Get(ctx, id)
	if err != nil {
		t.Fatalf("get %s: %v", id, err)
	}
	return rec
}

func hasAuditAction(entries []schema.AuditEntry, action schema.AuditAction) bool {
	for _, entry := range entries {
		if entry.Action == action {
			return true
		}
	}
	return false
}

func TestExponentialDecay(t *testing.T) {
	profile := schema.DecayProfile{
		Curve:           schema.DecayCurveExponential,
		HalfLifeSeconds: int64(time.Hour / time.Second),
		MinSalience:     0.2,
	}

	if got := decay.Exponential(0.8, float64(time.Hour/time.Second), profile); math.Abs(got-0.4) > 0.0001 {
		t.Fatalf("half-life decay = %.4f, want 0.4000", got)
	}

	if got := decay.Exponential(0.3, float64(10*time.Hour/time.Second), profile); got != 0.2 {
		t.Fatalf("floor decay = %.4f, want 0.2000", got)
	}

	profile.HalfLifeSeconds = 0
	if got := decay.Exponential(0.1, float64(time.Hour/time.Second), profile); got != 0.2 {
		t.Fatalf("non-positive half-life decay = %.4f, want floor 0.2000", got)
	}
}

func TestServiceApplyDecayReinforceAndPenalize(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	svc := decay.NewService(store)
	now := time.Now().UTC()

	rec := newRecord("decay-cycle", now.Add(-2*time.Hour))
	rec.Lifecycle.LastReinforcedAt = now.Add(-1 * time.Hour)
	mustCreate(t, ctx, store, rec)

	if err := svc.ApplyDecay(ctx, rec.ID); err != nil {
		t.Fatalf("ApplyDecay: %v", err)
	}
	decayed := mustGet(t, ctx, store, rec.ID)
	if math.Abs(decayed.Salience-0.4) > 0.02 {
		t.Fatalf("decayed salience = %.4f, want about 0.4000", decayed.Salience)
	}
	if !hasAuditAction(decayed.AuditLog, schema.AuditActionDecay) {
		t.Fatalf("expected decay audit entry, got %#v", decayed.AuditLog)
	}

	beforeReinforce := decayed.Lifecycle.LastReinforcedAt
	if err := svc.Reinforce(ctx, rec.ID, "tester", "useful"); err != nil {
		t.Fatalf("Reinforce: %v", err)
	}
	reinforced := mustGet(t, ctx, store, rec.ID)
	if reinforced.Salience <= decayed.Salience {
		t.Fatalf("reinforced salience = %.4f, want > %.4f", reinforced.Salience, decayed.Salience)
	}
	if !reinforced.Lifecycle.LastReinforcedAt.After(beforeReinforce) {
		t.Fatalf("expected LastReinforcedAt to advance")
	}
	if !hasAuditAction(reinforced.AuditLog, schema.AuditActionReinforce) {
		t.Fatalf("expected reinforce audit entry, got %#v", reinforced.AuditLog)
	}

	if err := svc.Penalize(ctx, rec.ID, 100, "tester", "not useful"); err != nil {
		t.Fatalf("Penalize: %v", err)
	}
	penalized := mustGet(t, ctx, store, rec.ID)
	if penalized.Salience != rec.Lifecycle.Decay.MinSalience {
		t.Fatalf("penalized salience = %.4f, want floor %.4f", penalized.Salience, rec.Lifecycle.Decay.MinSalience)
	}
}

func TestDecayFutureTimestampReinforceCapAndPenaltyAboveFloor(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	svc := decay.NewService(store)
	now := time.Now().UTC()

	future := newRecord("future-decay", now)
	future.Lifecycle.LastReinforcedAt = now.Add(time.Hour)
	mustCreate(t, ctx, store, future)
	if err := svc.ApplyDecay(ctx, future.ID); err != nil {
		t.Fatalf("ApplyDecay future timestamp: %v", err)
	}
	if got := mustGet(t, ctx, store, future.ID); math.Abs(got.Salience-future.Salience) > 0.0001 {
		t.Fatalf("future decay salience = %.4f, want unchanged %.4f", got.Salience, future.Salience)
	}

	capped := newRecord("cap-reinforce", now)
	capped.Salience = 0.95
	capped.Lifecycle.Decay.ReinforcementGain = 0.2
	mustCreate(t, ctx, store, capped)
	if err := svc.Reinforce(ctx, capped.ID, "tester", "cap"); err != nil {
		t.Fatalf("Reinforce cap: %v", err)
	}
	if got := mustGet(t, ctx, store, capped.ID); got.Salience != 1.0 {
		t.Fatalf("reinforced cap salience = %.4f, want 1.0", got.Salience)
	}

	penalty := newRecord("small-penalty", now)
	penalty.Salience = 0.8
	penalty.Lifecycle.Decay.MinSalience = 0.1
	mustCreate(t, ctx, store, penalty)
	if err := svc.Penalize(ctx, penalty.ID, 0.25, "tester", "small"); err != nil {
		t.Fatalf("Penalize small amount: %v", err)
	}
	if got := mustGet(t, ctx, store, penalty.ID); math.Abs(got.Salience-0.55) > 0.0001 {
		t.Fatalf("penalized salience = %.4f, want 0.55", got.Salience)
	}
}

func TestPenalizeRejectsInvalidAmount(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	svc := decay.NewService(store)
	now := time.Now().UTC()

	rec := newRecord("invalid-penalty", now)
	mustCreate(t, ctx, store, rec)

	for _, tc := range []struct {
		name   string
		amount float64
	}{
		{name: "negative", amount: -0.1},
		{name: "nan", amount: math.NaN()},
		{name: "positive infinity", amount: math.Inf(1)},
		{name: "negative infinity", amount: math.Inf(-1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := svc.Penalize(ctx, rec.ID, tc.amount, "tester", "invalid")
			if err == nil || !strings.Contains(err.Error(), "amount must be non-negative and finite") {
				t.Fatalf("Penalize error = %v, want amount validation error", err)
			}
		})
	}

	got := mustGet(t, ctx, store, rec.ID)
	if got.Salience != rec.Salience {
		t.Fatalf("salience changed after invalid penalties: got %.4f want %.4f", got.Salience, rec.Salience)
	}
	if len(got.AuditLog) != len(rec.AuditLog) {
		t.Fatalf("audit log changed after invalid penalties: got %d want %d", len(got.AuditLog), len(rec.AuditLog))
	}
}

func TestServiceReturnsErrorsForMissingRecords(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	svc := decay.NewService(store)

	for _, tc := range []struct {
		name string
		run  func() error
		want string
	}{
		{name: "apply decay", run: func() error { return svc.ApplyDecay(ctx, "missing") }, want: "decay: get record missing"},
		{name: "reinforce", run: func() error { return svc.Reinforce(ctx, "missing", "tester", "useful") }, want: "reinforce: get record missing"},
		{name: "penalize", run: func() error { return svc.Penalize(ctx, "missing", 0.1, "tester", "not useful") }, want: "penalize: get record missing"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.run()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("%s error = %v, want containing %q", tc.name, err, tc.want)
			}
		})
	}
}

func TestApplyDecayAllAndPruneReturnListErrors(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	if err := store.Close(); err != nil {
		t.Fatalf("Close store: %v", err)
	}
	svc := decay.NewService(store)

	if _, err := svc.ApplyDecayAll(ctx); err == nil || !strings.Contains(err.Error(), "decay-all: list records") {
		t.Fatalf("ApplyDecayAll closed store error = %v, want list error", err)
	}
	if _, err := svc.Prune(ctx); err == nil || !strings.Contains(err.Error(), "prune: list records") {
		t.Fatalf("Prune closed store error = %v, want list error", err)
	}
}

type failingDecayStore struct {
	records   []*schema.MemoryRecord
	beginErr  error
	tx        *failingDecayTx
	listErr   error
	listErrAt int
	listCalls int
}

func (s *failingDecayStore) Create(context.Context, *schema.MemoryRecord) error {
	return errors.New("unexpected create")
}

func (s *failingDecayStore) Get(context.Context, string) (*schema.MemoryRecord, error) {
	return nil, errors.New("unexpected get")
}

func (s *failingDecayStore) Update(context.Context, *schema.MemoryRecord) error {
	return errors.New("unexpected update")
}

func (s *failingDecayStore) Delete(context.Context, string) error {
	return errors.New("unexpected delete")
}

func (s *failingDecayStore) List(context.Context, storage.ListOptions) ([]*schema.MemoryRecord, error) {
	s.listCalls++
	if s.listErrAt > 0 && s.listCalls == s.listErrAt {
		return nil, s.listErr
	}
	return s.records, nil
}

func (s *failingDecayStore) ListByType(context.Context, schema.MemoryType) ([]*schema.MemoryRecord, error) {
	return nil, errors.New("unexpected list by type")
}

func (s *failingDecayStore) UpdateSalience(context.Context, string, float64) error {
	return errors.New("unexpected update salience")
}

func (s *failingDecayStore) AddAuditEntry(context.Context, string, schema.AuditEntry) error {
	return errors.New("unexpected add audit entry")
}

func (s *failingDecayStore) AddRelation(context.Context, string, schema.Relation) error {
	return errors.New("unexpected add relation")
}

func (s *failingDecayStore) GetRelations(context.Context, string) ([]schema.Relation, error) {
	return nil, errors.New("unexpected get relations")
}

func (s *failingDecayStore) Begin(context.Context) (storage.Transaction, error) {
	if s.beginErr != nil {
		return nil, s.beginErr
	}
	return s.tx, nil
}

func (s *failingDecayStore) Close() error {
	return nil
}

type failingDecayTx struct {
	record            *schema.MemoryRecord
	getErr            error
	updateErr         error
	updateSalienceErr error
	updatedSalience   float64
	auditErr          error
	deleteErr         error
	commitErr         error
	rolledBack        bool
}

func (tx *failingDecayTx) Create(context.Context, *schema.MemoryRecord) error {
	return errors.New("unexpected create")
}

func (tx *failingDecayTx) Get(context.Context, string) (*schema.MemoryRecord, error) {
	if tx.getErr != nil {
		return nil, tx.getErr
	}
	if tx.record != nil {
		return tx.record, nil
	}
	return nil, errors.New("unexpected get")
}

func (tx *failingDecayTx) Update(context.Context, *schema.MemoryRecord) error {
	return tx.updateErr
}

func (tx *failingDecayTx) Delete(context.Context, string) error {
	return tx.deleteErr
}

func (tx *failingDecayTx) List(context.Context, storage.ListOptions) ([]*schema.MemoryRecord, error) {
	return nil, errors.New("unexpected list")
}

func (tx *failingDecayTx) ListByType(context.Context, schema.MemoryType) ([]*schema.MemoryRecord, error) {
	return nil, errors.New("unexpected list by type")
}

func (tx *failingDecayTx) UpdateSalience(_ context.Context, _ string, salience float64) error {
	tx.updatedSalience = salience
	return tx.updateSalienceErr
}

func (tx *failingDecayTx) AddAuditEntry(context.Context, string, schema.AuditEntry) error {
	return tx.auditErr
}

func (tx *failingDecayTx) AddRelation(context.Context, string, schema.Relation) error {
	return errors.New("unexpected add relation")
}

func (tx *failingDecayTx) GetRelations(context.Context, string) ([]schema.Relation, error) {
	return nil, errors.New("unexpected get relations")
}

func (tx *failingDecayTx) Commit() error {
	return tx.commitErr
}

func (tx *failingDecayTx) Rollback() error {
	tx.rolledBack = true
	return nil
}

func TestApplyDecayAllAndPrunePropagatePerRecordErrors(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	rec := newRecord("failing-record", now)
	rec.Salience = 0
	rec.Lifecycle.Decay.MinSalience = 0

	t.Run("apply decay record error", func(t *testing.T) {
		tx := &failingDecayTx{getErr: errors.New("get failed")}
		svc := decay.NewService(&failingDecayStore{records: []*schema.MemoryRecord{rec}, tx: tx})

		count, err := svc.ApplyDecayAll(ctx)
		if count != 0 || err == nil || !strings.Contains(err.Error(), "decay-all: record failing-record") {
			t.Fatalf("ApplyDecayAll = %d/%v, want record error with zero count", count, err)
		}
		if !tx.rolledBack {
			t.Fatalf("ApplyDecayAll did not roll back failed transaction")
		}
	})

	for _, tc := range []struct {
		name string
		tx   *failingDecayTx
		err  error
		want string
	}{
		{name: "begin error", err: errors.New("begin failed"), want: "begin transaction"},
		{name: "audit error", tx: &failingDecayTx{auditErr: errors.New("audit failed")}, want: "add audit entry failing-record"},
		{name: "delete error", tx: &failingDecayTx{deleteErr: errors.New("delete failed")}, want: "delete record failing-record"},
		{name: "commit error", tx: &failingDecayTx{commitErr: errors.New("commit failed")}, want: "commit transaction"},
	} {
		t.Run("prune "+tc.name, func(t *testing.T) {
			store := &failingDecayStore{records: []*schema.MemoryRecord{rec}, beginErr: tc.err, tx: tc.tx}
			svc := decay.NewService(store)

			count, err := svc.Prune(ctx)
			if count != 0 || err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Prune = %d/%v, want %q error", count, err, tc.want)
			}
			if tc.tx != nil && tc.tx.commitErr == nil && !tc.tx.rolledBack {
				t.Fatalf("Prune did not roll back failed transaction")
			}
		})
	}
}

func TestApplyDecayClampsInvalidComputedSalience(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()

	for _, tc := range []struct {
		name     string
		salience float64
		min      float64
		want     float64
	}{
		{name: "infinite", salience: math.Inf(1), min: 0.25, want: 0.25},
		{name: "negative", salience: -0.4, min: 0.1, want: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			record := newRecord("invalid-"+tc.name, now.Add(-time.Hour))
			record.Salience = tc.salience
			record.Lifecycle.Decay.MinSalience = tc.min
			tx := &failingDecayTx{record: record}
			svc := decay.NewService(&failingDecayStore{tx: tx})

			if err := svc.ApplyDecay(ctx, record.ID); err != nil {
				t.Fatalf("ApplyDecay: %v", err)
			}
			if math.Abs(tx.updatedSalience-tc.want) > 0.0001 {
				t.Fatalf("updated salience = %v, want %v", tx.updatedSalience, tc.want)
			}
		})
	}
}

func TestServiceOperationTransactionErrors(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	record := newRecord("tx-failure", now.Add(-2*time.Hour))
	record.Lifecycle.LastReinforcedAt = now.Add(-time.Hour)

	for _, tc := range []struct {
		name string
		tx   *failingDecayTx
		run  func(*decay.Service) error
		want string
	}{
		{
			name: "apply decay update salience",
			tx:   &failingDecayTx{record: record, updateSalienceErr: errors.New("update salience failed")},
			run:  func(svc *decay.Service) error { return svc.ApplyDecay(ctx, record.ID) },
			want: "decay: update salience tx-failure",
		},
		{
			name: "apply decay audit",
			tx:   &failingDecayTx{record: record, auditErr: errors.New("audit failed")},
			run:  func(svc *decay.Service) error { return svc.ApplyDecay(ctx, record.ID) },
			want: "decay: add audit entry tx-failure",
		},
		{
			name: "reinforce update",
			tx:   &failingDecayTx{record: record, updateErr: errors.New("update failed")},
			run:  func(svc *decay.Service) error { return svc.Reinforce(ctx, record.ID, "tester", "useful") },
			want: "reinforce: update record tx-failure",
		},
		{
			name: "reinforce audit",
			tx:   &failingDecayTx{record: record, auditErr: errors.New("audit failed")},
			run:  func(svc *decay.Service) error { return svc.Reinforce(ctx, record.ID, "tester", "useful") },
			want: "reinforce: add audit entry tx-failure",
		},
		{
			name: "penalize update salience",
			tx:   &failingDecayTx{record: record, updateSalienceErr: errors.New("update salience failed")},
			run:  func(svc *decay.Service) error { return svc.Penalize(ctx, record.ID, 0.1, "tester", "bad") },
			want: "penalize: update salience tx-failure",
		},
		{
			name: "penalize audit",
			tx:   &failingDecayTx{record: record, auditErr: errors.New("audit failed")},
			run:  func(svc *decay.Service) error { return svc.Penalize(ctx, record.ID, 0.1, "tester", "bad") },
			want: "penalize: add audit entry tx-failure",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc := decay.NewService(&failingDecayStore{tx: tc.tx})
			err := tc.run(svc)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("operation error = %v, want %q", err, tc.want)
			}
			if !tc.tx.rolledBack {
				t.Fatalf("operation did not roll back failed transaction")
			}
		})
	}
}

func TestApplyDecayAllSkipsPinnedRecords(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	svc := decay.NewService(store)
	now := time.Now().UTC()

	active := newRecord("active", now.Add(-2*time.Hour))
	active.Lifecycle.LastReinforcedAt = now.Add(-1 * time.Hour)
	pinned := newRecord("pinned", now.Add(-2*time.Hour))
	pinned.Lifecycle.LastReinforcedAt = now.Add(-1 * time.Hour)
	pinned.Lifecycle.Pinned = true
	mustCreate(t, ctx, store, active)
	mustCreate(t, ctx, store, pinned)

	count, err := svc.ApplyDecayAll(ctx)
	if err != nil {
		t.Fatalf("ApplyDecayAll: %v", err)
	}
	if count != 1 {
		t.Fatalf("ApplyDecayAll count = %d, want 1", count)
	}

	if got := mustGet(t, ctx, store, pinned.ID); got.Salience != pinned.Salience {
		t.Fatalf("pinned salience = %.4f, want unchanged %.4f", got.Salience, pinned.Salience)
	}
	if got := mustGet(t, ctx, store, active.ID); got.Salience >= active.Salience {
		t.Fatalf("active salience = %.4f, want less than %.4f", got.Salience, active.Salience)
	}
}

func TestApplyDecayMaxAgeAndPrune(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	svc := decay.NewService(store)
	now := time.Now().UTC()

	expired := newRecord("expired", now.Add(-2*time.Hour))
	expired.Lifecycle.Decay.MinSalience = 0
	expired.Lifecycle.Decay.MaxAgeSeconds = 1
	mustCreate(t, ctx, store, expired)

	if err := svc.ApplyDecay(ctx, expired.ID); err != nil {
		t.Fatalf("ApplyDecay: %v", err)
	}
	if got := mustGet(t, ctx, store, expired.ID); got.Salience != 0 {
		t.Fatalf("expired salience = %.4f, want 0", got.Salience)
	}

	pruned, err := svc.Prune(ctx)
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	if pruned != 1 {
		t.Fatalf("Prune count = %d, want 1", pruned)
	}
	if _, err := store.Get(ctx, expired.ID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("Get expired after prune error = %v, want ErrNotFound", err)
	}
}

func TestPruneOnlyDeletesEligibleAutoPruneRecords(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	svc := decay.NewService(store)
	now := time.Now().UTC()

	eligible := newRecord("eligible", now)
	eligible.Salience = 0
	eligible.Lifecycle.Decay.MinSalience = 0

	pinned := newRecord("pinned-zero", now)
	pinned.Salience = 0
	pinned.Lifecycle.Decay.MinSalience = 0
	pinned.Lifecycle.Pinned = true

	manual := newRecord("manual-zero", now)
	manual.Salience = 0
	manual.Lifecycle.Decay.MinSalience = 0
	manual.Lifecycle.DeletionPolicy = schema.DeletionPolicyManualOnly

	nonZeroFloor := newRecord("floor", now)
	nonZeroFloor.Salience = 0.1
	nonZeroFloor.Lifecycle.Decay.MinSalience = 0.1

	for _, rec := range []*schema.MemoryRecord{eligible, pinned, manual, nonZeroFloor} {
		mustCreate(t, ctx, store, rec)
	}

	pruned, err := svc.Prune(ctx)
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	if pruned != 1 {
		t.Fatalf("Prune count = %d, want 1", pruned)
	}
	if _, err := store.Get(ctx, eligible.ID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("Get eligible after prune error = %v, want ErrNotFound", err)
	}
	for _, id := range []string{pinned.ID, manual.ID, nonZeroFloor.ID} {
		if _, err := store.Get(ctx, id); err != nil {
			t.Fatalf("Get %s after prune: %v", id, err)
		}
	}
}

func TestSchedulerStartAndStop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := newTestStore(t)
	svc := decay.NewService(store)
	now := time.Now().UTC()
	rec := newRecord("scheduled", now.Add(-2*time.Hour))
	rec.Lifecycle.LastReinforcedAt = now.Add(-1 * time.Hour)
	mustCreate(t, ctx, store, rec)

	scheduler := decay.NewScheduler(svc, time.Millisecond)
	scheduler.Start(ctx)
	time.Sleep(5 * time.Millisecond)
	scheduler.Stop()
	scheduler.Stop()

	got := mustGet(t, context.Background(), store, rec.ID)
	if got.Salience >= rec.Salience {
		t.Fatalf("scheduled salience = %.4f, want less than %.4f", got.Salience, rec.Salience)
	}
}

func TestSchedulerStopBeforeStart(t *testing.T) {
	store := newTestStore(t)
	scheduler := decay.NewScheduler(decay.NewService(store), time.Hour)
	scheduler.Stop()
}

func TestSchedulerStopsOnContextCancellationAndHandlesLoopFailures(t *testing.T) {
	t.Run("context cancellation", func(t *testing.T) {
		store := newTestStore(t)
		ctx, cancel := context.WithCancel(context.Background())
		scheduler := decay.NewScheduler(decay.NewService(store), time.Hour)
		scheduler.Start(ctx)
		cancel()
		time.Sleep(time.Millisecond)
		scheduler.Stop()
	})

	t.Run("closed store error", func(t *testing.T) {
		store := newTestStore(t)
		if err := store.Close(); err != nil {
			t.Fatalf("Close store: %v", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		scheduler := decay.NewScheduler(decay.NewService(store), time.Millisecond)
		scheduler.Start(ctx)
		time.Sleep(3 * time.Millisecond)
		scheduler.Stop()
	})

	t.Run("prune error", func(t *testing.T) {
		store := &failingDecayStore{
			listErr:   errors.New("prune list failed"),
			listErrAt: 2,
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		scheduler := decay.NewScheduler(decay.NewService(store), time.Millisecond)
		scheduler.Start(ctx)
		time.Sleep(3 * time.Millisecond)
		scheduler.Stop()
		if store.listCalls < 2 {
			t.Fatalf("List calls = %d, want scheduler to reach prune", store.listCalls)
		}
	})

	t.Run("context already cancelled", func(t *testing.T) {
		store := newTestStore(t)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		scheduler := decay.NewScheduler(decay.NewService(store), time.Hour)
		scheduler.Start(ctx)
		time.Sleep(time.Millisecond)
		scheduler.Stop()
	})

	t.Run("panic recovery", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		scheduler := decay.NewScheduler(nil, time.Millisecond)
		scheduler.Start(ctx)
		time.Sleep(3 * time.Millisecond)
		scheduler.Stop()
	})

	t.Run("logs pruned records", func(t *testing.T) {
		store := newTestStore(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		now := time.Now().UTC()
		rec := newRecord("scheduler-prune", now)
		rec.Salience = 0
		rec.Lifecycle.Decay.MinSalience = 0
		rec.Lifecycle.DeletionPolicy = schema.DeletionPolicyAutoPrune
		mustCreate(t, ctx, store, rec)

		scheduler := decay.NewScheduler(decay.NewService(store), time.Millisecond)
		scheduler.Start(ctx)
		time.Sleep(5 * time.Millisecond)
		scheduler.Stop()
		if _, err := store.Get(context.Background(), rec.ID); !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("scheduler-prune record err = %v, want ErrNotFound", err)
		}
	})
}
