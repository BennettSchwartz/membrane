package consolidation

import (
	"context"
	"testing"
	"time"
)

func TestSchedulerStartStopAndIdempotence(t *testing.T) {
	store := newConsolidationTestStore(t)
	scheduler := NewScheduler(NewService(store), time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	scheduler.Start(ctx)
	scheduler.Start(ctx)
	time.Sleep(5 * time.Millisecond)
	cancel()
	time.Sleep(time.Millisecond)
	scheduler.Stop()
	scheduler.Stop()
}

func TestSchedulerStopBeforeStart(t *testing.T) {
	store := newConsolidationTestStore(t)
	scheduler := NewScheduler(NewService(store), time.Hour)

	scheduler.Stop()
	scheduler.Stop()
}

func TestSchedulerStartWithCanceledContext(t *testing.T) {
	store := newConsolidationTestStore(t)
	scheduler := NewScheduler(NewService(store), time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	scheduler.Start(ctx)
	time.Sleep(time.Millisecond)
	scheduler.Stop()
}
