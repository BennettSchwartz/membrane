package decay

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// Scheduler runs periodic decay sweeps across all records.
type Scheduler struct {
	service    *Service
	interval   time.Duration
	stopCh     chan struct{}
	done       chan struct{}
	started    sync.Once
	wasStarted atomic.Bool
}

// NewScheduler creates a new decay scheduler that runs ApplyDecayAll
// at the given interval.
func NewScheduler(service *Service, interval time.Duration) *Scheduler {
	return &Scheduler{
		service:  service,
		interval: interval,
		stopCh:   make(chan struct{}),
		done:     make(chan struct{}),
	}
}

// Start begins the periodic decay loop in a goroutine. It runs until
// the context is cancelled or Stop is called.
func (s *Scheduler) Start(ctx context.Context) {
	s.started.Do(func() {
		s.wasStarted.Store(true)
		go func() {
			defer close(s.done)
			defer func() {
				if r := recover(); r != nil {
					log.Printf("decay scheduler: panic recovered: %v", r)
				}
			}()

			ticker := time.NewTicker(s.interval)
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					return
				case <-s.stopCh:
					return
				case <-ticker.C:
					count, err := s.service.ApplyDecayAll(ctx)
					if err != nil {
						log.Printf("decay scheduler: error applying decay: %v", err)
						continue
					}
					log.Printf("decay scheduler: decayed %d records", count)
				}
			}
		}()
	})
}

// Stop gracefully shuts down the scheduler and waits for the goroutine
// to finish. Safe to call even if Start was never called.
func (s *Scheduler) Stop() {
	select {
	case <-s.stopCh:
		// Already stopped.
		return
	default:
		close(s.stopCh)
	}
	if s.wasStarted.Load() {
		<-s.done
	}
}
