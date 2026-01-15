package consolidation

import (
	"context"
	"log"
	"sync"
	"time"
)

// Scheduler runs periodic consolidation sweeps in the background.
// It follows the same pattern as the decay scheduler: sync.Once for
// Start, panic recovery in the goroutine, and safe Stop semantics.
type Scheduler struct {
	service    *Service
	interval   time.Duration
	stopCh     chan struct{}
	done       chan struct{}
	started    sync.Once
	wasStarted bool
}

// NewScheduler creates a new consolidation scheduler that runs RunAll
// at the given interval.
func NewScheduler(service *Service, interval time.Duration) *Scheduler {
	return &Scheduler{
		service:  service,
		interval: interval,
		stopCh:   make(chan struct{}),
		done:     make(chan struct{}),
	}
}

// Start begins the periodic consolidation loop in a goroutine. It
// runs until the context is cancelled or Stop is called. Start is
// safe to call multiple times; only the first call starts the loop.
func (s *Scheduler) Start(ctx context.Context) {
	s.started.Do(func() {
		s.wasStarted = true
		go func() {
			defer close(s.done)
			defer func() {
				if r := recover(); r != nil {
					log.Printf("consolidation scheduler: panic recovered: %v", r)
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
					result, err := s.service.RunAll(ctx)
					if err != nil {
						log.Printf("consolidation scheduler: error: %v", err)
						continue
					}
					log.Printf("consolidation scheduler: compressed=%d semantic=%d competence=%d plangraph=%d",
						result.EpisodicCompressed,
						result.SemanticExtracted,
						result.CompetenceExtracted,
						result.PlanGraphsExtracted,
					)
				}
			}
		}()
	})
}

// Stop gracefully shuts down the scheduler and waits for the
// goroutine to finish. Safe to call even if Start was never called.
func (s *Scheduler) Stop() {
	select {
	case <-s.stopCh:
		// Already stopped.
		return
	default:
		close(s.stopCh)
	}
	if s.wasStarted {
		<-s.done
	}
}
