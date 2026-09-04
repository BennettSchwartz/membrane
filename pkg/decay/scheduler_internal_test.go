package decay

import (
	"testing"
	"time"
)

func TestNewSchedulerNormalizesNonPositiveInterval(t *testing.T) {
	for _, interval := range []time.Duration{0, -time.Second} {
		scheduler := NewScheduler(nil, interval)
		if scheduler.interval <= 0 {
			t.Fatalf("NewScheduler(%s) interval = %s, want positive fallback", interval, scheduler.interval)
		}
	}
}
