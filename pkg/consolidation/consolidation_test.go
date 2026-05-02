package consolidation

import (
	"context"
	"errors"
	"io"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/BennettSchwartz/membrane/pkg/schema"
)

type listErrorConsolidationStore struct {
	*fakeExtractionStore
	errOnCall int
	calls     int
	err       error
}

func (s *listErrorConsolidationStore) ListByType(_ context.Context, _ schema.MemoryType) ([]*schema.MemoryRecord, error) {
	s.calls++
	if s.calls == s.errOnCall {
		return nil, s.err
	}
	return nil, nil
}

func TestRunAllWrapsPipelineErrors(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name      string
		errOnCall int
		want      string
	}{
		{name: "episodic", errOnCall: 1, want: "episodic consolidation"},
		{name: "semantic", errOnCall: 2, want: "semantic consolidation"},
		{name: "competence", errOnCall: 4, want: "competence consolidation"},
		{name: "plan graph", errOnCall: 6, want: "plan graph consolidation"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &listErrorConsolidationStore{
				fakeExtractionStore: newFakeExtractionStore(),
				errOnCall:           tc.errOnCall,
				err:                 errors.New("list failed"),
			}
			result, err := NewService(store).RunAll(ctx)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("RunAll error = %v, result = %+v; want wrapper %q", err, result, tc.want)
			}
		})
	}
}

func TestRunAllWrapsExtractorErrors(t *testing.T) {
	ctx := context.Background()
	store := newFakeExtractionStore()
	store.cleanErr = errors.New("cleanup failed")

	result, err := NewServiceWithExtractor(store, nil, &fakeReinforcer{}, &fakeLLMClient{}, store).RunAll(ctx)
	if err == nil || !strings.Contains(err.Error(), "semantic extraction") {
		t.Fatalf("RunAll extractor error = %v, result = %+v; want semantic extraction wrapper", err, result)
	}
}

func TestSchedulerHandlesRunErrorsAndRecoversPanics(t *testing.T) {
	oldLogOutput := log.Writer()
	log.SetOutput(io.Discard)
	defer log.SetOutput(oldLogOutput)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errStore := &listErrorConsolidationStore{
		fakeExtractionStore: newFakeExtractionStore(),
		errOnCall:           1,
		err:                 errors.New("list failed"),
	}
	errorScheduler := NewScheduler(NewService(errStore), time.Nanosecond)
	errorScheduler.Start(ctx)
	time.Sleep(2 * time.Millisecond)
	errorScheduler.Stop()

	panicScheduler := NewScheduler((*Service)(nil), time.Nanosecond)
	panicScheduler.Start(ctx)
	select {
	case <-panicScheduler.done:
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("scheduler did not recover from panic")
	}
	panicScheduler.Stop()
}
