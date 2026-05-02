package grpc

import (
	"context"
	"math"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"

	pb "github.com/BennettSchwartz/membrane/api/grpc/gen/membranev1"
	membranepkg "github.com/BennettSchwartz/membrane/pkg/membrane"
	"github.com/BennettSchwartz/membrane/pkg/schema"
)

func newHandlerTest(t *testing.T) *Handler {
	t.Helper()

	cfg := membranepkg.DefaultConfig()
	cfg.DBPath = filepath.Join(t.TempDir(), "membrane.db")
	cfg.DecayInterval = time.Hour
	cfg.ConsolidationInterval = time.Hour

	m, err := membranepkg.New(cfg)
	if err != nil {
		t.Fatalf("membrane.New: %v", err)
	}
	t.Cleanup(func() {
		if err := m.Stop(); err != nil {
			t.Fatalf("Membrane.Stop: %v", err)
		}
	})

	return &Handler{membrane: m}
}

func testTrustPB() *pb.TrustContext {
	return &pb.TrustContext{
		MaxSensitivity: string(schema.SensitivityLow),
		Authenticated:  true,
		ActorId:        "tester",
		Scopes:         []string{"project:alpha"},
	}
}

func captureFactThroughHandler(t *testing.T, ctx context.Context, h *Handler) (*pb.CaptureMemoryResponse, string) {
	t.Helper()
	return captureSemanticFactThroughHandler(t, ctx, h, "Orchid", "deploy_target_for", "staging")
}

func captureSemanticFactThroughHandler(t *testing.T, ctx context.Context, h *Handler, subject, predicate string, object any) (*pb.CaptureMemoryResponse, string) {
	t.Helper()

	content, err := structpb.NewValue(map[string]any{
		"subject":   subject,
		"predicate": predicate,
		"object":    object,
	})
	if err != nil {
		t.Fatalf("NewValue: %v", err)
	}

	resp, err := h.CaptureMemory(ctx, &pb.CaptureMemoryRequest{
		Source:           "tester",
		SourceKind:       "observation",
		Content:          content,
		ReasonToRemember: "Deployment facts should be retrievable",
		Summary:          subject + " " + predicate,
		Tags:             []string{"deploy"},
		Scope:            "project:alpha",
		Sensitivity:      string(schema.SensitivityLow),
		Timestamp:        time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC).Format(time.RFC3339),
	})
	if err != nil {
		t.Fatalf("CaptureMemory: %v", err)
	}
	if resp.PrimaryRecord == nil || resp.PrimaryRecord.Id == "" {
		t.Fatalf("CaptureMemory primary = %+v, want persisted primary record", resp.PrimaryRecord)
	}

	var semanticID string
	for _, rec := range resp.CreatedRecords {
		if rec.GetType() == string(schema.MemoryTypeSemantic) {
			semanticID = rec.GetId()
			break
		}
	}
	if semanticID == "" {
		t.Fatalf("CaptureMemory created records = %+v, want derived semantic record", resp.CreatedRecords)
	}

	return resp, semanticID
}

func semanticRecordPBForHandler(t *testing.T, id, subject, predicate string, object any) *pb.MemoryRecord {
	t.Helper()

	rec := schema.NewMemoryRecord(id, schema.MemoryTypeSemantic, schema.SensitivityLow, &schema.SemanticPayload{
		Kind:      "semantic",
		Subject:   subject,
		Predicate: predicate,
		Object:    object,
		Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
		Evidence: []schema.ProvenanceRef{{
			SourceType: "observation",
			SourceID:   "handler-test",
		}},
	})
	rec.Scope = "project:alpha"
	wire, err := memoryRecordToPB(rec)
	if err != nil {
		t.Fatalf("memoryRecordToPB: %v", err)
	}
	return wire
}

func TestHandlerDelegatesCaptureRetrieveMutationsAndMetrics(t *testing.T) {
	ctx := context.Background()
	h := newHandlerTest(t)

	captureResp, semanticID := captureFactThroughHandler(t, ctx, h)

	gotPrimary, err := h.RetrieveByID(ctx, &pb.RetrieveByIDRequest{
		Id:    captureResp.PrimaryRecord.Id,
		Trust: testTrustPB(),
	})
	if err != nil {
		t.Fatalf("RetrieveByID primary: %v", err)
	}
	if gotPrimary.Record.GetId() != captureResp.PrimaryRecord.Id {
		t.Fatalf("RetrieveByID id = %q, want %q", gotPrimary.Record.GetId(), captureResp.PrimaryRecord.Id)
	}

	graph, err := h.RetrieveGraph(ctx, &pb.RetrieveGraphRequest{
		TaskDescriptor: "Orchid staging deployment",
		Trust:          testTrustPB(),
		MemoryTypes:    []string{string(schema.MemoryTypeSemantic)},
		RootLimit:      2,
		NodeLimit:      8,
		EdgeLimit:      8,
		MaxHops:        1,
	})
	if err != nil {
		t.Fatalf("RetrieveGraph: %v", err)
	}
	if len(graph.Nodes) == 0 {
		t.Fatalf("RetrieveGraph nodes = 0, want at least one semantic node")
	}

	if _, err := h.Contest(ctx, &pb.ContestRequest{
		Id:        semanticID,
		Actor:     "tester",
		Rationale: "conflicting observation under review",
	}); err != nil {
		t.Fatalf("Contest semantic record: %v", err)
	}
	if _, err := h.Reinforce(ctx, &pb.ReinforceRequest{
		Id:        captureResp.PrimaryRecord.Id,
		Actor:     "tester",
		Rationale: "recently useful",
	}); err != nil {
		t.Fatalf("Reinforce primary: %v", err)
	}
	if _, err := h.Penalize(ctx, &pb.PenalizeRequest{
		Id:        captureResp.PrimaryRecord.Id,
		Amount:    0.1,
		Actor:     "tester",
		Rationale: "less relevant after correction",
	}); err != nil {
		t.Fatalf("Penalize primary: %v", err)
	}

	metrics, err := h.GetMetrics(ctx, &pb.GetMetricsRequest{})
	if err != nil {
		t.Fatalf("GetMetrics: %v", err)
	}
	if metrics.Snapshot == nil {
		t.Fatal("GetMetrics snapshot = nil, want populated snapshot")
	}
}

func TestHandlerGetMetricsPropagatesCollectorErrors(t *testing.T) {
	ctx := context.Background()
	cfg := membranepkg.DefaultConfig()
	cfg.DBPath = filepath.Join(t.TempDir(), "membrane.db")

	m, err := membranepkg.New(cfg)
	if err != nil {
		t.Fatalf("membrane.New: %v", err)
	}
	if err := m.Stop(); err != nil {
		t.Fatalf("Membrane.Stop: %v", err)
	}
	h := &Handler{membrane: m}

	if _, err := h.GetMetrics(ctx, &pb.GetMetricsRequest{}); status.Code(err) != codes.Internal {
		t.Fatalf("GetMetrics closed store code = %v, want Internal; err=%v", status.Code(err), err)
	}
	if _, err := h.RetrieveGraph(ctx, &pb.RetrieveGraphRequest{
		Trust:     testTrustPB(),
		RootLimit: 1,
		NodeLimit: 1,
		EdgeLimit: 1,
		MaxHops:   1,
	}); status.Code(err) != codes.Internal {
		t.Fatalf("RetrieveGraph closed store code = %v, want Internal; err=%v", status.Code(err), err)
	}
}

func TestHandlerDelegatesRevisionMethods(t *testing.T) {
	ctx := context.Background()
	h := newHandlerTest(t)

	_, oldID := captureSemanticFactThroughHandler(t, ctx, h, "api", "rate_limit", "50 rps")
	superseded, err := h.Supersede(ctx, &pb.SupersedeRequest{
		OldId:     oldID,
		NewRecord: semanticRecordPBForHandler(t, "", "api", "rate_limit", "200 rps"),
		Actor:     "tester",
		Rationale: "rate limit changed",
	})
	if err != nil {
		t.Fatalf("Supersede: %v", err)
	}
	if superseded.Record.GetId() == "" {
		t.Fatal("Supersede record ID = empty")
	}

	_, forkSourceID := captureSemanticFactThroughHandler(t, ctx, h, "database", "primary", "postgres")
	forked, err := h.Fork(ctx, &pb.ForkRequest{
		SourceId:     forkSourceID,
		ForkedRecord: semanticRecordPBForHandler(t, "", "database", "primary", "sqlite"),
		Actor:        "tester",
		Rationale:    "local variant",
	})
	if err != nil {
		t.Fatalf("Fork: %v", err)
	}
	if forked.Record.GetId() == "" {
		t.Fatal("Fork record ID = empty")
	}
	if _, err := h.Retract(ctx, &pb.RetractRequest{
		Id:        forked.Record.Id,
		Actor:     "tester",
		Rationale: "variant obsolete",
	}); err != nil {
		t.Fatalf("Retract: %v", err)
	}

	_, mergeA := captureSemanticFactThroughHandler(t, ctx, h, "editor", "preferred", "vim")
	_, mergeB := captureSemanticFactThroughHandler(t, ctx, h, "editor", "preferred", "neovim")
	merged, err := h.Merge(ctx, &pb.MergeRequest{
		Ids:          []string{mergeA, mergeB},
		MergedRecord: semanticRecordPBForHandler(t, "", "editor", "preferred", "neovim"),
		Actor:        "tester",
		Rationale:    "merge editor preference",
	})
	if err != nil {
		t.Fatalf("Merge: %v", err)
	}
	if merged.Record.GetId() == "" || len(merged.Record.Relations) != 2 {
		t.Fatalf("Merge record = %+v, want generated record with two source relations", merged.Record)
	}
}

func TestHandlerPropagatesMutationServiceErrors(t *testing.T) {
	ctx := context.Background()
	h := newHandlerTest(t)
	replacement := semanticRecordPBForHandler(t, "", "api", "rate_limit", "200 rps")

	calls := []struct {
		name string
		call func() error
	}{
		{
			name: "Supersede",
			call: func() error {
				_, err := h.Supersede(ctx, &pb.SupersedeRequest{
					OldId:     "missing",
					NewRecord: replacement,
					Actor:     "tester",
					Rationale: "replacement",
				})
				return err
			},
		},
		{
			name: "Fork",
			call: func() error {
				_, err := h.Fork(ctx, &pb.ForkRequest{
					SourceId:     "missing",
					ForkedRecord: replacement,
					Actor:        "tester",
					Rationale:    "variant",
				})
				return err
			},
		},
		{
			name: "Retract",
			call: func() error {
				_, err := h.Retract(ctx, &pb.RetractRequest{Id: "missing", Actor: "tester", Rationale: "obsolete"})
				return err
			},
		},
		{
			name: "Merge",
			call: func() error {
				_, err := h.Merge(ctx, &pb.MergeRequest{
					Ids:          []string{"missing-a", "missing-b"},
					MergedRecord: replacement,
					Actor:        "tester",
					Rationale:    "dedupe",
				})
				return err
			},
		},
		{
			name: "Reinforce",
			call: func() error {
				_, err := h.Reinforce(ctx, &pb.ReinforceRequest{Id: "missing", Actor: "tester", Rationale: "useful"})
				return err
			},
		},
		{
			name: "Penalize",
			call: func() error {
				_, err := h.Penalize(ctx, &pb.PenalizeRequest{Id: "missing", Amount: 0.1, Actor: "tester", Rationale: "stale"})
				return err
			},
		},
		{
			name: "Contest",
			call: func() error {
				_, err := h.Contest(ctx, &pb.ContestRequest{Id: "missing", Actor: "tester", Rationale: "conflict"})
				return err
			},
		},
	}
	for _, tc := range calls {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.call(); status.Code(err) != codes.NotFound {
				t.Fatalf("%s missing record code = %v, want NotFound; err=%v", tc.name, status.Code(err), err)
			}
		})
	}
}

func TestHandlerRejectsInvalidRequestsBeforeDelegation(t *testing.T) {
	ctx := context.Background()
	h := newHandlerTest(t)

	content, err := structpb.NewValue(map[string]any{"text": "remember me"})
	if err != nil {
		t.Fatalf("NewValue: %v", err)
	}
	if _, err := h.CaptureMemory(ctx, &pb.CaptureMemoryRequest{
		Source:      "tester",
		SourceKind:  "event",
		Content:     content,
		Sensitivity: "private",
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("CaptureMemory invalid sensitivity code = %v, want InvalidArgument", status.Code(err))
	}
	if _, err := h.CaptureMemory(ctx, &pb.CaptureMemoryRequest{
		Source:     "tester",
		SourceKind: "event",
		Content:    content,
		Scope:      strings.Repeat("x", maxStringLength+1),
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("CaptureMemory long scope code = %v, want InvalidArgument", status.Code(err))
	}
	for _, tc := range []struct {
		name string
		req  *pb.CaptureMemoryRequest
	}{
		{name: "long source", req: &pb.CaptureMemoryRequest{Source: strings.Repeat("x", maxStringLength+1)}},
		{name: "long source_kind", req: &pb.CaptureMemoryRequest{SourceKind: strings.Repeat("x", maxStringLength+1)}},
		{name: "long reason", req: &pb.CaptureMemoryRequest{ReasonToRemember: strings.Repeat("x", maxStringLength+1)}},
		{name: "long summary", req: &pb.CaptureMemoryRequest{Summary: strings.Repeat("x", maxStringLength+1)}},
		{name: "too many tags", req: &pb.CaptureMemoryRequest{Tags: make([]string, maxTags+1)}},
		{name: "long tag", req: &pb.CaptureMemoryRequest{Tags: []string{strings.Repeat("x", maxTagLength+1)}}},
		{name: "bad proposed type", req: &pb.CaptureMemoryRequest{ProposedType: "unknown"}},
		{name: "bad timestamp", req: &pb.CaptureMemoryRequest{Timestamp: "not-time"}},
	} {
		t.Run("capture "+tc.name, func(t *testing.T) {
			if _, err := h.CaptureMemory(ctx, tc.req); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("CaptureMemory %s code = %v, want InvalidArgument", tc.name, status.Code(err))
			}
		})
	}
	largeValue, err := structpb.NewValue(strings.Repeat("x", maxPayloadSize+1))
	if err != nil {
		t.Fatalf("NewValue large: %v", err)
	}
	if _, err := h.CaptureMemory(ctx, &pb.CaptureMemoryRequest{Content: largeValue}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("CaptureMemory oversized content code = %v, want InvalidArgument", status.Code(err))
	}
	if _, err := h.CaptureMemory(ctx, &pb.CaptureMemoryRequest{Context: largeValue}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("CaptureMemory oversized context code = %v, want InvalidArgument", status.Code(err))
	}
	if _, err := h.CaptureMemory(ctx, &pb.CaptureMemoryRequest{
		SourceKind:  "event",
		Content:     content,
		Sensitivity: string(schema.SensitivityLow),
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("CaptureMemory service validation code = %v, want InvalidArgument", status.Code(err))
	}

	if _, err := h.RetrieveByID(ctx, &pb.RetrieveByIDRequest{Id: "missing"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("RetrieveByID missing trust code = %v, want InvalidArgument", status.Code(err))
	}
	if _, err := h.RetrieveByID(ctx, &pb.RetrieveByIDRequest{
		Id:    "missing",
		Trust: &pb.TrustContext{MaxSensitivity: "private"},
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("RetrieveByID invalid trust sensitivity code = %v, want InvalidArgument", status.Code(err))
	}
	if _, err := h.RetrieveByID(ctx, &pb.RetrieveByIDRequest{
		Id:    "missing",
		Trust: testTrustPB(),
	}); status.Code(err) != codes.NotFound {
		t.Fatalf("RetrieveByID missing record code = %v, want NotFound", status.Code(err))
	}

	if _, err := h.RetrieveGraph(ctx, &pb.RetrieveGraphRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("RetrieveGraph missing trust code = %v, want InvalidArgument", status.Code(err))
	}
	if _, err := h.RetrieveGraph(ctx, &pb.RetrieveGraphRequest{
		Trust:       testTrustPB(),
		MemoryTypes: []string{"unknown"},
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("RetrieveGraph invalid memory type code = %v, want InvalidArgument", status.Code(err))
	}
	if _, err := h.RetrieveGraph(ctx, &pb.RetrieveGraphRequest{
		Trust:          testTrustPB(),
		TaskDescriptor: strings.Repeat("x", maxStringLength+1),
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("RetrieveGraph long task descriptor code = %v, want InvalidArgument", status.Code(err))
	}
	if _, err := h.RetrieveGraph(ctx, &pb.RetrieveGraphRequest{
		Trust: &pb.TrustContext{MaxSensitivity: "private"},
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("RetrieveGraph invalid trust sensitivity code = %v, want InvalidArgument", status.Code(err))
	}
	if _, err := h.RetrieveGraph(ctx, &pb.RetrieveGraphRequest{
		Trust:       testTrustPB(),
		MinSalience: math.Inf(1),
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("RetrieveGraph invalid min_salience code = %v, want InvalidArgument", status.Code(err))
	}

	if _, err := h.RetrieveGraph(ctx, &pb.RetrieveGraphRequest{
		Trust:       testTrustPB(),
		RootLimit:   maxLimit + 1,
		NodeLimit:   1,
		EdgeLimit:   1,
		MaxHops:     1,
		MinSalience: 0.1,
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("RetrieveGraph invalid limit code = %v, want InvalidArgument", status.Code(err))
	}

	if _, err := h.RetrieveGraph(ctx, &pb.RetrieveGraphRequest{
		Trust:     testTrustPB(),
		RootLimit: 1,
		NodeLimit: 1,
		EdgeLimit: 1,
		MaxHops:   -1,
	}); err != nil {
		t.Fatalf("RetrieveGraph max_hops=-1: %v", err)
	}
	if _, err := h.RetrieveGraph(ctx, &pb.RetrieveGraphRequest{
		Trust:   testTrustPB(),
		MaxHops: -2,
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("RetrieveGraph invalid max_hops code = %v, want InvalidArgument", status.Code(err))
	}

	if _, err := h.Penalize(ctx, &pb.PenalizeRequest{
		Id:     "record-id",
		Amount: -0.1,
		Actor:  "tester",
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Penalize negative amount code = %v, want InvalidArgument", status.Code(err))
	}
	if _, err := h.Penalize(ctx, &pb.PenalizeRequest{
		Id:        "record-id",
		Amount:    math.NaN(),
		Actor:     "tester",
		Rationale: "invalid",
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Penalize NaN amount code = %v, want InvalidArgument", status.Code(err))
	}
	if _, err := h.Penalize(ctx, &pb.PenalizeRequest{
		Id:        "record-id",
		Amount:    0.1,
		Actor:     strings.Repeat("x", maxStringLength+1),
		Rationale: "invalid",
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Penalize long actor code = %v, want InvalidArgument", status.Code(err))
	}
	if _, err := h.Reinforce(ctx, &pb.ReinforceRequest{
		Id:        "record-id",
		Actor:     "tester",
		Rationale: strings.Repeat("x", maxStringLength+1),
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Reinforce long rationale code = %v, want InvalidArgument", status.Code(err))
	}
	if _, err := h.Reinforce(ctx, &pb.ReinforceRequest{
		Id:        "record-id",
		Actor:     strings.Repeat("x", maxStringLength+1),
		Rationale: "invalid",
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Reinforce long actor code = %v, want InvalidArgument", status.Code(err))
	}
	if _, err := h.Penalize(ctx, &pb.PenalizeRequest{
		Id:        "record-id",
		Amount:    0.1,
		Actor:     "tester",
		Rationale: strings.Repeat("x", maxStringLength+1),
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Penalize long rationale code = %v, want InvalidArgument", status.Code(err))
	}
	if _, err := h.Contest(ctx, &pb.ContestRequest{
		Id:        "record-id",
		Actor:     strings.Repeat("x", maxStringLength+1),
		Rationale: "invalid",
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Contest long actor code = %v, want InvalidArgument", status.Code(err))
	}
	if _, err := h.Contest(ctx, &pb.ContestRequest{
		Id:        "record-id",
		Actor:     "tester",
		Rationale: strings.Repeat("x", maxStringLength+1),
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Contest long rationale code = %v, want InvalidArgument", status.Code(err))
	}

	if _, err := h.Merge(ctx, &pb.MergeRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Merge empty ids code = %v, want InvalidArgument", status.Code(err))
	}
	if _, err := h.Merge(ctx, &pb.MergeRequest{
		Ids: make([]string, maxLimit+1),
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Merge too many ids code = %v, want InvalidArgument", status.Code(err))
	}
	if _, err := h.Merge(ctx, &pb.MergeRequest{
		Ids: []string{"record-a"},
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Merge nil merged_record code = %v, want InvalidArgument", status.Code(err))
	}
	if _, err := h.Supersede(ctx, &pb.SupersedeRequest{OldId: "record-a"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Supersede nil new_record code = %v, want InvalidArgument", status.Code(err))
	}
	if _, err := h.Fork(ctx, &pb.ForkRequest{SourceId: "record-a"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Fork nil forked_record code = %v, want InvalidArgument", status.Code(err))
	}
}
