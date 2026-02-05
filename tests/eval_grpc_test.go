package tests_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	grpcapi "github.com/GustyCube/membrane/api/grpc"
	pb "github.com/GustyCube/membrane/api/grpc/gen/membranev1"
	"github.com/GustyCube/membrane/pkg/membrane"
	"github.com/GustyCube/membrane/pkg/schema"
)

type grpcEnv struct {
	client pb.MembraneServiceClient
	conn   *grpc.ClientConn
	server *grpcapi.Server
	mem    *membrane.Membrane
	apiKey string
}

func newGRPCEnv(t *testing.T, apiKey string, rateLimit int) *grpcEnv {
	t.Helper()

	dbPath := t.TempDir() + "/membrane.db"
	cfg := membrane.DefaultConfig()
	cfg.DBPath = dbPath
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.APIKey = apiKey
	cfg.RateLimitPerSecond = rateLimit

	m, err := membrane.New(cfg)
	if err != nil {
		t.Fatalf("membrane.New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := m.Start(ctx); err != nil {
		t.Fatalf("membrane.Start: %v", err)
	}

	srv, err := grpcapi.NewServer(m, cfg)
	if err != nil {
		cancel()
		_ = m.Stop()
		t.Fatalf("grpc.NewServer: %v", err)
	}

	go func() {
		_ = srv.Start()
	}()

	dialCtx, cancelDial := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelDial()

	conn, err := grpc.DialContext(dialCtx, srv.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		srv.Stop()
		cancel()
		_ = m.Stop()
		t.Fatalf("grpc.Dial: %v", err)
	}

	t.Cleanup(func() {
		_ = conn.Close()
		srv.Stop()
		cancel()
		_ = m.Stop()
	})

	return &grpcEnv{
		client: pb.NewMembraneServiceClient(conn),
		conn:   conn,
		server: srv,
		mem:    m,
		apiKey: apiKey,
	}
}

func (e *grpcEnv) ctx() context.Context {
	if e.apiKey == "" {
		return context.Background()
	}
	md := metadata.New(map[string]string{"authorization": "Bearer " + e.apiKey})
	return metadata.NewOutgoingContext(context.Background(), md)
}

func decodeRecord(t *testing.T, data []byte) *schema.MemoryRecord {
	t.Helper()
	var rec schema.MemoryRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		t.Fatalf("unmarshal record: %v", err)
	}
	return &rec
}

func TestEvalGRPCAuth(t *testing.T) {
	env := newGRPCEnv(t, "secret", 0)

	_, err := env.client.GetMetrics(context.Background(), &pb.GetMetricsRequest{})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected unauthenticated, got %v", err)
	}

	_, err = env.client.GetMetrics(env.ctx(), &pb.GetMetricsRequest{})
	if err != nil {
		t.Fatalf("expected metrics success with auth, got %v", err)
	}
}

func TestEvalGRPCRateLimit(t *testing.T) {
	env := newGRPCEnv(t, "", 1)
	ctx := env.ctx()

	if _, err := env.client.GetMetrics(ctx, &pb.GetMetricsRequest{}); err != nil {
		t.Fatalf("first GetMetrics failed: %v", err)
	}

	exhausted := false
	for i := 0; i < 3; i++ {
		_, err := env.client.GetMetrics(ctx, &pb.GetMetricsRequest{})
		if status.Code(err) == codes.ResourceExhausted {
			exhausted = true
			break
		}
	}
	if !exhausted {
		t.Fatalf("expected rate limit to trigger")
	}
}

func TestEvalGRPCSurface(t *testing.T) {
	env := newGRPCEnv(t, "", 0)
	ctx := env.ctx()

	eventResp, err := env.client.IngestEvent(ctx, &pb.IngestEventRequest{
		Source:      "eval",
		EventKind:   "build",
		Ref:         "evt-1",
		Summary:     "ran build",
		Tags:        []string{"eval"},
		Scope:       "project:alpha",
		Sensitivity: "low",
	})
	if err != nil {
		t.Fatalf("IngestEvent: %v", err)
	}
	eventRec := decodeRecord(t, eventResp.Record)

	argsJSON, _ := json.Marshal(map[string]any{"cmd": "go test ./..."})
	resultJSON, _ := json.Marshal(map[string]any{"status": "ok"})
	toolResp, err := env.client.IngestToolOutput(ctx, &pb.IngestToolOutputRequest{
		Source:      "eval",
		ToolName:    "bash",
		Args:        argsJSON,
		Result:      resultJSON,
		Tags:        []string{"eval"},
		Scope:       "project:alpha",
		Sensitivity: "low",
	})
	if err != nil {
		t.Fatalf("IngestToolOutput: %v", err)
	}
	toolRec := decodeRecord(t, toolResp.Record)

	obsObj, _ := json.Marshal("Go")
	obsResp, err := env.client.IngestObservation(ctx, &pb.IngestObservationRequest{
		Source:      "eval",
		Subject:     "user",
		Predicate:   "prefers",
		Object:      obsObj,
		Tags:        []string{"eval"},
		Scope:       "project:alpha",
		Sensitivity: "low",
	})
	if err != nil {
		t.Fatalf("IngestObservation: %v", err)
	}
	obsRec := decodeRecord(t, obsResp.Record)

	_, err = env.client.IngestOutcome(ctx, &pb.IngestOutcomeRequest{
		Source:         "eval",
		TargetRecordId: eventRec.ID,
		OutcomeStatus:  string(schema.OutcomeStatusSuccess),
	})
	if err != nil {
		t.Fatalf("IngestOutcome: %v", err)
	}

	constraintsJSON, _ := json.Marshal([]schema.Constraint{{Type: "eq", Key: "region", Value: "us", Required: true}})
	workingResp, err := env.client.IngestWorkingState(ctx, &pb.IngestWorkingStateRequest{
		Source:            "eval",
		ThreadId:          "thread-1",
		State:             string(schema.TaskStateExecuting),
		NextActions:       []string{"run tests"},
		ContextSummary:    "testing",
		ActiveConstraints: constraintsJSON,
		Tags:              []string{"eval"},
		Scope:             "project:alpha",
		Sensitivity:       "low",
	})
	if err != nil {
		t.Fatalf("IngestWorkingState: %v", err)
	}
	workingRec := decodeRecord(t, workingResp.Record)

	trust := &pb.TrustContext{
		MaxSensitivity: "high",
		Authenticated:  true,
		ActorId:        "eval",
		Scopes:         []string{"project:alpha"},
	}

	retrieveResp, err := env.client.Retrieve(ctx, &pb.RetrieveRequest{
		Trust:       trust,
		MemoryTypes: []string{string(schema.MemoryTypeSemantic)},
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("Retrieve: %v", err)
	}
	if len(retrieveResp.Records) == 0 {
		t.Fatalf("expected retrieve records")
	}

	_, err = env.client.RetrieveByID(ctx, &pb.RetrieveByIDRequest{Id: obsRec.ID, Trust: trust})
	if err != nil {
		t.Fatalf("RetrieveByID: %v", err)
	}

	if _, err := env.client.Reinforce(ctx, &pb.ReinforceRequest{Id: eventRec.ID, Actor: "eval", Rationale: "useful"}); err != nil {
		t.Fatalf("Reinforce: %v", err)
	}
	if _, err := env.client.Penalize(ctx, &pb.PenalizeRequest{Id: toolRec.ID, Amount: 0.2, Actor: "eval", Rationale: "unused"}); err != nil {
		t.Fatalf("Penalize: %v", err)
	}

	newSemantic := schema.NewMemoryRecord("", schema.MemoryTypeSemantic, schema.SensitivityLow,
		&schema.SemanticPayload{
			Kind:      "semantic",
			Subject:   "user",
			Predicate: "prefers",
			Object:    "Rust",
			Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
			Evidence:  []schema.ProvenanceRef{{SourceType: "eval", SourceID: "grpc"}},
		},
	)
	newBytes, _ := json.Marshal(newSemantic)
	supResp, err := env.client.Supersede(ctx, &pb.SupersedeRequest{
		OldId:     obsRec.ID,
		NewRecord: newBytes,
		Actor:     "eval",
		Rationale: "update",
	})
	if err != nil {
		t.Fatalf("Supersede: %v", err)
	}
	_ = decodeRecord(t, supResp.Record)

	forkSource, err := env.client.IngestObservation(ctx, &pb.IngestObservationRequest{
		Source:      "eval",
		Subject:     "service",
		Predicate:   "uses_cache",
		Object:      obsObj,
		Scope:       "project:alpha",
		Sensitivity: "low",
	})
	if err != nil {
		t.Fatalf("IngestObservation fork source: %v", err)
	}
	forkSourceRec := decodeRecord(t, forkSource.Record)

	forked := schema.NewMemoryRecord("", schema.MemoryTypeSemantic, schema.SensitivityLow,
		&schema.SemanticPayload{
			Kind:           "semantic",
			Subject:        "service",
			Predicate:      "uses_cache",
			Object:         "Memcached",
			Validity:       schema.Validity{Mode: schema.ValidityModeConditional, Conditions: map[string]any{"env": "dev"}},
			Evidence:       []schema.ProvenanceRef{{SourceType: "eval", SourceID: "grpc"}},
			RevisionPolicy: "fork",
		},
	)
	forkBytes, _ := json.Marshal(forked)
	forkResp, err := env.client.Fork(ctx, &pb.ForkRequest{
		SourceId:     forkSourceRec.ID,
		ForkedRecord: forkBytes,
		Actor:        "eval",
		Rationale:    "dev env",
	})
	if err != nil {
		t.Fatalf("Fork: %v", err)
	}
	forkRec := decodeRecord(t, forkResp.Record)

	mergeLeft, err := env.client.IngestObservation(ctx, &pb.IngestObservationRequest{
		Source:      "eval",
		Subject:     "db",
		Predicate:   "uses",
		Object:      obsObj,
		Scope:       "project:alpha",
		Sensitivity: "low",
	})
	if err != nil {
		t.Fatalf("IngestObservation merge left: %v", err)
	}
	mergeRight, err := env.client.IngestObservation(ctx, &pb.IngestObservationRequest{
		Source:      "eval",
		Subject:     "db",
		Predicate:   "uses",
		Object:      obsObj,
		Scope:       "project:alpha",
		Sensitivity: "low",
	})
	if err != nil {
		t.Fatalf("IngestObservation merge right: %v", err)
	}
	mergeLeftRec := decodeRecord(t, mergeLeft.Record)
	mergeRightRec := decodeRecord(t, mergeRight.Record)

	merged := schema.NewMemoryRecord("", schema.MemoryTypeSemantic, schema.SensitivityLow,
		&schema.SemanticPayload{
			Kind:      "semantic",
			Subject:   "db",
			Predicate: "uses",
			Object:    "Postgres",
			Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
			Evidence:  []schema.ProvenanceRef{{SourceType: "eval", SourceID: "grpc"}},
		},
	)
	mergedBytes, _ := json.Marshal(merged)
	mergeResp, err := env.client.Merge(ctx, &pb.MergeRequest{
		Ids:          []string{mergeLeftRec.ID, mergeRightRec.ID},
		MergedRecord: mergedBytes,
		Actor:        "eval",
		Rationale:    "merge",
	})
	if err != nil {
		t.Fatalf("Merge: %v", err)
	}
	_ = decodeRecord(t, mergeResp.Record)

	if _, err := env.client.Contest(ctx, &pb.ContestRequest{Id: forkSourceRec.ID, ContestingRef: forkRec.ID, Actor: "eval", Rationale: "conflict"}); err != nil {
		t.Fatalf("Contest: %v", err)
	}

	if _, err := env.client.Retract(ctx, &pb.RetractRequest{Id: mergeLeftRec.ID, Actor: "eval", Rationale: "obsolete"}); err != nil {
		t.Fatalf("Retract: %v", err)
	}

	if _, err := env.client.GetMetrics(ctx, &pb.GetMetricsRequest{}); err != nil {
		t.Fatalf("GetMetrics: %v", err)
	}

	// Validate record JSON round-trip for working memory.
	_, err = env.client.RetrieveByID(ctx, &pb.RetrieveByIDRequest{Id: workingRec.ID, Trust: trust})
	if err != nil {
		t.Fatalf("RetrieveByID working: %v", err)
	}
}

func TestEvalGRPCValidation(t *testing.T) {
	env := newGRPCEnv(t, "", 0)
	ctx := env.ctx()

	trust := &pb.TrustContext{MaxSensitivity: "low", Authenticated: true}
	_, err := env.client.Retrieve(ctx, &pb.RetrieveRequest{Trust: trust, Limit: 20000})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected invalid argument for limit, got %v", err)
	}

	_, err = env.client.Retrieve(ctx, &pb.RetrieveRequest{Limit: 1})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected invalid argument for missing trust, got %v", err)
	}

	_, err = env.client.IngestToolOutput(ctx, &pb.IngestToolOutputRequest{Source: "eval", ToolName: "bash", Args: []byte("{"), Result: []byte("{}")})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected invalid argument for bad args JSON, got %v", err)
	}

	longTag := strings.Repeat("a", 300)
	_, err = env.client.IngestEvent(ctx, &pb.IngestEventRequest{Source: "eval", EventKind: "evt", Ref: "r1", Tags: []string{longTag}})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected invalid argument for long tag, got %v", err)
	}
}
