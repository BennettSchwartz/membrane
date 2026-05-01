package tests_test

import (
	"context"
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"

	grpcapi "github.com/BennettSchwartz/membrane/api/grpc"
	pb "github.com/BennettSchwartz/membrane/api/grpc/gen/membranev1"
	"github.com/BennettSchwartz/membrane/pkg/membrane"
	"github.com/BennettSchwartz/membrane/pkg/retrieval"
	"github.com/BennettSchwartz/membrane/pkg/schema"
)

type grpcEnv struct {
	client       pb.MembraneServiceClient
	healthClient healthpb.HealthClient
	conn         *grpc.ClientConn
	server       *grpcapi.Server
	mem          *membrane.Membrane
	apiKey       string
}

func newGRPCEnv(t *testing.T, apiKey string, rateLimit int) *grpcEnv {
	t.Helper()

	cfg := membrane.DefaultConfig()
	cfg.DBPath = t.TempDir() + "/membrane.db"
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

	conn, err := grpc.NewClient(srv.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		srv.Stop()
		cancel()
		_ = m.Stop()
		t.Fatalf("grpc.NewClient: %v", err)
	}
	conn.Connect()
	for state := conn.GetState(); state != connectivity.Ready; state = conn.GetState() {
		if !conn.WaitForStateChange(dialCtx, state) {
			_ = conn.Close()
			srv.Stop()
			cancel()
			_ = m.Stop()
			t.Fatalf("grpc connection not ready (state=%v): %v", state, dialCtx.Err())
		}
	}

	t.Cleanup(func() {
		_ = conn.Close()
		srv.Stop()
		cancel()
		_ = m.Stop()
	})

	return &grpcEnv{
		client:       pb.NewMembraneServiceClient(conn),
		healthClient: healthpb.NewHealthClient(conn),
		conn:         conn,
		server:       srv,
		mem:          m,
		apiKey:       apiKey,
	}
}

func (e *grpcEnv) ctx() context.Context {
	if e.apiKey == "" {
		return context.Background()
	}
	md := metadata.New(map[string]string{"authorization": "Bearer " + e.apiKey})
	return metadata.NewOutgoingContext(context.Background(), md)
}

func decodeRecord(t *testing.T, rec *pb.MemoryRecord) *schema.MemoryRecord {
	t.Helper()
	if rec == nil {
		t.Fatalf("record is nil")
	}
	return &schema.MemoryRecord{
		ID:          rec.Id,
		Type:        schema.MemoryType(rec.Type),
		Sensitivity: schema.Sensitivity(rec.Sensitivity),
		Confidence:  rec.Confidence,
		Salience:    rec.Salience,
		Scope:       rec.Scope,
		Tags:        rec.Tags,
	}
}

func mustMarshalJSON(t *testing.T, value any) *structpb.Value {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal json: %v", err)
	}
	var normalized any
	if err := json.Unmarshal(data, &normalized); err != nil {
		t.Fatalf("unmarshal json: %v", err)
	}
	out, err := structpb.NewValue(normalized)
	if err != nil {
		t.Fatalf("struct value: %v", err)
	}
	return out
}

func requireCreatedRecordOfType(t *testing.T, resp *pb.CaptureMemoryResponse, recordType schema.MemoryType) *schema.MemoryRecord {
	t.Helper()
	for _, raw := range resp.CreatedRecords {
		rec := decodeRecord(t, raw)
		if rec.Type == recordType {
			return rec
		}
	}
	t.Fatalf("capture response missing created record of type %q", recordType)
	return nil
}

func semanticRecordPB(t *testing.T, subject, predicate string, object any, validity *pb.Validity, revisionPolicy string) *pb.MemoryRecord {
	t.Helper()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if validity == nil {
		validity = &pb.Validity{Mode: string(schema.ValidityModeGlobal)}
	}
	return &pb.MemoryRecord{
		Type:        string(schema.MemoryTypeSemantic),
		Sensitivity: string(schema.SensitivityLow),
		Confidence:  1,
		Salience:    1,
		CreatedAt:   now,
		UpdatedAt:   now,
		Lifecycle: &pb.Lifecycle{
			Decay: &pb.DecayProfile{
				Curve:           string(schema.DecayCurveExponential),
				HalfLifeSeconds: 86400,
			},
			LastReinforcedAt: now,
			DeletionPolicy:   string(schema.DeletionPolicyAutoPrune),
		},
		Provenance: &pb.Provenance{Sources: []*pb.ProvenanceSource{}},
		Payload: &pb.Payload{Kind: &pb.Payload_Semantic{Semantic: &pb.SemanticPayload{
			Kind:           "semantic",
			Subject:        subject,
			Predicate:      predicate,
			Object:         mustMarshalJSON(t, object),
			Validity:       validity,
			Evidence:       []*pb.ProvenanceRef{{SourceType: "eval", SourceId: "grpc", Timestamp: now}},
			RevisionPolicy: revisionPolicy,
		}}},
		AuditLog: []*pb.AuditEntry{{
			Action:    string(schema.AuditActionCreate),
			Actor:     "eval",
			Timestamp: now,
			Rationale: "test record",
		}},
	}
}

func requireGRPCError(t *testing.T, err error, code codes.Code, messageContains string) {
	t.Helper()
	if status.Code(err) != code {
		t.Fatalf("expected grpc code %s, got %v", code, err)
	}
	if messageContains != "" && !strings.Contains(status.Convert(err).Message(), messageContains) {
		t.Fatalf("expected grpc message containing %q, got %q", messageContains, status.Convert(err).Message())
	}
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

func TestEvalGRPCRateLimitPerClient(t *testing.T) {
	env := newGRPCEnv(t, "", 1)

	secondConn, err := grpc.NewClient(env.server.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient second client: %v", err)
	}
	secondConn.Connect()
	t.Cleanup(func() {
		_ = secondConn.Close()
	})

	secondClient := pb.NewMembraneServiceClient(secondConn)
	ctx := context.Background()

	if _, err := env.client.GetMetrics(ctx, &pb.GetMetricsRequest{}); err != nil {
		t.Fatalf("first client initial GetMetrics failed: %v", err)
	}
	if _, err := env.client.GetMetrics(ctx, &pb.GetMetricsRequest{}); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("expected first client to hit rate limit, got %v", err)
	}

	if _, err := secondClient.GetMetrics(ctx, &pb.GetMetricsRequest{}); err != nil {
		t.Fatalf("second client should have independent quota, got %v", err)
	}
}

func TestEvalGRPCHealth(t *testing.T) {
	env := newGRPCEnv(t, "", 0)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	check := func(service string) {
		resp, err := env.healthClient.Check(ctx, &healthpb.HealthCheckRequest{Service: service})
		if err != nil {
			t.Fatalf("Health Check(%q): %v", service, err)
		}
		if resp.Status != healthpb.HealthCheckResponse_SERVING {
			t.Fatalf("expected %q to be SERVING, got %v", service, resp.Status)
		}
	}

	check("")
	check(pb.MembraneService_ServiceDesc.ServiceName)

	watchCtx, watchCancel := context.WithCancel(context.Background())
	defer watchCancel()

	stream, err := env.healthClient.Watch(watchCtx, &healthpb.HealthCheckRequest{
		Service: pb.MembraneService_ServiceDesc.ServiceName,
	})
	if err != nil {
		t.Fatalf("Health Watch: %v", err)
	}

	initial, err := stream.Recv()
	if err != nil {
		t.Fatalf("Health Watch initial recv: %v", err)
	}
	if initial.Status != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("expected initial health status SERVING, got %v", initial.Status)
	}

	stopped := make(chan struct{})
	go func() {
		env.server.Stop()
		close(stopped)
	}()

	update, err := stream.Recv()
	if err != nil {
		t.Fatalf("Health Watch shutdown recv: %v", err)
	}
	if update.Status != healthpb.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("expected shutdown health status NOT_SERVING, got %v", update.Status)
	}
	watchCancel()

	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatalf("server.Stop did not complete after health shutdown")
	}
}

func TestEvalGRPCSurface(t *testing.T) {
	env := newGRPCEnv(t, "", 0)
	ctx := env.ctx()

	eventResp, err := env.client.CaptureMemory(ctx, &pb.CaptureMemoryRequest{
		Source:           "eval",
		SourceKind:       "event",
		Content:          mustMarshalJSON(t, map[string]any{"ref": "evt-1", "text": "ran build"}),
		ReasonToRemember: "Keep build execution history",
		Summary:          "ran build",
		Tags:             []string{"eval"},
		Scope:            "project:alpha",
		Sensitivity:      "low",
	})
	if err != nil {
		t.Fatalf("CaptureMemory event: %v", err)
	}
	eventRec := decodeRecord(t, eventResp.PrimaryRecord)

	toolResp, err := env.client.CaptureMemory(ctx, &pb.CaptureMemoryRequest{
		Source:           "eval",
		SourceKind:       "tool_output",
		Content:          mustMarshalJSON(t, map[string]any{"tool_name": "bash", "args": map[string]any{"cmd": "go test ./..."}, "result": map[string]any{"status": "ok"}}),
		ReasonToRemember: "Keep tool execution output",
		Tags:             []string{"eval"},
		Scope:            "project:alpha",
		Sensitivity:      "low",
	})
	if err != nil {
		t.Fatalf("CaptureMemory tool_output: %v", err)
	}
	toolRec := decodeRecord(t, toolResp.PrimaryRecord)

	obsResp, err := env.client.CaptureMemory(ctx, &pb.CaptureMemoryRequest{
		Source:           "eval",
		SourceKind:       "observation",
		Content:          mustMarshalJSON(t, map[string]any{"subject": "user", "predicate": "prefers", "object": "Go"}),
		ReasonToRemember: "User language preference should be queryable",
		Summary:          "user prefers Go",
		Tags:             []string{"eval"},
		Scope:            "project:alpha",
		Sensitivity:      "low",
	})
	if err != nil {
		t.Fatalf("CaptureMemory observation: %v", err)
	}
	obsRec := requireCreatedRecordOfType(t, obsResp, schema.MemoryTypeSemantic)

	workingResp, err := env.client.CaptureMemory(ctx, &pb.CaptureMemoryRequest{
		Source:           "eval",
		SourceKind:       "working_state",
		Content:          mustMarshalJSON(t, map[string]any{"thread_id": "thread-1", "state": string(schema.TaskStateExecuting), "next_actions": []string{"run tests"}, "context_summary": "testing", "active_constraints": []schema.Constraint{{Type: "eq", Key: "region", Value: "us", Required: true}}}),
		ReasonToRemember: "Preserve active task state",
		Summary:          "thread is executing",
		Tags:             []string{"eval"},
		Scope:            "project:alpha",
		Sensitivity:      "low",
	})
	if err != nil {
		t.Fatalf("CaptureMemory working_state: %v", err)
	}
	workingRec := decodeRecord(t, workingResp.PrimaryRecord)

	trust := &pb.TrustContext{
		MaxSensitivity: "high",
		Authenticated:  true,
		ActorId:        "eval",
		Scopes:         []string{"project:alpha"},
	}

	retrieveResp, err := env.client.RetrieveGraph(ctx, &pb.RetrieveGraphRequest{
		TaskDescriptor: "user prefers Go",
		Trust:          trust,
		MemoryTypes:    []string{string(schema.MemoryTypeSemantic)},
		RootLimit:      10,
		NodeLimit:      20,
		EdgeLimit:      20,
		MaxHops:        1,
	})
	if err != nil {
		t.Fatalf("RetrieveGraph: %v", err)
	}
	if len(retrieveResp.RootIds) == 0 {
		t.Fatalf("expected retrieve graph roots")
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

	supResp, err := env.client.Supersede(ctx, &pb.SupersedeRequest{
		OldId:     obsRec.ID,
		NewRecord: semanticRecordPB(t, "user", "prefers", "Rust", nil, ""),
		Actor:     "eval",
		Rationale: "update",
	})
	if err != nil {
		t.Fatalf("Supersede: %v", err)
	}
	_ = decodeRecord(t, supResp.Record)

	forkSource, err := env.client.CaptureMemory(ctx, &pb.CaptureMemoryRequest{
		Source:           "eval",
		SourceKind:       "observation",
		Content:          mustMarshalJSON(t, map[string]any{"subject": "service", "predicate": "uses_cache", "object": "Go"}),
		ReasonToRemember: "capture fork source",
		Scope:            "project:alpha",
		Sensitivity:      "low",
	})
	if err != nil {
		t.Fatalf("CaptureMemory fork source: %v", err)
	}
	forkSourceRec := requireCreatedRecordOfType(t, forkSource, schema.MemoryTypeSemantic)

	forkResp, err := env.client.Fork(ctx, &pb.ForkRequest{
		SourceId: forkSourceRec.ID,
		ForkedRecord: semanticRecordPB(t, "service", "uses_cache", "Memcached", &pb.Validity{
			Mode:       string(schema.ValidityModeConditional),
			Conditions: map[string]*structpb.Value{"env": mustMarshalJSON(t, "dev")},
		}, "fork"),
		Actor:     "eval",
		Rationale: "dev env",
	})
	if err != nil {
		t.Fatalf("Fork: %v", err)
	}
	forkRec := decodeRecord(t, forkResp.Record)

	mergeLeft, err := env.client.CaptureMemory(ctx, &pb.CaptureMemoryRequest{
		Source:           "eval",
		SourceKind:       "observation",
		Content:          mustMarshalJSON(t, map[string]any{"subject": "db", "predicate": "uses", "object": "Go"}),
		ReasonToRemember: "left merge source",
		Scope:            "project:alpha",
		Sensitivity:      "low",
	})
	if err != nil {
		t.Fatalf("CaptureMemory merge left: %v", err)
	}
	mergeRight, err := env.client.CaptureMemory(ctx, &pb.CaptureMemoryRequest{
		Source:           "eval",
		SourceKind:       "observation",
		Content:          mustMarshalJSON(t, map[string]any{"subject": "db", "predicate": "uses", "object": "Go"}),
		ReasonToRemember: "right merge source",
		Scope:            "project:alpha",
		Sensitivity:      "low",
	})
	if err != nil {
		t.Fatalf("CaptureMemory merge right: %v", err)
	}
	mergeLeftRec := requireCreatedRecordOfType(t, mergeLeft, schema.MemoryTypeSemantic)
	mergeRightRec := requireCreatedRecordOfType(t, mergeRight, schema.MemoryTypeSemantic)

	mergeResp, err := env.client.Merge(ctx, &pb.MergeRequest{
		Ids:          []string{mergeLeftRec.ID, mergeRightRec.ID},
		MergedRecord: semanticRecordPB(t, "db", "uses", "Postgres", nil, ""),
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

func TestEvalGRPCCaptureMemoryAndRetrieveGraph(t *testing.T) {
	env := newGRPCEnv(t, "", 0)
	ctx := env.ctx()

	content := mustMarshalJSON(t, map[string]any{
		"ref":     "evt-capture-1",
		"text":    "Remember Orchid as the staging deploy target",
		"project": "Orchid",
	})
	contextValue := mustMarshalJSON(t, map[string]any{"thread_id": "thread-1"})

	captureResp, err := env.client.CaptureMemory(ctx, &pb.CaptureMemoryRequest{
		Source:           "grpc-test",
		SourceKind:       "event",
		Content:          content,
		Context:          contextValue,
		ReasonToRemember: "Deployment jargon should be recoverable",
		Summary:          "Remember Orchid",
		Tags:             []string{"grpc", "orchid"},
		Scope:            "project:alpha",
		Sensitivity:      "low",
		Timestamp:        time.Now().UTC().Format(time.RFC3339),
	})
	if err != nil {
		t.Fatalf("CaptureMemory: %v", err)
	}

	primary := decodeRecord(t, captureResp.PrimaryRecord)
	if primary.Type != schema.MemoryTypeEpisodic {
		t.Fatalf("PrimaryRecord.Type = %q, want episodic", primary.Type)
	}
	if len(captureResp.CreatedRecords) != 1 {
		t.Fatalf("CreatedRecords len = %d, want 1", len(captureResp.CreatedRecords))
	}
	entity := decodeRecord(t, captureResp.CreatedRecords[0])
	if entity.Type != schema.MemoryTypeEntity {
		t.Fatalf("Created entity type = %q, want entity", entity.Type)
	}
	if len(captureResp.Edges) != 2 {
		t.Fatalf("capture edges len = %d, want 2", len(captureResp.Edges))
	}

	graphResp, err := env.client.RetrieveGraph(ctx, &pb.RetrieveGraphRequest{
		TaskDescriptor: "Orchid deploy target",
		Trust: &pb.TrustContext{
			MaxSensitivity: "low",
			Authenticated:  true,
			ActorId:        "grpc-test",
			Scopes:         []string{"project:alpha"},
		},
		RootLimit: 10,
		NodeLimit: 20,
		EdgeLimit: 20,
		MaxHops:   1,
	})
	if err != nil {
		t.Fatalf("RetrieveGraph: %v", err)
	}

	if len(graphResp.RootIds) == 0 {
		t.Fatalf("RootIds = empty, want at least one root")
	}
	foundPrimary := false
	foundEntity := false
	for _, node := range graphResp.Nodes {
		if node.Record != nil && node.Record.Id == primary.ID {
			foundPrimary = true
		}
		if node.Record != nil && node.Record.Id == entity.ID {
			foundEntity = true
		}
	}
	if !foundPrimary || !foundEntity {
		t.Fatalf("graph nodes missing primary/entity: foundPrimary=%v foundEntity=%v nodes=%+v", foundPrimary, foundEntity, graphResp.Nodes)
	}
	hasMentionEdge := false
	for _, edge := range graphResp.Edges {
		if edge.Predicate == "mentions_entity" {
			hasMentionEdge = true
			break
		}
	}
	if !hasMentionEdge {
		t.Fatalf("expected mentions_entity edge, got %+v", graphResp.Edges)
	}
}

func TestEvalGRPCRetrieveGraphReturnsRedactedRecord(t *testing.T) {
	env := newGRPCEnv(t, "", 0)
	ctx := env.ctx()

	captureResp, err := env.client.CaptureMemory(ctx, &pb.CaptureMemoryRequest{
		Source:      "grpc-test",
		SourceKind:  "event",
		Content:     mustMarshalJSON(t, map[string]any{"ref": "evt-redacted-1", "text": "Confidential deployment note"}),
		Summary:     "Confidential deployment note",
		Sensitivity: "medium",
	})
	if err != nil {
		t.Fatalf("CaptureMemory: %v", err)
	}
	primary := decodeRecord(t, captureResp.PrimaryRecord)

	graphResp, err := env.client.RetrieveGraph(ctx, &pb.RetrieveGraphRequest{
		TaskDescriptor: "deployment note",
		Trust: &pb.TrustContext{
			MaxSensitivity: "low",
			Authenticated:  true,
			ActorId:        "grpc-test",
		},
		MemoryTypes: []string{"episodic"},
		RootLimit:   5,
		NodeLimit:   5,
		MaxHops:     0,
	})
	if err != nil {
		t.Fatalf("RetrieveGraph redacted: %v", err)
	}

	for _, node := range graphResp.Nodes {
		if node.Record == nil || node.Record.Id != primary.ID {
			continue
		}
		if node.Record.Payload != nil {
			t.Fatalf("redacted payload = %+v, want nil", node.Record.Payload)
		}
		if node.Record.Sensitivity != "medium" {
			t.Fatalf("redacted sensitivity = %q, want medium", node.Record.Sensitivity)
		}
		return
	}
	t.Fatalf("graph nodes missing redacted record %s: %+v", primary.ID, graphResp.Nodes)
}

func TestEvalGRPCCaptureMemoryLinksDerivedSemanticThroughEntity(t *testing.T) {
	env := newGRPCEnv(t, "", 0)
	ctx := env.ctx()

	captureResp, err := env.client.CaptureMemory(ctx, &pb.CaptureMemoryRequest{
		Source:      "grpc-test",
		SourceKind:  "observation",
		Content:     mustMarshalJSON(t, map[string]any{"subject": "Orchid", "predicate": "deploy_target_for", "object": "staging"}),
		Scope:       "project:alpha",
		Sensitivity: "low",
		Tags:        []string{"deploy"},
	})
	if err != nil {
		t.Fatalf("CaptureMemory: %v", err)
	}

	entity := requireCreatedRecordOfType(t, captureResp, schema.MemoryTypeEntity)
	semantic := requireCreatedRecordOfType(t, captureResp, schema.MemoryTypeSemantic)

	foundSemanticEntityLink := false
	for _, edge := range captureResp.Edges {
		if edge.SourceId == semantic.ID && edge.Predicate == "subject_entity" && edge.TargetId == entity.ID {
			foundSemanticEntityLink = true
			break
		}
	}
	if !foundSemanticEntityLink {
		t.Fatalf("capture edges = %+v, want semantic -> entity link", captureResp.Edges)
	}

	graphResp, err := env.client.RetrieveGraph(ctx, &pb.RetrieveGraphRequest{
		TaskDescriptor: "Orchid deploy target",
		Trust: &pb.TrustContext{
			MaxSensitivity: "low",
			Authenticated:  true,
			ActorId:        "grpc-test",
			Scopes:         []string{"project:alpha"},
		},
		MemoryTypes: []string{"semantic"},
		RootLimit:   1,
		NodeLimit:   3,
		EdgeLimit:   6,
		MaxHops:     1,
	})
	if err != nil {
		t.Fatalf("RetrieveGraph: %v", err)
	}

	if len(graphResp.RootIds) != 1 || graphResp.RootIds[0] != semantic.ID {
		t.Fatalf("RootIds = %v, want [%s]", graphResp.RootIds, semantic.ID)
	}
	foundEntityNeighbor := false
	for _, node := range graphResp.Nodes {
		if node.Record != nil && node.Record.Id == entity.ID && node.Hop == 1 {
			foundEntityNeighbor = true
			break
		}
	}
	if !foundEntityNeighbor {
		t.Fatalf("graph nodes = %+v, want entity neighbor", graphResp.Nodes)
	}
}

func TestEvalGRPCValidation(t *testing.T) {
	env := newGRPCEnv(t, "", 0)
	ctx := env.ctx()

	trust := &pb.TrustContext{MaxSensitivity: "low", Authenticated: true}
	_, err := env.client.RetrieveGraph(ctx, &pb.RetrieveGraphRequest{Trust: trust, RootLimit: 20000})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected invalid argument for root_limit, got %v", err)
	}

	_, err = env.client.RetrieveGraph(ctx, &pb.RetrieveGraphRequest{RootLimit: 1})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected invalid argument for missing trust, got %v", err)
	}

	longTag := strings.Repeat("a", 300)
	_, err = env.client.CaptureMemory(ctx, &pb.CaptureMemoryRequest{
		Source:      "eval",
		SourceKind:  "event",
		Content:     mustMarshalJSON(t, map[string]any{"ref": "r1"}),
		Tags:        []string{longTag},
		Sensitivity: "low",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected invalid argument for long tag, got %v", err)
	}

	_, err = env.client.CaptureMemory(ctx, &pb.CaptureMemoryRequest{
		Source:      "eval",
		SourceKind:  "event",
		Content:     mustMarshalJSON(t, map[string]any{"ref": "r2"}),
		Sensitivity: "anything",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected invalid argument for invalid sensitivity, got %v", err)
	}

	_, err = env.client.RetrieveGraph(ctx, &pb.RetrieveGraphRequest{
		Trust: &pb.TrustContext{
			MaxSensitivity: "anything",
			Authenticated:  true,
		},
		RootLimit: 1,
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected invalid argument for invalid trust sensitivity, got %v", err)
	}

	_, err = env.client.RetrieveByID(ctx, &pb.RetrieveByIDRequest{
		Id: "does-not-matter",
		Trust: &pb.TrustContext{
			MaxSensitivity: "anything",
			Authenticated:  true,
		},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected invalid argument for invalid RetrieveByID trust sensitivity, got %v", err)
	}

	_, err = env.client.CaptureMemory(ctx, &pb.CaptureMemoryRequest{
		Source:       "eval",
		SourceKind:   "event",
		Content:      mustMarshalJSON(t, map[string]any{"ref": "rec-1"}),
		ProposedType: "anything",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected invalid argument for invalid proposed_type, got %v", err)
	}

	_, err = env.client.RetrieveGraph(ctx, &pb.RetrieveGraphRequest{
		Trust:       trust,
		MemoryTypes: []string{"anything"},
		RootLimit:   1,
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected invalid argument for invalid memory_types, got %v", err)
	}

	semanticResp, err := env.client.CaptureMemory(ctx, &pb.CaptureMemoryRequest{
		Source:      "eval",
		SourceKind:  "observation",
		Content:     mustMarshalJSON(t, map[string]any{"subject": "service", "predicate": "mode", "object": "active"}),
		Sensitivity: "low",
	})
	if err != nil {
		t.Fatalf("CaptureMemory: %v", err)
	}
	semanticRec := requireCreatedRecordOfType(t, semanticResp, schema.MemoryTypeSemantic)

	_, err = env.client.RetrieveByID(ctx, &pb.RetrieveByIDRequest{
		Id: "missing-record",
		Trust: &pb.TrustContext{
			MaxSensitivity: "low",
			Authenticated:  true,
		},
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("expected not found for missing record, got %v", err)
	}

	_, err = env.client.Supersede(ctx, &pb.SupersedeRequest{
		OldId:     semanticRec.ID,
		NewRecord: &pb.MemoryRecord{Id: "replacement-1", Type: string(schema.MemoryTypeSemantic), Sensitivity: string(schema.SensitivityLow), Confidence: 1, Salience: 1},
		Actor:     "eval",
		Rationale: "validate payload mapping",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected invalid argument for missing typed payload, got %v", err)
	}
}

func TestEvalGRPCNegativeContract(t *testing.T) {
	env := newGRPCEnv(t, "", 0)
	ctx := env.ctx()
	trust := &pb.TrustContext{MaxSensitivity: "low", Authenticated: true}
	oversized := structpb.NewStringValue(strings.Repeat("a", 10*1024*1024+1))

	tests := []struct {
		name            string
		call            func() error
		code            codes.Code
		messageContains string
	}{
		{
			name: "invalid-sensitivity-enum",
			call: func() error {
				_, err := env.client.CaptureMemory(ctx, &pb.CaptureMemoryRequest{
					Source:      "eval",
					SourceKind:  "event",
					Content:     mustMarshalJSON(t, map[string]any{"ref": "bad-sensitivity"}),
					Sensitivity: "anything",
				})
				return err
			},
			code:            codes.InvalidArgument,
			messageContains: "sensitivity must be one of: public, low, medium, high, hyper",
		},
		{
			name: "invalid-proposed-type",
			call: func() error {
				_, err := env.client.CaptureMemory(ctx, &pb.CaptureMemoryRequest{
					Source:       "eval",
					SourceKind:   "event",
					Content:      mustMarshalJSON(t, map[string]any{"ref": "rec-1"}),
					ProposedType: "anything",
				})
				return err
			},
			code:            codes.InvalidArgument,
			messageContains: "proposed_type must be one of: episodic, working, semantic, competence, plan_graph, entity",
		},
		{
			name: "invalid-memory-type-enum",
			call: func() error {
				_, err := env.client.RetrieveGraph(ctx, &pb.RetrieveGraphRequest{Trust: trust, MemoryTypes: []string{"anything"}, RootLimit: 1})
				return err
			},
			code:            codes.InvalidArgument,
			messageContains: "memory_types[0] must be one of: episodic, working, semantic, competence, plan_graph, entity",
		},
		{
			name: "oversized-content-payload",
			call: func() error {
				_, err := env.client.CaptureMemory(ctx, &pb.CaptureMemoryRequest{Source: "eval", SourceKind: "event", Content: oversized})
				return err
			},
			code:            codes.InvalidArgument,
			messageContains: "content exceeds maximum payload size of 10485760 bytes",
		},
		{
			name: "retrieve-root-limit-too-large",
			call: func() error {
				_, err := env.client.RetrieveGraph(ctx, &pb.RetrieveGraphRequest{Trust: trust, RootLimit: 20000})
				return err
			},
			code:            codes.InvalidArgument,
			messageContains: "root_limit must be between 0 and 10000",
		},
		{
			name: "penalize-negative-amount",
			call: func() error {
				_, err := env.client.Penalize(ctx, &pb.PenalizeRequest{
					Id:        "rec-1",
					Amount:    -0.1,
					Actor:     "eval",
					Rationale: "bad",
				})
				return err
			},
			code:            codes.InvalidArgument,
			messageContains: "amount must be non-negative and finite",
		},
		{
			name: "penalize-nan-amount",
			call: func() error {
				_, err := env.client.Penalize(ctx, &pb.PenalizeRequest{
					Id:        "rec-1",
					Amount:    math.NaN(),
					Actor:     "eval",
					Rationale: "bad",
				})
				return err
			},
			code:            codes.InvalidArgument,
			messageContains: "amount must be non-negative and finite",
		},
		{
			name: "invalid-capture-timestamp",
			call: func() error {
				_, err := env.client.CaptureMemory(ctx, &pb.CaptureMemoryRequest{
					Source:     "eval",
					SourceKind: "event",
					Content:    mustMarshalJSON(t, map[string]any{"ref": "bad-time"}),
					Timestamp:  "not-a-time",
				})
				return err
			},
			code:            codes.InvalidArgument,
			messageContains: "invalid timestamp:",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			requireGRPCError(t, tc.call(), tc.code, tc.messageContains)
		})
	}
}
