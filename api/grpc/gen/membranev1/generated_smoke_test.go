package membranev1

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/known/structpb"
)

type fakeClientConn struct {
	methods []string
	err     error
}

func (f *fakeClientConn) Invoke(_ context.Context, method string, _ any, _ any, _ ...grpc.CallOption) error {
	f.methods = append(f.methods, method)
	return f.err
}

func (f *fakeClientConn) NewStream(context.Context, *grpc.StreamDesc, string, ...grpc.CallOption) (grpc.ClientStream, error) {
	return nil, errors.New("streaming is not used by this service")
}

type recordingGeneratedServer struct {
	UnimplementedMembraneServiceServer
	methods []string
}

func (s *recordingGeneratedServer) CaptureMemory(context.Context, *CaptureMemoryRequest) (*CaptureMemoryResponse, error) {
	s.methods = append(s.methods, MembraneService_CaptureMemory_FullMethodName)
	return &CaptureMemoryResponse{}, nil
}

func (s *recordingGeneratedServer) RetrieveGraph(context.Context, *RetrieveGraphRequest) (*RetrieveGraphResponse, error) {
	s.methods = append(s.methods, MembraneService_RetrieveGraph_FullMethodName)
	return &RetrieveGraphResponse{}, nil
}

func (s *recordingGeneratedServer) RetrieveByID(context.Context, *RetrieveByIDRequest) (*MemoryRecordResponse, error) {
	s.methods = append(s.methods, MembraneService_RetrieveByID_FullMethodName)
	return &MemoryRecordResponse{}, nil
}

func (s *recordingGeneratedServer) Supersede(context.Context, *SupersedeRequest) (*MemoryRecordResponse, error) {
	s.methods = append(s.methods, MembraneService_Supersede_FullMethodName)
	return &MemoryRecordResponse{}, nil
}

func (s *recordingGeneratedServer) Fork(context.Context, *ForkRequest) (*MemoryRecordResponse, error) {
	s.methods = append(s.methods, MembraneService_Fork_FullMethodName)
	return &MemoryRecordResponse{}, nil
}

func (s *recordingGeneratedServer) Retract(context.Context, *RetractRequest) (*RetractResponse, error) {
	s.methods = append(s.methods, MembraneService_Retract_FullMethodName)
	return &RetractResponse{}, nil
}

func (s *recordingGeneratedServer) Merge(context.Context, *MergeRequest) (*MemoryRecordResponse, error) {
	s.methods = append(s.methods, MembraneService_Merge_FullMethodName)
	return &MemoryRecordResponse{}, nil
}

func (s *recordingGeneratedServer) Reinforce(context.Context, *ReinforceRequest) (*ReinforceResponse, error) {
	s.methods = append(s.methods, MembraneService_Reinforce_FullMethodName)
	return &ReinforceResponse{}, nil
}

func (s *recordingGeneratedServer) Penalize(context.Context, *PenalizeRequest) (*PenalizeResponse, error) {
	s.methods = append(s.methods, MembraneService_Penalize_FullMethodName)
	return &PenalizeResponse{}, nil
}

func (s *recordingGeneratedServer) GetMetrics(context.Context, *GetMetricsRequest) (*MetricsResponse, error) {
	s.methods = append(s.methods, MembraneService_GetMetrics_FullMethodName)
	return &MetricsResponse{}, nil
}

func (s *recordingGeneratedServer) Contest(context.Context, *ContestRequest) (*ContestResponse, error) {
	s.methods = append(s.methods, MembraneService_Contest_FullMethodName)
	return &ContestResponse{}, nil
}

func TestGeneratedMessageAccessorsAndReflection(t *testing.T) {
	content, err := structpb.NewValue(map[string]any{"text": "remember this"})
	if err != nil {
		t.Fatalf("NewValue: %v", err)
	}
	req := &CaptureMemoryRequest{
		Source:           "tester",
		SourceKind:       "event",
		Content:          content,
		Context:          content,
		ReasonToRemember: "important",
		ProposedType:     "episodic",
		Summary:          "summary",
		Tags:             []string{"tag-a"},
		Scope:            "project",
		Sensitivity:      "low",
		Timestamp:        "2026-05-01T12:00:00Z",
	}
	if req.GetSource() != "tester" || req.GetSourceKind() != "event" || req.GetContent() == nil || req.GetContext() == nil {
		t.Fatalf("CaptureMemoryRequest getters returned unexpected values: %+v", req)
	}
	if req.GetReasonToRemember() != "important" || req.GetProposedType() != "episodic" || req.GetSummary() != "summary" {
		t.Fatalf("CaptureMemoryRequest text getters returned unexpected values: %+v", req)
	}
	if len(req.GetTags()) != 1 || req.GetScope() != "project" || req.GetSensitivity() != "low" || req.GetTimestamp() == "" {
		t.Fatalf("CaptureMemoryRequest metadata getters returned unexpected values: %+v", req)
	}
	if ((*CaptureMemoryRequest)(nil)).GetSource() != "" || ((*CaptureMemoryRequest)(nil)).GetContent() != nil {
		t.Fatalf("nil CaptureMemoryRequest getters returned non-zero values")
	}
	if req.String() == "" || req.ProtoReflect().Descriptor().FullName() != "membrane.v1.CaptureMemoryRequest" {
		t.Fatalf("CaptureMemoryRequest reflection/string failed")
	}
	if raw, idx := req.Descriptor(); len(raw) == 0 || len(idx) != 1 || idx[0] != 0 {
		t.Fatalf("CaptureMemoryRequest descriptor = len:%d idx:%v, want raw descriptor index 0", len(raw), idx)
	}
	req.Reset()
	if req.GetSource() != "" {
		t.Fatalf("Reset source = %q, want empty", req.GetSource())
	}

	trust := &TrustContext{MaxSensitivity: "high", Authenticated: true, ActorId: "actor", Scopes: []string{"scope"}}
	graphReq := &RetrieveGraphRequest{
		TaskDescriptor: "task",
		Trust:          trust,
		MemoryTypes:    []string{"semantic"},
		MinSalience:    0.2,
		RootLimit:      3,
		NodeLimit:      4,
		EdgeLimit:      5,
		MaxHops:        2,
	}
	if graphReq.GetTrust().GetActorId() != "actor" || graphReq.GetRootLimit() != 3 || graphReq.GetMaxHops() != 2 {
		t.Fatalf("RetrieveGraphRequest getters returned unexpected values: %+v", graphReq)
	}

	object, err := structpb.NewValue("staging")
	if err != nil {
		t.Fatalf("NewValue object: %v", err)
	}
	payload := &Payload{Kind: &Payload_Semantic{Semantic: &SemanticPayload{
		Kind:      "semantic",
		Subject:   "Orchid",
		Predicate: "deploys_to",
		Object:    object,
		Validity:  &Validity{Mode: "global"},
		Evidence:  []*ProvenanceRef{{SourceType: "observation", SourceId: "obs-1", Timestamp: "2026-05-01T12:00:00Z"}},
		Revision:  &RevisionState{Status: "active"},
	}}}
	if payload.GetSemantic().GetSubject() != "Orchid" || payload.GetEpisodic() != nil {
		t.Fatalf("Payload oneof getters returned unexpected values: %+v", payload)
	}

	record := &MemoryRecord{
		Id:             "rec-1",
		Type:           "semantic",
		Sensitivity:    "low",
		Confidence:     0.8,
		Salience:       0.7,
		Scope:          "project",
		Tags:           []string{"tag-a"},
		CreatedAt:      "2026-05-01T12:00:00Z",
		UpdatedAt:      "2026-05-01T12:01:00Z",
		Lifecycle:      &Lifecycle{Decay: &DecayProfile{Curve: "exponential", HalfLifeSeconds: 60}, DeletionPolicy: "auto_prune"},
		Provenance:     &Provenance{Sources: []*ProvenanceSource{{Kind: "observation", Ref: "obs-1"}}},
		Relations:      []*Relation{{Predicate: "related_to", TargetId: "rec-2", Weight: 1}},
		Payload:        payload,
		Interpretation: &Interpretation{Status: "resolved", Summary: "summary"},
		AuditLog:       []*AuditEntry{{Action: "create", Actor: "tester"}},
	}
	if record.GetId() != "rec-1" || record.GetLifecycle().GetDecay().GetHalfLifeSeconds() != 60 || record.GetPayload().GetSemantic().GetObject().GetStringValue() != "staging" {
		t.Fatalf("MemoryRecord getters returned unexpected values: %+v", record)
	}
	response := &RetrieveGraphResponse{
		Nodes:     []*GraphNode{{Record: record, Root: true, Hop: 0}},
		Edges:     []*GraphEdge{{SourceId: "rec-1", Predicate: "related_to", TargetId: "rec-2"}},
		RootIds:   []string{"rec-1"},
		Selection: &SelectionResult{Selected: []*MemoryRecord{record}, Confidence: 0.9},
	}
	if len(response.GetNodes()) != 1 || len(response.GetEdges()) != 1 || response.GetSelection().GetConfidence() != 0.9 {
		t.Fatalf("RetrieveGraphResponse getters returned unexpected values: %+v", response)
	}
}

func TestGeneratedMessageMethodsForAllRegisteredMessages(t *testing.T) {
	file_membrane_proto_init()
	exerciseGeneratedDescriptors(t, File_membrane_proto.Messages())
}

func exerciseGeneratedDescriptors(t *testing.T, messages protoreflect.MessageDescriptors) {
	t.Helper()
	for i := 0; i < messages.Len(); i++ {
		desc := messages.Get(i)
		mt, err := protoregistry.GlobalTypes.FindMessageByName(desc.FullName())
		if err != nil {
			if desc.IsMapEntry() {
				continue
			}
			t.Fatalf("FindMessageByName(%s): %v", desc.FullName(), err)
		}
		exerciseGeneratedMessage(t, mt.New().Interface())
		exerciseGeneratedDescriptors(t, desc.Messages())
	}
}

func exerciseGeneratedMessage(t *testing.T, msg proto.Message) {
	t.Helper()
	value := reflect.ValueOf(msg)
	if value.Kind() != reflect.Pointer {
		t.Fatalf("message %T is not a pointer", msg)
	}
	typ := value.Type()
	nilValue := reflect.Zero(typ)

	for i := 0; i < typ.NumMethod(); i++ {
		method := typ.Method(i)
		if !strings.HasPrefix(method.Name, "Get") || method.Type.NumIn() != 1 || method.Type.NumOut() != 1 {
			continue
		}
		method.Func.Call([]reflect.Value{value})
		method.Func.Call([]reflect.Value{nilValue})
	}

	for _, name := range []string{"String", "ProtoMessage", "ProtoReflect", "Descriptor", "Reset"} {
		method, ok := typ.MethodByName(name)
		if !ok || method.Type.NumIn() != 1 {
			continue
		}
		method.Func.Call([]reflect.Value{value})
	}
	if method, ok := typ.MethodByName("ProtoReflect"); ok && method.Type.NumIn() == 1 {
		method.Func.Call([]reflect.Value{nilValue})
	}
}

func TestGeneratedPayloadOneofAccessors(t *testing.T) {
	payloads := []*Payload{
		{Kind: &Payload_Episodic{Episodic: &EpisodicPayload{Kind: "episodic"}}},
		{Kind: &Payload_Working{Working: &WorkingPayload{Kind: "working"}}},
		{Kind: &Payload_Semantic{Semantic: &SemanticPayload{Kind: "semantic"}}},
		{Kind: &Payload_Competence{Competence: &CompetencePayload{Kind: "competence"}}},
		{Kind: &Payload_PlanGraph{PlanGraph: &PlanGraphPayload{Kind: "plan_graph"}}},
		{Kind: &Payload_Entity{Entity: &EntityPayload{Kind: "entity"}}},
		{},
	}

	for _, payload := range payloads {
		_ = payload.GetEpisodic()
		_ = payload.GetWorking()
		_ = payload.GetSemantic()
		_ = payload.GetCompetence()
		_ = payload.GetPlanGraph()
		_ = payload.GetEntity()
	}
	if ((*Payload)(nil)).GetEpisodic() != nil || ((*Payload)(nil)).GetEntity() != nil {
		t.Fatalf("nil Payload oneof getters returned non-nil payload")
	}
}

func TestGeneratedUnaryHandlers(t *testing.T) {
	ctx := context.Background()
	server := &recordingGeneratedServer{}
	type handlerFunc func(interface{}, context.Context, func(interface{}) error, grpc.UnaryServerInterceptor) (interface{}, error)
	handlers := []struct {
		method  string
		handler handlerFunc
	}{
		{MembraneService_CaptureMemory_FullMethodName, _MembraneService_CaptureMemory_Handler},
		{MembraneService_RetrieveGraph_FullMethodName, _MembraneService_RetrieveGraph_Handler},
		{MembraneService_RetrieveByID_FullMethodName, _MembraneService_RetrieveByID_Handler},
		{MembraneService_Supersede_FullMethodName, _MembraneService_Supersede_Handler},
		{MembraneService_Fork_FullMethodName, _MembraneService_Fork_Handler},
		{MembraneService_Retract_FullMethodName, _MembraneService_Retract_Handler},
		{MembraneService_Merge_FullMethodName, _MembraneService_Merge_Handler},
		{MembraneService_Reinforce_FullMethodName, _MembraneService_Reinforce_Handler},
		{MembraneService_Penalize_FullMethodName, _MembraneService_Penalize_Handler},
		{MembraneService_GetMetrics_FullMethodName, _MembraneService_GetMetrics_Handler},
		{MembraneService_Contest_FullMethodName, _MembraneService_Contest_Handler},
	}

	for _, item := range handlers {
		t.Run(item.method, func(t *testing.T) {
			decodes := 0
			decode := func(v interface{}) error {
				if reflect.ValueOf(v).Kind() != reflect.Pointer {
					t.Fatalf("decode target = %T, want pointer", v)
				}
				decodes++
				return nil
			}
			if resp, err := item.handler(server, ctx, decode, nil); err != nil || resp == nil {
				t.Fatalf("handler without interceptor = %+v, %v; want response nil error", resp, err)
			}

			intercepted := false
			interceptor := func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
				if info.FullMethod != item.method {
					t.Fatalf("FullMethod = %q, want %q", info.FullMethod, item.method)
				}
				intercepted = true
				return handler(ctx, req)
			}
			if resp, err := item.handler(server, ctx, decode, interceptor); err != nil || resp == nil {
				t.Fatalf("handler with interceptor = %+v, %v; want response nil error", resp, err)
			}
			if !intercepted || decodes != 2 {
				t.Fatalf("intercepted=%v decodes=%d, want true and two decodes", intercepted, decodes)
			}

			decodeErr := errors.New("decode failed")
			if resp, err := item.handler(server, ctx, func(interface{}) error { return decodeErr }, nil); !errors.Is(err, decodeErr) || resp != nil {
				t.Fatalf("handler decode error = %+v, %v; want decode failure and nil response", resp, err)
			}
		})
	}
	if len(server.methods) != len(handlers)*2 {
		t.Fatalf("server methods = %d, want %d", len(server.methods), len(handlers)*2)
	}
}

func TestGeneratedClientAndUnimplementedServer(t *testing.T) {
	ctx := context.Background()
	conn := &fakeClientConn{}
	client := NewMembraneServiceClient(conn)

	calls := []struct {
		name string
		call func() error
	}{
		{MembraneService_CaptureMemory_FullMethodName, func() error { _, err := client.CaptureMemory(ctx, &CaptureMemoryRequest{}); return err }},
		{MembraneService_RetrieveGraph_FullMethodName, func() error { _, err := client.RetrieveGraph(ctx, &RetrieveGraphRequest{}); return err }},
		{MembraneService_RetrieveByID_FullMethodName, func() error { _, err := client.RetrieveByID(ctx, &RetrieveByIDRequest{}); return err }},
		{MembraneService_Supersede_FullMethodName, func() error { _, err := client.Supersede(ctx, &SupersedeRequest{}); return err }},
		{MembraneService_Fork_FullMethodName, func() error { _, err := client.Fork(ctx, &ForkRequest{}); return err }},
		{MembraneService_Retract_FullMethodName, func() error { _, err := client.Retract(ctx, &RetractRequest{}); return err }},
		{MembraneService_Merge_FullMethodName, func() error { _, err := client.Merge(ctx, &MergeRequest{}); return err }},
		{MembraneService_Reinforce_FullMethodName, func() error { _, err := client.Reinforce(ctx, &ReinforceRequest{}); return err }},
		{MembraneService_Penalize_FullMethodName, func() error { _, err := client.Penalize(ctx, &PenalizeRequest{}); return err }},
		{MembraneService_GetMetrics_FullMethodName, func() error { _, err := client.GetMetrics(ctx, &GetMetricsRequest{}); return err }},
		{MembraneService_Contest_FullMethodName, func() error { _, err := client.Contest(ctx, &ContestRequest{}); return err }},
	}
	for _, call := range calls {
		if err := call.call(); err != nil {
			t.Fatalf("%s client call: %v", call.name, err)
		}
	}
	if len(conn.methods) != len(calls) {
		t.Fatalf("client methods = %v, want %d calls", conn.methods, len(calls))
	}
	for i, call := range calls {
		if conn.methods[i] != call.name {
			t.Fatalf("method[%d] = %q, want %q", i, conn.methods[i], call.name)
		}
	}

	errConn := &fakeClientConn{err: errors.New("invoke failed")}
	errClient := NewMembraneServiceClient(errConn)
	errorCalls := []struct {
		name string
		call func() (any, error)
	}{
		{MembraneService_CaptureMemory_FullMethodName, func() (any, error) { return errClient.CaptureMemory(ctx, &CaptureMemoryRequest{}) }},
		{MembraneService_RetrieveGraph_FullMethodName, func() (any, error) { return errClient.RetrieveGraph(ctx, &RetrieveGraphRequest{}) }},
		{MembraneService_RetrieveByID_FullMethodName, func() (any, error) { return errClient.RetrieveByID(ctx, &RetrieveByIDRequest{}) }},
		{MembraneService_Supersede_FullMethodName, func() (any, error) { return errClient.Supersede(ctx, &SupersedeRequest{}) }},
		{MembraneService_Fork_FullMethodName, func() (any, error) { return errClient.Fork(ctx, &ForkRequest{}) }},
		{MembraneService_Retract_FullMethodName, func() (any, error) { return errClient.Retract(ctx, &RetractRequest{}) }},
		{MembraneService_Merge_FullMethodName, func() (any, error) { return errClient.Merge(ctx, &MergeRequest{}) }},
		{MembraneService_Reinforce_FullMethodName, func() (any, error) { return errClient.Reinforce(ctx, &ReinforceRequest{}) }},
		{MembraneService_Penalize_FullMethodName, func() (any, error) { return errClient.Penalize(ctx, &PenalizeRequest{}) }},
		{MembraneService_GetMetrics_FullMethodName, func() (any, error) { return errClient.GetMetrics(ctx, &GetMetricsRequest{}) }},
		{MembraneService_Contest_FullMethodName, func() (any, error) { return errClient.Contest(ctx, &ContestRequest{}) }},
	}
	for _, call := range errorCalls {
		resp, err := call.call()
		if err == nil || !isNilResponse(resp) {
			t.Fatalf("%s error response = %v/%v, want invoke error and nil response", call.name, resp, err)
		}
	}

	server := UnimplementedMembraneServiceServer{}
	unimplemented := []error{
		func() error { _, err := server.CaptureMemory(ctx, &CaptureMemoryRequest{}); return err }(),
		func() error { _, err := server.RetrieveGraph(ctx, &RetrieveGraphRequest{}); return err }(),
		func() error { _, err := server.RetrieveByID(ctx, &RetrieveByIDRequest{}); return err }(),
		func() error { _, err := server.Supersede(ctx, &SupersedeRequest{}); return err }(),
		func() error { _, err := server.Fork(ctx, &ForkRequest{}); return err }(),
		func() error { _, err := server.Retract(ctx, &RetractRequest{}); return err }(),
		func() error { _, err := server.Merge(ctx, &MergeRequest{}); return err }(),
		func() error { _, err := server.Reinforce(ctx, &ReinforceRequest{}); return err }(),
		func() error { _, err := server.Penalize(ctx, &PenalizeRequest{}); return err }(),
		func() error { _, err := server.GetMetrics(ctx, &GetMetricsRequest{}); return err }(),
		func() error { _, err := server.Contest(ctx, &ContestRequest{}); return err }(),
	}
	for _, err := range unimplemented {
		if status.Code(err) != codes.Unimplemented {
			t.Fatalf("unimplemented error = %v, want Unimplemented", err)
		}
	}
	server.mustEmbedUnimplementedMembraneServiceServer()
	server.testEmbeddedByValue()

	grpcServer := grpc.NewServer()
	RegisterMembraneServiceServer(grpcServer, server)
	grpcServer.Stop()
}

func isNilResponse(resp any) bool {
	if resp == nil {
		return true
	}
	value := reflect.ValueOf(resp)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
