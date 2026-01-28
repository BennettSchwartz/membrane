package grpc

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"time"

	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/GustyCube/membrane/pkg/ingestion"
	"github.com/GustyCube/membrane/pkg/membrane"
	"github.com/GustyCube/membrane/pkg/retrieval"
	"github.com/GustyCube/membrane/pkg/schema"
)

// ---------------------------------------------------------------------------
// Service descriptor – hand-written, no protoc needed.
// ---------------------------------------------------------------------------

const serviceName = "membrane.v1.MembraneService"

// serviceDesc is a gRPC ServiceDesc that mirrors the proto service definition.
var serviceDesc = grpclib.ServiceDesc{
	ServiceName: serviceName,
	HandlerType: (*MembraneServiceServer)(nil),
	Methods: []grpclib.MethodDesc{
		{MethodName: "IngestEvent", Handler: handleIngestEvent},
		{MethodName: "IngestToolOutput", Handler: handleIngestToolOutput},
		{MethodName: "IngestObservation", Handler: handleIngestObservation},
		{MethodName: "IngestOutcome", Handler: handleIngestOutcome},
		{MethodName: "IngestWorkingState", Handler: handleIngestWorkingState},
		{MethodName: "Retrieve", Handler: handleRetrieve},
		{MethodName: "RetrieveByID", Handler: handleRetrieveByID},
		{MethodName: "Supersede", Handler: handleSupersede},
		{MethodName: "Fork", Handler: handleFork},
		{MethodName: "Retract", Handler: handleRetract},
		{MethodName: "Merge", Handler: handleMerge},
		{MethodName: "Reinforce", Handler: handleReinforce},
		{MethodName: "Penalize", Handler: handlePenalize},
		{MethodName: "GetMetrics", Handler: handleGetMetrics},
	},
	Streams:  []grpclib.StreamDesc{},
	Metadata: "membrane/v1/membrane.proto",
}

// registerMembraneService registers the handler with the gRPC server.
func registerMembraneService(s *grpclib.Server, srv MembraneServiceServer) {
	s.RegisterService(&serviceDesc, srv)
}

// MembraneServiceServer is the interface that the Handler must satisfy.
// It is referenced by the ServiceDesc.HandlerType so gRPC can type-assert
// the registered implementation.
type MembraneServiceServer interface {
	IngestEvent(ctx context.Context, req *IngestEventRequest) (*IngestResponse, error)
	IngestToolOutput(ctx context.Context, req *IngestToolOutputRequest) (*IngestResponse, error)
	IngestObservation(ctx context.Context, req *IngestObservationRequest) (*IngestResponse, error)
	IngestOutcome(ctx context.Context, req *IngestOutcomeRequest) (*IngestResponse, error)
	IngestWorkingState(ctx context.Context, req *IngestWorkingStateRequest) (*IngestResponse, error)
	Retrieve(ctx context.Context, req *RetrieveRequest) (*RetrieveResponse, error)
	RetrieveByID(ctx context.Context, req *RetrieveByIDRequest) (*MemoryRecordResponse, error)
	Supersede(ctx context.Context, req *SupersedeRequest) (*MemoryRecordResponse, error)
	Fork(ctx context.Context, req *ForkRequest) (*MemoryRecordResponse, error)
	Retract(ctx context.Context, req *RetractRequest) (*RetractResponse, error)
	Merge(ctx context.Context, req *MergeRequest) (*MemoryRecordResponse, error)
	Reinforce(ctx context.Context, req *ReinforceRequest) (*ReinforceResponse, error)
	Penalize(ctx context.Context, req *PenalizeRequest) (*PenalizeResponse, error)
	GetMetrics(ctx context.Context, req *GetMetricsRequest) (*MetricsResponse, error)
}

// ---------------------------------------------------------------------------
// Request / Response types (JSON-serializable, mirror the proto messages)
// ---------------------------------------------------------------------------

// IngestEventRequest mirrors the proto IngestEventRequest.
type IngestEventRequest struct {
	Source      string   `json:"source"`
	EventKind  string   `json:"event_kind"`
	Ref        string   `json:"ref"`
	Summary    string   `json:"summary"`
	Timestamp  string   `json:"timestamp"` // RFC 3339
	Tags       []string `json:"tags"`
	Scope      string   `json:"scope"`
	Sensitivity string  `json:"sensitivity"`
}

// IngestToolOutputRequest mirrors the proto IngestToolOutputRequest.
type IngestToolOutputRequest struct {
	Source      string          `json:"source"`
	ToolName   string          `json:"tool_name"`
	Args       json.RawMessage `json:"args"`
	Result     json.RawMessage `json:"result"`
	DependsOn  []string        `json:"depends_on"`
	Timestamp  string          `json:"timestamp"` // RFC 3339
	Tags       []string        `json:"tags"`
	Scope      string          `json:"scope"`
	Sensitivity string         `json:"sensitivity"`
}

// IngestObservationRequest mirrors the proto IngestObservationRequest.
type IngestObservationRequest struct {
	Source      string          `json:"source"`
	Subject    string          `json:"subject"`
	Predicate  string          `json:"predicate"`
	Object     json.RawMessage `json:"object"`
	Timestamp  string          `json:"timestamp"` // RFC 3339
	Tags       []string        `json:"tags"`
	Scope      string          `json:"scope"`
	Sensitivity string         `json:"sensitivity"`
}

// IngestOutcomeRequest mirrors the proto IngestOutcomeRequest.
type IngestOutcomeRequest struct {
	Source         string `json:"source"`
	TargetRecordID string `json:"target_record_id"`
	OutcomeStatus  string `json:"outcome_status"`
	Timestamp      string `json:"timestamp"` // RFC 3339
}

// IngestWorkingStateRequest mirrors the proto IngestWorkingStateRequest.
type IngestWorkingStateRequest struct {
	Source            string              `json:"source"`
	ThreadID          string              `json:"thread_id"`
	State             string              `json:"state"`
	NextActions       []string            `json:"next_actions"`
	OpenQuestions     []string            `json:"open_questions"`
	ContextSummary    string              `json:"context_summary"`
	ActiveConstraints json.RawMessage     `json:"active_constraints"`
	Timestamp         string              `json:"timestamp"` // RFC 3339
	Tags              []string            `json:"tags"`
	Scope             string              `json:"scope"`
	Sensitivity       string              `json:"sensitivity"`
}

// IngestResponse wraps a JSON-encoded MemoryRecord.
type IngestResponse struct {
	Record json.RawMessage `json:"record"`
}

// TrustContextMsg mirrors the proto TrustContext.
type TrustContextMsg struct {
	MaxSensitivity string   `json:"max_sensitivity"`
	Authenticated  bool     `json:"authenticated"`
	ActorID        string   `json:"actor_id"`
	Scopes         []string `json:"scopes"`
}

// RetrieveRequest mirrors the proto RetrieveRequest.
type RetrieveRequest struct {
	TaskDescriptor string           `json:"task_descriptor"`
	Trust          *TrustContextMsg `json:"trust"`
	MemoryTypes    []string         `json:"memory_types"`
	MinSalience    float64          `json:"min_salience"`
	Limit          int32            `json:"limit"`
}

// RetrieveResponse wraps the retrieval results.
type RetrieveResponse struct {
	Records   []json.RawMessage `json:"records"`
	Selection json.RawMessage   `json:"selection,omitempty"`
}

// RetrieveByIDRequest mirrors the proto RetrieveByIDRequest.
type RetrieveByIDRequest struct {
	ID    string           `json:"id"`
	Trust *TrustContextMsg `json:"trust"`
}

// MemoryRecordResponse wraps a single JSON-encoded MemoryRecord.
type MemoryRecordResponse struct {
	Record json.RawMessage `json:"record"`
}

// SupersedeRequest mirrors the proto SupersedeRequest.
type SupersedeRequest struct {
	OldID     string          `json:"old_id"`
	NewRecord json.RawMessage `json:"new_record"`
	Actor     string          `json:"actor"`
	Rationale string          `json:"rationale"`
}

// ForkRequest mirrors the proto ForkRequest.
type ForkRequest struct {
	SourceID     string          `json:"source_id"`
	ForkedRecord json.RawMessage `json:"forked_record"`
	Actor        string          `json:"actor"`
	Rationale    string          `json:"rationale"`
}

// RetractRequest mirrors the proto RetractRequest.
type RetractRequest struct {
	ID        string `json:"id"`
	Actor     string `json:"actor"`
	Rationale string `json:"rationale"`
}

// RetractResponse is an empty acknowledgement.
type RetractResponse struct{}

// MergeRequest mirrors the proto MergeRequest.
type MergeRequest struct {
	IDs          []string        `json:"ids"`
	MergedRecord json.RawMessage `json:"merged_record"`
	Actor        string          `json:"actor"`
	Rationale    string          `json:"rationale"`
}

// ReinforceRequest mirrors the proto ReinforceRequest.
type ReinforceRequest struct {
	ID        string `json:"id"`
	Actor     string `json:"actor"`
	Rationale string `json:"rationale"`
}

// ReinforceResponse is an empty acknowledgement.
type ReinforceResponse struct{}

// PenalizeRequest mirrors the proto PenalizeRequest.
type PenalizeRequest struct {
	ID        string  `json:"id"`
	Amount    float64 `json:"amount"`
	Actor     string  `json:"actor"`
	Rationale string  `json:"rationale"`
}

// PenalizeResponse is an empty acknowledgement.
type PenalizeResponse struct{}

// GetMetricsRequest is an empty request.
type GetMetricsRequest struct{}

// MetricsResponse wraps a JSON-encoded metrics.Snapshot.
type MetricsResponse struct {
	Snapshot json.RawMessage `json:"snapshot"`
}

// ---------------------------------------------------------------------------
// Handler implements MembraneServiceServer.
// ---------------------------------------------------------------------------

// Handler implements the MembraneServiceServer interface by delegating to
// the Membrane API.
type Handler struct {
	membrane *membrane.Membrane
}

const (
	maxPayloadSize  = 10 * 1024 * 1024 // 10 MB
	maxStringLength = 100_000           // 100 KB
	maxTags         = 100
	maxTagLength    = 256
	maxLimit        = 10_000
)

// validateStringField checks that a string doesn't exceed the maximum length.
func validateStringField(name, value string) error {
	if len(value) > maxStringLength {
		return status.Errorf(codes.InvalidArgument, "%s exceeds maximum length of %d", name, maxStringLength)
	}
	return nil
}

// validateTags checks tag count and individual tag lengths.
func validateTags(tags []string) error {
	if len(tags) > maxTags {
		return status.Errorf(codes.InvalidArgument, "too many tags: %d (max %d)", len(tags), maxTags)
	}
	for _, tag := range tags {
		if len(tag) > maxTagLength {
			return status.Errorf(codes.InvalidArgument, "tag exceeds maximum length of %d", maxTagLength)
		}
	}
	return nil
}

// validateJSONPayload checks that a JSON payload doesn't exceed the max size.
func validateJSONPayload(name string, data []byte) error {
	if len(data) > maxPayloadSize {
		return status.Errorf(codes.InvalidArgument, "%s exceeds maximum payload size of %d bytes", name, maxPayloadSize)
	}
	return nil
}

// compile-time assertion
var _ MembraneServiceServer = (*Handler)(nil)

// IngestEvent converts the gRPC request and delegates to Membrane.IngestEvent.
func (h *Handler) IngestEvent(ctx context.Context, req *IngestEventRequest) (*IngestResponse, error) {
	if err := validateStringField("source", req.Source); err != nil { return nil, err }
	if err := validateStringField("event_kind", req.EventKind); err != nil { return nil, err }
	if err := validateStringField("ref", req.Ref); err != nil { return nil, err }
	if err := validateStringField("summary", req.Summary); err != nil { return nil, err }
	if err := validateTags(req.Tags); err != nil { return nil, err }

	ts, err := parseOptionalTime(req.Timestamp)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid timestamp: %v", err)
	}

	rec, err := h.membrane.IngestEvent(ctx, ingestion.IngestEventRequest{
		Source:      req.Source,
		EventKind:  req.EventKind,
		Ref:        req.Ref,
		Summary:    req.Summary,
		Timestamp:  ts,
		Tags:       req.Tags,
		Scope:      req.Scope,
		Sensitivity: schema.Sensitivity(req.Sensitivity),
	})
	if err != nil {
		return nil, internalErr(err)
	}

	return marshalRecordResponse(rec)
}

// IngestToolOutput converts the gRPC request and delegates to Membrane.IngestToolOutput.
func (h *Handler) IngestToolOutput(ctx context.Context, req *IngestToolOutputRequest) (*IngestResponse, error) {
	if err := validateStringField("source", req.Source); err != nil { return nil, err }
	if err := validateStringField("tool_name", req.ToolName); err != nil { return nil, err }
	if err := validateJSONPayload("args", req.Args); err != nil { return nil, err }
	if err := validateJSONPayload("result", req.Result); err != nil { return nil, err }
	if err := validateTags(req.Tags); err != nil { return nil, err }

	ts, err := parseOptionalTime(req.Timestamp)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid timestamp: %v", err)
	}

	var args map[string]any
	if len(req.Args) > 0 {
		if err := json.Unmarshal(req.Args, &args); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid args JSON: %v", err)
		}
	}

	var result any
	if len(req.Result) > 0 {
		if err := json.Unmarshal(req.Result, &result); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid result JSON: %v", err)
		}
	}

	rec, err := h.membrane.IngestToolOutput(ctx, ingestion.IngestToolOutputRequest{
		Source:      req.Source,
		ToolName:   req.ToolName,
		Args:       args,
		Result:     result,
		DependsOn:  req.DependsOn,
		Timestamp:  ts,
		Tags:       req.Tags,
		Scope:      req.Scope,
		Sensitivity: schema.Sensitivity(req.Sensitivity),
	})
	if err != nil {
		return nil, internalErr(err)
	}

	return marshalRecordResponse(rec)
}

// IngestObservation converts the gRPC request and delegates to Membrane.IngestObservation.
func (h *Handler) IngestObservation(ctx context.Context, req *IngestObservationRequest) (*IngestResponse, error) {
	if err := validateStringField("source", req.Source); err != nil { return nil, err }
	if err := validateStringField("subject", req.Subject); err != nil { return nil, err }
	if err := validateStringField("predicate", req.Predicate); err != nil { return nil, err }
	if err := validateJSONPayload("object", req.Object); err != nil { return nil, err }
	if err := validateTags(req.Tags); err != nil { return nil, err }

	ts, err := parseOptionalTime(req.Timestamp)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid timestamp: %v", err)
	}

	var obj any
	if len(req.Object) > 0 {
		if err := json.Unmarshal(req.Object, &obj); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid object JSON: %v", err)
		}
	}

	rec, err := h.membrane.IngestObservation(ctx, ingestion.IngestObservationRequest{
		Source:      req.Source,
		Subject:    req.Subject,
		Predicate:  req.Predicate,
		Object:     obj,
		Timestamp:  ts,
		Tags:       req.Tags,
		Scope:      req.Scope,
		Sensitivity: schema.Sensitivity(req.Sensitivity),
	})
	if err != nil {
		return nil, internalErr(err)
	}

	return marshalRecordResponse(rec)
}

// IngestOutcome converts the gRPC request and delegates to Membrane.IngestOutcome.
func (h *Handler) IngestOutcome(ctx context.Context, req *IngestOutcomeRequest) (*IngestResponse, error) {
	ts, err := parseOptionalTime(req.Timestamp)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid timestamp: %v", err)
	}

	rec, err := h.membrane.IngestOutcome(ctx, ingestion.IngestOutcomeRequest{
		Source:         req.Source,
		TargetRecordID: req.TargetRecordID,
		OutcomeStatus:  schema.OutcomeStatus(req.OutcomeStatus),
		Timestamp:      ts,
	})
	if err != nil {
		return nil, internalErr(err)
	}

	return marshalRecordResponse(rec)
}

// IngestWorkingState converts the gRPC request and delegates to Membrane.IngestWorkingState.
func (h *Handler) IngestWorkingState(ctx context.Context, req *IngestWorkingStateRequest) (*IngestResponse, error) {
	if err := validateStringField("source", req.Source); err != nil { return nil, err }
	if err := validateStringField("thread_id", req.ThreadID); err != nil { return nil, err }
	if err := validateStringField("context_summary", req.ContextSummary); err != nil { return nil, err }
	if err := validateTags(req.Tags); err != nil { return nil, err }

	ts, err := parseOptionalTime(req.Timestamp)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid timestamp: %v", err)
	}

	var constraints []schema.Constraint
	if len(req.ActiveConstraints) > 0 {
		if err := validateJSONPayload("active_constraints", req.ActiveConstraints); err != nil { return nil, err }
		if err := json.Unmarshal(req.ActiveConstraints, &constraints); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid active_constraints JSON: %v", err)
		}
	}

	rec, err := h.membrane.IngestWorkingState(ctx, ingestion.IngestWorkingStateRequest{
		Source:            req.Source,
		ThreadID:          req.ThreadID,
		State:             schema.TaskState(req.State),
		NextActions:       req.NextActions,
		OpenQuestions:     req.OpenQuestions,
		ContextSummary:    req.ContextSummary,
		ActiveConstraints: constraints,
		Timestamp:         ts,
		Tags:              req.Tags,
		Scope:             req.Scope,
		Sensitivity:       schema.Sensitivity(req.Sensitivity),
	})
	if err != nil {
		return nil, internalErr(err)
	}

	return marshalRecordResponse(rec)
}

// Retrieve converts the gRPC request and delegates to Membrane.Retrieve.
func (h *Handler) Retrieve(ctx context.Context, req *RetrieveRequest) (*RetrieveResponse, error) {
	if req.Trust == nil {
		return nil, status.Error(codes.InvalidArgument, "trust context is required")
	}

	if req.MinSalience < 0 || math.IsNaN(req.MinSalience) || math.IsInf(req.MinSalience, 0) {
		return nil, status.Error(codes.InvalidArgument, "min_salience must be non-negative and finite")
	}
	if req.Limit < 0 || req.Limit > maxLimit {
		return nil, status.Errorf(codes.InvalidArgument, "limit must be between 0 and %d", maxLimit)
	}

	memTypes := make([]schema.MemoryType, len(req.MemoryTypes))
	for i, mt := range req.MemoryTypes {
		memTypes[i] = schema.MemoryType(mt)
	}

	trust := toTrustContext(req.Trust)

	resp, err := h.membrane.Retrieve(ctx, &retrieval.RetrieveRequest{
		TaskDescriptor: req.TaskDescriptor,
		Trust:          trust,
		MemoryTypes:    memTypes,
		MinSalience:    req.MinSalience,
		Limit:          int(req.Limit),
	})
	if err != nil {
		return nil, internalErr(err)
	}

	records := make([]json.RawMessage, len(resp.Records))
	for i, rec := range resp.Records {
		data, err := json.Marshal(rec)
		if err != nil {
			return nil, internalErr(fmt.Errorf("marshal record: %w", err))
		}
		records[i] = data
	}

	var selBytes json.RawMessage
	if resp.Selection != nil {
		selBytes, err = json.Marshal(resp.Selection)
		if err != nil {
			return nil, internalErr(fmt.Errorf("marshal selection: %w", err))
		}
	}

	return &RetrieveResponse{
		Records:   records,
		Selection: selBytes,
	}, nil
}

// RetrieveByID converts the gRPC request and delegates to Membrane.RetrieveByID.
func (h *Handler) RetrieveByID(ctx context.Context, req *RetrieveByIDRequest) (*MemoryRecordResponse, error) {
	if req.Trust == nil {
		return nil, status.Error(codes.InvalidArgument, "trust context is required")
	}

	trust := toTrustContext(req.Trust)

	rec, err := h.membrane.RetrieveByID(ctx, req.ID, trust)
	if err != nil {
		return nil, internalErr(err)
	}

	return marshalMemoryRecordResponse(rec)
}

// Supersede converts the gRPC request and delegates to Membrane.Supersede.
func (h *Handler) Supersede(ctx context.Context, req *SupersedeRequest) (*MemoryRecordResponse, error) {
	if err := validateJSONPayload("new_record", req.NewRecord); err != nil { return nil, err }

	newRec, err := unmarshalRecord(req.NewRecord)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid new_record: %v", err)
	}

	rec, err := h.membrane.Supersede(ctx, req.OldID, newRec, req.Actor, req.Rationale)
	if err != nil {
		return nil, internalErr(err)
	}

	return marshalMemoryRecordResponse(rec)
}

// Fork converts the gRPC request and delegates to Membrane.Fork.
func (h *Handler) Fork(ctx context.Context, req *ForkRequest) (*MemoryRecordResponse, error) {
	if err := validateJSONPayload("forked_record", req.ForkedRecord); err != nil { return nil, err }

	forkedRec, err := unmarshalRecord(req.ForkedRecord)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid forked_record: %v", err)
	}

	rec, err := h.membrane.Fork(ctx, req.SourceID, forkedRec, req.Actor, req.Rationale)
	if err != nil {
		return nil, internalErr(err)
	}

	return marshalMemoryRecordResponse(rec)
}

// Retract converts the gRPC request and delegates to Membrane.Retract.
func (h *Handler) Retract(ctx context.Context, req *RetractRequest) (*RetractResponse, error) {
	if err := h.membrane.Retract(ctx, req.ID, req.Actor, req.Rationale); err != nil {
		return nil, internalErr(err)
	}
	return &RetractResponse{}, nil
}

// Merge converts the gRPC request and delegates to Membrane.Merge.
func (h *Handler) Merge(ctx context.Context, req *MergeRequest) (*MemoryRecordResponse, error) {
	if err := validateJSONPayload("merged_record", req.MergedRecord); err != nil { return nil, err }

	mergedRec, err := unmarshalRecord(req.MergedRecord)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid merged_record: %v", err)
	}

	rec, err := h.membrane.Merge(ctx, req.IDs, mergedRec, req.Actor, req.Rationale)
	if err != nil {
		return nil, internalErr(err)
	}

	return marshalMemoryRecordResponse(rec)
}

// Reinforce converts the gRPC request and delegates to Membrane.Reinforce.
func (h *Handler) Reinforce(ctx context.Context, req *ReinforceRequest) (*ReinforceResponse, error) {
	if err := validateStringField("actor", req.Actor); err != nil { return nil, err }
	if err := validateStringField("rationale", req.Rationale); err != nil { return nil, err }

	if err := h.membrane.Reinforce(ctx, req.ID, req.Actor, req.Rationale); err != nil {
		return nil, internalErr(err)
	}
	return &ReinforceResponse{}, nil
}

// Penalize converts the gRPC request and delegates to Membrane.Penalize.
func (h *Handler) Penalize(ctx context.Context, req *PenalizeRequest) (*PenalizeResponse, error) {
	if req.Amount < 0 || math.IsNaN(req.Amount) || math.IsInf(req.Amount, 0) {
		return nil, status.Error(codes.InvalidArgument, "amount must be non-negative and finite")
	}
	if err := validateStringField("actor", req.Actor); err != nil { return nil, err }
	if err := validateStringField("rationale", req.Rationale); err != nil { return nil, err }

	if err := h.membrane.Penalize(ctx, req.ID, req.Amount, req.Actor, req.Rationale); err != nil {
		return nil, internalErr(err)
	}
	return &PenalizeResponse{}, nil
}

// GetMetrics delegates to Membrane.GetMetrics and returns a JSON-encoded snapshot.
func (h *Handler) GetMetrics(ctx context.Context, _ *GetMetricsRequest) (*MetricsResponse, error) {
	snap, err := h.membrane.GetMetrics(ctx)
	if err != nil {
		return nil, internalErr(err)
	}

	data, err := json.Marshal(snap)
	if err != nil {
		return nil, internalErr(fmt.Errorf("marshal metrics: %w", err))
	}

	return &MetricsResponse{Snapshot: data}, nil
}

// ---------------------------------------------------------------------------
// gRPC method handler adapters
// ---------------------------------------------------------------------------
// These top-level functions match the grpc.methodHandler signature required
// by grpc.MethodDesc. Each one decodes the request, calls the typed handler
// method, and returns the response.

func handleIngestEvent(srv any, ctx context.Context, dec func(any) error, _ grpclib.UnaryServerInterceptor) (any, error) {
	req := new(IngestEventRequest)
	if err := dec(req); err != nil {
		return nil, err
	}
	return srv.(MembraneServiceServer).IngestEvent(ctx, req)
}

func handleIngestToolOutput(srv any, ctx context.Context, dec func(any) error, _ grpclib.UnaryServerInterceptor) (any, error) {
	req := new(IngestToolOutputRequest)
	if err := dec(req); err != nil {
		return nil, err
	}
	return srv.(MembraneServiceServer).IngestToolOutput(ctx, req)
}

func handleIngestObservation(srv any, ctx context.Context, dec func(any) error, _ grpclib.UnaryServerInterceptor) (any, error) {
	req := new(IngestObservationRequest)
	if err := dec(req); err != nil {
		return nil, err
	}
	return srv.(MembraneServiceServer).IngestObservation(ctx, req)
}

func handleIngestOutcome(srv any, ctx context.Context, dec func(any) error, _ grpclib.UnaryServerInterceptor) (any, error) {
	req := new(IngestOutcomeRequest)
	if err := dec(req); err != nil {
		return nil, err
	}
	return srv.(MembraneServiceServer).IngestOutcome(ctx, req)
}

func handleIngestWorkingState(srv any, ctx context.Context, dec func(any) error, _ grpclib.UnaryServerInterceptor) (any, error) {
	req := new(IngestWorkingStateRequest)
	if err := dec(req); err != nil {
		return nil, err
	}
	return srv.(MembraneServiceServer).IngestWorkingState(ctx, req)
}

func handleRetrieve(srv any, ctx context.Context, dec func(any) error, _ grpclib.UnaryServerInterceptor) (any, error) {
	req := new(RetrieveRequest)
	if err := dec(req); err != nil {
		return nil, err
	}
	return srv.(MembraneServiceServer).Retrieve(ctx, req)
}

func handleRetrieveByID(srv any, ctx context.Context, dec func(any) error, _ grpclib.UnaryServerInterceptor) (any, error) {
	req := new(RetrieveByIDRequest)
	if err := dec(req); err != nil {
		return nil, err
	}
	return srv.(MembraneServiceServer).RetrieveByID(ctx, req)
}

func handleSupersede(srv any, ctx context.Context, dec func(any) error, _ grpclib.UnaryServerInterceptor) (any, error) {
	req := new(SupersedeRequest)
	if err := dec(req); err != nil {
		return nil, err
	}
	return srv.(MembraneServiceServer).Supersede(ctx, req)
}

func handleFork(srv any, ctx context.Context, dec func(any) error, _ grpclib.UnaryServerInterceptor) (any, error) {
	req := new(ForkRequest)
	if err := dec(req); err != nil {
		return nil, err
	}
	return srv.(MembraneServiceServer).Fork(ctx, req)
}

func handleRetract(srv any, ctx context.Context, dec func(any) error, _ grpclib.UnaryServerInterceptor) (any, error) {
	req := new(RetractRequest)
	if err := dec(req); err != nil {
		return nil, err
	}
	return srv.(MembraneServiceServer).Retract(ctx, req)
}

func handleMerge(srv any, ctx context.Context, dec func(any) error, _ grpclib.UnaryServerInterceptor) (any, error) {
	req := new(MergeRequest)
	if err := dec(req); err != nil {
		return nil, err
	}
	return srv.(MembraneServiceServer).Merge(ctx, req)
}

func handleReinforce(srv any, ctx context.Context, dec func(any) error, _ grpclib.UnaryServerInterceptor) (any, error) {
	req := new(ReinforceRequest)
	if err := dec(req); err != nil {
		return nil, err
	}
	return srv.(MembraneServiceServer).Reinforce(ctx, req)
}

func handlePenalize(srv any, ctx context.Context, dec func(any) error, _ grpclib.UnaryServerInterceptor) (any, error) {
	req := new(PenalizeRequest)
	if err := dec(req); err != nil {
		return nil, err
	}
	return srv.(MembraneServiceServer).Penalize(ctx, req)
}

func handleGetMetrics(srv any, ctx context.Context, dec func(any) error, _ grpclib.UnaryServerInterceptor) (any, error) {
	req := new(GetMetricsRequest)
	if err := dec(req); err != nil {
		return nil, err
	}
	return srv.(MembraneServiceServer).GetMetrics(ctx, req)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// parseOptionalTime parses an RFC 3339 timestamp string. If the string is
// empty, the zero time.Time is returned (the downstream service will default
// to time.Now()).
func parseOptionalTime(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	return time.Parse(time.RFC3339, s)
}

// toTrustContext converts a wire TrustContextMsg to a retrieval.TrustContext.
func toTrustContext(tc *TrustContextMsg) *retrieval.TrustContext {
	return retrieval.NewTrustContext(
		schema.Sensitivity(tc.MaxSensitivity),
		tc.Authenticated,
		tc.ActorID,
		tc.Scopes,
	)
}

// unmarshalRecord deserialises a JSON-encoded MemoryRecord.
func unmarshalRecord(data json.RawMessage) (*schema.MemoryRecord, error) {
	var rec schema.MemoryRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		return nil, err
	}
	return &rec, nil
}

// marshalRecordResponse JSON-encodes a MemoryRecord into an IngestResponse.
func marshalRecordResponse(rec *schema.MemoryRecord) (*IngestResponse, error) {
	data, err := json.Marshal(rec)
	if err != nil {
		return nil, internalErr(fmt.Errorf("marshal record: %w", err))
	}
	return &IngestResponse{Record: data}, nil
}

// marshalMemoryRecordResponse JSON-encodes a MemoryRecord into a MemoryRecordResponse.
func marshalMemoryRecordResponse(rec *schema.MemoryRecord) (*MemoryRecordResponse, error) {
	data, err := json.Marshal(rec)
	if err != nil {
		return nil, internalErr(fmt.Errorf("marshal record: %w", err))
	}
	return &MemoryRecordResponse{Record: data}, nil
}

// internalErr wraps an error as a gRPC Internal status.
func internalErr(err error) error {
	return status.Errorf(codes.Internal, "%v", err)
}
