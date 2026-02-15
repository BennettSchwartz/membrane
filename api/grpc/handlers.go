package grpc

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/GustyCube/membrane/api/grpc/gen/membranev1"
	"github.com/GustyCube/membrane/pkg/ingestion"
	"github.com/GustyCube/membrane/pkg/membrane"
	"github.com/GustyCube/membrane/pkg/retrieval"
	"github.com/GustyCube/membrane/pkg/schema"
)

// ---------------------------------------------------------------------------
// Handler implements pb.MembraneServiceServer.
// ---------------------------------------------------------------------------

// Handler implements the MembraneServiceServer interface by delegating to
// the Membrane API.
type Handler struct {
	pb.UnimplementedMembraneServiceServer
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
var _ pb.MembraneServiceServer = (*Handler)(nil)

// IngestEvent converts the gRPC request and delegates to Membrane.IngestEvent.
func (h *Handler) IngestEvent(ctx context.Context, req *pb.IngestEventRequest) (*pb.IngestResponse, error) {
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
func (h *Handler) IngestToolOutput(ctx context.Context, req *pb.IngestToolOutputRequest) (*pb.IngestResponse, error) {
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
func (h *Handler) IngestObservation(ctx context.Context, req *pb.IngestObservationRequest) (*pb.IngestResponse, error) {
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
func (h *Handler) IngestOutcome(ctx context.Context, req *pb.IngestOutcomeRequest) (*pb.IngestResponse, error) {
	if err := validateStringField("source", req.Source); err != nil { return nil, err }
	if err := validateStringField("target_record_id", req.TargetRecordId); err != nil { return nil, err }
	if err := validateStringField("outcome_status", req.OutcomeStatus); err != nil { return nil, err }

	ts, err := parseOptionalTime(req.Timestamp)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid timestamp: %v", err)
	}

	rec, err := h.membrane.IngestOutcome(ctx, ingestion.IngestOutcomeRequest{
		Source:         req.Source,
		TargetRecordID: req.TargetRecordId,
		OutcomeStatus:  schema.OutcomeStatus(req.OutcomeStatus),
		Timestamp:      ts,
	})
	if err != nil {
		return nil, internalErr(err)
	}

	return marshalRecordResponse(rec)
}

// IngestWorkingState converts the gRPC request and delegates to Membrane.IngestWorkingState.
func (h *Handler) IngestWorkingState(ctx context.Context, req *pb.IngestWorkingStateRequest) (*pb.IngestResponse, error) {
	if err := validateStringField("source", req.Source); err != nil { return nil, err }
	if err := validateStringField("thread_id", req.ThreadId); err != nil { return nil, err }
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
		ThreadID:          req.ThreadId,
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
func (h *Handler) Retrieve(ctx context.Context, req *pb.RetrieveRequest) (*pb.RetrieveResponse, error) {
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

	records := make([][]byte, len(resp.Records))
	for i, rec := range resp.Records {
		data, err := json.Marshal(rec)
		if err != nil {
			return nil, internalErr(fmt.Errorf("marshal record: %w", err))
		}
		records[i] = data
	}

	var selBytes []byte
	if resp.Selection != nil {
		selBytes, err = json.Marshal(resp.Selection)
		if err != nil {
			return nil, internalErr(fmt.Errorf("marshal selection: %w", err))
		}
	}

	return &pb.RetrieveResponse{
		Records:   records,
		Selection: selBytes,
	}, nil
}

// RetrieveByID converts the gRPC request and delegates to Membrane.RetrieveByID.
func (h *Handler) RetrieveByID(ctx context.Context, req *pb.RetrieveByIDRequest) (*pb.MemoryRecordResponse, error) {
	if req.Trust == nil {
		return nil, status.Error(codes.InvalidArgument, "trust context is required")
	}

	trust := toTrustContext(req.Trust)

	rec, err := h.membrane.RetrieveByID(ctx, req.Id, trust)
	if err != nil {
		return nil, internalErr(err)
	}

	return marshalMemoryRecordResponse(rec)
}

// Supersede converts the gRPC request and delegates to Membrane.Supersede.
func (h *Handler) Supersede(ctx context.Context, req *pb.SupersedeRequest) (*pb.MemoryRecordResponse, error) {
	if err := validateJSONPayload("new_record", req.NewRecord); err != nil { return nil, err }

	newRec, err := unmarshalRecord(req.NewRecord)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid new_record: %v", err)
	}

	rec, err := h.membrane.Supersede(ctx, req.OldId, newRec, req.Actor, req.Rationale)
	if err != nil {
		return nil, internalErr(err)
	}

	return marshalMemoryRecordResponse(rec)
}

// Fork converts the gRPC request and delegates to Membrane.Fork.
func (h *Handler) Fork(ctx context.Context, req *pb.ForkRequest) (*pb.MemoryRecordResponse, error) {
	if err := validateJSONPayload("forked_record", req.ForkedRecord); err != nil { return nil, err }

	forkedRec, err := unmarshalRecord(req.ForkedRecord)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid forked_record: %v", err)
	}

	rec, err := h.membrane.Fork(ctx, req.SourceId, forkedRec, req.Actor, req.Rationale)
	if err != nil {
		return nil, internalErr(err)
	}

	return marshalMemoryRecordResponse(rec)
}

// Retract converts the gRPC request and delegates to Membrane.Retract.
func (h *Handler) Retract(ctx context.Context, req *pb.RetractRequest) (*pb.RetractResponse, error) {
	if err := h.membrane.Retract(ctx, req.Id, req.Actor, req.Rationale); err != nil {
		return nil, internalErr(err)
	}
	return &pb.RetractResponse{}, nil
}

// Merge converts the gRPC request and delegates to Membrane.Merge.
func (h *Handler) Merge(ctx context.Context, req *pb.MergeRequest) (*pb.MemoryRecordResponse, error) {
	if len(req.Ids) == 0 {
		return nil, status.Error(codes.InvalidArgument, "ids must not be empty")
	}
	if len(req.Ids) > maxLimit {
		return nil, status.Errorf(codes.InvalidArgument, "too many ids: %d (max %d)", len(req.Ids), maxLimit)
	}
	if err := validateJSONPayload("merged_record", req.MergedRecord); err != nil { return nil, err }

	mergedRec, err := unmarshalRecord(req.MergedRecord)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid merged_record: %v", err)
	}

	rec, err := h.membrane.Merge(ctx, req.Ids, mergedRec, req.Actor, req.Rationale)
	if err != nil {
		return nil, internalErr(err)
	}

	return marshalMemoryRecordResponse(rec)
}

// Reinforce converts the gRPC request and delegates to Membrane.Reinforce.
func (h *Handler) Reinforce(ctx context.Context, req *pb.ReinforceRequest) (*pb.ReinforceResponse, error) {
	if err := validateStringField("actor", req.Actor); err != nil { return nil, err }
	if err := validateStringField("rationale", req.Rationale); err != nil { return nil, err }

	if err := h.membrane.Reinforce(ctx, req.Id, req.Actor, req.Rationale); err != nil {
		return nil, internalErr(err)
	}
	return &pb.ReinforceResponse{}, nil
}

// Penalize converts the gRPC request and delegates to Membrane.Penalize.
func (h *Handler) Penalize(ctx context.Context, req *pb.PenalizeRequest) (*pb.PenalizeResponse, error) {
	if req.Amount < 0 || math.IsNaN(req.Amount) || math.IsInf(req.Amount, 0) {
		return nil, status.Error(codes.InvalidArgument, "amount must be non-negative and finite")
	}
	if err := validateStringField("actor", req.Actor); err != nil { return nil, err }
	if err := validateStringField("rationale", req.Rationale); err != nil { return nil, err }

	if err := h.membrane.Penalize(ctx, req.Id, req.Amount, req.Actor, req.Rationale); err != nil {
		return nil, internalErr(err)
	}
	return &pb.PenalizeResponse{}, nil
}

// GetMetrics delegates to Membrane.GetMetrics and returns a JSON-encoded snapshot.
func (h *Handler) GetMetrics(ctx context.Context, _ *pb.GetMetricsRequest) (*pb.MetricsResponse, error) {
	snap, err := h.membrane.GetMetrics(ctx)
	if err != nil {
		return nil, internalErr(err)
	}

	data, err := json.Marshal(snap)
	if err != nil {
		return nil, internalErr(fmt.Errorf("marshal metrics: %w", err))
	}

	return &pb.MetricsResponse{Snapshot: data}, nil
}

// Contest converts the gRPC request and delegates to Membrane.Contest.
func (h *Handler) Contest(ctx context.Context, req *pb.ContestRequest) (*pb.ContestResponse, error) {
	if err := validateStringField("actor", req.Actor); err != nil { return nil, err }
	if err := validateStringField("rationale", req.Rationale); err != nil { return nil, err }

	if err := h.membrane.Contest(ctx, req.Id, req.ContestingRef, req.Actor, req.Rationale); err != nil {
		return nil, internalErr(err)
	}
	return &pb.ContestResponse{}, nil
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

// toTrustContext converts a wire pb.TrustContext to a retrieval.TrustContext.
func toTrustContext(tc *pb.TrustContext) *retrieval.TrustContext {
	return retrieval.NewTrustContext(
		schema.Sensitivity(tc.MaxSensitivity),
		tc.Authenticated,
		tc.ActorId,
		tc.Scopes,
	)
}

// unmarshalRecord deserialises a JSON-encoded MemoryRecord.
func unmarshalRecord(data []byte) (*schema.MemoryRecord, error) {
	var rec schema.MemoryRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		return nil, err
	}
	return &rec, nil
}

// marshalRecordResponse JSON-encodes a MemoryRecord into an IngestResponse.
func marshalRecordResponse(rec *schema.MemoryRecord) (*pb.IngestResponse, error) {
	data, err := json.Marshal(rec)
	if err != nil {
		return nil, internalErr(fmt.Errorf("marshal record: %w", err))
	}
	return &pb.IngestResponse{Record: data}, nil
}

// marshalMemoryRecordResponse JSON-encodes a MemoryRecord into a MemoryRecordResponse.
func marshalMemoryRecordResponse(rec *schema.MemoryRecord) (*pb.MemoryRecordResponse, error) {
	data, err := json.Marshal(rec)
	if err != nil {
		return nil, internalErr(fmt.Errorf("marshal record: %w", err))
	}
	return &pb.MemoryRecordResponse{Record: data}, nil
}

// internalErr wraps an error as a gRPC Internal status.
func internalErr(err error) error {
	return status.Errorf(codes.Internal, "%v", err)
}
