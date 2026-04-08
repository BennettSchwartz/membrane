package grpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/GustyCube/membrane/api/grpc/gen/membranev1"
	"github.com/GustyCube/membrane/pkg/ingestion"
	"github.com/GustyCube/membrane/pkg/membrane"
	"github.com/GustyCube/membrane/pkg/retrieval"
	"github.com/GustyCube/membrane/pkg/revision"
	"github.com/GustyCube/membrane/pkg/schema"
	"github.com/GustyCube/membrane/pkg/storage"
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
	maxStringLength = 100_000          // 100 KB
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

// validateSensitivity checks that a sensitivity value is either empty (when optional)
// or one of the supported enum values.
func validateSensitivity(name, value string, required bool) error {
	if value == "" {
		if required {
			return status.Errorf(codes.InvalidArgument, "%s is required", name)
		}
		return nil
	}
	if !schema.IsValidSensitivity(schema.Sensitivity(value)) {
		return status.Errorf(codes.InvalidArgument, "%s must be one of: public, low, medium, high, hyper", name)
	}
	return nil
}

func validateMemoryType(name, value string) error {
	if !schema.IsValidMemoryType(schema.MemoryType(value)) {
		return status.Errorf(codes.InvalidArgument, "%s must be one of: episodic, working, semantic, competence, plan_graph, entity", name)
	}
	return nil
}

// compile-time assertion
var _ pb.MembraneServiceServer = (*Handler)(nil)

// CaptureMemory converts the gRPC request and delegates to Membrane.CaptureMemory.
func (h *Handler) CaptureMemory(ctx context.Context, req *pb.CaptureMemoryRequest) (*pb.CaptureMemoryResponse, error) {
	if err := validateStringField("source", req.Source); err != nil {
		return nil, err
	}
	if err := validateStringField("source_kind", req.SourceKind); err != nil {
		return nil, err
	}
	if err := validateStringField("reason_to_remember", req.ReasonToRemember); err != nil {
		return nil, err
	}
	if err := validateStringField("summary", req.Summary); err != nil {
		return nil, err
	}
	if err := validateTags(req.Tags); err != nil {
		return nil, err
	}
	if err := validateSensitivity("sensitivity", req.Sensitivity, false); err != nil {
		return nil, err
	}
	if req.ProposedType != "" {
		if err := validateMemoryType("proposed_type", req.ProposedType); err != nil {
			return nil, err
		}
	}
	if err := validateJSONPayload("content", req.Content); err != nil {
		return nil, err
	}
	if err := validateJSONPayload("context", req.Context); err != nil {
		return nil, err
	}

	ts, err := parseOptionalTime(req.Timestamp)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid timestamp: %v", err)
	}

	var content any
	if len(req.Content) > 0 {
		if err := json.Unmarshal(req.Content, &content); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid content JSON: %v", err)
		}
	}
	var captureContext any
	if len(req.Context) > 0 {
		if err := json.Unmarshal(req.Context, &captureContext); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid context JSON: %v", err)
		}
	}

	resp, err := h.membrane.CaptureMemory(ctx, ingestion.CaptureMemoryRequest{
		Source:           req.Source,
		SourceKind:       req.SourceKind,
		Content:          content,
		Context:          captureContext,
		ReasonToRemember: req.ReasonToRemember,
		ProposedType:     schema.MemoryType(req.ProposedType),
		Summary:          req.Summary,
		Tags:             req.Tags,
		Scope:            req.Scope,
		Sensitivity:      schema.Sensitivity(req.Sensitivity),
		Timestamp:        ts,
	})
	if err != nil {
		return nil, serviceErr(err)
	}

	return marshalCaptureMemoryResponse(resp)
}

// RetrieveGraph converts the gRPC request and delegates to Membrane.RetrieveGraph.
func (h *Handler) RetrieveGraph(ctx context.Context, req *pb.RetrieveGraphRequest) (*pb.RetrieveGraphResponse, error) {
	if req.Trust == nil {
		return nil, status.Error(codes.InvalidArgument, "trust context is required")
	}
	if err := validateSensitivity("trust.max_sensitivity", req.Trust.MaxSensitivity, true); err != nil {
		return nil, err
	}
	if req.MinSalience < 0 || math.IsNaN(req.MinSalience) || math.IsInf(req.MinSalience, 0) {
		return nil, status.Error(codes.InvalidArgument, "min_salience must be non-negative and finite")
	}
	for _, v := range []struct {
		name  string
		value int32
	}{
		{name: "root_limit", value: req.RootLimit},
		{name: "node_limit", value: req.NodeLimit},
		{name: "edge_limit", value: req.EdgeLimit},
		{name: "max_hops", value: req.MaxHops},
	} {
		if v.value < 0 || v.value > maxLimit {
			return nil, status.Errorf(codes.InvalidArgument, "%s must be between 0 and %d", v.name, maxLimit)
		}
	}

	memTypes := make([]schema.MemoryType, len(req.MemoryTypes))
	for i, mt := range req.MemoryTypes {
		if err := validateMemoryType(fmt.Sprintf("memory_types[%d]", i), mt); err != nil {
			return nil, err
		}
		memTypes[i] = schema.MemoryType(mt)
	}

	resp, err := h.membrane.RetrieveGraph(ctx, &retrieval.RetrieveGraphRequest{
		TaskDescriptor: req.TaskDescriptor,
		Trust:          toTrustContext(req.Trust),
		MemoryTypes:    memTypes,
		MinSalience:    req.MinSalience,
		RootLimit:      int(req.RootLimit),
		NodeLimit:      int(req.NodeLimit),
		EdgeLimit:      int(req.EdgeLimit),
		MaxHops:        int(req.MaxHops),
	})
	if err != nil {
		return nil, serviceErr(err)
	}

	return marshalRetrieveGraphResponse(resp)
}

// RetrieveByID converts the gRPC request and delegates to Membrane.RetrieveByID.
func (h *Handler) RetrieveByID(ctx context.Context, req *pb.RetrieveByIDRequest) (*pb.MemoryRecordResponse, error) {
	if req.Trust == nil {
		return nil, status.Error(codes.InvalidArgument, "trust context is required")
	}
	if err := validateSensitivity("trust.max_sensitivity", req.Trust.MaxSensitivity, true); err != nil {
		return nil, err
	}

	trust := toTrustContext(req.Trust)

	rec, err := h.membrane.RetrieveByID(ctx, req.Id, trust)
	if err != nil {
		return nil, serviceErr(err)
	}

	return marshalMemoryRecordResponse(rec)
}

// Supersede converts the gRPC request and delegates to Membrane.Supersede.
func (h *Handler) Supersede(ctx context.Context, req *pb.SupersedeRequest) (*pb.MemoryRecordResponse, error) {
	if err := validateJSONPayload("new_record", req.NewRecord); err != nil {
		return nil, err
	}

	newRec, err := unmarshalRecord(req.NewRecord)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid new_record: %v", err)
	}

	rec, err := h.membrane.Supersede(ctx, req.OldId, newRec, req.Actor, req.Rationale)
	if err != nil {
		return nil, serviceErr(err)
	}

	return marshalMemoryRecordResponse(rec)
}

// Fork converts the gRPC request and delegates to Membrane.Fork.
func (h *Handler) Fork(ctx context.Context, req *pb.ForkRequest) (*pb.MemoryRecordResponse, error) {
	if err := validateJSONPayload("forked_record", req.ForkedRecord); err != nil {
		return nil, err
	}

	forkedRec, err := unmarshalRecord(req.ForkedRecord)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid forked_record: %v", err)
	}

	rec, err := h.membrane.Fork(ctx, req.SourceId, forkedRec, req.Actor, req.Rationale)
	if err != nil {
		return nil, serviceErr(err)
	}

	return marshalMemoryRecordResponse(rec)
}

// Retract converts the gRPC request and delegates to Membrane.Retract.
func (h *Handler) Retract(ctx context.Context, req *pb.RetractRequest) (*pb.RetractResponse, error) {
	if err := h.membrane.Retract(ctx, req.Id, req.Actor, req.Rationale); err != nil {
		return nil, serviceErr(err)
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
	if err := validateJSONPayload("merged_record", req.MergedRecord); err != nil {
		return nil, err
	}

	mergedRec, err := unmarshalRecord(req.MergedRecord)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid merged_record: %v", err)
	}

	rec, err := h.membrane.Merge(ctx, req.Ids, mergedRec, req.Actor, req.Rationale)
	if err != nil {
		return nil, serviceErr(err)
	}

	return marshalMemoryRecordResponse(rec)
}

// Reinforce converts the gRPC request and delegates to Membrane.Reinforce.
func (h *Handler) Reinforce(ctx context.Context, req *pb.ReinforceRequest) (*pb.ReinforceResponse, error) {
	if err := validateStringField("actor", req.Actor); err != nil {
		return nil, err
	}
	if err := validateStringField("rationale", req.Rationale); err != nil {
		return nil, err
	}

	if err := h.membrane.Reinforce(ctx, req.Id, req.Actor, req.Rationale); err != nil {
		return nil, serviceErr(err)
	}
	return &pb.ReinforceResponse{}, nil
}

// Penalize converts the gRPC request and delegates to Membrane.Penalize.
func (h *Handler) Penalize(ctx context.Context, req *pb.PenalizeRequest) (*pb.PenalizeResponse, error) {
	if req.Amount < 0 || math.IsNaN(req.Amount) || math.IsInf(req.Amount, 0) {
		return nil, status.Error(codes.InvalidArgument, "amount must be non-negative and finite")
	}
	if err := validateStringField("actor", req.Actor); err != nil {
		return nil, err
	}
	if err := validateStringField("rationale", req.Rationale); err != nil {
		return nil, err
	}

	if err := h.membrane.Penalize(ctx, req.Id, req.Amount, req.Actor, req.Rationale); err != nil {
		return nil, serviceErr(err)
	}
	return &pb.PenalizeResponse{}, nil
}

// GetMetrics delegates to Membrane.GetMetrics and returns a JSON-encoded snapshot.
func (h *Handler) GetMetrics(ctx context.Context, _ *pb.GetMetricsRequest) (*pb.MetricsResponse, error) {
	snap, err := h.membrane.GetMetrics(ctx)
	if err != nil {
		return nil, serviceErr(err)
	}

	data, err := json.Marshal(snap)
	if err != nil {
		return nil, internalErr(fmt.Errorf("marshal metrics: %w", err))
	}

	return &pb.MetricsResponse{Snapshot: data}, nil
}

// Contest converts the gRPC request and delegates to Membrane.Contest.
func (h *Handler) Contest(ctx context.Context, req *pb.ContestRequest) (*pb.ContestResponse, error) {
	if err := validateStringField("actor", req.Actor); err != nil {
		return nil, err
	}
	if err := validateStringField("rationale", req.Rationale); err != nil {
		return nil, err
	}

	if err := h.membrane.Contest(ctx, req.Id, req.ContestingRef, req.Actor, req.Rationale); err != nil {
		return nil, serviceErr(err)
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

// marshalMemoryRecordResponse JSON-encodes a MemoryRecord into a MemoryRecordResponse.
func marshalMemoryRecordResponse(rec *schema.MemoryRecord) (*pb.MemoryRecordResponse, error) {
	data, err := json.Marshal(rec)
	if err != nil {
		return nil, internalErr(fmt.Errorf("marshal record: %w", err))
	}
	return &pb.MemoryRecordResponse{Record: data}, nil
}

func marshalCaptureMemoryResponse(resp *ingestion.CaptureMemoryResponse) (*pb.CaptureMemoryResponse, error) {
	if resp == nil {
		return &pb.CaptureMemoryResponse{}, nil
	}
	primaryBytes, err := json.Marshal(resp.PrimaryRecord)
	if err != nil {
		return nil, internalErr(fmt.Errorf("marshal primary_record: %w", err))
	}
	createdRecords := make([][]byte, 0, len(resp.CreatedRecords))
	for _, rec := range resp.CreatedRecords {
		data, err := json.Marshal(rec)
		if err != nil {
			return nil, internalErr(fmt.Errorf("marshal created_record: %w", err))
		}
		createdRecords = append(createdRecords, data)
	}
	edgeBytes, err := json.Marshal(resp.Edges)
	if err != nil {
		return nil, internalErr(fmt.Errorf("marshal capture edges: %w", err))
	}
	return &pb.CaptureMemoryResponse{
		PrimaryRecord:  primaryBytes,
		CreatedRecords: createdRecords,
		Edges:          edgeBytes,
	}, nil
}

func marshalRetrieveGraphResponse(resp *retrieval.RetrieveGraphResponse) (*pb.RetrieveGraphResponse, error) {
	if resp == nil {
		return &pb.RetrieveGraphResponse{}, nil
	}
	nodeBytes, err := json.Marshal(resp.Nodes)
	if err != nil {
		return nil, internalErr(fmt.Errorf("marshal graph nodes: %w", err))
	}
	edgeBytes, err := json.Marshal(resp.Edges)
	if err != nil {
		return nil, internalErr(fmt.Errorf("marshal graph edges: %w", err))
	}
	var selBytes []byte
	if resp.Selection != nil {
		selBytes, err = json.Marshal(resp.Selection)
		if err != nil {
			return nil, internalErr(fmt.Errorf("marshal graph selection: %w", err))
		}
	}
	return &pb.RetrieveGraphResponse{
		Nodes:     nodeBytes,
		Edges:     edgeBytes,
		RootIds:   resp.RootIDs,
		Selection: selBytes,
	}, nil
}

func serviceErr(err error) error {
	if err == nil {
		return nil
	}
	if st, ok := status.FromError(err); ok && st.Code() != codes.Unknown {
		return err
	}

	var validationErr *schema.ValidationError
	switch {
	case errors.As(err, &validationErr):
		return status.Error(codes.InvalidArgument, validationErr.Error())
	case errors.Is(err, storage.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, storage.ErrAlreadyExists):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, retrieval.ErrAccessDenied):
		return status.Error(codes.PermissionDenied, err.Error())
	case errors.Is(err, retrieval.ErrNilTrust):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, revision.ErrEpisodicImmutable):
		return status.Error(codes.FailedPrecondition, err.Error())
	case isInvalidInputError(err):
		return status.Error(codes.InvalidArgument, err.Error())
	default:
		return internalErr(err)
	}
}

func isInvalidInputError(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "ingestion policy:") ||
		strings.Contains(msg, "semantic revision requires evidence") ||
		strings.HasSuffix(msg, " is not episodic")
}

// internalErr wraps an error as a gRPC Internal status.
func internalErr(err error) error {
	return status.Errorf(codes.Internal, "%v", err)
}
