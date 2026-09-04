package grpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"

	pb "github.com/BennettSchwartz/membrane/api/grpc/gen/membranev1"
	"github.com/BennettSchwartz/membrane/pkg/ingestion"
	"github.com/BennettSchwartz/membrane/pkg/membrane"
	"github.com/BennettSchwartz/membrane/pkg/metrics"
	"github.com/BennettSchwartz/membrane/pkg/retrieval"
	"github.com/BennettSchwartz/membrane/pkg/revision"
	"github.com/BennettSchwartz/membrane/pkg/schema"
	"github.com/BennettSchwartz/membrane/pkg/storage"
)

// ---------------------------------------------------------------------------
// Handler implements pb.MembraneServiceServer.
// ---------------------------------------------------------------------------

// Handler implements the MembraneServiceServer interface by delegating to
// the Membrane API.
type Handler struct {
	pb.UnimplementedMembraneServiceServer
	membrane       *membrane.Membrane
	access         *accessPolicy
	boundedRecords boundedHandlerRecords
}

type boundedHandlerRecords interface {
	RetrieveProjectedByID(context.Context, string, *retrieval.TrustContext) (*retrieval.ProjectedRecordResponse, error)
	GetAuthorizationMetadata(context.Context, []string) ([]storage.RecordAuthorizationMetadata, error)
}

const (
	maxPayloadSize          = 10 * 1024 * 1024 // 10 MB
	maxStringLength         = 100_000          // 100 KB
	maxTags                 = 100
	maxTagLength            = 256
	maxLimit                = 10_000
	maxRecordRelations      = 100
	maxAuthorizationTargets = storage.MaxAuthorizationMetadataIDs
	maxJSONDepth            = 64
	maxJSONValues           = 100_000
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

func validateValuePayload(name string, value *structpb.Value) error {
	if value == nil {
		return nil
	}
	budget := jsonPayloadBudget{values: maxJSONValues, bytes: maxPayloadSize}
	if err := budget.validate(value, 0); err != nil {
		return status.Errorf(codes.InvalidArgument, "%s %v", name, err)
	}
	data, err := json.Marshal(value.AsInterface())
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "%s must be JSON-compatible: %v", name, err)
	}
	return validateJSONPayload(name, data)
}

// Bound traversal and encoded size before AsInterface and Marshal allocate a
// second representation. Keep only the top-level field name in errors: building
// a complete path at every level makes deeply nested keys quadratic in size.
type jsonPayloadBudget struct {
	values int
	bytes  int
}

func (b *jsonPayloadBudget) consume(size int) error {
	if size > b.bytes {
		return fmt.Errorf("exceeds maximum payload size of %d bytes", maxPayloadSize)
	}
	b.bytes -= size
	return nil
}

func (b *jsonPayloadBudget) validate(value *structpb.Value, depth int) error {
	if depth > maxJSONDepth {
		return fmt.Errorf("exceeds maximum JSON depth of %d", maxJSONDepth)
	}
	if b.values <= 0 {
		return fmt.Errorf("exceeds maximum JSON value count of %d", maxJSONValues)
	}
	b.values--
	if value == nil {
		return b.consume(4) // null
	}
	switch kind := value.Kind.(type) {
	case *structpb.Value_NumberValue:
		if math.IsNaN(kind.NumberValue) || math.IsInf(kind.NumberValue, 0) {
			return errors.New("must be finite")
		}
		// Match encoding/json's finite float formatting without allocating.
		format := byte('f')
		abs := math.Abs(kind.NumberValue)
		if abs != 0 && (abs < 1e-6 || abs >= 1e21) {
			format = 'e'
		}
		var scratch [32]byte
		encoded := strconv.AppendFloat(scratch[:0], kind.NumberValue, format, -1, 64)
		size := len(encoded)
		if format == 'e' && encoded[size-4] == 'e' && encoded[size-3] == '-' && encoded[size-2] == '0' {
			size-- // encoding/json removes the zero in negative exponents.
		}
		return b.consume(size)
	case *structpb.Value_StringValue:
		return b.consumeString(kind.StringValue)
	case *structpb.Value_BoolValue:
		if kind.BoolValue {
			return b.consume(4)
		}
		return b.consume(5)
	case *structpb.Value_StructValue:
		fields := kind.StructValue.GetFields()
		if len(fields) > b.values {
			return fmt.Errorf("exceeds maximum JSON value count of %d", maxJSONValues)
		}
		if err := b.consume(2 + max(0, len(fields)-1) + len(fields)); err != nil { // braces, commas, colons
			return err
		}
		for key, child := range fields {
			if err := b.consumeString(key); err != nil {
				return err
			}
			if err := b.validate(child, depth+1); err != nil {
				return err
			}
		}
	case *structpb.Value_ListValue:
		values := kind.ListValue.GetValues()
		if len(values) > b.values {
			return fmt.Errorf("exceeds maximum JSON value count of %d", maxJSONValues)
		}
		if err := b.consume(2 + max(0, len(values)-1)); err != nil { // brackets and commas
			return err
		}
		for _, child := range values {
			if err := b.validate(child, depth+1); err != nil {
				return err
			}
		}
	default:
		return b.consume(4) // null, including an unset protobuf Value
	}
	return nil
}

func (b *jsonPayloadBudget) consumeString(value string) error {
	if err := b.consume(2); err != nil { // quotes
		return err
	}
	if err := b.consume(len(value)); err != nil {
		return err
	}
	for i := 0; i < len(value); {
		c := value[i]
		extra := 0
		if c < utf8.RuneSelf {
			i++
			switch c {
			case '"', '\\', '\n', '\r', '\t', '\b', '\f':
				extra = 1
			case '<', '>', '&':
				extra = 5
			default:
				if c < 0x20 {
					extra = 5
				}
			}
		} else {
			r, width := utf8.DecodeRuneInString(value[i:])
			i += width
			if r == utf8.RuneError && width == 1 {
				extra = 5 // invalid UTF-8 becomes \ufffd
			} else if r == '\u2028' || r == '\u2029' {
				extra = 3
			}
		}
		if err := b.consume(extra); err != nil {
			return err
		}
	}
	return nil
}

func validateFloat32Slice(name string, values []float32) error {
	nonZero := false
	for i, value := range values {
		asFloat64 := float64(value)
		if math.IsNaN(asFloat64) || math.IsInf(asFloat64, 0) {
			return status.Errorf(codes.InvalidArgument, "%s[%d] must be finite", name, i)
		}
		if value != 0 {
			nonZero = true
		}
	}
	if len(values) > 0 && !nonZero {
		return status.Errorf(codes.InvalidArgument, "%s must be non-zero", name)
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

func validateTrustContext(trust *pb.TrustContext) error {
	if trust == nil {
		return status.Error(codes.InvalidArgument, "trust context is required")
	}
	if err := validateSensitivity("trust.max_sensitivity", trust.MaxSensitivity, true); err != nil {
		return err
	}
	if err := validateStringField("trust.actor_id", trust.ActorId); err != nil {
		return err
	}
	if len(trust.Scopes) > maxTags {
		return status.Errorf(codes.InvalidArgument, "too many trust.scopes: %d (max %d)", len(trust.Scopes), maxTags)
	}
	for i, scope := range trust.Scopes {
		name := fmt.Sprintf("trust.scopes[%d]", i)
		if err := validateStringField(name, scope); err != nil {
			return err
		}
		if strings.TrimSpace(scope) == "" {
			return status.Errorf(codes.InvalidArgument, "%s must be non-empty", name)
		}
	}
	return nil
}

func validateSourceKind(name, value string) error {
	if !ingestion.IsValidSourceKind(value) {
		return status.Errorf(codes.InvalidArgument, "%s must be one of: event, tool_output, observation, working_state, agent_turn", name)
	}
	return nil
}

func validateMemoryType(name, value string) error {
	if !schema.IsValidMemoryType(schema.MemoryType(value)) {
		return status.Errorf(codes.InvalidArgument, "%s must be one of: episodic, working, semantic, competence, plan_graph, entity", name)
	}
	return nil
}

type validationField struct {
	name     string
	value    string
	required bool
}

func requiredField(name, value string) validationField {
	return validationField{name: name, value: value, required: true}
}

func optionalField(name, value string) validationField {
	return validationField{name: name, value: value}
}

func validateRevisionStrings(fields ...validationField) error {
	for _, field := range fields {
		if err := validateStringField(field.name, field.value); err != nil {
			return err
		}
		if field.required && strings.TrimSpace(field.value) == "" {
			return status.Errorf(codes.InvalidArgument, "%s is required", field.name)
		}
	}
	return nil
}

// compile-time assertion
var _ pb.MembraneServiceServer = (*Handler)(nil)

// CaptureMemory converts the gRPC request and delegates to Membrane.CaptureMemory.
func (h *Handler) CaptureMemory(ctx context.Context, req *pb.CaptureMemoryRequest) (*pb.CaptureMemoryResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if err := validateStringField("source", req.Source); err != nil {
		return nil, err
	}
	if err := validateStringField("source_kind", req.SourceKind); err != nil {
		return nil, err
	}
	if err := validateSourceKind("source_kind", req.SourceKind); err != nil {
		return nil, err
	}
	if err := validateStringField("reason_to_remember", req.ReasonToRemember); err != nil {
		return nil, err
	}
	if err := validateStringField("summary", req.Summary); err != nil {
		return nil, err
	}
	if err := validateStringField("scope", req.Scope); err != nil {
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
	if err := validateValuePayload("content", req.Content); err != nil {
		return nil, err
	}
	if err := validateValuePayload("context", req.Context); err != nil {
		return nil, err
	}
	if err := h.authorizeNewMemory(ctx, req.Scope, schema.Sensitivity(req.Sensitivity)); err != nil {
		return nil, err
	}

	ts, err := parseOptionalTime(req.Timestamp)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid timestamp: %v", err)
	}

	captureReq := ingestion.CaptureMemoryRequest{
		Source:           req.Source,
		SourceKind:       req.SourceKind,
		Content:          valueFromPB(req.Content),
		Context:          valueFromPB(req.Context),
		ReasonToRemember: req.ReasonToRemember,
		ProposedType:     schema.MemoryType(req.ProposedType),
		Summary:          req.Summary,
		Tags:             req.Tags,
		Scope:            req.Scope,
		Sensitivity:      schema.Sensitivity(req.Sensitivity),
		Timestamp:        ts,
	}
	var resp *ingestion.CaptureMemoryResponse
	if h.access == nil {
		resp, err = h.membrane.CaptureMemory(ctx, captureReq)
	} else {
		readTrust, trustErr := h.access.readTrust(ctx, h.access.readMax, nil)
		if trustErr != nil {
			return nil, trustErr
		}
		resp, err = h.membrane.CaptureMemoryWithAccess(ctx, captureReq, ingestion.CaptureAccess{
			CanRead: readTrust.Allows,
			CanWrite: func(record *schema.MemoryRecord) bool {
				return h.access.allowsWriteRecord(ctx, record)
			},
		})
	}
	if err != nil {
		return nil, serviceErr(err)
	}

	return marshalCaptureMemoryResponse(resp)
}

// RetrieveGraph converts the gRPC request and delegates to Membrane.RetrieveGraph.
func (h *Handler) RetrieveGraph(ctx context.Context, req *pb.RetrieveGraphRequest) (*pb.RetrieveGraphResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if req.Trust == nil {
		return nil, status.Error(codes.InvalidArgument, "trust context is required")
	}
	if err := validateStringField("task_descriptor", req.TaskDescriptor); err != nil {
		return nil, err
	}
	if err := validateTrustContext(req.Trust); err != nil {
		return nil, err
	}
	if req.MinSalience < 0 || req.MinSalience > 1 || math.IsNaN(req.MinSalience) || math.IsInf(req.MinSalience, 0) {
		return nil, status.Error(codes.InvalidArgument, "min_salience must be finite and between 0 and 1")
	}
	if err := validateFloat32Slice("query_embedding", req.QueryEmbedding); err != nil {
		return nil, err
	}
	for _, v := range []struct {
		name  string
		value int32
		min   int32
	}{
		{name: "root_limit", value: req.RootLimit, min: 0},
		{name: "node_limit", value: req.NodeLimit, min: 0},
		{name: "edge_limit", value: req.EdgeLimit, min: 0},
		{name: "max_hops", value: req.MaxHops, min: -1},
	} {
		if v.value < v.min || v.value > maxLimit {
			return nil, status.Errorf(codes.InvalidArgument, "%s must be between %d and %d", v.name, v.min, maxLimit)
		}
	}

	memTypes := make([]schema.MemoryType, len(req.MemoryTypes))
	for i, mt := range req.MemoryTypes {
		if err := validateMemoryType(fmt.Sprintf("memory_types[%d]", i), mt); err != nil {
			return nil, err
		}
		memTypes[i] = schema.MemoryType(mt)
	}

	trust, err := h.effectiveReadTrust(ctx, req.Trust)
	if err != nil {
		return nil, err
	}

	resp, err := h.membrane.RetrieveGraph(ctx, &retrieval.RetrieveGraphRequest{
		TaskDescriptor: req.TaskDescriptor,
		QueryEmbedding: req.QueryEmbedding,
		Trust:          trust,
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

// RetrieveByID returns the bounded network projection for one exact ID. The
// trusted in-process Membrane.RetrieveByID API remains complete.
func (h *Handler) RetrieveByID(ctx context.Context, req *pb.RetrieveByIDRequest) (*pb.MemoryRecordResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if req.Trust == nil {
		return nil, status.Error(codes.InvalidArgument, "trust context is required")
	}
	if err := validateRevisionStrings(requiredField("id", req.Id)); err != nil {
		return nil, err
	}
	if err := validateTrustContext(req.Trust); err != nil {
		return nil, err
	}

	trust, err := h.effectiveReadTrust(ctx, req.Trust)
	if err != nil {
		return nil, err
	}

	bounded := h.boundedRecordSource()
	if bounded == nil {
		return nil, internalErr(retrieval.ErrBoundedRetrievalUnsupported)
	}
	result, err := bounded.RetrieveProjectedByID(ctx, req.Id, trust)
	if err != nil {
		return nil, serviceErr(err)
	}
	if result == nil {
		return nil, internalErr(errors.New("retrieve projected record returned nil response"))
	}
	return marshalMemoryRecordResponseWithProjection(result.Record, result.Projection)
}

// Supersede converts the gRPC request and delegates to Membrane.Supersede.
func (h *Handler) Supersede(ctx context.Context, req *pb.SupersedeRequest) (*pb.MemoryRecordResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if err := validateRevisionStrings(
		requiredField("old_id", req.OldId),
		optionalField("actor", req.Actor),
		optionalField("rationale", req.Rationale),
	); err != nil {
		return nil, err
	}
	newRec, err := validatedRevisionNewRecordFromPB(req.NewRecord)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid new_record: %v", err)
	}
	// Revision records created through the public transport always receive a
	// fresh server-assigned ID. Besides making identity server-owned, clearing
	// it before relation authorization prevents a caller-declared self target
	// from being excluded from referenced-record policy checks.
	if err := h.authorizeExistingMemory(ctx, req.OldId); err != nil {
		return nil, err
	}
	if err := h.authorizeMemoryRecord(ctx, newRec); err != nil {
		return nil, err
	}
	if err := h.authorizeReferencedMemories(ctx, newRec); err != nil {
		return nil, err
	}

	rec, err := h.membrane.Supersede(ctx, req.OldId, newRec, h.authorizedActor(ctx, req.Actor), req.Rationale)
	if err != nil {
		return nil, serviceErr(err)
	}

	return marshalMemoryRecordResponse(rec)
}

// Fork converts the gRPC request and delegates to Membrane.Fork.
func (h *Handler) Fork(ctx context.Context, req *pb.ForkRequest) (*pb.MemoryRecordResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if err := validateRevisionStrings(
		requiredField("source_id", req.SourceId),
		optionalField("actor", req.Actor),
		optionalField("rationale", req.Rationale),
	); err != nil {
		return nil, err
	}
	forkedRec, err := validatedRevisionNewRecordFromPB(req.ForkedRecord)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid forked_record: %v", err)
	}
	if err := h.authorizeExistingMemory(ctx, req.SourceId); err != nil {
		return nil, err
	}
	if err := h.authorizeMemoryRecord(ctx, forkedRec); err != nil {
		return nil, err
	}
	if err := h.authorizeReferencedMemories(ctx, forkedRec); err != nil {
		return nil, err
	}

	rec, err := h.membrane.Fork(ctx, req.SourceId, forkedRec, h.authorizedActor(ctx, req.Actor), req.Rationale)
	if err != nil {
		return nil, serviceErr(err)
	}

	return marshalMemoryRecordResponse(rec)
}

// Retract converts the gRPC request and delegates to Membrane.Retract.
func (h *Handler) Retract(ctx context.Context, req *pb.RetractRequest) (*pb.RetractResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if err := validateRevisionStrings(
		requiredField("id", req.Id),
		optionalField("actor", req.Actor),
		optionalField("rationale", req.Rationale),
	); err != nil {
		return nil, err
	}
	if err := h.authorizeExistingMemory(ctx, req.Id); err != nil {
		return nil, err
	}
	if err := h.membrane.Retract(ctx, req.Id, h.authorizedActor(ctx, req.Actor), req.Rationale); err != nil {
		return nil, serviceErr(err)
	}
	return &pb.RetractResponse{}, nil
}

// Merge converts the gRPC request and delegates to Membrane.Merge.
func (h *Handler) Merge(ctx context.Context, req *pb.MergeRequest) (*pb.MemoryRecordResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if len(req.Ids) == 0 {
		return nil, status.Error(codes.InvalidArgument, "ids must not be empty")
	}
	if len(req.Ids) > maxAuthorizationTargets {
		return nil, status.Errorf(codes.InvalidArgument, "too many ids: %d (max %d)", len(req.Ids), maxAuthorizationTargets)
	}
	for i, id := range req.Ids {
		if err := validateRevisionStrings(requiredField(fmt.Sprintf("ids[%d]", i), id)); err != nil {
			return nil, err
		}
	}
	if err := validateUniqueMergeIDs(req.Ids); err != nil {
		return nil, err
	}
	if err := validateRevisionStrings(
		optionalField("actor", req.Actor),
		optionalField("rationale", req.Rationale),
	); err != nil {
		return nil, err
	}
	mergedRec, err := validatedRevisionNewRecordFromPB(req.MergedRecord)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid merged_record: %v", err)
	}
	if err := h.authorizeExistingMemories(ctx, req.Ids); err != nil {
		return nil, err
	}
	if err := h.authorizeMemoryRecord(ctx, mergedRec); err != nil {
		return nil, err
	}
	if err := h.authorizeReferencedMemories(ctx, mergedRec); err != nil {
		return nil, err
	}

	rec, err := h.membrane.Merge(ctx, req.Ids, mergedRec, h.authorizedActor(ctx, req.Actor), req.Rationale)
	if err != nil {
		return nil, serviceErr(err)
	}

	return marshalMemoryRecordResponse(rec)
}

func validateUniqueMergeIDs(ids []string) error {
	seen := make(map[string]struct{}, len(ids))
	for i, id := range ids {
		normalized := strings.TrimSpace(id)
		if _, ok := seen[normalized]; ok {
			return status.Errorf(codes.InvalidArgument, "ids[%d] duplicates earlier merge source ID %q", i, normalized)
		}
		seen[normalized] = struct{}{}
	}
	return nil
}

// Reinforce converts the gRPC request and delegates to Membrane.Reinforce.
func (h *Handler) Reinforce(ctx context.Context, req *pb.ReinforceRequest) (*pb.ReinforceResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if err := validateRevisionStrings(
		requiredField("id", req.Id),
		optionalField("actor", req.Actor),
		optionalField("rationale", req.Rationale),
	); err != nil {
		return nil, err
	}

	if err := h.authorizeExistingMemory(ctx, req.Id); err != nil {
		return nil, err
	}
	if err := h.membrane.Reinforce(ctx, req.Id, h.authorizedActor(ctx, req.Actor), req.Rationale); err != nil {
		return nil, serviceErr(err)
	}
	return &pb.ReinforceResponse{}, nil
}

// Penalize converts the gRPC request and delegates to Membrane.Penalize.
func (h *Handler) Penalize(ctx context.Context, req *pb.PenalizeRequest) (*pb.PenalizeResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if req.Amount < 0 || math.IsNaN(req.Amount) || math.IsInf(req.Amount, 0) {
		return nil, status.Error(codes.InvalidArgument, "amount must be non-negative and finite")
	}
	if err := validateRevisionStrings(
		requiredField("id", req.Id),
		optionalField("actor", req.Actor),
		optionalField("rationale", req.Rationale),
	); err != nil {
		return nil, err
	}

	if err := h.authorizeExistingMemory(ctx, req.Id); err != nil {
		return nil, err
	}
	if err := h.membrane.Penalize(ctx, req.Id, req.Amount, h.authorizedActor(ctx, req.Actor), req.Rationale); err != nil {
		return nil, serviceErr(err)
	}
	return &pb.PenalizeResponse{}, nil
}

// GetMetrics derives observability visibility from the server read policy and
// returns only aggregates over records the caller may read.
func (h *Handler) GetMetrics(ctx context.Context, _ *pb.GetMetricsRequest) (*pb.MetricsResponse, error) {
	trust, err := h.metricsReadTrust(ctx)
	if err != nil {
		return nil, err
	}
	var snap *metrics.Snapshot
	if trust == nil {
		snap, err = h.membrane.GetMetrics(ctx)
	} else {
		snap, err = h.membrane.GetMetricsForTrust(ctx, trust)
	}
	if err != nil {
		return nil, serviceErr(err)
	}

	value, _ := valueToPB(snap)
	return &pb.MetricsResponse{Snapshot: value}, nil
}

// Contest converts the gRPC request and delegates to Membrane.Contest.
func (h *Handler) Contest(ctx context.Context, req *pb.ContestRequest) (*pb.ContestResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if err := validateRevisionStrings(
		requiredField("id", req.Id),
		optionalField("contesting_ref", req.ContestingRef),
		optionalField("actor", req.Actor),
		optionalField("rationale", req.Rationale),
	); err != nil {
		return nil, err
	}

	if err := h.authorizeExistingMemory(ctx, req.Id); err != nil {
		return nil, err
	}
	actor := h.authorizedActor(ctx, req.Actor)
	var err error
	if h.access == nil {
		err = h.membrane.Contest(ctx, req.Id, req.ContestingRef, actor, req.Rationale)
	} else {
		err = h.membrane.ContestWithAccess(ctx, req.Id, req.ContestingRef, actor, req.Rationale, func(record *schema.MemoryRecord) bool {
			return h.access.allowsWriteRecord(ctx, record)
		})
	}
	if err != nil {
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

func (h *Handler) effectiveReadTrust(ctx context.Context, requested *pb.TrustContext) (*retrieval.TrustContext, error) {
	if h.access == nil {
		return toTrustContext(requested), nil
	}
	return h.access.readTrust(ctx, schema.Sensitivity(requested.MaxSensitivity), requested.Scopes)
}

func (h *Handler) metricsReadTrust(ctx context.Context) (*retrieval.TrustContext, error) {
	if h.access == nil {
		return nil, nil
	}
	return h.access.readTrust(ctx, h.access.readMax, nil)
}

func (h *Handler) boundedRecordSource() boundedHandlerRecords {
	if h.boundedRecords != nil {
		return h.boundedRecords
	}
	if h.membrane != nil {
		return h.membrane
	}
	return nil
}

func (h *Handler) authorizeNewMemory(ctx context.Context, scope string, sensitivity schema.Sensitivity) error {
	if h.access == nil {
		return nil
	}
	if sensitivity == "" {
		sensitivity = h.access.defaultSensitivity
	}
	return h.authorizeScopeSensitivity(ctx, scope, sensitivity)
}

func (h *Handler) authorizeMemoryRecord(ctx context.Context, rec *schema.MemoryRecord) error {
	if h.access == nil || rec == nil {
		return nil
	}
	return h.authorizeScopeSensitivity(ctx, rec.Scope, rec.Sensitivity)
}

func (h *Handler) authorizeScopeSensitivity(ctx context.Context, scope string, sensitivity schema.Sensitivity) error {
	trust := h.access.writeTrust(ctx)
	if !isSensitivityAllowed(sensitivity, trust.MaxSensitivity) {
		return status.Errorf(codes.PermissionDenied, "sensitivity %q exceeds the server write policy", sensitivity)
	}
	if strings.TrimSpace(scope) == "" {
		return status.Error(codes.PermissionDenied, "scope is required by the server write policy")
	}
	if !scopeAllowed(scope, trust.Scopes) {
		return status.Errorf(codes.PermissionDenied, "scope %q is outside the server write policy", scope)
	}
	return nil
}

func (h *Handler) authorizeExistingMemory(ctx context.Context, id string) error {
	if h.access == nil {
		return nil
	}
	return h.authorizeExistingMemories(ctx, []string{id})
}

const unavailableRelationTargetMessage = "relation target is unavailable under the server write policy"

func (h *Handler) authorizeExistingMemories(ctx context.Context, ids []string) error {
	if h.access == nil || len(ids) == 0 {
		return nil
	}
	if len(ids) > maxAuthorizationTargets {
		return status.Errorf(codes.InvalidArgument, "too many authorization targets: %d (max %d)", len(ids), maxAuthorizationTargets)
	}
	metadata, err := h.getAuthorizationMetadata(ctx, ids)
	if err != nil {
		return serviceErr(err)
	}
	byID := sanitizeAuthorizationMetadata(metadata, ids)
	for _, id := range ids {
		record, ok := byID[id]
		if !ok || !h.authorizationMetadataAllowed(ctx, record) {
			// Primary/source mutation targets use the same opaque disposition
			// whether the ID is absent or exists outside the write policy.
			return serviceErr(storage.ErrNotFound)
		}
	}
	return nil
}

func (h *Handler) authorizeReferencedMemories(ctx context.Context, rec *schema.MemoryRecord) error {
	if h.access == nil || rec == nil {
		return nil
	}
	if len(rec.Relations) > maxAuthorizationTargets {
		return status.Error(codes.PermissionDenied, unavailableRelationTargetMessage)
	}
	for _, relation := range rec.Relations {
		if strings.TrimSpace(relation.TargetID) == "" {
			return status.Error(codes.PermissionDenied, unavailableRelationTargetMessage)
		}
	}
	ids := referencedMemoryIDs(rec)
	if len(ids) == 0 {
		return nil
	}
	if len(ids) > maxAuthorizationTargets {
		return status.Error(codes.PermissionDenied, unavailableRelationTargetMessage)
	}
	metadata, err := h.getAuthorizationMetadata(ctx, ids)
	if err != nil {
		return status.Error(codes.PermissionDenied, unavailableRelationTargetMessage)
	}
	byID := sanitizeAuthorizationMetadata(metadata, ids)
	for _, id := range ids {
		record, ok := byID[id]
		if !ok || !h.authorizationMetadataAllowed(ctx, record) {
			return status.Error(codes.PermissionDenied, unavailableRelationTargetMessage)
		}
	}
	return nil
}

func (h *Handler) getAuthorizationMetadata(ctx context.Context, ids []string) ([]storage.RecordAuthorizationMetadata, error) {
	bounded := h.boundedRecordSource()
	if bounded == nil {
		return nil, storage.ErrAuthorizationMetadataUnsupported
	}
	return bounded.GetAuthorizationMetadata(ctx, ids)
}

func (h *Handler) authorizationMetadataAllowed(ctx context.Context, record storage.RecordAuthorizationMetadata) bool {
	if h.access == nil || strings.TrimSpace(record.Scope) == "" {
		return false
	}
	trust := h.access.writeTrust(ctx)
	return isSensitivityAllowed(record.Sensitivity, trust.MaxSensitivity) && scopeAllowed(record.Scope, trust.Scopes)
}

func sanitizeAuthorizationMetadata(records []storage.RecordAuthorizationMetadata, requested []string) map[string]storage.RecordAuthorizationMetadata {
	limit := len(requested)
	if limit > maxAuthorizationTargets {
		limit = maxAuthorizationTargets
	}
	if len(records) > limit {
		records = records[:limit]
	}
	requestedSet := make(map[string]struct{}, limit)
	for i, id := range requested {
		if i >= limit {
			break
		}
		requestedSet[id] = struct{}{}
	}
	byID := make(map[string]storage.RecordAuthorizationMetadata, limit)
	for _, record := range records {
		if _, ok := requestedSet[record.ID]; !ok {
			continue
		}
		if _, exists := byID[record.ID]; !exists {
			byID[record.ID] = record
		}
	}
	return byID
}

func referencedMemoryIDs(rec *schema.MemoryRecord) []string {
	if rec == nil {
		return nil
	}
	capacity := len(rec.Relations)
	if capacity > maxAuthorizationTargets+1 {
		capacity = maxAuthorizationTargets + 1
	}
	ids := make([]string, 0, capacity)
	seen := make(map[string]struct{}, capacity)
	add := func(id string) {
		if len(ids) > maxAuthorizationTargets {
			return
		}
		if strings.TrimSpace(id) == "" || id == rec.ID {
			return
		}
		if _, ok := seen[id]; ok {
			return
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	for _, rel := range rec.Relations {
		add(rel.TargetID)
		if len(ids) > maxAuthorizationTargets {
			break
		}
	}
	return ids
}

func (h *Handler) authorizedActor(ctx context.Context, requested string) string {
	if h.access == nil {
		return requested
	}
	return h.access.actor(ctx, requested)
}

func valueFromPB(value *structpb.Value) any {
	if value == nil {
		return nil
	}
	return value.AsInterface()
}

func valueToPB(value any) (*structpb.Value, error) {
	if value == nil {
		return structpb.NewNullValue(), nil
	}
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var normalized any
	_ = json.Unmarshal(data, &normalized)
	return structpb.NewValue(normalized)
}

func valueMapToPB(values map[string]any) (map[string]*structpb.Value, error) {
	if len(values) == 0 {
		return nil, nil
	}
	out := make(map[string]*structpb.Value, len(values))
	for key, value := range values {
		pbValue, err := valueToPB(value)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", key, err)
		}
		out[key] = pbValue
	}
	return out, nil
}

func valueMapFromPB(values map[string]*structpb.Value) map[string]any {
	if len(values) == 0 {
		return nil
	}
	out := make(map[string]any, len(values))
	for key, value := range values {
		out[key] = valueFromPB(value)
	}
	return out
}

func timeToString(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

func timePtrToString(t *time.Time) string {
	if t == nil {
		return ""
	}
	return timeToString(*t)
}

func parseTimeValue(value string) (time.Time, error) {
	if value == "" {
		return time.Time{}, nil
	}
	return time.Parse(time.RFC3339Nano, value)
}

func parseTimePtr(value string) (*time.Time, error) {
	parsed, err := parseTimeValue(value)
	if err != nil || parsed.IsZero() {
		return nil, err
	}
	return &parsed, nil
}

func memoryRecordToPB(rec *schema.MemoryRecord) (*pb.MemoryRecord, error) {
	if rec == nil {
		return nil, nil
	}
	var payload *pb.Payload
	if rec.Payload != nil {
		var err error
		payload, err = payloadToPB(rec.Payload)
		if err != nil {
			return nil, err
		}
	}
	return &pb.MemoryRecord{
		Id:             rec.ID,
		Type:           string(rec.Type),
		Sensitivity:    string(rec.Sensitivity),
		Confidence:     rec.Confidence,
		Salience:       rec.Salience,
		Scope:          rec.Scope,
		Tags:           rec.Tags,
		CreatedAt:      timeToString(rec.CreatedAt),
		UpdatedAt:      timeToString(rec.UpdatedAt),
		Lifecycle:      lifecycleToPB(rec.Lifecycle),
		Provenance:     provenanceToPB(rec.Provenance),
		Relations:      relationsToPB(rec.Relations),
		Payload:        payload,
		Interpretation: interpretationToPB(rec.Interpretation),
		AuditLog:       auditLogToPB(rec.AuditLog),
	}, nil
}

func memoryRecordFromPB(rec *pb.MemoryRecord) (*schema.MemoryRecord, error) {
	if rec == nil {
		return nil, errors.New("record is required")
	}
	payload, err := payloadFromPB(rec.Payload)
	if err != nil {
		return nil, err
	}
	createdAt, err := parseTimeValue(rec.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("created_at: %w", err)
	}
	updatedAt, err := parseTimeValue(rec.UpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("updated_at: %w", err)
	}
	lifecycle, err := lifecycleFromPB(rec.Lifecycle)
	if err != nil {
		return nil, err
	}
	provenance, err := provenanceFromPB(rec.Provenance)
	if err != nil {
		return nil, err
	}
	auditLog, err := auditLogFromPB(rec.AuditLog)
	if err != nil {
		return nil, err
	}
	relations, err := relationsFromPB(rec.Relations)
	if err != nil {
		return nil, err
	}
	return &schema.MemoryRecord{
		ID:             rec.Id,
		Type:           schema.MemoryType(rec.Type),
		Sensitivity:    schema.Sensitivity(rec.Sensitivity),
		Confidence:     rec.Confidence,
		Salience:       rec.Salience,
		Scope:          rec.Scope,
		Tags:           rec.Tags,
		CreatedAt:      createdAt,
		UpdatedAt:      updatedAt,
		Lifecycle:      lifecycle,
		Provenance:     provenance,
		Relations:      relations,
		Payload:        payload,
		Interpretation: interpretationFromPB(rec.Interpretation),
		AuditLog:       auditLog,
	}, nil
}

func validatedMemoryRecordFromPB(rec *pb.MemoryRecord) (*schema.MemoryRecord, error) {
	if rec != nil && len(rec.Relations) > maxRecordRelations {
		return nil, fmt.Errorf("too many relations: %d (max %d)", len(rec.Relations), maxRecordRelations)
	}
	out, err := memoryRecordFromPB(rec)
	if err != nil {
		return nil, err
	}
	if err := out.Validate(); err != nil {
		return nil, err
	}
	return out, nil
}

// validatedRevisionNewRecordFromPB validates a public revision record while
// keeping its identity server-owned. The converted record's ID is cleared
// before validation and is never restored; a private copy receives a sentinel
// solely so MemoryRecord.Validate can check every non-ID invariant.
func validatedRevisionNewRecordFromPB(rec *pb.MemoryRecord) (*schema.MemoryRecord, error) {
	if rec != nil && len(rec.Relations) > maxRecordRelations {
		return nil, fmt.Errorf("too many relations: %d (max %d)", len(rec.Relations), maxRecordRelations)
	}
	out, err := memoryRecordFromPB(rec)
	if err != nil {
		return nil, err
	}
	out.ID = ""
	validationRecord := *out
	validationRecord.ID = "server-assigned-revision-id"
	if err := validationRecord.Validate(); err != nil {
		return nil, err
	}
	return out, nil
}

func lifecycleToPB(lifecycle schema.Lifecycle) *pb.Lifecycle {
	return &pb.Lifecycle{
		Decay:            decayProfileToPB(lifecycle.Decay),
		LastReinforcedAt: timeToString(lifecycle.LastReinforcedAt),
		Pinned:           lifecycle.Pinned,
		DeletionPolicy:   string(lifecycle.DeletionPolicy),
	}
}

func lifecycleFromPB(lifecycle *pb.Lifecycle) (schema.Lifecycle, error) {
	if lifecycle == nil {
		return schema.Lifecycle{}, nil
	}
	lastReinforcedAt, err := parseTimeValue(lifecycle.LastReinforcedAt)
	if err != nil {
		return schema.Lifecycle{}, fmt.Errorf("lifecycle.last_reinforced_at: %w", err)
	}
	return schema.Lifecycle{
		Decay:            decayProfileFromPB(lifecycle.Decay),
		LastReinforcedAt: lastReinforcedAt,
		Pinned:           lifecycle.Pinned,
		DeletionPolicy:   schema.DeletionPolicy(lifecycle.DeletionPolicy),
	}, nil
}

func decayProfileToPB(decay schema.DecayProfile) *pb.DecayProfile {
	return &pb.DecayProfile{
		Curve:             string(decay.Curve),
		HalfLifeSeconds:   decay.HalfLifeSeconds,
		MinSalience:       decay.MinSalience,
		MaxAgeSeconds:     decay.MaxAgeSeconds,
		ReinforcementGain: decay.ReinforcementGain,
	}
}

func decayProfileFromPB(decay *pb.DecayProfile) schema.DecayProfile {
	if decay == nil {
		return schema.DecayProfile{}
	}
	return schema.DecayProfile{
		Curve:             schema.DecayCurve(decay.Curve),
		HalfLifeSeconds:   decay.HalfLifeSeconds,
		MinSalience:       decay.MinSalience,
		MaxAgeSeconds:     decay.MaxAgeSeconds,
		ReinforcementGain: decay.ReinforcementGain,
	}
}

func provenanceToPB(provenance schema.Provenance) *pb.Provenance {
	return &pb.Provenance{
		Sources:   provenanceSourcesToPB(provenance.Sources),
		CreatedBy: provenance.CreatedBy,
	}
}

func provenanceFromPB(provenance *pb.Provenance) (schema.Provenance, error) {
	if provenance == nil {
		return schema.Provenance{}, nil
	}
	sources, err := provenanceSourcesFromPB(provenance.Sources)
	if err != nil {
		return schema.Provenance{}, err
	}
	return schema.Provenance{Sources: sources, CreatedBy: provenance.CreatedBy}, nil
}

func provenanceSourcesToPB(sources []schema.ProvenanceSource) []*pb.ProvenanceSource {
	out := make([]*pb.ProvenanceSource, 0, len(sources))
	for _, source := range sources {
		out = append(out, &pb.ProvenanceSource{
			Kind:      string(source.Kind),
			Ref:       source.Ref,
			Timestamp: timeToString(source.Timestamp),
			Hash:      source.Hash,
			CreatedBy: source.CreatedBy,
		})
	}
	return out
}

func provenanceSourcesFromPB(sources []*pb.ProvenanceSource) ([]schema.ProvenanceSource, error) {
	out := make([]schema.ProvenanceSource, 0, len(sources))
	for i, source := range sources {
		if source == nil {
			continue
		}
		timestamp, err := parseTimeValue(source.Timestamp)
		if err != nil {
			return nil, fmt.Errorf("provenance.sources[%d].timestamp: %w", i, err)
		}
		out = append(out, schema.ProvenanceSource{
			Kind:      schema.ProvenanceKind(source.Kind),
			Ref:       source.Ref,
			Hash:      source.Hash,
			CreatedBy: source.CreatedBy,
			Timestamp: timestamp,
		})
	}
	return out, nil
}

func relationsToPB(relations []schema.Relation) []*pb.Relation {
	out := make([]*pb.Relation, 0, len(relations))
	for _, relation := range relations {
		out = append(out, &pb.Relation{
			Predicate: relation.Predicate,
			TargetId:  relation.TargetID,
			Weight:    relation.Weight,
			CreatedAt: timeToString(relation.CreatedAt),
		})
	}
	return out
}

func relationsFromPB(relations []*pb.Relation) ([]schema.Relation, error) {
	out := make([]schema.Relation, 0, len(relations))
	for i, relation := range relations {
		if relation == nil {
			continue
		}
		createdAt, err := parseTimeValue(relation.CreatedAt)
		if err != nil {
			return nil, fmt.Errorf("relations[%d].created_at: %w", i, err)
		}
		out = append(out, schema.Relation{
			Predicate: relation.Predicate,
			TargetID:  relation.TargetId,
			Weight:    relation.Weight,
			CreatedAt: createdAt,
		})
	}
	return out, nil
}

func graphEdgesToPB(edges []schema.GraphEdge) []*pb.GraphEdge {
	out := make([]*pb.GraphEdge, 0, len(edges))
	for _, edge := range edges {
		out = append(out, &pb.GraphEdge{
			SourceId:  edge.SourceID,
			Predicate: edge.Predicate,
			TargetId:  edge.TargetID,
			Weight:    edge.Weight,
			CreatedAt: timeToString(edge.CreatedAt),
		})
	}
	return out
}

func graphNodeToPB(node retrieval.GraphNode) (*pb.GraphNode, error) {
	rec, err := memoryRecordToPB(node.Record)
	if err != nil {
		return nil, err
	}
	return &pb.GraphNode{Record: rec, Root: node.Root, Hop: int32(node.Hop)}, nil
}

func selectionToPB(selection *retrieval.SelectionResult) (*pb.SelectionResult, error) {
	if selection == nil {
		return nil, nil
	}
	selected := make([]*pb.MemoryRecord, 0, len(selection.Selected))
	for _, rec := range selection.Selected {
		out, err := memoryRecordToPB(rec)
		if err != nil {
			return nil, err
		}
		selected = append(selected, out)
	}
	return &pb.SelectionResult{
		Selected:   selected,
		Confidence: selection.Confidence,
		NeedsMore:  selection.NeedsMore,
		Scores:     selection.Scores,
	}, nil
}

func auditLogToPB(entries []schema.AuditEntry) []*pb.AuditEntry {
	out := make([]*pb.AuditEntry, 0, len(entries))
	for _, entry := range entries {
		out = append(out, &pb.AuditEntry{
			Action:    string(entry.Action),
			Actor:     entry.Actor,
			Timestamp: timeToString(entry.Timestamp),
			Rationale: entry.Rationale,
		})
	}
	return out
}

func auditLogFromPB(entries []*pb.AuditEntry) ([]schema.AuditEntry, error) {
	out := make([]schema.AuditEntry, 0, len(entries))
	for i, entry := range entries {
		if entry == nil {
			continue
		}
		timestamp, err := parseTimeValue(entry.Timestamp)
		if err != nil {
			return nil, fmt.Errorf("audit_log[%d].timestamp: %w", i, err)
		}
		out = append(out, schema.AuditEntry{
			Action:    schema.AuditAction(entry.Action),
			Actor:     entry.Actor,
			Timestamp: timestamp,
			Rationale: entry.Rationale,
		})
	}
	return out, nil
}

func interpretationToPB(interpretation *schema.Interpretation) *pb.Interpretation {
	if interpretation == nil {
		return nil
	}
	return &pb.Interpretation{
		Status:               string(interpretation.Status),
		Summary:              interpretation.Summary,
		ProposedType:         string(interpretation.ProposedType),
		TopicalLabels:        interpretation.TopicalLabels,
		Mentions:             mentionsToPB(interpretation.Mentions),
		RelationCandidates:   relationCandidatesToPB(interpretation.RelationCandidates),
		ReferenceCandidates:  referenceCandidatesToPB(interpretation.ReferenceCandidates),
		ExtractionConfidence: interpretation.ExtractionConfidence,
	}
}

func interpretationFromPB(interpretation *pb.Interpretation) *schema.Interpretation {
	if interpretation == nil {
		return nil
	}
	return &schema.Interpretation{
		Status:               schema.InterpretationStatus(interpretation.Status),
		Summary:              interpretation.Summary,
		ProposedType:         schema.MemoryType(interpretation.ProposedType),
		TopicalLabels:        interpretation.TopicalLabels,
		Mentions:             mentionsFromPB(interpretation.Mentions),
		RelationCandidates:   relationCandidatesFromPB(interpretation.RelationCandidates),
		ReferenceCandidates:  referenceCandidatesFromPB(interpretation.ReferenceCandidates),
		ExtractionConfidence: interpretation.ExtractionConfidence,
	}
}

func mentionsToPB(mentions []schema.Mention) []*pb.Mention {
	out := make([]*pb.Mention, 0, len(mentions))
	for _, mention := range mentions {
		out = append(out, &pb.Mention{
			Surface:           mention.Surface,
			EntityKind:        string(mention.EntityKind),
			CanonicalEntityId: mention.CanonicalEntityID,
			Confidence:        mention.Confidence,
			Aliases:           mention.Aliases,
		})
	}
	return out
}

func mentionsFromPB(mentions []*pb.Mention) []schema.Mention {
	out := make([]schema.Mention, 0, len(mentions))
	for _, mention := range mentions {
		if mention == nil {
			continue
		}
		out = append(out, schema.Mention{
			Surface:           mention.Surface,
			EntityKind:        schema.EntityKind(mention.EntityKind),
			CanonicalEntityID: mention.CanonicalEntityId,
			Confidence:        mention.Confidence,
			Aliases:           mention.Aliases,
		})
	}
	return out
}

func relationCandidatesToPB(candidates []schema.RelationCandidate) []*pb.RelationCandidate {
	out := make([]*pb.RelationCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		out = append(out, &pb.RelationCandidate{
			Predicate:      candidate.Predicate,
			TargetRecordId: candidate.TargetRecordID,
			TargetEntityId: candidate.TargetEntityID,
			Confidence:     candidate.Confidence,
			Resolved:       candidate.Resolved,
		})
	}
	return out
}

func relationCandidatesFromPB(candidates []*pb.RelationCandidate) []schema.RelationCandidate {
	out := make([]schema.RelationCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate == nil {
			continue
		}
		out = append(out, schema.RelationCandidate{
			Predicate:      candidate.Predicate,
			TargetRecordID: candidate.TargetRecordId,
			TargetEntityID: candidate.TargetEntityId,
			Confidence:     candidate.Confidence,
			Resolved:       candidate.Resolved,
		})
	}
	return out
}

func referenceCandidatesToPB(candidates []schema.ReferenceCandidate) []*pb.ReferenceCandidate {
	out := make([]*pb.ReferenceCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		out = append(out, &pb.ReferenceCandidate{
			Ref:            candidate.Ref,
			TargetRecordId: candidate.TargetRecordID,
			TargetEntityId: candidate.TargetEntityID,
			Confidence:     candidate.Confidence,
			Resolved:       candidate.Resolved,
		})
	}
	return out
}

func referenceCandidatesFromPB(candidates []*pb.ReferenceCandidate) []schema.ReferenceCandidate {
	out := make([]schema.ReferenceCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate == nil {
			continue
		}
		out = append(out, schema.ReferenceCandidate{
			Ref:            candidate.Ref,
			TargetRecordID: candidate.TargetRecordId,
			TargetEntityID: candidate.TargetEntityId,
			Confidence:     candidate.Confidence,
			Resolved:       candidate.Resolved,
		})
	}
	return out
}

func payloadToPB(payload schema.Payload) (*pb.Payload, error) {
	switch p := payload.(type) {
	case *schema.EpisodicPayload:
		out, err := episodicPayloadToPB(p)
		if err != nil {
			return nil, err
		}
		return &pb.Payload{Kind: &pb.Payload_Episodic{Episodic: out}}, nil
	case schema.EpisodicPayload:
		out, err := episodicPayloadToPB(&p)
		if err != nil {
			return nil, err
		}
		return &pb.Payload{Kind: &pb.Payload_Episodic{Episodic: out}}, nil
	case *schema.WorkingPayload:
		out, err := workingPayloadToPB(p)
		if err != nil {
			return nil, err
		}
		return &pb.Payload{Kind: &pb.Payload_Working{Working: out}}, nil
	case schema.WorkingPayload:
		out, err := workingPayloadToPB(&p)
		if err != nil {
			return nil, err
		}
		return &pb.Payload{Kind: &pb.Payload_Working{Working: out}}, nil
	case *schema.SemanticPayload:
		out, err := semanticPayloadToPB(p)
		if err != nil {
			return nil, err
		}
		return &pb.Payload{Kind: &pb.Payload_Semantic{Semantic: out}}, nil
	case schema.SemanticPayload:
		out, err := semanticPayloadToPB(&p)
		if err != nil {
			return nil, err
		}
		return &pb.Payload{Kind: &pb.Payload_Semantic{Semantic: out}}, nil
	case *schema.CompetencePayload:
		out, err := competencePayloadToPB(p)
		if err != nil {
			return nil, err
		}
		return &pb.Payload{Kind: &pb.Payload_Competence{Competence: out}}, nil
	case schema.CompetencePayload:
		out, err := competencePayloadToPB(&p)
		if err != nil {
			return nil, err
		}
		return &pb.Payload{Kind: &pb.Payload_Competence{Competence: out}}, nil
	case *schema.PlanGraphPayload:
		out, err := planGraphPayloadToPB(p)
		if err != nil {
			return nil, err
		}
		return &pb.Payload{Kind: &pb.Payload_PlanGraph{PlanGraph: out}}, nil
	case schema.PlanGraphPayload:
		out, err := planGraphPayloadToPB(&p)
		if err != nil {
			return nil, err
		}
		return &pb.Payload{Kind: &pb.Payload_PlanGraph{PlanGraph: out}}, nil
	case *schema.EntityPayload:
		return &pb.Payload{Kind: &pb.Payload_Entity{Entity: entityPayloadToPB(p)}}, nil
	case schema.EntityPayload:
		return &pb.Payload{Kind: &pb.Payload_Entity{Entity: entityPayloadToPB(&p)}}, nil
	default:
		return nil, fmt.Errorf("unsupported payload type %T", payload)
	}
}

func payloadFromPB(payload *pb.Payload) (schema.Payload, error) {
	if payload == nil || payload.GetKind() == nil {
		return nil, errors.New("payload is required")
	}
	switch p := payload.GetKind().(type) {
	case *pb.Payload_Episodic:
		return episodicPayloadFromPB(p.Episodic)
	case *pb.Payload_Working:
		return workingPayloadFromPB(p.Working), nil
	case *pb.Payload_Semantic:
		return semanticPayloadFromPB(p.Semantic)
	case *pb.Payload_Competence:
		return competencePayloadFromPB(p.Competence)
	case *pb.Payload_PlanGraph:
		return planGraphPayloadFromPB(p.PlanGraph)
	case *pb.Payload_Entity:
		return entityPayloadFromPB(p.Entity), nil
	default:
		return nil, fmt.Errorf("unsupported payload variant %T", p)
	}
}

func episodicPayloadToPB(payload *schema.EpisodicPayload) (*pb.EpisodicPayload, error) {
	if payload == nil {
		return nil, nil
	}
	toolGraph := make([]*pb.ToolNode, 0, len(payload.ToolGraph))
	for _, node := range payload.ToolGraph {
		args, err := valueMapToPB(node.Args)
		if err != nil {
			return nil, fmt.Errorf("tool_graph[%s].args: %w", node.ID, err)
		}
		result, err := valueToPB(node.Result)
		if err != nil {
			return nil, fmt.Errorf("tool_graph[%s].result: %w", node.ID, err)
		}
		toolGraph = append(toolGraph, &pb.ToolNode{
			Id:        node.ID,
			Tool:      node.Tool,
			Args:      args,
			Result:    result,
			Timestamp: timeToString(node.Timestamp),
			DependsOn: node.DependsOn,
		})
	}
	timeline := make([]*pb.TimelineEvent, 0, len(payload.Timeline))
	for _, event := range payload.Timeline {
		timeline = append(timeline, &pb.TimelineEvent{
			T:         timeToString(event.T),
			EventKind: event.EventKind,
			Ref:       event.Ref,
			Summary:   event.Summary,
		})
	}
	environment, err := environmentToPB(payload.Environment)
	if err != nil {
		return nil, err
	}
	return &pb.EpisodicPayload{
		Kind:         payload.Kind,
		Timeline:     timeline,
		ToolGraph:    toolGraph,
		Environment:  environment,
		Outcome:      string(payload.Outcome),
		Artifacts:    payload.Artifacts,
		ToolGraphRef: payload.ToolGraphRef,
	}, nil
}

func episodicPayloadFromPB(payload *pb.EpisodicPayload) (*schema.EpisodicPayload, error) {
	if payload == nil {
		return nil, errors.New("episodic payload is required")
	}
	timeline := make([]schema.TimelineEvent, 0, len(payload.Timeline))
	for i, event := range payload.Timeline {
		if event == nil {
			continue
		}
		t, err := parseTimeValue(event.T)
		if err != nil {
			return nil, fmt.Errorf("timeline[%d].t: %w", i, err)
		}
		timeline = append(timeline, schema.TimelineEvent{
			T:         t,
			EventKind: event.EventKind,
			Ref:       event.Ref,
			Summary:   event.Summary,
		})
	}
	toolGraph := make([]schema.ToolNode, 0, len(payload.ToolGraph))
	for i, node := range payload.ToolGraph {
		if node == nil {
			continue
		}
		timestamp, err := parseTimeValue(node.Timestamp)
		if err != nil {
			return nil, fmt.Errorf("tool_graph[%d].timestamp: %w", i, err)
		}
		toolGraph = append(toolGraph, schema.ToolNode{
			ID:        node.Id,
			Tool:      node.Tool,
			Args:      valueMapFromPB(node.Args),
			Result:    valueFromPB(node.Result),
			Timestamp: timestamp,
			DependsOn: node.DependsOn,
		})
	}
	return &schema.EpisodicPayload{
		Kind:         payload.Kind,
		Timeline:     timeline,
		ToolGraph:    toolGraph,
		Environment:  environmentFromPB(payload.Environment),
		Outcome:      schema.OutcomeStatus(payload.Outcome),
		Artifacts:    payload.Artifacts,
		ToolGraphRef: payload.ToolGraphRef,
	}, nil
}

func environmentToPB(environment *schema.EnvironmentSnapshot) (*pb.EnvironmentSnapshot, error) {
	if environment == nil {
		return nil, nil
	}
	context, err := valueMapToPB(environment.Context)
	if err != nil {
		return nil, fmt.Errorf("environment.context: %w", err)
	}
	return &pb.EnvironmentSnapshot{
		Os:               environment.OS,
		OsVersion:        environment.OSVersion,
		ToolVersions:     environment.ToolVersions,
		WorkingDirectory: environment.WorkingDirectory,
		Context:          context,
	}, nil
}

func environmentFromPB(environment *pb.EnvironmentSnapshot) *schema.EnvironmentSnapshot {
	if environment == nil {
		return nil
	}
	return &schema.EnvironmentSnapshot{
		OS:               environment.Os,
		OSVersion:        environment.OsVersion,
		ToolVersions:     environment.ToolVersions,
		WorkingDirectory: environment.WorkingDirectory,
		Context:          valueMapFromPB(environment.Context),
	}
}

func workingPayloadToPB(payload *schema.WorkingPayload) (*pb.WorkingPayload, error) {
	if payload == nil {
		return nil, nil
	}
	constraints := make([]*pb.Constraint, 0, len(payload.ActiveConstraints))
	for _, constraint := range payload.ActiveConstraints {
		value, err := valueToPB(constraint.Value)
		if err != nil {
			return nil, fmt.Errorf("active_constraints[%s].value: %w", constraint.Key, err)
		}
		constraints = append(constraints, &pb.Constraint{
			Type:     constraint.Type,
			Key:      constraint.Key,
			Value:    value,
			Required: constraint.Required,
		})
	}
	return &pb.WorkingPayload{
		Kind:              payload.Kind,
		ThreadId:          payload.ThreadID,
		State:             string(payload.State),
		ActiveConstraints: constraints,
		NextActions:       payload.NextActions,
		OpenQuestions:     payload.OpenQuestions,
		ContextSummary:    payload.ContextSummary,
	}, nil
}

func workingPayloadFromPB(payload *pb.WorkingPayload) *schema.WorkingPayload {
	if payload == nil {
		return nil
	}
	constraints := make([]schema.Constraint, 0, len(payload.ActiveConstraints))
	for _, constraint := range payload.ActiveConstraints {
		if constraint == nil {
			continue
		}
		constraints = append(constraints, schema.Constraint{
			Type:     constraint.Type,
			Key:      constraint.Key,
			Value:    valueFromPB(constraint.Value),
			Required: constraint.Required,
		})
	}
	return &schema.WorkingPayload{
		Kind:              payload.Kind,
		ThreadID:          payload.ThreadId,
		State:             schema.TaskState(payload.State),
		ActiveConstraints: constraints,
		NextActions:       payload.NextActions,
		OpenQuestions:     payload.OpenQuestions,
		ContextSummary:    payload.ContextSummary,
	}
}

func semanticPayloadToPB(payload *schema.SemanticPayload) (*pb.SemanticPayload, error) {
	if payload == nil {
		return nil, nil
	}
	object, err := valueToPB(payload.Object)
	if err != nil {
		return nil, fmt.Errorf("object: %w", err)
	}
	conditions, err := valueMapToPB(payload.Validity.Conditions)
	if err != nil {
		return nil, fmt.Errorf("validity.conditions: %w", err)
	}
	return &pb.SemanticPayload{
		Kind:      payload.Kind,
		Subject:   payload.Subject,
		Predicate: payload.Predicate,
		Object:    object,
		Validity: &pb.Validity{
			Mode:       string(payload.Validity.Mode),
			Conditions: conditions,
			Start:      timePtrToString(payload.Validity.Start),
			End:        timePtrToString(payload.Validity.End),
		},
		Evidence:       evidenceToPB(payload.Evidence),
		RevisionPolicy: payload.RevisionPolicy,
		Revision:       revisionStateToPB(payload.Revision),
	}, nil
}

func semanticPayloadFromPB(payload *pb.SemanticPayload) (*schema.SemanticPayload, error) {
	if payload == nil {
		return nil, errors.New("semantic payload is required")
	}
	validity, err := validityFromPB(payload.Validity)
	if err != nil {
		return nil, err
	}
	evidence, err := evidenceFromPB(payload.Evidence)
	if err != nil {
		return nil, err
	}
	return &schema.SemanticPayload{
		Kind:           payload.Kind,
		Subject:        payload.Subject,
		Predicate:      payload.Predicate,
		Object:         valueFromPB(payload.Object),
		Validity:       validity,
		Evidence:       evidence,
		RevisionPolicy: payload.RevisionPolicy,
		Revision:       revisionStateFromPB(payload.Revision),
	}, nil
}

func validityFromPB(validity *pb.Validity) (schema.Validity, error) {
	if validity == nil {
		return schema.Validity{}, nil
	}
	start, err := parseTimePtr(validity.Start)
	if err != nil {
		return schema.Validity{}, fmt.Errorf("validity.start: %w", err)
	}
	end, err := parseTimePtr(validity.End)
	if err != nil {
		return schema.Validity{}, fmt.Errorf("validity.end: %w", err)
	}
	return schema.Validity{
		Mode:       schema.ValidityMode(validity.Mode),
		Conditions: valueMapFromPB(validity.Conditions),
		Start:      start,
		End:        end,
	}, nil
}

func evidenceToPB(evidence []schema.ProvenanceRef) []*pb.ProvenanceRef {
	out := make([]*pb.ProvenanceRef, 0, len(evidence))
	for _, ref := range evidence {
		out = append(out, &pb.ProvenanceRef{
			SourceType: ref.SourceType,
			SourceId:   ref.SourceID,
			Timestamp:  timeToString(ref.Timestamp),
		})
	}
	return out
}

func evidenceFromPB(evidence []*pb.ProvenanceRef) ([]schema.ProvenanceRef, error) {
	out := make([]schema.ProvenanceRef, 0, len(evidence))
	for i, ref := range evidence {
		if ref == nil {
			continue
		}
		timestamp, err := parseTimeValue(ref.Timestamp)
		if err != nil {
			return nil, fmt.Errorf("evidence[%d].timestamp: %w", i, err)
		}
		out = append(out, schema.ProvenanceRef{
			SourceType: ref.SourceType,
			SourceID:   ref.SourceId,
			Timestamp:  timestamp,
		})
	}
	return out, nil
}

func revisionStateToPB(revision *schema.RevisionState) *pb.RevisionState {
	if revision == nil {
		return nil
	}
	return &pb.RevisionState{
		Supersedes:   revision.Supersedes,
		SupersededBy: revision.SupersededBy,
		Status:       string(revision.Status),
	}
}

func revisionStateFromPB(revision *pb.RevisionState) *schema.RevisionState {
	if revision == nil {
		return nil
	}
	return &schema.RevisionState{
		Supersedes:   revision.Supersedes,
		SupersededBy: revision.SupersededBy,
		Status:       schema.RevisionStatus(revision.Status),
	}
}

func entityPayloadToPB(payload *schema.EntityPayload) *pb.EntityPayload {
	if payload == nil {
		return nil
	}
	return &pb.EntityPayload{
		Kind:          payload.Kind,
		CanonicalName: payload.CanonicalName,
		PrimaryType:   payload.PrimaryType,
		Types:         payload.Types,
		Aliases:       entityAliasesToPB(payload.Aliases),
		Identifiers:   entityIdentifiersToPB(payload.Identifiers),
		Summary:       payload.Summary,
	}
}

func entityPayloadFromPB(payload *pb.EntityPayload) *schema.EntityPayload {
	if payload == nil {
		return nil
	}
	return &schema.EntityPayload{
		Kind:          payload.Kind,
		CanonicalName: payload.CanonicalName,
		PrimaryType:   payload.PrimaryType,
		Types:         payload.Types,
		Aliases:       entityAliasesFromPB(payload.Aliases),
		Identifiers:   entityIdentifiersFromPB(payload.Identifiers),
		Summary:       payload.Summary,
	}
}

func entityAliasesToPB(aliases []schema.EntityAlias) []*pb.EntityAlias {
	out := make([]*pb.EntityAlias, 0, len(aliases))
	for _, alias := range aliases {
		out = append(out, &pb.EntityAlias{Value: alias.Value, Kind: alias.Kind, Locale: alias.Locale})
	}
	return out
}

func entityAliasesFromPB(aliases []*pb.EntityAlias) []schema.EntityAlias {
	out := make([]schema.EntityAlias, 0, len(aliases))
	for _, alias := range aliases {
		if alias == nil {
			continue
		}
		out = append(out, schema.EntityAlias{Value: alias.Value, Kind: alias.Kind, Locale: alias.Locale})
	}
	return out
}

func entityIdentifiersToPB(identifiers []schema.EntityIdentifier) []*pb.EntityIdentifier {
	out := make([]*pb.EntityIdentifier, 0, len(identifiers))
	for _, identifier := range identifiers {
		out = append(out, &pb.EntityIdentifier{Namespace: identifier.Namespace, Value: identifier.Value})
	}
	return out
}

func entityIdentifiersFromPB(identifiers []*pb.EntityIdentifier) []schema.EntityIdentifier {
	out := make([]schema.EntityIdentifier, 0, len(identifiers))
	for _, identifier := range identifiers {
		if identifier == nil {
			continue
		}
		out = append(out, schema.EntityIdentifier{Namespace: identifier.Namespace, Value: identifier.Value})
	}
	return out
}

func competencePayloadToPB(payload *schema.CompetencePayload) (*pb.CompetencePayload, error) {
	if payload == nil {
		return nil, nil
	}
	triggers := make([]*pb.Trigger, 0, len(payload.Triggers))
	for _, trigger := range payload.Triggers {
		conditions, err := valueMapToPB(trigger.Conditions)
		if err != nil {
			return nil, fmt.Errorf("triggers[%s].conditions: %w", trigger.Signal, err)
		}
		triggers = append(triggers, &pb.Trigger{Signal: trigger.Signal, Conditions: conditions})
	}
	recipe := make([]*pb.RecipeStep, 0, len(payload.Recipe))
	for _, step := range payload.Recipe {
		argsSchema, err := valueMapToPB(step.ArgsSchema)
		if err != nil {
			return nil, fmt.Errorf("recipe[%s].args_schema: %w", step.Step, err)
		}
		recipe = append(recipe, &pb.RecipeStep{
			Step:       step.Step,
			Tool:       step.Tool,
			ArgsSchema: argsSchema,
			Validation: step.Validation,
		})
	}
	return &pb.CompetencePayload{
		Kind:          payload.Kind,
		SkillName:     payload.SkillName,
		Triggers:      triggers,
		Recipe:        recipe,
		RequiredTools: payload.RequiredTools,
		FailureModes:  payload.FailureModes,
		Fallbacks:     payload.Fallbacks,
		Performance:   performanceStatsToPB(payload.Performance),
		Version:       payload.Version,
	}, nil
}

func competencePayloadFromPB(payload *pb.CompetencePayload) (*schema.CompetencePayload, error) {
	if payload == nil {
		return nil, nil
	}
	triggers := make([]schema.Trigger, 0, len(payload.Triggers))
	for _, trigger := range payload.Triggers {
		if trigger == nil {
			continue
		}
		triggers = append(triggers, schema.Trigger{
			Signal:     trigger.Signal,
			Conditions: valueMapFromPB(trigger.Conditions),
		})
	}
	recipe := make([]schema.RecipeStep, 0, len(payload.Recipe))
	for _, step := range payload.Recipe {
		if step == nil {
			continue
		}
		recipe = append(recipe, schema.RecipeStep{
			Step:       step.Step,
			Tool:       step.Tool,
			ArgsSchema: valueMapFromPB(step.ArgsSchema),
			Validation: step.Validation,
		})
	}
	performance, err := performanceStatsFromPB(payload.Performance)
	if err != nil {
		return nil, err
	}
	return &schema.CompetencePayload{
		Kind:          payload.Kind,
		SkillName:     payload.SkillName,
		Triggers:      triggers,
		Recipe:        recipe,
		RequiredTools: payload.RequiredTools,
		FailureModes:  payload.FailureModes,
		Fallbacks:     payload.Fallbacks,
		Performance:   performance,
		Version:       payload.Version,
	}, nil
}

func performanceStatsToPB(stats *schema.PerformanceStats) *pb.PerformanceStats {
	if stats == nil {
		return nil
	}
	return &pb.PerformanceStats{
		SuccessCount: stats.SuccessCount,
		FailureCount: stats.FailureCount,
		SuccessRate:  stats.SuccessRate,
		AvgLatencyMs: stats.AvgLatencyMs,
		LastUsedAt:   timePtrToString(stats.LastUsedAt),
	}
}

func performanceStatsFromPB(stats *pb.PerformanceStats) (*schema.PerformanceStats, error) {
	if stats == nil {
		return nil, nil
	}
	lastUsedAt, err := parseTimePtr(stats.LastUsedAt)
	if err != nil {
		return nil, fmt.Errorf("performance.last_used_at: %w", err)
	}
	return &schema.PerformanceStats{
		SuccessCount: stats.SuccessCount,
		FailureCount: stats.FailureCount,
		SuccessRate:  stats.SuccessRate,
		AvgLatencyMs: stats.AvgLatencyMs,
		LastUsedAt:   lastUsedAt,
	}, nil
}

func planGraphPayloadToPB(payload *schema.PlanGraphPayload) (*pb.PlanGraphPayload, error) {
	if payload == nil {
		return nil, nil
	}
	constraints, err := valueMapToPB(payload.Constraints)
	if err != nil {
		return nil, fmt.Errorf("constraints: %w", err)
	}
	inputsSchema, err := valueMapToPB(payload.InputsSchema)
	if err != nil {
		return nil, fmt.Errorf("inputs_schema: %w", err)
	}
	outputsSchema, err := valueMapToPB(payload.OutputsSchema)
	if err != nil {
		return nil, fmt.Errorf("outputs_schema: %w", err)
	}
	nodes := make([]*pb.PlanNode, 0, len(payload.Nodes))
	for _, node := range payload.Nodes {
		params, err := valueMapToPB(node.Params)
		if err != nil {
			return nil, fmt.Errorf("nodes[%s].params: %w", node.ID, err)
		}
		guards, err := valueMapToPB(node.Guards)
		if err != nil {
			return nil, fmt.Errorf("nodes[%s].guards: %w", node.ID, err)
		}
		nodes = append(nodes, &pb.PlanNode{
			Id:     node.ID,
			Op:     node.Op,
			Params: params,
			Guards: guards,
		})
	}
	edges := make([]*pb.PlanEdge, 0, len(payload.Edges))
	for _, edge := range payload.Edges {
		edges = append(edges, &pb.PlanEdge{From: edge.From, To: edge.To, Kind: string(edge.Kind)})
	}
	return &pb.PlanGraphPayload{
		Kind:          payload.Kind,
		PlanId:        payload.PlanID,
		Version:       payload.Version,
		Intent:        payload.Intent,
		Constraints:   constraints,
		InputsSchema:  inputsSchema,
		OutputsSchema: outputsSchema,
		Nodes:         nodes,
		Edges:         edges,
		Metrics:       planMetricsToPB(payload.Metrics),
	}, nil
}

func planGraphPayloadFromPB(payload *pb.PlanGraphPayload) (*schema.PlanGraphPayload, error) {
	if payload == nil {
		return nil, nil
	}
	nodes := make([]schema.PlanNode, 0, len(payload.Nodes))
	for _, node := range payload.Nodes {
		if node == nil {
			continue
		}
		nodes = append(nodes, schema.PlanNode{
			ID:     node.Id,
			Op:     node.Op,
			Params: valueMapFromPB(node.Params),
			Guards: valueMapFromPB(node.Guards),
		})
	}
	edges := make([]schema.PlanEdge, 0, len(payload.Edges))
	for _, edge := range payload.Edges {
		if edge == nil {
			continue
		}
		edges = append(edges, schema.PlanEdge{
			From: edge.From,
			To:   edge.To,
			Kind: schema.EdgeKind(edge.Kind),
		})
	}
	metrics, err := planMetricsFromPB(payload.Metrics)
	if err != nil {
		return nil, err
	}
	return &schema.PlanGraphPayload{
		Kind:          payload.Kind,
		PlanID:        payload.PlanId,
		Version:       payload.Version,
		Intent:        payload.Intent,
		Constraints:   valueMapFromPB(payload.Constraints),
		InputsSchema:  valueMapFromPB(payload.InputsSchema),
		OutputsSchema: valueMapFromPB(payload.OutputsSchema),
		Nodes:         nodes,
		Edges:         edges,
		Metrics:       metrics,
	}, nil
}

func planMetricsToPB(metrics *schema.PlanMetrics) *pb.PlanMetrics {
	if metrics == nil {
		return nil
	}
	return &pb.PlanMetrics{
		AvgLatencyMs:   metrics.AvgLatencyMs,
		FailureRate:    metrics.FailureRate,
		ExecutionCount: metrics.ExecutionCount,
		LastExecutedAt: timePtrToString(metrics.LastExecutedAt),
	}
}

func planMetricsFromPB(metrics *pb.PlanMetrics) (*schema.PlanMetrics, error) {
	if metrics == nil {
		return nil, nil
	}
	lastExecutedAt, err := parseTimePtr(metrics.LastExecutedAt)
	if err != nil {
		return nil, fmt.Errorf("plan_metrics.last_executed_at: %w", err)
	}
	return &schema.PlanMetrics{
		AvgLatencyMs:   metrics.AvgLatencyMs,
		FailureRate:    metrics.FailureRate,
		ExecutionCount: metrics.ExecutionCount,
		LastExecutedAt: lastExecutedAt,
	}, nil
}

// marshalMemoryRecordResponse converts a MemoryRecord into a typed MemoryRecordResponse.
func marshalMemoryRecordResponse(rec *schema.MemoryRecord) (*pb.MemoryRecordResponse, error) {
	return marshalMemoryRecordResponseWithProjection(rec, retrieval.RecordProjection{})
}

func marshalMemoryRecordResponseWithProjection(rec *schema.MemoryRecord, projection retrieval.RecordProjection) (*pb.MemoryRecordResponse, error) {
	out, err := memoryRecordToPB(rec)
	if err != nil {
		return nil, internalErr(fmt.Errorf("marshal record: %w", err))
	}
	response := &pb.MemoryRecordResponse{Record: out}
	if projection != (retrieval.RecordProjection{}) {
		response.Projection = recordProjectionToPB(projection)
	}
	return response, nil
}

func marshalCaptureMemoryResponse(resp *ingestion.CaptureMemoryResponse) (*pb.CaptureMemoryResponse, error) {
	if resp == nil {
		return &pb.CaptureMemoryResponse{}, nil
	}
	primary, err := memoryRecordToPB(resp.PrimaryRecord)
	if err != nil {
		return nil, internalErr(fmt.Errorf("marshal primary_record: %w", err))
	}
	createdRecords := make([]*pb.MemoryRecord, 0, len(resp.CreatedRecords))
	for _, rec := range resp.CreatedRecords {
		out, err := memoryRecordToPB(rec)
		if err != nil {
			return nil, internalErr(fmt.Errorf("marshal created_record: %w", err))
		}
		createdRecords = append(createdRecords, out)
	}
	edges := graphEdgesToPB(resp.Edges)
	return &pb.CaptureMemoryResponse{
		PrimaryRecord:  primary,
		CreatedRecords: createdRecords,
		Edges:          edges,
	}, nil
}

func marshalRetrieveGraphResponse(resp *retrieval.RetrieveGraphResponse) (*pb.RetrieveGraphResponse, error) {
	if resp == nil {
		return &pb.RetrieveGraphResponse{}, nil
	}
	nodes := make([]*pb.GraphNode, 0, len(resp.Nodes))
	for _, node := range resp.Nodes {
		out, err := graphNodeToPB(node)
		if err != nil {
			return nil, internalErr(fmt.Errorf("marshal graph node: %w", err))
		}
		nodes = append(nodes, out)
	}
	selection, err := selectionToPB(resp.Selection)
	if err != nil {
		return nil, internalErr(fmt.Errorf("marshal graph selection: %w", err))
	}
	return &pb.RetrieveGraphResponse{
		Nodes:       nodes,
		Edges:       graphEdgesToPB(resp.Edges),
		RootIds:     resp.RootIDs,
		Selection:   selection,
		Diagnostics: retrievalDiagnosticsToPB(resp.Diagnostics),
		Projection:  recordProjectionToPB(resp.Projection),
	}, nil
}

func recordProjectionToPB(projection retrieval.RecordProjection) *pb.RecordProjection {
	return &pb.RecordProjection{
		RelationsOmitted:   projection.RelationsOmitted,
		RelationsTruncated: projection.RelationsTruncated,
		HistoryOmitted:     projection.HistoryOmitted,
		RecordsTruncated:   projection.RecordsTruncated,
	}
}

func retrievalDiagnosticsToPB(diagnostics []retrieval.RetrievalDiagnostic) []*pb.RetrievalDiagnostic {
	if len(diagnostics) == 0 {
		return nil
	}
	out := make([]*pb.RetrievalDiagnostic, 0, len(diagnostics))
	for _, diagnostic := range diagnostics {
		out = append(out, &pb.RetrievalDiagnostic{
			Code:    diagnostic.Code,
			Message: diagnostic.Message,
		})
	}
	return out
}

func serviceErr(err error) error {
	if err == nil {
		return nil
	}
	if st, ok := status.FromError(err); ok && st.Code() != codes.Unknown {
		return err
	}

	var validationErr *schema.ValidationError
	var projectedTooLarge *retrieval.ProjectedRecordTooLargeError
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
	case errors.As(err, &projectedTooLarge):
		return status.Error(codes.ResourceExhausted, projectedTooLarge.Error())
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
		strings.Contains(msg, "ingestion: invalid source_kind") ||
		strings.Contains(msg, "ingestion: invalid proposed_type") ||
		strings.Contains(msg, "semantic revision requires evidence") ||
		strings.HasSuffix(msg, " is not episodic")
}

// internalErr wraps an error as a gRPC Internal status.
func internalErr(err error) error {
	return status.Errorf(codes.Internal, "%v", err)
}
