package grpc

import (
	"context"
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/BennettSchwartz/membrane/api/grpc/gen/membranev1"
	"github.com/BennettSchwartz/membrane/pkg/retrieval"
	"github.com/BennettSchwartz/membrane/pkg/schema"
	"github.com/BennettSchwartz/membrane/pkg/storage"
)

type fakeBoundedHandlerRecords struct {
	projected     *retrieval.ProjectedRecordResponse
	projectedErr  error
	projectedIDs  []string
	metadata      []storage.RecordAuthorizationMetadata
	metadataErr   error
	metadataCalls [][]string
}

func (f *fakeBoundedHandlerRecords) RetrieveProjectedByID(_ context.Context, id string, _ *retrieval.TrustContext) (*retrieval.ProjectedRecordResponse, error) {
	f.projectedIDs = append(f.projectedIDs, id)
	return f.projected, f.projectedErr
}

func (f *fakeBoundedHandlerRecords) GetAuthorizationMetadata(_ context.Context, ids []string) ([]storage.RecordAuthorizationMetadata, error) {
	f.metadataCalls = append(f.metadataCalls, append([]string(nil), ids...))
	return append([]storage.RecordAuthorizationMetadata(nil), f.metadata...), f.metadataErr
}

func TestHandlerRetrieveByIDUsesProjectedBoundedPath(t *testing.T) {
	record := schema.NewMemoryRecord("bounded-network-id", schema.MemoryTypeSemantic, schema.SensitivityLow, &schema.SemanticPayload{
		Kind: "semantic", Subject: "subject", Predicate: "is", Object: "bounded",
		Validity: schema.Validity{Mode: schema.ValidityModeGlobal},
	})
	record.Scope = "project:alpha"
	fake := &fakeBoundedHandlerRecords{projected: &retrieval.ProjectedRecordResponse{
		Record:     record,
		Projection: retrieval.RecordProjection{RelationsOmitted: true, HistoryOmitted: true},
	}}
	h := &Handler{boundedRecords: fake}

	resp, err := h.RetrieveByID(context.Background(), &pb.RetrieveByIDRequest{
		Id:    record.ID,
		Trust: &pb.TrustContext{MaxSensitivity: string(schema.SensitivityLow), Authenticated: true, Scopes: []string{"project:alpha"}},
	})
	if err != nil {
		t.Fatalf("RetrieveByID: %v", err)
	}
	if len(fake.projectedIDs) != 1 || fake.projectedIDs[0] != record.ID {
		t.Fatalf("projected IDs = %v, want [%s]", fake.projectedIDs, record.ID)
	}
	if resp.GetRecord().GetId() != record.ID || resp.GetProjection() == nil || !resp.GetProjection().GetRelationsOmitted() || !resp.GetProjection().GetHistoryOmitted() {
		t.Fatalf("response = %+v, want bounded record and projection", resp)
	}
}

func TestHandlerRetrieveByIDMapsProjectedOversizeToResourceExhausted(t *testing.T) {
	h := &Handler{boundedRecords: &fakeBoundedHandlerRecords{
		projectedErr: &retrieval.ProjectedRecordTooLargeError{Limit: retrieval.MaxProjectedResponseBytes},
	}}
	_, err := h.RetrieveByID(context.Background(), &pb.RetrieveByIDRequest{
		Id:    "oversize",
		Trust: &pb.TrustContext{MaxSensitivity: string(schema.SensitivityLow)},
	})
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("code = %v, want ResourceExhausted; err=%v", status.Code(err), err)
	}
}

func TestHandlerAuthorizationUsesOneMetadataBatchAndGenericRelationDenial(t *testing.T) {
	policy := &accessPolicy{
		writeMax: schema.SensitivityLow, writeScopes: []string{"project:alpha"},
		defaultSensitivity: schema.SensitivityLow,
	}
	recordWithTargets := func(ids ...string) *schema.MemoryRecord {
		rec := schema.NewMemoryRecord("new", schema.MemoryTypeSemantic, schema.SensitivityLow, &schema.SemanticPayload{
			Kind: "semantic", Subject: "subject", Predicate: "is", Object: "object",
			Validity: schema.Validity{Mode: schema.ValidityModeGlobal},
		})
		rec.Scope = "project:alpha"
		for _, id := range ids {
			rec.Relations = append(rec.Relations, schema.Relation{Predicate: "related_to", TargetID: id, Weight: 1})
		}
		return rec
	}

	fake := &fakeBoundedHandlerRecords{metadata: []storage.RecordAuthorizationMetadata{
		{ID: "target-a", Scope: "project:alpha", Sensitivity: schema.SensitivityLow},
		{ID: "target-b", Scope: "project:alpha", Sensitivity: schema.SensitivityLow},
	}}
	h := &Handler{access: policy, boundedRecords: fake}
	if err := h.authorizeReferencedMemories(context.Background(), recordWithTargets("target-a", "target-b", "target-a")); err != nil {
		t.Fatalf("authorizeReferencedMemories: %v", err)
	}
	if len(fake.metadataCalls) != 1 || len(fake.metadataCalls[0]) != 2 {
		t.Fatalf("metadata calls = %v, want one deduplicated two-ID batch", fake.metadataCalls)
	}

	missing := &fakeBoundedHandlerRecords{metadata: []storage.RecordAuthorizationMetadata{
		{ID: "target-a", Scope: "project:alpha", Sensitivity: schema.SensitivityLow},
	}}
	h.boundedRecords = missing
	missingErr := h.authorizeReferencedMemories(context.Background(), recordWithTargets("target-a", "target-b"))

	hidden := &fakeBoundedHandlerRecords{metadata: []storage.RecordAuthorizationMetadata{
		{ID: "target-a", Scope: "project:alpha", Sensitivity: schema.SensitivityLow},
		{ID: "target-b", Scope: "project:secret", Sensitivity: schema.SensitivityHigh},
	}}
	h.boundedRecords = hidden
	hiddenErr := h.authorizeReferencedMemories(context.Background(), recordWithTargets("target-a", "target-b"))
	if status.Code(missingErr) != codes.PermissionDenied || status.Code(hiddenErr) != codes.PermissionDenied || status.Convert(missingErr).Message() != status.Convert(hiddenErr).Message() {
		t.Fatalf("missing/hidden disposition = %v / %v, want identical PermissionDenied", missingErr, hiddenErr)
	}

	h.boundedRecords = &fakeBoundedHandlerRecords{metadataErr: storage.ErrAuthorizationMetadataLimit}
	if err := h.authorizeReferencedMemories(context.Background(), recordWithTargets("target")); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("metadata failure code = %v, want fail-closed PermissionDenied", status.Code(err))
	}

	overLimitIDs := make([]string, maxAuthorizationTargets+1)
	for i := range overLimitIDs {
		overLimitIDs[i] = fmt.Sprintf("target-%03d", i)
	}
	noLookup := &fakeBoundedHandlerRecords{}
	h.boundedRecords = noLookup
	if err := h.authorizeReferencedMemories(context.Background(), recordWithTargets(overLimitIDs...)); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("over-limit relation code = %v, want PermissionDenied", status.Code(err))
	}
	if len(noLookup.metadataCalls) != 0 {
		t.Fatalf("over-limit relation issued metadata lookup: %v", noLookup.metadataCalls)
	}
}

func TestHandlerAuthorizeExistingMemoryUsesMetadataNotFullRecord(t *testing.T) {
	policy := &accessPolicy{writeMax: schema.SensitivityLow, writeScopes: []string{"project:alpha"}}
	fake := &fakeBoundedHandlerRecords{metadata: []storage.RecordAuthorizationMetadata{{
		ID: "existing", Scope: "project:alpha", Sensitivity: schema.SensitivityLow,
	}}}
	h := &Handler{access: policy, boundedRecords: fake}
	if err := h.authorizeExistingMemory(context.Background(), "existing"); err != nil {
		t.Fatalf("authorizeExistingMemory: %v", err)
	}
	if len(fake.metadataCalls) != 1 || len(fake.metadataCalls[0]) != 1 || fake.metadataCalls[0][0] != "existing" {
		t.Fatalf("metadata calls = %v, want one exact ID", fake.metadataCalls)
	}

	h.boundedRecords = &fakeBoundedHandlerRecords{}
	missingErr := h.authorizeExistingMemory(context.Background(), "target")
	h.boundedRecords = &fakeBoundedHandlerRecords{metadata: []storage.RecordAuthorizationMetadata{{
		ID: "target", Scope: "project:secret", Sensitivity: schema.SensitivityHigh,
	}}}
	hiddenErr := h.authorizeExistingMemory(context.Background(), "target")
	if status.Code(missingErr) != codes.NotFound || status.Code(hiddenErr) != codes.NotFound {
		t.Fatalf("missing/hidden codes = %v/%v, want both NotFound", status.Code(missingErr), status.Code(hiddenErr))
	}
	if status.Convert(missingErr).Message() != status.Convert(hiddenErr).Message() {
		t.Fatalf("primary authorization leaked existence: missing=%q hidden=%q", status.Convert(missingErr).Message(), status.Convert(hiddenErr).Message())
	}
}

func TestHandlerPrimaryMutationTargetsHideExistenceBeforeDelegation(t *testing.T) {
	policy := &accessPolicy{
		writeMax: schema.SensitivityLow, writeScopes: []string{"project:alpha"},
		defaultSensitivity: schema.SensitivityLow,
	}
	newRecord := func() *pb.MemoryRecord {
		return semanticRecordPBForHandler(t, "caller-id-is-ignored", "subject", "is", "object")
	}
	calls := []struct {
		name       string
		mergeBatch bool
		call       func(*Handler) error
	}{
		{name: "retract", call: func(h *Handler) error {
			_, err := h.Retract(context.Background(), &pb.RetractRequest{Id: "target"})
			return err
		}},
		{name: "reinforce", call: func(h *Handler) error {
			_, err := h.Reinforce(context.Background(), &pb.ReinforceRequest{Id: "target"})
			return err
		}},
		{name: "penalize", call: func(h *Handler) error {
			_, err := h.Penalize(context.Background(), &pb.PenalizeRequest{Id: "target", Amount: 0.1})
			return err
		}},
		{name: "contest", call: func(h *Handler) error {
			_, err := h.Contest(context.Background(), &pb.ContestRequest{Id: "target"})
			return err
		}},
		{name: "supersede", call: func(h *Handler) error {
			_, err := h.Supersede(context.Background(), &pb.SupersedeRequest{OldId: "target", NewRecord: newRecord()})
			return err
		}},
		{name: "fork", call: func(h *Handler) error {
			_, err := h.Fork(context.Background(), &pb.ForkRequest{SourceId: "target", ForkedRecord: newRecord()})
			return err
		}},
		{name: "merge batch", mergeBatch: true, call: func(h *Handler) error {
			_, err := h.Merge(context.Background(), &pb.MergeRequest{Ids: []string{"allowed", "target"}, MergedRecord: newRecord()})
			return err
		}},
	}
	allowed := storage.RecordAuthorizationMetadata{
		ID: "allowed", Scope: "project:alpha", Sensitivity: schema.SensitivityLow,
	}
	hidden := storage.RecordAuthorizationMetadata{
		ID: "target", Scope: "project:secret", Sensitivity: schema.SensitivityHigh,
	}
	for _, tc := range calls {
		t.Run(tc.name, func(t *testing.T) {
			var missingMetadata []storage.RecordAuthorizationMetadata
			hiddenMetadata := []storage.RecordAuthorizationMetadata{hidden}
			if tc.mergeBatch {
				missingMetadata = []storage.RecordAuthorizationMetadata{allowed}
				hiddenMetadata = []storage.RecordAuthorizationMetadata{allowed, hidden}
			}
			missingErr := tc.call(&Handler{
				access: policy, boundedRecords: &fakeBoundedHandlerRecords{metadata: missingMetadata},
			})
			hiddenErr := tc.call(&Handler{
				access: policy, boundedRecords: &fakeBoundedHandlerRecords{metadata: hiddenMetadata},
			})
			if status.Code(missingErr) != codes.NotFound || status.Code(hiddenErr) != codes.NotFound {
				t.Fatalf("missing/hidden codes = %v/%v, want both NotFound", status.Code(missingErr), status.Code(hiddenErr))
			}
			if status.Convert(missingErr).Message() != status.Convert(hiddenErr).Message() {
				t.Fatalf("primary target leaked existence: missing=%q hidden=%q", status.Convert(missingErr).Message(), status.Convert(hiddenErr).Message())
			}
		})
	}
}

func TestHandlerAuthorizeExistingMemoriesUsesOneCappedBatch(t *testing.T) {
	policy := &accessPolicy{writeMax: schema.SensitivityLow, writeScopes: []string{"project:alpha"}}
	ids := make([]string, maxAuthorizationTargets)
	metadata := make([]storage.RecordAuthorizationMetadata, maxAuthorizationTargets)
	for i := range ids {
		ids[i] = fmt.Sprintf("record-%03d", i)
		metadata[i] = storage.RecordAuthorizationMetadata{
			ID: ids[i], Scope: "project:alpha", Sensitivity: schema.SensitivityLow,
		}
	}
	fake := &fakeBoundedHandlerRecords{metadata: metadata}
	h := &Handler{access: policy, boundedRecords: fake}
	if err := h.authorizeExistingMemories(context.Background(), ids); err != nil {
		t.Fatalf("authorizeExistingMemories: %v", err)
	}
	if len(fake.metadataCalls) != 1 || len(fake.metadataCalls[0]) != maxAuthorizationTargets {
		t.Fatalf("metadata calls = %d/%d targets, want one/%d", len(fake.metadataCalls), len(fake.metadataCalls[0]), maxAuthorizationTargets)
	}

	overLimit := append(append([]string(nil), ids...), "one-too-many")
	if err := h.authorizeExistingMemories(context.Background(), overLimit); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("over-limit code = %v, want InvalidArgument", status.Code(err))
	}
	if len(fake.metadataCalls) != 1 {
		t.Fatalf("over-limit lookup issued storage work: calls=%d", len(fake.metadataCalls))
	}
}

func TestHandlerMergeCapsAuthorizationTargetsBeforeValidationWork(t *testing.T) {
	ids := make([]string, maxAuthorizationTargets+1)
	_, err := (&Handler{}).Merge(context.Background(), &pb.MergeRequest{Ids: ids})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Merge over-limit code = %v, want InvalidArgument", status.Code(err))
	}
}
