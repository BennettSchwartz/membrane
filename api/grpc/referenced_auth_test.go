package grpc

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"

	pb "github.com/BennettSchwartz/membrane/api/grpc/gen/membranev1"
	"github.com/BennettSchwartz/membrane/pkg/schema"
	"github.com/BennettSchwartz/membrane/pkg/storage"
)

func TestReferencedMemoryIDsIncludesOnlyMaterializedRelationTargets(t *testing.T) {
	rec := schema.NewMemoryRecord("new-record", schema.MemoryTypeSemantic, schema.SensitivityLow, &schema.SemanticPayload{
		Kind:      "semantic",
		Subject:   "subject-entity",
		Predicate: "related_to",
		Object:    "object-entity",
		Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
		Evidence:  []schema.ProvenanceRef{{SourceType: "observation", SourceID: "evidence-record"}},
		Revision:  &schema.RevisionState{Supersedes: "supersedes-record", SupersededBy: "superseded-by-record"},
	})
	rec.Relations = []schema.Relation{{TargetID: "relation-target", Predicate: "related_to", Weight: 1}}
	rec.Provenance.Sources = []schema.ProvenanceSource{{Kind: schema.ProvenanceKindObservation, Ref: "provenance-record"}}
	rec.Interpretation = &schema.Interpretation{
		Mentions: []schema.Mention{{CanonicalEntityID: "mentioned-entity"}},
		ReferenceCandidates: []schema.ReferenceCandidate{{
			TargetRecordID: "reference-record", TargetEntityID: "reference-entity",
		}},
		RelationCandidates: []schema.RelationCandidate{{
			TargetRecordID: "candidate-record", TargetEntityID: "candidate-entity",
		}},
	}

	got := make(map[string]bool)
	for _, id := range referencedMemoryIDs(rec) {
		got[id] = true
	}
	if !got["relation-target"] || len(got) != 1 {
		t.Fatalf("referencedMemoryIDs = %v, want only persisted relation target", got)
	}
}

func TestReferencedMemoryIDsPreservesExactPersistedTargetID(t *testing.T) {
	rec := schema.NewMemoryRecord("source", schema.MemoryTypeSemantic, schema.SensitivityLow, &schema.SemanticPayload{
		Kind: "semantic", Subject: "subject", Predicate: "is", Object: "object",
		Validity: schema.Validity{Mode: schema.ValidityModeGlobal},
	})
	rec.Relations = []schema.Relation{{Predicate: "related_to", TargetID: " target ", Weight: 1}}
	ids := referencedMemoryIDs(rec)
	if len(ids) != 1 || ids[0] != " target " {
		t.Fatalf("referencedMemoryIDs = %q, want exact persisted target ID", ids)
	}
}

func TestAccessPolicyAllowsWriteRecordRejectsUnscoped(t *testing.T) {
	policy := &accessPolicy{
		writeMax:    schema.SensitivityLow,
		writeScopes: []string{"project:alpha"},
	}
	unscoped := schema.NewMemoryRecord("global", schema.MemoryTypeSemantic, schema.SensitivityLow, &schema.SemanticPayload{
		Kind: "semantic", Subject: "subject", Predicate: "is", Object: "global",
		Validity: schema.Validity{Mode: schema.ValidityModeGlobal},
	})
	if policy.allowsWriteRecord(context.Background(), unscoped) {
		t.Fatal("write policy allowed an unscoped record")
	}
	unscoped.Scope = "project:alpha"
	if !policy.allowsWriteRecord(context.Background(), unscoped) {
		t.Fatal("write policy rejected an in-scope low-sensitivity record")
	}
}

func TestValidatedMemoryRecordRejectsRelationQueryAmplification(t *testing.T) {
	rec := semanticRecordPBForHandler(t, "too-many-relations", "subject", "is", "object")
	rec.Relations = make([]*pb.Relation, maxRecordRelations+1)
	for i := range rec.Relations {
		rec.Relations[i] = &pb.Relation{Predicate: "related_to", TargetId: "target", Weight: 1}
	}
	if _, err := validatedMemoryRecordFromPB(rec); err == nil {
		t.Fatalf("validatedMemoryRecordFromPB accepted %d relations", len(rec.Relations))
	}
}

func TestHandlerRevisionRejectsUnauthorizedNestedTarget(t *testing.T) {
	tests := []struct {
		name string
		run  func(context.Context, *Handler, string, string, *pb.MemoryRecord) error
	}{
		{
			name: "supersede",
			run: func(ctx context.Context, h *Handler, firstID, _ string, replacement *pb.MemoryRecord) error {
				_, err := h.Supersede(ctx, &pb.SupersedeRequest{
					OldId: firstID, NewRecord: replacement, Actor: "spoofed", Rationale: "nested target",
				})
				return err
			},
		},
		{
			name: "fork",
			run: func(ctx context.Context, h *Handler, firstID, _ string, replacement *pb.MemoryRecord) error {
				_, err := h.Fork(ctx, &pb.ForkRequest{
					SourceId: firstID, ForkedRecord: replacement, Actor: "spoofed", Rationale: "nested target",
				})
				return err
			},
		},
		{
			name: "merge",
			run: func(ctx context.Context, h *Handler, firstID, secondID string, replacement *pb.MemoryRecord) error {
				_, err := h.Merge(ctx, &pb.MergeRequest{
					Ids: []string{firstID, secondID}, MergedRecord: replacement, Actor: "spoofed", Rationale: "nested target",
				})
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			h := newHandlerTest(t)
			_, firstID := captureSemanticFactThroughHandler(t, ctx, h, "source-a", "is", "visible")
			_, secondID := captureSemanticFactThroughHandler(t, ctx, h, "source-b", "is", "visible")
			secretContent, err := structpb.NewValue(map[string]any{"note": "classified"})
			if err != nil {
				t.Fatalf("NewValue: %v", err)
			}
			secret, err := h.CaptureMemory(ctx, &pb.CaptureMemoryRequest{
				Source: "fixture", SourceKind: "event", Content: secretContent,
				Scope: "project:alpha", Sensitivity: string(schema.SensitivityHigh),
			})
			if err != nil {
				t.Fatalf("Capture secret: %v", err)
			}
			h.access = &accessPolicy{
				readMax: schema.SensitivityLow, readScopes: []string{"project:alpha"},
				writeMax: schema.SensitivityLow, writeScopes: []string{"project:alpha"},
				defaultSensitivity: schema.SensitivityLow,
			}

			replacement := semanticRecordPBForHandler(t, "replacement-"+tt.name, "source", "is", "updated")
			replacement.Relations = append(replacement.Relations, &pb.Relation{
				Predicate: "related_to", TargetId: secret.PrimaryRecord.Id, Weight: 1,
			})
			if err := tt.run(ctx, h, firstID, secondID, replacement); status.Code(err) != codes.PermissionDenied {
				t.Fatalf("%s nested target code = %v, want PermissionDenied; err=%v", tt.name, status.Code(err), err)
			}
		})
	}
}

func TestHandlerRevisionClearsCallerIDBeforeSelfTargetAuthorization(t *testing.T) {
	policy := &accessPolicy{
		readMax: schema.SensitivityLow, readScopes: []string{"project:alpha"},
		writeMax: schema.SensitivityLow, writeScopes: []string{"project:alpha"},
		defaultSensitivity: schema.SensitivityLow,
	}
	tests := []struct {
		name     string
		sourceID []string
		run      func(context.Context, *Handler, *pb.MemoryRecord) error
	}{
		{
			name:     "supersede",
			sourceID: []string{"source-a"},
			run: func(ctx context.Context, h *Handler, record *pb.MemoryRecord) error {
				_, err := h.Supersede(ctx, &pb.SupersedeRequest{OldId: "source-a", NewRecord: record})
				return err
			},
		},
		{
			name:     "fork",
			sourceID: []string{"source-a"},
			run: func(ctx context.Context, h *Handler, record *pb.MemoryRecord) error {
				_, err := h.Fork(ctx, &pb.ForkRequest{SourceId: "source-a", ForkedRecord: record})
				return err
			},
		},
		{
			name:     "merge",
			sourceID: []string{"source-a", "source-b"},
			run: func(ctx context.Context, h *Handler, record *pb.MemoryRecord) error {
				_, err := h.Merge(ctx, &pb.MergeRequest{Ids: []string{"source-a", "source-b"}, MergedRecord: record})
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metadata := make([]storage.RecordAuthorizationMetadata, 0, len(tt.sourceID))
			for _, id := range tt.sourceID {
				metadata = append(metadata, storage.RecordAuthorizationMetadata{
					ID: id, Scope: "project:alpha", Sensitivity: schema.SensitivityLow,
				})
			}
			h := &Handler{
				access:         policy,
				boundedRecords: &fakeBoundedHandlerRecords{metadata: metadata},
			}
			record := semanticRecordPBForHandler(t, "hidden-or-missing-id", "subject", "is", "object")
			record.Relations = []*pb.Relation{{
				Predicate: "related_to", TargetId: record.Id, Weight: 1,
			}}

			var (
				err      error
				panicked any
			)
			func() {
				defer func() { panicked = recover() }()
				err = tt.run(context.Background(), h, record)
			}()
			if panicked != nil {
				t.Fatalf("public revision delegated before clearing and authorizing caller ID: %v", panicked)
			}
			if status.Code(err) != codes.PermissionDenied || status.Convert(err).Message() != unavailableRelationTargetMessage {
				t.Fatalf("self target disposition = %v, want generic PermissionDenied", err)
			}
		})
	}
}

func TestHandlerRevisionCallerIDCannotProbeHiddenRecordCollision(t *testing.T) {
	ctx := context.Background()
	h := newHandlerTest(t)
	_, sourceID := captureSemanticFactThroughHandler(t, ctx, h, "visible-source", "is", "visible")
	secretContent, err := structpb.NewValue(map[string]any{"note": "classified"})
	if err != nil {
		t.Fatalf("NewValue: %v", err)
	}
	secret, err := h.CaptureMemory(ctx, &pb.CaptureMemoryRequest{
		Source: "fixture", SourceKind: "event", Content: secretContent,
		Scope: "project:secret", Sensitivity: string(schema.SensitivityHigh),
	})
	if err != nil {
		t.Fatalf("Capture secret: %v", err)
	}
	hiddenID := secret.PrimaryRecord.Id
	h.access = &accessPolicy{
		readMax: schema.SensitivityLow, readScopes: []string{"project:alpha"},
		writeMax: schema.SensitivityLow, writeScopes: []string{"project:alpha"},
		defaultSensitivity: schema.SensitivityLow,
	}
	replacement := semanticRecordPBForHandler(t, hiddenID, "visible-source", "is", "variant")
	resp, err := h.Fork(ctx, &pb.ForkRequest{SourceId: sourceID, ForkedRecord: replacement})
	if err != nil {
		t.Fatalf("Fork with colliding caller ID disclosed hidden record: %v", err)
	}
	if resp.GetRecord().GetId() == "" || resp.GetRecord().GetId() == hiddenID {
		t.Fatalf("assigned ID = %q, want fresh server ID distinct from hidden collision", resp.GetRecord().GetId())
	}
}

func TestHandlerCaptureDoesNotResolveUnauthorizedReference(t *testing.T) {
	ctx := context.Background()
	h := newHandlerTest(t)
	secretContent, err := structpb.NewValue(map[string]any{"note": "classified"})
	if err != nil {
		t.Fatalf("NewValue: %v", err)
	}
	secret, err := h.CaptureMemory(ctx, &pb.CaptureMemoryRequest{
		Source: "fixture", SourceKind: "event", Content: secretContent,
		Scope: "project:alpha", Sensitivity: string(schema.SensitivityHigh),
	})
	if err != nil {
		t.Fatalf("Capture secret: %v", err)
	}
	h.access = &accessPolicy{
		readMax: schema.SensitivityLow, readScopes: []string{"project:alpha"},
		writeMax: schema.SensitivityLow, writeScopes: []string{"project:alpha"},
		defaultSensitivity: schema.SensitivityLow,
	}
	content, err := structpb.NewValue(map[string]any{"record_id": secret.PrimaryRecord.Id})
	if err != nil {
		t.Fatalf("NewValue: %v", err)
	}
	resp, err := h.CaptureMemory(ctx, &pb.CaptureMemoryRequest{
		Source: "caller", SourceKind: "event", Content: content,
		Scope: "project:alpha", Sensitivity: string(schema.SensitivityLow),
	})
	if err != nil {
		t.Fatalf("CaptureMemory: %v", err)
	}
	refs := resp.PrimaryRecord.GetInterpretation().GetReferenceCandidates()
	if len(refs) != 1 || refs[0].GetResolved() || refs[0].GetTargetRecordId() != "" {
		t.Fatalf("reference candidates = %+v, want unauthorized target unresolved", refs)
	}
}

func TestHandlerRejectsMutationOfUnscopedExistingRecord(t *testing.T) {
	ctx := context.Background()
	h := newHandlerTest(t)
	content, err := structpb.NewValue(map[string]any{"note": "global"})
	if err != nil {
		t.Fatalf("NewValue: %v", err)
	}
	unscoped, err := h.CaptureMemory(ctx, &pb.CaptureMemoryRequest{
		Source: "fixture", SourceKind: "event", Content: content,
		Sensitivity: string(schema.SensitivityLow),
	})
	if err != nil {
		t.Fatalf("Capture unscoped record: %v", err)
	}
	h.access = &accessPolicy{
		readMax: schema.SensitivityLow, readScopes: []string{"project:alpha"},
		writeMax: schema.SensitivityLow, writeScopes: []string{"project:alpha"},
		defaultSensitivity: schema.SensitivityLow,
	}

	_, err = h.Reinforce(ctx, &pb.ReinforceRequest{Id: unscoped.PrimaryRecord.Id})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("Reinforce unscoped record code = %v, want opaque NotFound; err=%v", status.Code(err), err)
	}
}

func TestHandlerRevisionReferencePreflightIsFieldAwareAndNonOracular(t *testing.T) {
	ctx := context.Background()
	h := newHandlerTest(t)
	secretContent, err := structpb.NewValue(map[string]any{"note": "classified"})
	if err != nil {
		t.Fatalf("NewValue: %v", err)
	}
	secret, err := h.CaptureMemory(ctx, &pb.CaptureMemoryRequest{
		Source: "fixture", SourceKind: "event", Content: secretContent,
		Scope: "project:secret", Sensitivity: string(schema.SensitivityHigh),
	})
	if err != nil {
		t.Fatalf("Capture secret: %v", err)
	}
	h.access = &accessPolicy{
		readMax: schema.SensitivityLow, readScopes: []string{"project:alpha"},
		writeMax: schema.SensitivityLow, writeScopes: []string{"project:alpha"},
		defaultSensitivity: schema.SensitivityLow,
	}

	opaque := schema.NewMemoryRecord("opaque-collision", schema.MemoryTypeSemantic, schema.SensitivityLow, &schema.SemanticPayload{
		Kind:      "semantic",
		Subject:   secret.PrimaryRecord.Id,
		Predicate: "describes",
		Object:    secret.PrimaryRecord.Id,
		Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
		Evidence: []schema.ProvenanceRef{{
			SourceType: "observation", SourceID: secret.PrimaryRecord.Id,
		}},
	})
	opaque.Scope = "project:alpha"
	opaque.Provenance.Sources = []schema.ProvenanceSource{{
		Kind: schema.ProvenanceKindObservation, Ref: secret.PrimaryRecord.Id,
	}}
	if err := h.authorizeReferencedMemories(ctx, opaque); err != nil {
		t.Fatalf("opaque values must not be probed as record IDs: %v", err)
	}

	withRelation := func(target string) *schema.MemoryRecord {
		rec := schema.NewMemoryRecord("relation-preflight", schema.MemoryTypeSemantic, schema.SensitivityLow, &schema.SemanticPayload{
			Kind: "semantic", Subject: "subject", Predicate: "describes", Object: "object",
			Validity: schema.Validity{Mode: schema.ValidityModeGlobal},
		})
		rec.Scope = "project:alpha"
		rec.Relations = []schema.Relation{{Predicate: "related_to", TargetID: target, Weight: 1}}
		return rec
	}
	missingErr := h.authorizeReferencedMemories(ctx, withRelation("missing-record"))
	hiddenErr := h.authorizeReferencedMemories(ctx, withRelation(secret.PrimaryRecord.Id))
	if status.Code(missingErr) != codes.PermissionDenied || status.Code(hiddenErr) != codes.PermissionDenied {
		t.Fatalf("relation preflight codes = missing:%v hidden:%v, want both PermissionDenied", status.Code(missingErr), status.Code(hiddenErr))
	}
	if status.Convert(missingErr).Message() != status.Convert(hiddenErr).Message() {
		t.Fatalf("relation preflight leaked existence: missing=%q hidden=%q", status.Convert(missingErr).Message(), status.Convert(hiddenErr).Message())
	}
}
