package revision

import (
	"context"
	"testing"
	"time"

	"github.com/BennettSchwartz/membrane/pkg/schema"
)

func TestRevisionOwnsNewRecordAuditAndProvenanceMetadata(t *testing.T) {
	ctx := context.Background()
	forgedTime := time.Date(1999, 1, 2, 3, 4, 5, 0, time.UTC)

	tests := []struct {
		name           string
		wantSupersedes string
		run            func(*Service, *schema.MemoryRecord) (*schema.MemoryRecord, error)
	}{
		{
			name:           "supersede",
			wantSupersedes: "source-a",
			run: func(svc *Service, replacement *schema.MemoryRecord) (*schema.MemoryRecord, error) {
				return svc.Supersede(ctx, "source-a", replacement, "authenticated-actor", "replace")
			},
		},
		{
			name: "fork",
			run: func(svc *Service, replacement *schema.MemoryRecord) (*schema.MemoryRecord, error) {
				return svc.Fork(ctx, "source-a", replacement, "authenticated-actor", "fork")
			},
		},
		{
			name: "merge",
			run: func(svc *Service, replacement *schema.MemoryRecord) (*schema.MemoryRecord, error) {
				return svc.Merge(ctx, []string{"source-a", "source-b"}, replacement, "authenticated-actor", "merge")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newRevisionTestStore(t)
			for _, id := range []string{"source-a", "source-b"} {
				if err := store.Create(ctx, semanticRevisionRecord(id, "source", id)); err != nil {
					t.Fatalf("Create %s: %v", id, err)
				}
			}
			replacement := semanticRevisionRecord("replacement-"+tt.name, "replacement", tt.name)
			replacement.Relations = []schema.Relation{{
				Predicate: "related_to",
				TargetID:  "source-b",
				Weight:    1,
				CreatedAt: forgedTime,
			}}
			replacement.Lifecycle.Pinned = true
			replacement.Lifecycle.DeletionPolicy = schema.DeletionPolicyNever
			replacement.Lifecycle.LastReinforcedAt = forgedTime.AddDate(100, 0, 0)
			replacement.Lifecycle.Decay.HalfLifeSeconds = 1<<62 - 1
			replacement.Lifecycle.Decay.MinSalience = 1
			replacement.AuditLog = []schema.AuditEntry{{
				Action: schema.AuditActionMerge, Actor: "forged-admin", Timestamp: forgedTime, Rationale: "forged",
			}}
			replacement.Provenance.CreatedBy = "forged-admin"
			replacement.Provenance.Sources = []schema.ProvenanceSource{{
				Kind: schema.ProvenanceKindObservation, Ref: "external-evidence", Hash: "sha256:trusted-content",
				CreatedBy: "forged-admin", Timestamp: forgedTime,
			}}
			payload := replacement.Payload.(*schema.SemanticPayload)
			payload.Revision = &schema.RevisionState{
				Supersedes: "forged-source", SupersededBy: "forged-successor", Status: schema.RevisionStatusContested,
			}
			payload.Evidence = []schema.ProvenanceRef{{
				SourceType: "observation", SourceID: "external-evidence", Timestamp: forgedTime,
			}}

			before := time.Now().UTC()
			got, err := tt.run(NewService(store), replacement)
			after := time.Now().UTC()
			if err != nil {
				t.Fatalf("revision: %v", err)
			}
			if len(got.AuditLog) != 1 || got.AuditLog[0].Actor != "authenticated-actor" || got.AuditLog[0].Action != schema.AuditActionCreate {
				t.Fatalf("AuditLog = %+v, want one server-owned create entry", got.AuditLog)
			}
			if got.Provenance.CreatedBy != "authenticated-actor" {
				t.Fatalf("Provenance.CreatedBy = %q, want authenticated actor", got.Provenance.CreatedBy)
			}
			if got.Lifecycle.Pinned || got.Lifecycle.DeletionPolicy == schema.DeletionPolicyNever || got.Lifecycle.Decay.HalfLifeSeconds == 1<<62-1 || got.Lifecycle.Decay.MinSalience == 1 {
				t.Fatalf("Lifecycle = %+v, retained caller-controlled lifecycle policy", got.Lifecycle)
			}
			if got.Lifecycle.LastReinforcedAt.Before(before) || got.Lifecycle.LastReinforcedAt.After(after) {
				t.Fatalf("LastReinforcedAt = %s, want operation time in [%s, %s]", got.Lifecycle.LastReinforcedAt, before, after)
			}
			var evidence *schema.ProvenanceSource
			for i := range got.Provenance.Sources {
				if got.Provenance.Sources[i].Ref == "external-evidence" {
					evidence = &got.Provenance.Sources[i]
					break
				}
			}
			if evidence == nil || evidence.Hash != "sha256:trusted-content" || evidence.CreatedBy != "authenticated-actor" {
				t.Fatalf("Provenance.Sources = %+v, want preserved evidence with server-owned creator", got.Provenance.Sources)
			}
			if evidence.Timestamp.Before(before) || evidence.Timestamp.After(after) {
				t.Fatalf("evidence timestamp = %s, want operation time in [%s, %s]", evidence.Timestamp, before, after)
			}
			semantic := got.Payload.(*schema.SemanticPayload)
			if semantic.Revision == nil || semantic.Revision.Supersedes != tt.wantSupersedes || semantic.Revision.SupersededBy != "" {
				t.Fatalf("semantic revision = %+v, want operation-owned lineage supersedes=%q superseded_by empty", semantic.Revision, tt.wantSupersedes)
			}
			if len(semantic.Evidence) != 1 || semantic.Evidence[0].SourceID != "external-evidence" || semantic.Evidence[0].Timestamp.Before(before) || semantic.Evidence[0].Timestamp.After(after) {
				t.Fatalf("semantic evidence = %+v, want identity preserved and timestamp server-owned", semantic.Evidence)
			}
			var attributedRelation *schema.Relation
			for i := range got.Relations {
				if got.Relations[i].Predicate == "related_to" && got.Relations[i].TargetID == "source-b" {
					attributedRelation = &got.Relations[i]
					break
				}
			}
			if attributedRelation == nil {
				t.Fatalf("relations = %+v, want caller relation preserved", got.Relations)
			}
			if attributedRelation.CreatedAt.Before(before) || attributedRelation.CreatedAt.After(after) {
				t.Fatalf("caller relation CreatedAt = %s, want operation time in [%s, %s]", attributedRelation.CreatedAt, before, after)
			}
		})
	}
}
