package ingestion

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/BennettSchwartz/membrane/pkg/schema"
	sqlitestore "github.com/BennettSchwartz/membrane/pkg/storage/sqlite"
)

func newObservationEntity(id, name, scope string) *schema.MemoryRecord {
	rec := schema.NewMemoryRecord(id, schema.MemoryTypeEntity, schema.SensitivityLow, &schema.EntityPayload{
		Kind:          "entity",
		CanonicalName: name,
		PrimaryType:   schema.EntityTypeProject,
		Types:         []string{schema.EntityTypeProject},
		Aliases:       []schema.EntityAlias{{Value: strings.ToLower(name)}},
		Summary:       name + " entity",
	})
	rec.Scope = scope
	return rec
}

func TestIngestObservationCreatesSemanticRecord(t *testing.T) {
	svc, store := newCaptureTestService(t, nil)
	ctx := context.Background()
	ts := time.Date(2026, 5, 1, 10, 30, 0, 0, time.UTC)

	rec, err := svc.IngestObservation(ctx, IngestObservationRequest{
		Source:      "observer",
		Subject:     "Orchid",
		Predicate:   "deploys_to",
		Object:      map[string]any{"cluster": "staging", "region": "iad"},
		Timestamp:   ts,
		Tags:        []string{"deploy", "fact"},
		Scope:       "project:alpha",
		Sensitivity: schema.SensitivityMedium,
	})
	if err != nil {
		t.Fatalf("IngestObservation: %v", err)
	}

	if rec.Type != schema.MemoryTypeSemantic {
		t.Fatalf("Type = %q, want semantic", rec.Type)
	}
	if rec.Sensitivity != schema.SensitivityMedium {
		t.Fatalf("Sensitivity = %q, want medium", rec.Sensitivity)
	}
	if rec.Confidence != 0.7 {
		t.Fatalf("Confidence = %.2f, want 0.70", rec.Confidence)
	}
	if rec.Scope != "project:alpha" {
		t.Fatalf("Scope = %q, want project:alpha", rec.Scope)
	}

	payload, ok := rec.Payload.(*schema.SemanticPayload)
	if !ok {
		t.Fatalf("Payload type = %T, want semantic", rec.Payload)
	}
	if payload.Subject != "Orchid" || payload.Predicate != "deploys_to" {
		t.Fatalf("payload fact = %s %s, want Orchid deploys_to", payload.Subject, payload.Predicate)
	}
	if payload.Validity.Mode != schema.ValidityModeGlobal {
		t.Fatalf("Validity.Mode = %q, want global", payload.Validity.Mode)
	}
	if payload.RevisionPolicy != "replace" {
		t.Fatalf("RevisionPolicy = %q, want replace", payload.RevisionPolicy)
	}
	if len(payload.Evidence) != 1 || payload.Evidence[0].SourceType != "observation" || payload.Evidence[0].SourceID != "observer" || !payload.Evidence[0].Timestamp.Equal(ts) {
		t.Fatalf("Evidence = %+v, want observation evidence from observer at %s", payload.Evidence, ts)
	}
	if len(rec.Provenance.Sources) != 1 || rec.Provenance.Sources[0].Kind != schema.ProvenanceKindObservation || rec.Provenance.Sources[0].Ref != "observer" {
		t.Fatalf("Provenance = %+v, want observation source ref to observer", rec.Provenance.Sources)
	}

	got, err := store.Get(ctx, rec.ID)
	if err != nil {
		t.Fatalf("Get stored observation: %v", err)
	}
	if got.ID != rec.ID {
		t.Fatalf("stored ID = %q, want %q", got.ID, rec.ID)
	}
}

func TestIngestObservationCanonicalizesEntitySubjectAndObject(t *testing.T) {
	svc, store := newCaptureTestService(t, nil)
	ctx := context.Background()

	subject := newObservationEntity("entity-orchid", "Orchid", "project:alpha")
	object := newObservationEntity("entity-borealis", "Borealis", "project:alpha")
	for _, rec := range []*schema.MemoryRecord{subject, object} {
		if err := store.Create(ctx, rec); err != nil {
			t.Fatalf("Create %s: %v", rec.ID, err)
		}
	}

	rec, err := svc.IngestObservation(ctx, IngestObservationRequest{
		Source:    "observer",
		Subject:   "Orchid",
		Predicate: "depends_on",
		Object:    "Borealis",
		Scope:     "project:alpha",
	})
	if err != nil {
		t.Fatalf("IngestObservation: %v", err)
	}

	payload := rec.Payload.(*schema.SemanticPayload)
	if payload.Subject != subject.ID {
		t.Fatalf("Subject = %q, want canonical entity %q", payload.Subject, subject.ID)
	}
	if payload.Object != object.ID {
		t.Fatalf("Object = %#v, want canonical entity %q", payload.Object, object.ID)
	}
	if len(rec.Relations) != 2 {
		t.Fatalf("Relations = %+v, want subject and object entity relations", rec.Relations)
	}

	subjectRelations, err := store.GetRelations(ctx, subject.ID)
	if err != nil {
		t.Fatalf("GetRelations subject: %v", err)
	}
	if len(subjectRelations) != 1 || subjectRelations[0].Predicate != "fact_subject_of" || subjectRelations[0].TargetID != rec.ID {
		t.Fatalf("subject reverse relations = %+v, want fact_subject_of %s", subjectRelations, rec.ID)
	}
	objectRelations, err := store.GetRelations(ctx, object.ID)
	if err != nil {
		t.Fatalf("GetRelations object: %v", err)
	}
	if len(objectRelations) != 1 || objectRelations[0].Predicate != "fact_object_of" || objectRelations[0].TargetID != rec.ID {
		t.Fatalf("object reverse relations = %+v, want fact_object_of %s", objectRelations, rec.ID)
	}
}

func TestIngestObservationValidationErrors(t *testing.T) {
	svc, _ := newCaptureTestService(t, nil)
	ctx := context.Background()

	for _, tc := range []struct {
		name string
		req  IngestObservationRequest
		want string
	}{
		{
			name: "missing subject",
			req:  IngestObservationRequest{Source: "observer", Predicate: "is", Object: "ready"},
			want: "subject is required",
		},
		{
			name: "missing predicate",
			req:  IngestObservationRequest{Source: "observer", Subject: "Orchid", Object: "ready"},
			want: "predicate is required",
		},
		{
			name: "invalid sensitivity",
			req:  IngestObservationRequest{Source: "observer", Subject: "Orchid", Predicate: "is", Object: "ready", Sensitivity: schema.Sensitivity("secret")},
			want: "invalid sensitivity",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := svc.IngestObservation(ctx, tc.req)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("IngestObservation error = %v, want containing %q", err, tc.want)
			}
		})
	}
}

func TestIngestObservationRollsBackWhenReverseRelationFails(t *testing.T) {
	base, err := sqlitestore.Open(":memory:", "")
	if err != nil {
		t.Fatalf("Open sqlite store: %v", err)
	}
	t.Cleanup(func() { _ = base.Close() })

	ctx := context.Background()
	entity := newObservationEntity("entity-orchid", "Orchid", "project:alpha")
	if err := base.Create(ctx, entity); err != nil {
		t.Fatalf("Create entity: %v", err)
	}

	store := &failingRelationStore{Store: base}
	svc := NewService(store, NewClassifier(), NewPolicyEngine(DefaultPolicyDefaults()))

	_, err = svc.IngestObservation(ctx, IngestObservationRequest{
		Source:    "observer",
		Subject:   "Orchid",
		Predicate: "is",
		Object:    "ready",
		Scope:     "project:alpha",
	})
	if err == nil || !strings.Contains(err.Error(), "forced relation write failure") {
		t.Fatalf("IngestObservation error = %v, want forced relation write failure", err)
	}

	semantics, err := base.ListByType(ctx, schema.MemoryTypeSemantic)
	if err != nil {
		t.Fatalf("ListByType semantic: %v", err)
	}
	if len(semantics) != 0 {
		t.Fatalf("semantic records after failed observation = %+v, want rollback", semantics)
	}
	entities, err := base.ListByType(ctx, schema.MemoryTypeEntity)
	if err != nil {
		t.Fatalf("ListByType entity: %v", err)
	}
	if len(entities) != 1 || entities[0].ID != entity.ID {
		t.Fatalf("entities after rollback = %+v, want original entity only", entities)
	}
}
