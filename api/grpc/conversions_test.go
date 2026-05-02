package grpc

import (
	"testing"
	"time"
	"unsafe"

	pb "github.com/BennettSchwartz/membrane/api/grpc/gen/membranev1"
	"github.com/BennettSchwartz/membrane/pkg/ingestion"
	"github.com/BennettSchwartz/membrane/pkg/retrieval"
	"github.com/BennettSchwartz/membrane/pkg/schema"
)

type invalidPayloadKind struct{}

func (*invalidPayloadKind) isPayload_Kind() {}

type interfaceWords struct {
	tab  unsafe.Pointer
	data unsafe.Pointer
}

func TestPayloadConversionsRoundTrip(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	later := now.Add(time.Hour)

	payloads := []schema.Payload{
		&schema.EpisodicPayload{
			Kind: "episodic",
			Timeline: []schema.TimelineEvent{{
				T:         now,
				EventKind: "user_input",
				Ref:       "evt-1",
				Summary:   "remember Orchid",
			}},
			ToolGraph: []schema.ToolNode{{
				ID:        "tool-1",
				Tool:      "search",
				Args:      map[string]any{"q": "orchid"},
				Result:    map[string]any{"ok": true},
				Timestamp: now,
				DependsOn: []string{"tool-0"},
			}},
			Environment: &schema.EnvironmentSnapshot{
				OS:               "darwin",
				OSVersion:        "15",
				ToolVersions:     map[string]string{"go": "1.24"},
				WorkingDirectory: "/tmp/membrane",
				Context:          map[string]any{"repo": "membrane"},
			},
			Outcome:      schema.OutcomeStatusSuccess,
			Artifacts:    []string{"log.txt"},
			ToolGraphRef: "graph-1",
		},
		&schema.WorkingPayload{
			Kind:     "working",
			ThreadID: "thread-1",
			State:    schema.TaskStateExecuting,
			ActiveConstraints: []schema.Constraint{{
				Type:     "scope",
				Key:      "project",
				Value:    "orchid",
				Required: true,
			}},
			NextActions:    []string{"test"},
			OpenQuestions:  []string{"coverage?"},
			ContextSummary: "working summary",
		},
		&schema.SemanticPayload{
			Kind:      "semantic",
			Subject:   "Orchid",
			Predicate: "depends_on",
			Object:    map[string]any{"service": "Borealis"},
			Validity: schema.Validity{
				Mode:       schema.ValidityModeTimeboxed,
				Conditions: map[string]any{"env": "staging"},
				Start:      &now,
				End:        &later,
			},
			Evidence:       []schema.ProvenanceRef{{SourceType: "observation", SourceID: "obs-1", Timestamp: now}},
			RevisionPolicy: "replace",
			Revision:       &schema.RevisionState{Supersedes: "old", SupersededBy: "new", Status: schema.RevisionStatusActive},
		},
		&schema.EntityPayload{
			Kind:          "entity",
			CanonicalName: "Orchid",
			PrimaryType:   schema.EntityTypeProject,
			Types:         []string{schema.EntityTypeProject},
			Aliases:       []schema.EntityAlias{{Value: "orchid", Kind: "alias", Locale: "en"}},
			Identifiers:   []schema.EntityIdentifier{{Namespace: "slug", Value: "orchid"}},
			Summary:       "project entity",
		},
		&schema.CompetencePayload{
			Kind:      "competence",
			SkillName: "debug",
			Triggers:  []schema.Trigger{{Signal: "panic", Conditions: map[string]any{"lang": "go"}}},
			Recipe:    []schema.RecipeStep{{Step: "read logs", Tool: "grep", ArgsSchema: map[string]any{"pattern": "string"}, Validation: "match found"}},
			RequiredTools: []string{
				"grep",
			},
			FailureModes: []string{"missing logs"},
			Fallbacks:    []string{"ask user"},
			Performance:  &schema.PerformanceStats{SuccessCount: 9, FailureCount: 1, SuccessRate: 0.9, AvgLatencyMs: 12.5, LastUsedAt: &now},
			Version:      "v1",
		},
		&schema.PlanGraphPayload{
			Kind:          "plan_graph",
			PlanID:        "plan-1",
			Version:       "v1",
			Intent:        "deploy",
			Constraints:   map[string]any{"risk": "low"},
			InputsSchema:  map[string]any{"env": "string"},
			OutputsSchema: map[string]any{"url": "string"},
			Nodes:         []schema.PlanNode{{ID: "n1", Op: "test", Params: map[string]any{"pkg": "./..."}, Guards: map[string]any{"clean": true}}},
			Edges:         []schema.PlanEdge{{From: "n1", To: "n2", Kind: schema.EdgeKindControl}},
			Metrics:       &schema.PlanMetrics{AvgLatencyMs: 20, FailureRate: 0.1, ExecutionCount: 3, LastExecutedAt: &later},
		},
	}

	for _, payload := range payloads {
		t.Run(payload.PayloadKind(), func(t *testing.T) {
			wire, err := payloadToPB(payload)
			if err != nil {
				t.Fatalf("payloadToPB: %v", err)
			}
			roundTrip, err := payloadFromPB(wire)
			if err != nil {
				t.Fatalf("payloadFromPB: %v", err)
			}
			if roundTrip.PayloadKind() != payload.PayloadKind() {
				t.Fatalf("PayloadKind = %q, want %q", roundTrip.PayloadKind(), payload.PayloadKind())
			}
		})
	}
}

func TestPayloadToPBValuePayloadVariants(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	payloads := []schema.Payload{
		schema.EpisodicPayload{
			Kind:     "episodic",
			Timeline: []schema.TimelineEvent{{T: now, EventKind: "event", Ref: "evt-1"}},
		},
		schema.WorkingPayload{
			Kind:     "working",
			ThreadID: "thread-1",
			State:    schema.TaskStatePlanning,
		},
		schema.SemanticPayload{
			Kind:      "semantic",
			Subject:   "Orchid",
			Predicate: "is",
			Object:    "ready",
			Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
		},
		schema.EntityPayload{
			Kind:          "entity",
			CanonicalName: "Orchid",
		},
		schema.CompetencePayload{
			Kind:      "competence",
			SkillName: "debug",
		},
		schema.PlanGraphPayload{
			Kind:    "plan_graph",
			PlanID:  "plan-1",
			Version: "v1",
		},
	}

	for _, payload := range payloads {
		t.Run(payload.PayloadKind(), func(t *testing.T) {
			wire, err := payloadToPB(payload)
			if err != nil {
				t.Fatalf("payloadToPB value payload: %v", err)
			}
			roundTrip, err := payloadFromPB(wire)
			if err != nil {
				t.Fatalf("payloadFromPB: %v", err)
			}
			if roundTrip.PayloadKind() != payload.PayloadKind() {
				t.Fatalf("PayloadKind = %q, want %q", roundTrip.PayloadKind(), payload.PayloadKind())
			}
		})
	}
}

func TestMemoryRecordAndResponseConversions(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	rec := schema.NewMemoryRecord("semantic-1", schema.MemoryTypeSemantic, schema.SensitivityLow, &schema.SemanticPayload{
		Kind:      "semantic",
		Subject:   "Go",
		Predicate: "is",
		Object:    "typed",
		Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
	})
	rec.Confidence = 0.8
	rec.Salience = 0.7
	rec.Scope = "project"
	rec.Tags = []string{"tag-a"}
	rec.CreatedAt = now
	rec.UpdatedAt = now
	rec.Lifecycle.Decay.MinSalience = 0.2
	rec.Lifecycle.LastReinforcedAt = now
	rec.Lifecycle.Pinned = true
	rec.Lifecycle.DeletionPolicy = schema.DeletionPolicyManualOnly
	rec.Provenance = schema.Provenance{
		CreatedBy: "tester",
		Sources: []schema.ProvenanceSource{{
			Kind:      schema.ProvenanceKindObservation,
			Ref:       "obs-1",
			Hash:      "hash",
			CreatedBy: "tester",
			Timestamp: now,
		}},
	}
	rec.Relations = []schema.Relation{{Predicate: "supports", TargetID: "semantic-2", Weight: 0.6, CreatedAt: now}}
	rec.Interpretation = &schema.Interpretation{
		Status:               schema.InterpretationStatusResolved,
		Summary:              "summary",
		ProposedType:         schema.MemoryTypeSemantic,
		TopicalLabels:        []string{"label"},
		Mentions:             []schema.Mention{{Surface: "Go", EntityKind: schema.EntityKindConcept, CanonicalEntityID: "entity-go", Confidence: 0.9, Aliases: []string{"golang"}}},
		RelationCandidates:   []schema.RelationCandidate{{Predicate: "supports", TargetRecordID: "semantic-2", Confidence: 0.8, Resolved: true}},
		ReferenceCandidates:  []schema.ReferenceCandidate{{Ref: "obs-1", TargetRecordID: "semantic-1", Confidence: 0.7, Resolved: true}},
		ExtractionConfidence: 0.85,
	}
	rec.AuditLog = []schema.AuditEntry{{Action: schema.AuditActionCreate, Actor: "tester", Timestamp: now, Rationale: "created"}}

	wire, err := memoryRecordToPB(rec)
	if err != nil {
		t.Fatalf("memoryRecordToPB: %v", err)
	}
	roundTrip, err := memoryRecordFromPB(wire)
	if err != nil {
		t.Fatalf("memoryRecordFromPB: %v", err)
	}
	if roundTrip.ID != rec.ID || roundTrip.Type != rec.Type || roundTrip.Scope != rec.Scope {
		t.Fatalf("roundTrip = %+v, want key metadata from source", roundTrip)
	}
	if roundTrip.Interpretation == nil || len(roundTrip.Interpretation.Mentions) != 1 || len(roundTrip.Relations) != 1 || len(roundTrip.AuditLog) != 1 {
		t.Fatalf("roundTrip nested fields = %+v, want populated nested fields", roundTrip)
	}

	if response, err := marshalMemoryRecordResponse(rec); err != nil || response.Record.Id != rec.ID {
		t.Fatalf("marshalMemoryRecordResponse = %+v, %v; want record %s", response, err, rec.ID)
	}
	capture, err := marshalCaptureMemoryResponse(&ingestion.CaptureMemoryResponse{
		PrimaryRecord:  rec,
		CreatedRecords: []*schema.MemoryRecord{rec},
		Edges:          []schema.GraphEdge{{SourceID: "a", Predicate: "p", TargetID: "b", Weight: 1, CreatedAt: now}},
	})
	if err != nil || capture.PrimaryRecord.Id != rec.ID || len(capture.CreatedRecords) != 1 || len(capture.Edges) != 1 {
		t.Fatalf("marshalCaptureMemoryResponse = %+v, %v; want populated response", capture, err)
	}
	graph, err := marshalRetrieveGraphResponse(&retrieval.RetrieveGraphResponse{
		Nodes:   []retrieval.GraphNode{{Record: rec, Root: true, Hop: 0}},
		Edges:   []schema.GraphEdge{{SourceID: "a", Predicate: "p", TargetID: "b", Weight: 1, CreatedAt: now}},
		RootIDs: []string{rec.ID},
		Selection: &retrieval.SelectionResult{
			Selected:   []*schema.MemoryRecord{rec},
			Confidence: 0.9,
			NeedsMore:  false,
			Scores:     map[string]float64{rec.ID: 0.9},
		},
	})
	if err != nil || len(graph.Nodes) != 1 || len(graph.Edges) != 1 || graph.Selection == nil || len(graph.Selection.Selected) != 1 {
		t.Fatalf("marshalRetrieveGraphResponse = %+v, %v; want populated response", graph, err)
	}
}

func TestConversionNilAndErrorPaths(t *testing.T) {
	if rec, err := memoryRecordToPB(nil); err != nil || rec != nil {
		t.Fatalf("memoryRecordToPB nil = %+v, %v; want nil nil", rec, err)
	}
	if _, err := memoryRecordFromPB(nil); err == nil {
		t.Fatalf("memoryRecordFromPB nil error = nil")
	}
	if _, err := payloadFromPB(nil); err == nil {
		t.Fatalf("payloadFromPB nil error = nil")
	}
	invalidPayload := &pb.Payload{}
	// The protobuf oneof interface is sealed to its generated package. Injecting an
	// invalid implementation is the only way to exercise the defensive default branch.
	var localKind interface{ isPayload_Kind() } = &invalidPayloadKind{}
	*(*interfaceWords)(unsafe.Pointer(&invalidPayload.Kind)) = *(*interfaceWords)(unsafe.Pointer(&localKind))
	if payload, err := payloadFromPB(invalidPayload); err == nil || payload != nil {
		t.Fatalf("payloadFromPB invalid variant = %+v, %v; want unsupported variant error", payload, err)
	}
	if _, err := memoryRecordFromPB(&pb.MemoryRecord{}); err == nil {
		t.Fatalf("memoryRecordFromPB nil payload error = nil")
	}
	if payload, err := payloadToPB(struct{ schema.Payload }{}); err == nil || payload != nil {
		t.Fatalf("payloadToPB unsupported = %+v, %v; want nil error", payload, err)
	}
	if _, err := episodicPayloadFromPB(&pb.EpisodicPayload{Timeline: []*pb.TimelineEvent{{T: "bad"}}}); err == nil {
		t.Fatalf("episodicPayloadFromPB bad timeline error = nil")
	}
	if _, err := episodicPayloadFromPB(&pb.EpisodicPayload{ToolGraph: []*pb.ToolNode{{Timestamp: "bad"}}}); err == nil {
		t.Fatalf("episodicPayloadFromPB bad tool timestamp error = nil")
	}
	if _, err := episodicPayloadToPB(&schema.EpisodicPayload{
		ToolGraph: []schema.ToolNode{{ID: "tool-args", Args: map[string]any{"bad": make(chan int)}}},
	}); err == nil {
		t.Fatalf("episodicPayloadToPB bad tool args error = nil")
	}
	if _, err := episodicPayloadToPB(&schema.EpisodicPayload{
		ToolGraph: []schema.ToolNode{{ID: "tool-result", Result: make(chan int)}},
	}); err == nil {
		t.Fatalf("episodicPayloadToPB bad tool result error = nil")
	}
	if _, err := semanticPayloadFromPB(&pb.SemanticPayload{Validity: &pb.Validity{Start: "bad"}}); err == nil {
		t.Fatalf("semanticPayloadFromPB bad validity error = nil")
	}
	if _, err := semanticPayloadFromPB(&pb.SemanticPayload{Validity: &pb.Validity{End: "bad"}}); err == nil {
		t.Fatalf("semanticPayloadFromPB bad validity end error = nil")
	}
	if _, err := semanticPayloadFromPB(&pb.SemanticPayload{Evidence: []*pb.ProvenanceRef{{Timestamp: "bad"}}}); err == nil {
		t.Fatalf("semanticPayloadFromPB bad evidence error = nil")
	}
	if _, err := evidenceFromPB([]*pb.ProvenanceRef{{Timestamp: "bad"}}); err == nil {
		t.Fatalf("evidenceFromPB bad timestamp error = nil")
	}
	if _, err := provenanceSourcesFromPB([]*pb.ProvenanceSource{{Timestamp: "bad"}}); err == nil {
		t.Fatalf("provenanceSourcesFromPB bad timestamp error = nil")
	}
	if _, err := auditLogFromPB([]*pb.AuditEntry{{Timestamp: "bad"}}); err == nil {
		t.Fatalf("auditLogFromPB bad timestamp error = nil")
	}
	if _, err := relationsFromPB([]*pb.Relation{{CreatedAt: "bad"}}); err == nil {
		t.Fatalf("relationsFromPB bad timestamp error = nil")
	}
	if _, err := lifecycleFromPB(&pb.Lifecycle{LastReinforcedAt: "bad"}); err == nil {
		t.Fatalf("lifecycleFromPB bad timestamp error = nil")
	}
	if _, err := memoryRecordFromPB(&pb.MemoryRecord{
		Payload:   &pb.Payload{Kind: &pb.Payload_Semantic{Semantic: &pb.SemanticPayload{Kind: "semantic"}}},
		Relations: []*pb.Relation{{CreatedAt: "bad"}},
	}); err == nil {
		t.Fatalf("memoryRecordFromPB bad relation timestamp error = nil")
	}
	validRecord := func() *pb.MemoryRecord {
		now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC).Format(time.RFC3339Nano)
		return &pb.MemoryRecord{
			Id:          "rec-1",
			Type:        string(schema.MemoryTypeSemantic),
			Sensitivity: string(schema.SensitivityLow),
			CreatedAt:   now,
			UpdatedAt:   now,
			Payload:     &pb.Payload{Kind: &pb.Payload_Semantic{Semantic: &pb.SemanticPayload{Kind: "semantic"}}},
		}
	}
	badCreated := validRecord()
	badCreated.CreatedAt = "bad"
	if _, err := memoryRecordFromPB(badCreated); err == nil {
		t.Fatalf("memoryRecordFromPB bad created_at error = nil")
	}
	badUpdated := validRecord()
	badUpdated.UpdatedAt = "bad"
	if _, err := memoryRecordFromPB(badUpdated); err == nil {
		t.Fatalf("memoryRecordFromPB bad updated_at error = nil")
	}
	badLifecycle := validRecord()
	badLifecycle.Lifecycle = &pb.Lifecycle{LastReinforcedAt: "bad"}
	if _, err := memoryRecordFromPB(badLifecycle); err == nil {
		t.Fatalf("memoryRecordFromPB bad lifecycle error = nil")
	}
	badProvenance := validRecord()
	badProvenance.Provenance = &pb.Provenance{Sources: []*pb.ProvenanceSource{{Timestamp: "bad"}}}
	if _, err := memoryRecordFromPB(badProvenance); err == nil {
		t.Fatalf("memoryRecordFromPB bad provenance error = nil")
	}
	badAudit := validRecord()
	badAudit.AuditLog = []*pb.AuditEntry{{Timestamp: "bad"}}
	if _, err := memoryRecordFromPB(badAudit); err == nil {
		t.Fatalf("memoryRecordFromPB bad audit error = nil")
	}
	if _, err := payloadFromPB(&pb.Payload{Kind: &pb.Payload_Competence{Competence: &pb.CompetencePayload{
		Performance: &pb.PerformanceStats{LastUsedAt: "bad"},
	}}}); err == nil {
		t.Fatalf("payloadFromPB bad competence performance timestamp error = nil")
	}
	if _, err := payloadFromPB(&pb.Payload{Kind: &pb.Payload_PlanGraph{PlanGraph: &pb.PlanGraphPayload{
		Metrics: &pb.PlanMetrics{LastExecutedAt: "bad"},
	}}}); err == nil {
		t.Fatalf("payloadFromPB bad plan metrics timestamp error = nil")
	}
	if response, err := marshalCaptureMemoryResponse(nil); err != nil || response == nil {
		t.Fatalf("marshalCaptureMemoryResponse nil = %+v, %v; want empty response", response, err)
	}
	if response, err := marshalRetrieveGraphResponse(nil); err != nil || response == nil {
		t.Fatalf("marshalRetrieveGraphResponse nil = %+v, %v; want empty response", response, err)
	}

	badRecord := schema.NewMemoryRecord("bad", schema.MemoryTypeCompetence, schema.SensitivityLow, &schema.CompetencePayload{
		Kind: "competence",
		Triggers: []schema.Trigger{{
			Signal:     "bad",
			Conditions: map[string]any{"bad": make(chan int)},
		}},
	})
	if _, err := marshalMemoryRecordResponse(badRecord); err == nil {
		t.Fatalf("marshalMemoryRecordResponse bad payload error = nil")
	}
	if _, err := graphNodeToPB(retrieval.GraphNode{Record: badRecord}); err == nil {
		t.Fatalf("graphNodeToPB bad record error = nil")
	}
	if _, err := marshalCaptureMemoryResponse(&ingestion.CaptureMemoryResponse{PrimaryRecord: badRecord}); err == nil {
		t.Fatalf("marshalCaptureMemoryResponse bad primary error = nil")
	}
	if _, err := marshalCaptureMemoryResponse(&ingestion.CaptureMemoryResponse{CreatedRecords: []*schema.MemoryRecord{badRecord}}); err == nil {
		t.Fatalf("marshalCaptureMemoryResponse bad created record error = nil")
	}
	if _, err := marshalRetrieveGraphResponse(&retrieval.RetrieveGraphResponse{Nodes: []retrieval.GraphNode{{Record: badRecord}}}); err == nil {
		t.Fatalf("marshalRetrieveGraphResponse bad node error = nil")
	}
	if _, err := marshalRetrieveGraphResponse(&retrieval.RetrieveGraphResponse{
		Selection: &retrieval.SelectionResult{Selected: []*schema.MemoryRecord{badRecord}},
	}); err == nil {
		t.Fatalf("marshalRetrieveGraphResponse bad selection error = nil")
	}
}

func TestConversionNilBranches(t *testing.T) {
	if got, err := episodicPayloadToPB(nil); got != nil || err != nil {
		t.Fatalf("episodicPayloadToPB nil = %+v, %v; want nil nil", got, err)
	}
	if _, err := episodicPayloadFromPB(nil); err == nil {
		t.Fatalf("episodicPayloadFromPB nil error = nil")
	}
	if got, err := workingPayloadToPB(nil); got != nil || err != nil {
		t.Fatalf("workingPayloadToPB nil = %+v, %v; want nil nil", got, err)
	}
	if got := workingPayloadFromPB(nil); got != nil {
		t.Fatalf("workingPayloadFromPB nil = %+v, want nil", got)
	}
	if got, err := semanticPayloadToPB(nil); got != nil || err != nil {
		t.Fatalf("semanticPayloadToPB nil = %+v, %v; want nil nil", got, err)
	}
	if _, err := semanticPayloadFromPB(nil); err == nil {
		t.Fatalf("semanticPayloadFromPB nil error = nil")
	}
	if got := entityPayloadToPB(nil); got != nil {
		t.Fatalf("entityPayloadToPB nil = %+v, want nil", got)
	}
	if got := entityPayloadFromPB(nil); got != nil {
		t.Fatalf("entityPayloadFromPB nil = %+v, want nil", got)
	}
	if got, err := competencePayloadToPB(nil); got != nil || err != nil {
		t.Fatalf("competencePayloadToPB nil = %+v, %v; want nil nil", got, err)
	}
	if got, err := competencePayloadFromPB(nil); got != nil || err != nil {
		t.Fatalf("competencePayloadFromPB nil = %+v, %v; want nil nil", got, err)
	}
	if got, err := planGraphPayloadToPB(nil); got != nil || err != nil {
		t.Fatalf("planGraphPayloadToPB nil = %+v, %v; want nil nil", got, err)
	}
	if got, err := planGraphPayloadFromPB(nil); got != nil || err != nil {
		t.Fatalf("planGraphPayloadFromPB nil = %+v, %v; want nil nil", got, err)
	}
	if got := decayProfileFromPB(nil); got != (schema.DecayProfile{}) {
		t.Fatalf("decayProfileFromPB nil = %+v, want zero profile", got)
	}
	if got, err := selectionToPB(nil); got != nil || err != nil {
		t.Fatalf("selectionToPB nil = %+v, %v; want nil nil", got, err)
	}
}

func TestConversionSkipsNilRepeatedEntries(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC).Format(time.RFC3339Nano)

	if got, err := provenanceSourcesFromPB([]*pb.ProvenanceSource{nil, {Kind: string(schema.ProvenanceKindObservation), Ref: "obs-1", Timestamp: now}}); err != nil || len(got) != 1 {
		t.Fatalf("provenanceSourcesFromPB = %+v, %v; want one source", got, err)
	}
	if got, err := evidenceFromPB([]*pb.ProvenanceRef{nil, {SourceType: "observation", SourceId: "obs-1", Timestamp: now}}); err != nil || len(got) != 1 {
		t.Fatalf("evidenceFromPB = %+v, %v; want one ref", got, err)
	}
	if got := workingPayloadFromPB(&pb.WorkingPayload{
		ActiveConstraints: []*pb.Constraint{nil, {Key: "scope"}},
	}); got == nil || len(got.ActiveConstraints) != 1 || got.ActiveConstraints[0].Key != "scope" {
		t.Fatalf("workingPayloadFromPB = %+v, want one constraint", got)
	}
	if got, err := relationsFromPB([]*pb.Relation{nil, {Predicate: "supports", TargetId: "rec-2", CreatedAt: now}}); err != nil || len(got) != 1 {
		t.Fatalf("relationsFromPB = %+v, %v; want one relation", got, err)
	}
	if got, err := auditLogFromPB([]*pb.AuditEntry{nil, {Action: string(schema.AuditActionCreate), Timestamp: now}}); err != nil || len(got) != 1 {
		t.Fatalf("auditLogFromPB = %+v, %v; want one entry", got, err)
	}
	if got := mentionsFromPB([]*pb.Mention{nil, {Surface: "Go"}}); len(got) != 1 || got[0].Surface != "Go" {
		t.Fatalf("mentionsFromPB = %+v, want one mention", got)
	}
	if got := relationCandidatesFromPB([]*pb.RelationCandidate{nil, {Predicate: "supports"}}); len(got) != 1 || got[0].Predicate != "supports" {
		t.Fatalf("relationCandidatesFromPB = %+v, want one candidate", got)
	}
	if got := referenceCandidatesFromPB([]*pb.ReferenceCandidate{nil, {Ref: "obs-1"}}); len(got) != 1 || got[0].Ref != "obs-1" {
		t.Fatalf("referenceCandidatesFromPB = %+v, want one candidate", got)
	}
	if got := entityAliasesFromPB([]*pb.EntityAlias{nil, {Value: "golang"}}); len(got) != 1 || got[0].Value != "golang" {
		t.Fatalf("entityAliasesFromPB = %+v, want one alias", got)
	}
	if got := entityIdentifiersFromPB([]*pb.EntityIdentifier{nil, {Namespace: "slug", Value: "go"}}); len(got) != 1 || got[0].Value != "go" {
		t.Fatalf("entityIdentifiersFromPB = %+v, want one identifier", got)
	}
	competence, err := competencePayloadFromPB(&pb.CompetencePayload{
		Triggers: []*pb.Trigger{nil, {Signal: "panic"}},
		Recipe:   []*pb.RecipeStep{nil, {Step: "inspect"}},
	})
	if err != nil || len(competence.Triggers) != 1 || len(competence.Recipe) != 1 {
		t.Fatalf("competencePayloadFromPB = %+v, %v; want nil entries skipped", competence, err)
	}
	episode, err := episodicPayloadFromPB(&pb.EpisodicPayload{
		Timeline:  []*pb.TimelineEvent{nil, {T: now, EventKind: "event"}},
		ToolGraph: []*pb.ToolNode{nil, {Id: "tool-1", Timestamp: now}},
	})
	if err != nil || len(episode.Timeline) != 1 || len(episode.ToolGraph) != 1 {
		t.Fatalf("episodicPayloadFromPB = %+v, %v; want nil entries skipped", episode, err)
	}
	plan, err := planGraphPayloadFromPB(&pb.PlanGraphPayload{
		Nodes: []*pb.PlanNode{nil, {Id: "n1"}},
		Edges: []*pb.PlanEdge{nil, {From: "n1", To: "n2", Kind: string(schema.EdgeKindControl)}},
	})
	if err != nil || len(plan.Nodes) != 1 || len(plan.Edges) != 1 {
		t.Fatalf("planGraphPayloadFromPB = %+v, %v; want nil entries skipped", plan, err)
	}
	if node, err := graphNodeToPB(retrieval.GraphNode{}); err != nil || node.Record != nil {
		t.Fatalf("graphNodeToPB nil record = %+v, %v; want nil record", node, err)
	}
}

func TestPayloadToPBRejectsUnsupportedNestedValues(t *testing.T) {
	for _, tc := range []struct {
		name    string
		payload schema.Payload
	}{
		{name: "episodic pointer", payload: &schema.EpisodicPayload{Environment: &schema.EnvironmentSnapshot{Context: map[string]any{"bad": make(chan int)}}}},
		{name: "episodic value", payload: schema.EpisodicPayload{Environment: &schema.EnvironmentSnapshot{Context: map[string]any{"bad": make(chan int)}}}},
		{name: "working pointer", payload: &schema.WorkingPayload{ActiveConstraints: []schema.Constraint{{Key: "bad", Value: make(chan int)}}}},
		{name: "working value", payload: schema.WorkingPayload{ActiveConstraints: []schema.Constraint{{Key: "bad", Value: make(chan int)}}}},
		{name: "semantic pointer", payload: &schema.SemanticPayload{Object: make(chan int)}},
		{name: "semantic value", payload: schema.SemanticPayload{Object: make(chan int)}},
		{name: "competence pointer", payload: &schema.CompetencePayload{Triggers: []schema.Trigger{{Signal: "bad", Conditions: map[string]any{"bad": make(chan int)}}}}},
		{name: "competence value", payload: schema.CompetencePayload{Triggers: []schema.Trigger{{Signal: "bad", Conditions: map[string]any{"bad": make(chan int)}}}}},
		{name: "plan pointer", payload: &schema.PlanGraphPayload{Constraints: map[string]any{"bad": make(chan int)}}},
		{name: "plan value", payload: schema.PlanGraphPayload{Constraints: map[string]any{"bad": make(chan int)}}},
	} {
		t.Run("payloadToPB "+tc.name, func(t *testing.T) {
			if _, err := payloadToPB(tc.payload); err == nil {
				t.Fatalf("payloadToPB %s error = nil", tc.name)
			}
		})
	}

	if _, err := episodicPayloadToPB(&schema.EpisodicPayload{
		Kind:        "episodic",
		Environment: &schema.EnvironmentSnapshot{Context: map[string]any{"bad": make(chan int)}},
	}); err == nil {
		t.Fatalf("episodicPayloadToPB bad environment context error = nil")
	}
	if _, err := workingPayloadToPB(&schema.WorkingPayload{
		Kind:              "working",
		ThreadID:          "thread",
		State:             schema.TaskStatePlanning,
		ActiveConstraints: []schema.Constraint{{Key: "bad", Value: make(chan int)}},
	}); err == nil {
		t.Fatalf("workingPayloadToPB bad constraint value error = nil")
	}
	if _, err := semanticPayloadToPB(&schema.SemanticPayload{
		Kind:      "semantic",
		Subject:   "subject",
		Predicate: "predicate",
		Object:    make(chan int),
		Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
	}); err == nil {
		t.Fatalf("semanticPayloadToPB bad object error = nil")
	}
	if _, err := semanticPayloadToPB(&schema.SemanticPayload{
		Kind:      "semantic",
		Subject:   "subject",
		Predicate: "predicate",
		Object:    "object",
		Validity:  schema.Validity{Mode: schema.ValidityModeGlobal, Conditions: map[string]any{"bad": make(chan int)}},
	}); err == nil {
		t.Fatalf("semanticPayloadToPB bad validity conditions error = nil")
	}
	if _, err := competencePayloadToPB(&schema.CompetencePayload{
		Kind:     "competence",
		Triggers: []schema.Trigger{{Signal: "panic", Conditions: map[string]any{"bad": make(chan int)}}},
	}); err == nil {
		t.Fatalf("competencePayloadToPB bad trigger conditions error = nil")
	}
	if _, err := competencePayloadToPB(&schema.CompetencePayload{
		Kind:   "competence",
		Recipe: []schema.RecipeStep{{Step: "bad", ArgsSchema: map[string]any{"bad": make(chan int)}}},
	}); err == nil {
		t.Fatalf("competencePayloadToPB bad recipe args schema error = nil")
	}
	if _, err := planGraphPayloadToPB(&schema.PlanGraphPayload{
		Kind:        "plan_graph",
		Constraints: map[string]any{"bad": make(chan int)},
	}); err == nil {
		t.Fatalf("planGraphPayloadToPB bad constraints error = nil")
	}
	if _, err := planGraphPayloadToPB(&schema.PlanGraphPayload{
		Kind:         "plan_graph",
		InputsSchema: map[string]any{"bad": make(chan int)},
	}); err == nil {
		t.Fatalf("planGraphPayloadToPB bad inputs schema error = nil")
	}
	if _, err := planGraphPayloadToPB(&schema.PlanGraphPayload{
		Kind:          "plan_graph",
		OutputsSchema: map[string]any{"bad": make(chan int)},
	}); err == nil {
		t.Fatalf("planGraphPayloadToPB bad outputs schema error = nil")
	}
	if _, err := planGraphPayloadToPB(&schema.PlanGraphPayload{
		Kind:  "plan_graph",
		Nodes: []schema.PlanNode{{ID: "n1", Params: map[string]any{"bad": make(chan int)}}},
	}); err == nil {
		t.Fatalf("planGraphPayloadToPB bad node params error = nil")
	}
	if _, err := planGraphPayloadToPB(&schema.PlanGraphPayload{
		Kind:  "plan_graph",
		Nodes: []schema.PlanNode{{ID: "n1", Guards: map[string]any{"bad": make(chan int)}}},
	}); err == nil {
		t.Fatalf("planGraphPayloadToPB bad node guards error = nil")
	}
}
