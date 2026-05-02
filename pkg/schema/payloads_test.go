package schema

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestPayloadKinds(t *testing.T) {
	for _, tc := range []struct {
		payload Payload
		want    string
	}{
		{payload: EpisodicPayload{Kind: "episodic"}, want: "episodic"},
		{payload: WorkingPayload{Kind: "working"}, want: "working"},
		{payload: SemanticPayload{Kind: "semantic"}, want: "semantic"},
		{payload: EntityPayload{Kind: "entity"}, want: "entity"},
		{payload: CompetencePayload{Kind: "competence"}, want: "competence"},
		{payload: PlanGraphPayload{Kind: "plan_graph"}, want: "plan_graph"},
	} {
		if got := tc.payload.PayloadKind(); got != tc.want {
			t.Fatalf("%T PayloadKind = %q, want %q", tc.payload, got, tc.want)
		}
		tc.payload.isPayload()
	}
}

func TestConcretePayloadMarkerMethods(t *testing.T) {
	EpisodicPayload{}.isPayload()
	WorkingPayload{}.isPayload()
	SemanticPayload{}.isPayload()
	EntityPayload{}.isPayload()
	CompetencePayload{}.isPayload()
	PlanGraphPayload{}.isPayload()
}

func TestPayloadWrapperMarshalAndUnmarshal(t *testing.T) {
	for _, tc := range []struct {
		name    string
		payload Payload
		want    any
	}{
		{
			name: "episodic",
			payload: &EpisodicPayload{
				Kind:     "episodic",
				Timeline: []TimelineEvent{{EventKind: "event", Ref: "evt-1", Summary: "summary"}},
			},
			want: &EpisodicPayload{},
		},
		{
			name:    "working",
			payload: &WorkingPayload{Kind: "working", ThreadID: "thread-1", State: TaskStatePlanning},
			want:    &WorkingPayload{},
		},
		{
			name: "semantic",
			payload: &SemanticPayload{
				Kind:      "semantic",
				Subject:   "Go",
				Predicate: "is",
				Object:    "typed",
				Validity:  Validity{Mode: ValidityModeGlobal},
			},
			want: &SemanticPayload{},
		},
		{
			name: "entity",
			payload: &EntityPayload{
				Kind:          "entity",
				CanonicalName: "Orchid",
				Aliases:       []EntityAlias{{Value: "orchid"}},
			},
			want: &EntityPayload{},
		},
		{
			name: "competence",
			payload: &CompetencePayload{
				Kind:      "competence",
				SkillName: "debug",
				Triggers:  []Trigger{{Signal: "panic"}},
				Recipe:    []RecipeStep{{Step: "read logs"}},
			},
			want: &CompetencePayload{},
		},
		{
			name: "plan graph",
			payload: &PlanGraphPayload{
				Kind:    "plan_graph",
				PlanID:  "plan-1",
				Version: "1",
				Nodes:   []PlanNode{{ID: "n1", Op: "test"}},
				Edges:   []PlanEdge{{From: "n1", To: "n2", Kind: EdgeKindControl}},
			},
			want: &PlanGraphPayload{},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data, err := json.Marshal(PayloadWrapper{Payload: tc.payload})
			if err != nil {
				t.Fatalf("Marshal PayloadWrapper: %v", err)
			}

			var got PayloadWrapper
			if err := json.Unmarshal(data, &got); err != nil {
				t.Fatalf("Unmarshal PayloadWrapper: %v", err)
			}
			if got.Payload == nil {
				t.Fatalf("Payload = nil, want %T", tc.want)
			}
			if got.Payload.PayloadKind() != tc.payload.PayloadKind() {
				t.Fatalf("PayloadKind = %q, want %q", got.Payload.PayloadKind(), tc.payload.PayloadKind())
			}
		})
	}
}

func TestPayloadWrapperUnknownAndInvalidPayloads(t *testing.T) {
	var unknown PayloadWrapper
	if err := json.Unmarshal([]byte(`{"kind":"unknown","value":1}`), &unknown); err != nil {
		t.Fatalf("Unmarshal unknown payload: %v", err)
	}
	if unknown.Payload != nil {
		t.Fatalf("unknown Payload = %#v, want nil", unknown.Payload)
	}

	var invalidKind PayloadWrapper
	if err := json.Unmarshal([]byte(`{`), &invalidKind); err == nil {
		t.Fatalf("Unmarshal invalid JSON error = nil, want error")
	}
	if err := invalidKind.UnmarshalJSON([]byte(`{`)); err == nil {
		t.Fatalf("direct UnmarshalJSON invalid JSON error = nil, want error")
	}

	var invalidPayload PayloadWrapper
	if err := json.Unmarshal([]byte(`{"kind":"entity","aliases":[{"value":1}]}`), &invalidPayload); err == nil {
		t.Fatalf("Unmarshal invalid entity payload error = nil, want error")
	}
}

func TestValidationErrorError(t *testing.T) {
	err := (&ValidationError{Field: "id", Message: "id is required"}).Error()
	if err != "validation error for id: id is required" {
		t.Fatalf("ValidationError.Error = %q", err)
	}
}

func TestEntityHelpersAndAliasJSON(t *testing.T) {
	if got := NormalizeEntityTerm("  Orchid Project  "); got != "orchid project" {
		t.Fatalf("NormalizeEntityTerm = %q, want orchid project", got)
	}
	aliases := []EntityAlias{
		{Value: " Orchid "},
		{Value: "   "},
		{Value: "Staging", Kind: "nickname", Locale: "en"},
	}
	if got := EntityAliasValues(aliases); !reflect.DeepEqual(got, []string{"Orchid", "Staging"}) {
		t.Fatalf("EntityAliasValues = %#v, want trimmed non-empty aliases", got)
	}
	if got := EntityTypes(nil); got != nil {
		t.Fatalf("EntityTypes nil = %#v, want nil", got)
	}
	payload := &EntityPayload{
		PrimaryType: EntityTypeProject,
		Types:       []string{" project ", EntityTypeService, "", "SERVICE"},
	}
	if got := EntityTypes(payload); !reflect.DeepEqual(got, []string{EntityTypeProject, EntityTypeService}) {
		t.Fatalf("EntityTypes = %#v, want primary plus deduped types", got)
	}

	plainAlias, err := json.Marshal(EntityAlias{Value: "Orchid"})
	if err != nil {
		t.Fatalf("Marshal plain alias: %v", err)
	}
	if string(plainAlias) != `"Orchid"` {
		t.Fatalf("plain alias JSON = %s, want string form", plainAlias)
	}
	typedAlias, err := json.Marshal(EntityAlias{Value: "Orchid", Kind: "nickname", Locale: "en"})
	if err != nil {
		t.Fatalf("Marshal typed alias: %v", err)
	}
	if !strings.Contains(string(typedAlias), `"kind":"nickname"`) {
		t.Fatalf("typed alias JSON = %s, want object form", typedAlias)
	}
	var fromString EntityAlias
	if err := json.Unmarshal([]byte(`"Orchid"`), &fromString); err != nil {
		t.Fatalf("Unmarshal alias string: %v", err)
	}
	if fromString.Value != "Orchid" || fromString.Kind != "" || fromString.Locale != "" {
		t.Fatalf("alias from string = %+v, want value only", fromString)
	}
	var fromObject EntityAlias
	if err := json.Unmarshal([]byte(`{"value":"Orchid","kind":"nickname","locale":"en"}`), &fromObject); err != nil {
		t.Fatalf("Unmarshal alias object: %v", err)
	}
	if fromObject.Value != "Orchid" || fromObject.Kind != "nickname" || fromObject.Locale != "en" {
		t.Fatalf("alias from object = %+v, want full object", fromObject)
	}
}

func TestMemoryRecordValidateRejectsRequiredAndRangeFields(t *testing.T) {
	for _, tc := range []struct {
		name string
		rec  *MemoryRecord
		want string
	}{
		{name: "missing id", rec: &MemoryRecord{Type: MemoryTypeSemantic, Sensitivity: SensitivityLow, Payload: &SemanticPayload{}}, want: "id"},
		{name: "missing type", rec: &MemoryRecord{ID: "id", Sensitivity: SensitivityLow, Payload: &SemanticPayload{}}, want: "type"},
		{name: "missing sensitivity", rec: &MemoryRecord{ID: "id", Type: MemoryTypeSemantic, Payload: &SemanticPayload{}}, want: "sensitivity"},
		{name: "confidence low", rec: &MemoryRecord{ID: "id", Type: MemoryTypeSemantic, Sensitivity: SensitivityLow, Confidence: -0.1, Payload: &SemanticPayload{}}, want: "confidence"},
		{name: "confidence high", rec: &MemoryRecord{ID: "id", Type: MemoryTypeSemantic, Sensitivity: SensitivityLow, Confidence: 1.1, Payload: &SemanticPayload{}}, want: "confidence"},
		{name: "negative salience", rec: &MemoryRecord{ID: "id", Type: MemoryTypeSemantic, Sensitivity: SensitivityLow, Salience: -0.1, Payload: &SemanticPayload{}}, want: "salience"},
		{name: "missing payload", rec: &MemoryRecord{ID: "id", Type: MemoryTypeSemantic, Sensitivity: SensitivityLow, Confidence: 1}, want: "payload"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.rec.Validate()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Validate error = %v, want containing %q", err, tc.want)
			}
		})
	}
}
