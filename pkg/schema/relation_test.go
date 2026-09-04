package schema

import (
	"math"
	"testing"
)

func TestNormalizeGraphPredicate(t *testing.T) {
	tests := []struct {
		value string
		want  string
	}{
		{value: "Depends_On", want: GraphPredicateDependsOn},
		{value: " Depends On ", want: GraphPredicateDependsOn},
		{value: "Depends-On", want: GraphPredicateDependsOn},
		{value: "dependsOn", want: GraphPredicateDependsOn},
		{value: "factSubjectOf", want: GraphPredicateFactSubjectOf},
		{value: "HTTPServerUses", want: "http_server_uses"},
		{value: "Custom Predicate", want: "custom_predicate"},
		{value: "  many---separators__here  ", want: "many_separators_here"},
		{value: "  many   spaces  ", want: "many_spaces"},
		{value: "", want: ""},
		{value: "   ", want: ""},
	}

	for _, tc := range tests {
		if got := NormalizeGraphPredicate(tc.value); got != tc.want {
			t.Fatalf("NormalizeGraphPredicate(%q) = %q, want %q", tc.value, got, tc.want)
		}
	}
}

func TestGraphPredicateConstantsAndInverses(t *testing.T) {
	tests := []struct {
		predicate string
		inverse   string
	}{
		{GraphPredicateMentionsEntity, GraphPredicateMentionedIn},
		{GraphPredicateMentionedIn, GraphPredicateMentionsEntity},
		{GraphPredicateReferencesRecord, GraphPredicateReferencedBy},
		{GraphPredicateReferencedBy, GraphPredicateReferencesRecord},
		{GraphPredicateDerivedSemantic, GraphPredicateDerivedFrom},
		{GraphPredicateDerivedFrom, GraphPredicateDerivedSemantic},
		{GraphPredicateSubjectEntity, GraphPredicateFactSubjectOf},
		{GraphPredicateFactSubjectOf, GraphPredicateSubjectEntity},
		{GraphPredicateObjectEntity, GraphPredicateFactObjectOf},
		{GraphPredicateFactObjectOf, GraphPredicateObjectEntity},
		{GraphPredicateDependsOn, GraphPredicateDependencyOf},
		{GraphPredicateDependencyOf, GraphPredicateDependsOn},
		{GraphPredicateUses, GraphPredicateUsedBy},
		{GraphPredicateUsedBy, GraphPredicateUses},
		{GraphPredicateCausedBy, GraphPredicateCauses},
		{GraphPredicateCauses, GraphPredicateCausedBy},
		{GraphPredicateSupports, GraphPredicateSupportedBy},
		{GraphPredicateSupportedBy, GraphPredicateSupports},
		{GraphPredicateContradicts, GraphPredicateContradictedBy},
		{GraphPredicateContradictedBy, GraphPredicateContradicts},
		{GraphPredicateSupersedes, GraphPredicateSupersededBy},
		{GraphPredicateSupersededBy, GraphPredicateSupersedes},
		{GraphPredicateContestedBy, GraphPredicateContests},
		{GraphPredicateContests, GraphPredicateContestedBy},
	}

	for _, tc := range tests {
		if got := InverseGraphPredicate(tc.predicate); got != tc.inverse {
			t.Fatalf("InverseGraphPredicate(%q) = %q, want %q", tc.predicate, got, tc.inverse)
		}
	}
	if got := InverseGraphPredicate("custom"); got != "inverse_of_custom" {
		t.Fatalf("InverseGraphPredicate custom = %q, want inverse_of_custom", got)
	}
	if got := InverseGraphPredicate(" Depends_On "); got != GraphPredicateDependencyOf {
		t.Fatalf("InverseGraphPredicate normalized = %q, want %q", got, GraphPredicateDependencyOf)
	}
	if got := InverseGraphPredicate("mentionsEntity"); got != GraphPredicateMentionedIn {
		t.Fatalf("InverseGraphPredicate camelCase = %q, want %q", got, GraphPredicateMentionedIn)
	}
	if got := InverseGraphPredicate(" Custom Predicate "); got != "inverse_of_custom_predicate" {
		t.Fatalf("InverseGraphPredicate normalized custom = %q, want inverse_of_custom_predicate", got)
	}
}

func TestRelationValidate(t *testing.T) {
	valid := Relation{Predicate: "Depends-On", TargetID: "target", Weight: 0.5}
	if err := valid.Validate(); err != nil {
		t.Fatalf("Validate valid relation: %v", err)
	}

	tests := []struct {
		name string
		rel  Relation
		want string
	}{
		{name: "empty predicate", rel: Relation{TargetID: "target", Weight: 0.5}, want: "relation.predicate"},
		{name: "empty target", rel: Relation{Predicate: "supports", Weight: 0.5}, want: "relation.target_id"},
		{name: "negative weight", rel: Relation{Predicate: "supports", TargetID: "target", Weight: -0.1}, want: "relation.weight"},
		{name: "overweight", rel: Relation{Predicate: "supports", TargetID: "target", Weight: 1.1}, want: "relation.weight"},
		{name: "nan weight", rel: Relation{Predicate: "supports", TargetID: "target", Weight: math.NaN()}, want: "relation.weight"},
		{name: "infinite weight", rel: Relation{Predicate: "supports", TargetID: "target", Weight: math.Inf(1)}, want: "relation.weight"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.rel.Validate()
			verr, ok := err.(*ValidationError)
			if !ok {
				t.Fatalf("Validate error = %T/%v, want ValidationError", err, err)
			}
			if verr.Field != tc.want {
				t.Fatalf("Validate field = %q, want %q", verr.Field, tc.want)
			}
		})
	}
}

func TestGraphEdgeValidate(t *testing.T) {
	valid := GraphEdge{SourceID: "source", Predicate: "mentions entity", TargetID: "target", Weight: 1}
	if err := valid.Validate(); err != nil {
		t.Fatalf("Validate valid graph edge: %v", err)
	}

	tests := []struct {
		name string
		edge GraphEdge
		want string
	}{
		{name: "empty source", edge: GraphEdge{Predicate: "supports", TargetID: "target", Weight: 0.5}, want: "edge.source_id"},
		{name: "empty predicate", edge: GraphEdge{SourceID: "source", TargetID: "target", Weight: 0.5}, want: "edge.predicate"},
		{name: "empty target", edge: GraphEdge{SourceID: "source", Predicate: "supports", Weight: 0.5}, want: "edge.target_id"},
		{name: "invalid weight", edge: GraphEdge{SourceID: "source", Predicate: "supports", TargetID: "target", Weight: math.Inf(-1)}, want: "edge.weight"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.edge.Validate()
			verr, ok := err.(*ValidationError)
			if !ok {
				t.Fatalf("Validate error = %T/%v, want ValidationError", err, err)
			}
			if verr.Field != tc.want {
				t.Fatalf("Validate field = %q, want %q", verr.Field, tc.want)
			}
		})
	}
}
