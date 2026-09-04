package schema

import (
	"encoding/json"
	"fmt"
	"strings"
)

// NormalizeSemanticPredicate returns the canonical storage spelling for semantic
// subject-predicate-object fact predicates.
func NormalizeSemanticPredicate(predicate string) string {
	return NormalizeGraphPredicate(predicate)
}

// SemanticObjectKey returns the stable object representation used for exact
// semantic fact lookup. String facts stay human-readable; structured facts use
// deterministic JSON so equivalent objects do not depend on Go map formatting.
func SemanticObjectKey(object any) string {
	if object == nil {
		return ""
	}
	if value, ok := object.(string); ok {
		return strings.TrimSpace(value)
	}
	encoded, err := json.Marshal(object)
	if err == nil {
		return strings.TrimSpace(string(encoded))
	}
	return strings.TrimSpace(fmt.Sprint(object))
}
