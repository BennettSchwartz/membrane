package schema

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	EntityTypePerson              = "Person"
	EntityTypeOrganization        = "Organization"
	EntityTypeTeam                = "Team"
	EntityTypeAgent               = "Agent"
	EntityTypeProject             = "Project"
	EntityTypeRepository          = "Repository"
	EntityTypeFile                = "File"
	EntityTypeDirectory           = "Directory"
	EntityTypeSymbol              = "Symbol"
	EntityTypeAPI                 = "API"
	EntityTypeService             = "Service"
	EntityTypeDatabase            = "Database"
	EntityTypePackage             = "Package"
	EntityTypeDependency          = "Dependency"
	EntityTypeTool                = "Tool"
	EntityTypeCommand             = "Command"
	EntityTypeRuntime             = "Runtime"
	EntityTypeEnvironment         = "Environment"
	EntityTypeTask                = "Task"
	EntityTypeIssue               = "Issue"
	EntityTypePullRequest         = "PullRequest"
	EntityTypeDecision            = "Decision"
	EntityTypeRequirement         = "Requirement"
	EntityTypeIncident            = "Incident"
	EntityTypeDocument            = "Document"
	EntityTypeURL                 = "URL"
	EntityTypeDataset             = "Dataset"
	EntityTypeMetric              = "Metric"
	EntityTypeConcept             = "Concept"
	EntityTypeEvent               = "Event"
	EntityTypeOther               = "Other"
	EntityTermKindCanonical       = "canonical"
	EntityTermKindAlias           = "alias"
	EntityTermKindIdentifier      = "identifier"
	EntityIdentifierNamespaceSelf = "membrane"
)

var BuiltinEntityTypes = []string{
	EntityTypePerson,
	EntityTypeOrganization,
	EntityTypeTeam,
	EntityTypeAgent,
	EntityTypeProject,
	EntityTypeRepository,
	EntityTypeFile,
	EntityTypeDirectory,
	EntityTypeSymbol,
	EntityTypeAPI,
	EntityTypeService,
	EntityTypeDatabase,
	EntityTypePackage,
	EntityTypeDependency,
	EntityTypeTool,
	EntityTypeCommand,
	EntityTypeRuntime,
	EntityTypeEnvironment,
	EntityTypeTask,
	EntityTypeIssue,
	EntityTypePullRequest,
	EntityTypeDecision,
	EntityTypeRequirement,
	EntityTypeIncident,
	EntityTypeDocument,
	EntityTypeURL,
	EntityTypeDataset,
	EntityTypeMetric,
	EntityTypeConcept,
	EntityTypeEvent,
	EntityTypeOther,
}

// NormalizeEntityTerm normalizes entity lookup terms for indexing.
func NormalizeEntityTerm(value string) string {
	return strings.Join(strings.Fields(strings.ToLower(value)), " ")
}

// NormalizeEntityIdentifierNamespace normalizes external identifier namespaces.
// Identifier values are left to their source system because they may be
// case-sensitive, but namespaces such as "github" should not fragment by case.
func NormalizeEntityIdentifierNamespace(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

// ParseEntityIdentifierTokens extracts explicit namespace:value entity
// identifiers from user-facing text. It is intentionally conservative so
// ordinary URLs, email addresses, and clock times are not treated as entity
// IDs.
func ParseEntityIdentifierTokens(text string) []EntityIdentifier {
	return ParseEntityIdentifierTokensBounded(text, len(text), len(text))
}

// ParseEntityIdentifierTokensBounded extracts at most maxIdentifiers explicit
// identifiers after examining at most maxInputBytes of text. If the byte cap
// lands in the middle of a token, that partial token is ignored.
func ParseEntityIdentifierTokensBounded(text string, maxInputBytes, maxIdentifiers int) []EntityIdentifier {
	if maxInputBytes <= 0 || maxIdentifiers <= 0 || text == "" {
		return nil
	}
	if maxInputBytes < len(text) {
		text = text[:maxInputBytes]
		lastRune, _ := utf8.DecodeLastRuneInString(text)
		if len(text) > 0 && !unicode.IsSpace(lastRune) {
			lastBoundary := strings.LastIndexFunc(text, unicode.IsSpace)
			if lastBoundary < 0 {
				return nil
			}
			text = text[:lastBoundary]
		}
	}
	identifiers := make([]EntityIdentifier, 0, min(maxIdentifiers, 8))
	seen := make(map[string]struct{}, min(maxIdentifiers, 8))
	for field := range strings.FieldsSeq(text) {
		if len(identifiers) >= maxIdentifiers {
			break
		}
		token := strings.Trim(field, " \t\r\n\"'`([{<>,.;!?")
		idx := strings.Index(token, ":")
		if idx <= 0 || idx >= len(token)-1 {
			continue
		}
		namespace := NormalizeEntityIdentifierNamespace(token[:idx])
		if !explicitEntityIdentifierNamespace(namespace) {
			continue
		}
		value := strings.Trim(token[idx+1:], " \t\r\n\"'`)]}>.,;!?")
		if value == "" {
			continue
		}
		key := namespace + "\x00" + value
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		identifiers = append(identifiers, EntityIdentifier{Namespace: namespace, Value: value})
	}
	return identifiers
}

func explicitEntityIdentifierNamespace(namespace string) bool {
	switch namespace {
	case "", "http", "https", "mailto":
		return false
	}
	first := namespace[0]
	if !((first >= 'a' && first <= 'z') || first == '_') {
		return false
	}
	for i := 0; i < len(namespace); i++ {
		ch := namespace[i]
		if (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '_' || ch == '-' || ch == '.' {
			continue
		}
		return false
	}
	return true
}

// EntityTermMatchesQuery reports whether an indexed entity term should match
// a caller query or task descriptor. It accepts exact matches and bounded
// phrase containment in either direction, so "debug Project Orchid rollout"
// can find an entity indexed as "Project Orchid" without matching "go" inside
// "postgres".
func EntityTermMatchesQuery(indexedTerm, query string) bool {
	return EntityTermMatchRank(indexedTerm, query) < 3
}

// NormalizedEntityTermMatchesQuery is EntityTermMatchesQuery for values that
// have already been normalized with NormalizeEntityTerm.
func NormalizedEntityTermMatchesQuery(normalizedIndexedTerm, normalizedQuery string) bool {
	return NormalizedEntityTermMatchRank(normalizedIndexedTerm, normalizedQuery) < 3
}

// EntityTermMatchRank returns a relevance rank for an indexed entity term
// against a caller query. Lower values are better: exact match, indexed term
// contained in query, query contained in indexed term, then no match.
func EntityTermMatchRank(indexedTerm, query string) int {
	indexed := NormalizeEntityTerm(indexedTerm)
	q := NormalizeEntityTerm(query)
	return NormalizedEntityTermMatchRank(indexed, q)
}

// NormalizedEntityTermMatchRank ranks strings already normalized by
// NormalizeEntityTerm. Its phrase searches take linear time in the input sizes.
func NormalizedEntityTermMatchRank(indexed, q string) int {
	switch {
	case indexed == "" || q == "":
		return 3
	case indexed == q:
		return 0
	case boundedPhraseContains(q, indexed):
		return 1
	case boundedPhraseContains(indexed, q):
		return 2
	default:
		return 3
	}
}

// EntityTermMatchSpecificity returns a tie-break score for terms with the same
// EntityTermMatchRank. Higher values are better.
func EntityTermMatchSpecificity(indexedTerm, query string) int {
	indexed := NormalizeEntityTerm(indexedTerm)
	q := NormalizeEntityTerm(query)
	switch NormalizedEntityTermMatchRank(indexed, q) {
	case 1:
		return len(indexed)
	case 2:
		return -len(indexed)
	default:
		return 0
	}
}

func boundedPhraseContains(haystack, needle string) bool {
	matched, _ := entityPhraseMatch(haystack, needle)
	return matched
}

// entityPhraseMatch uses prefix fallbacks (KMP), retaining overlap information
// after a boundary rejection instead of searching the same suffix again. Work
// counts character comparisons and boundary checks for deterministic budget tests.
func entityPhraseMatch(haystack, needle string) (matched bool, work int) {
	if needle == "" || len(needle) > len(haystack) {
		return false, 0
	}
	prefix := make([]int, len(needle))
	for i, j := 1, 0; i < len(needle); i++ {
		for j > 0 {
			work++
			if needle[i] == needle[j] {
				break
			}
			j = prefix[j-1]
		}
		if j == 0 {
			work++
			if needle[i] == needle[0] {
				j++
			}
		} else {
			j++
		}
		prefix[i] = j
	}
	for i, j := 0, 0; i < len(haystack); i++ {
		for j > 0 {
			work++
			if haystack[i] == needle[j] {
				break
			}
			j = prefix[j-1]
		}
		if j == 0 {
			work++
			if haystack[i] == needle[0] {
				j++
			}
		} else {
			j++
		}
		if j == len(needle) {
			work += 2
			if isEntityTermBoundary(haystack, i-len(needle)) && isEntityTermBoundary(haystack, i+1) {
				return true, work
			}
			j = prefix[j-1]
		}
	}
	return false, work
}

func isEntityTermBoundary(value string, idx int) bool {
	if idx < 0 || idx >= len(value) {
		return true
	}
	ch := value[idx]
	return !((ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9'))
}

// EntityAliasValues returns all non-empty alias strings.
func EntityAliasValues(aliases []EntityAlias) []string {
	values := make([]string, 0, len(aliases))
	for _, alias := range aliases {
		if value := strings.TrimSpace(alias.Value); value != "" {
			values = append(values, value)
		}
	}
	return values
}

// EntityTypes returns a deduplicated type list including PrimaryType.
func EntityTypes(payload *EntityPayload) []string {
	if payload == nil {
		return nil
	}
	values := make([]string, 0, len(payload.Types)+1)
	if payload.PrimaryType != "" {
		values = append(values, payload.PrimaryType)
	}
	values = append(values, payload.Types...)
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		key := strings.ToLower(value)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, value)
	}
	return out
}
