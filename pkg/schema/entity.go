package schema

import "strings"

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
	return strings.ToLower(strings.TrimSpace(value))
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
