export const MemoryType = {
  EPISODIC: "episodic",
  WORKING: "working",
  SEMANTIC: "semantic",
  COMPETENCE: "competence",
  PLAN_GRAPH: "plan_graph",
  ENTITY: "entity"
} as const;

export type MemoryType = (typeof MemoryType)[keyof typeof MemoryType];

export const Sensitivity = {
  PUBLIC: "public",
  LOW: "low",
  MEDIUM: "medium",
  HIGH: "high",
  HYPER: "hyper"
} as const;

export type Sensitivity = (typeof Sensitivity)[keyof typeof Sensitivity];

export const OutcomeStatus = {
  SUCCESS: "success",
  FAILURE: "failure",
  PARTIAL: "partial"
} as const;

export type OutcomeStatus = (typeof OutcomeStatus)[keyof typeof OutcomeStatus];

export const DecayCurve = {
  EXPONENTIAL: "exponential"
} as const;

export type DecayCurve = (typeof DecayCurve)[keyof typeof DecayCurve];

export const DeletionPolicy = {
  AUTO_PRUNE: "auto_prune",
  MANUAL_ONLY: "manual_only",
  NEVER: "never"
} as const;

export type DeletionPolicy = (typeof DeletionPolicy)[keyof typeof DeletionPolicy];

export const RevisionStatus = {
  ACTIVE: "active",
  CONTESTED: "contested",
  RETRACTED: "retracted"
} as const;

export type RevisionStatus = (typeof RevisionStatus)[keyof typeof RevisionStatus];

export const ValidityMode = {
  GLOBAL: "global",
  CONDITIONAL: "conditional",
  TIMEBOXED: "timeboxed"
} as const;

export type ValidityMode = (typeof ValidityMode)[keyof typeof ValidityMode];

export const TaskState = {
  PLANNING: "planning",
  EXECUTING: "executing",
  BLOCKED: "blocked",
  WAITING: "waiting",
  DONE: "done"
} as const;

export type TaskState = (typeof TaskState)[keyof typeof TaskState];

export const AuditAction = {
  CREATE: "create",
  REVISE: "revise",
  FORK: "fork",
  MERGE: "merge",
  DELETE: "delete",
  REINFORCE: "reinforce",
  DECAY: "decay"
} as const;

export type AuditAction = (typeof AuditAction)[keyof typeof AuditAction];

export const ProvenanceKind = {
  EVENT: "event",
  ARTIFACT: "artifact",
  TOOL_CALL: "tool_call",
  OBSERVATION: "observation",
  OUTCOME: "outcome"
} as const;

export type ProvenanceKind = (typeof ProvenanceKind)[keyof typeof ProvenanceKind];

export const EdgeKind = {
  DATA: "data",
  CONTROL: "control"
} as const;

export type EdgeKind = (typeof EdgeKind)[keyof typeof EdgeKind];

export const EntityKind = {
  PERSON: "person",
  TOOL: "tool",
  PROJECT: "project",
  FILE: "file",
  CONCEPT: "concept",
  OTHER: "other"
} as const;

export type EntityKind = (typeof EntityKind)[keyof typeof EntityKind];

export const EntityType = {
  PERSON: "Person",
  ORGANIZATION: "Organization",
  TEAM: "Team",
  AGENT: "Agent",
  PROJECT: "Project",
  REPOSITORY: "Repository",
  FILE: "File",
  DIRECTORY: "Directory",
  SYMBOL: "Symbol",
  API: "API",
  SERVICE: "Service",
  DATABASE: "Database",
  PACKAGE: "Package",
  DEPENDENCY: "Dependency",
  TOOL: "Tool",
  COMMAND: "Command",
  RUNTIME: "Runtime",
  ENVIRONMENT: "Environment",
  TASK: "Task",
  ISSUE: "Issue",
  PULL_REQUEST: "PullRequest",
  DECISION: "Decision",
  REQUIREMENT: "Requirement",
  INCIDENT: "Incident",
  DOCUMENT: "Document",
  URL: "URL",
  DATASET: "Dataset",
  METRIC: "Metric",
  CONCEPT: "Concept",
  EVENT: "Event",
  OTHER: "Other"
} as const;

export type EntityType = (typeof EntityType)[keyof typeof EntityType] | string;

export const BuiltinEntityTypes = Object.values(EntityType);

export const GraphPredicate = {
  MENTIONS_ENTITY: "mentions_entity",
  MENTIONED_IN: "mentioned_in",
  SUBJECT_ENTITY: "subject_entity",
  FACT_SUBJECT_OF: "fact_subject_of",
  OBJECT_ENTITY: "object_entity",
  FACT_OBJECT_OF: "fact_object_of",
  DERIVED_FROM: "derived_from",
  DERIVED_SEMANTIC: "derived_semantic",
  REFERENCES_RECORD: "references_record",
  REFERENCED_BY: "referenced_by",
  DEPENDS_ON: "depends_on",
  DEPENDENCY_OF: "dependency_of",
  USES: "uses",
  USED_BY: "used_by",
  CAUSED_BY: "caused_by",
  CAUSES: "causes",
  SUPPORTS: "supports",
  SUPPORTED_BY: "supported_by",
  CONTRADICTS: "contradicts",
  CONTRADICTED_BY: "contradicted_by",
  SUPERSEDES: "supersedes",
  SUPERSEDED_BY: "superseded_by",
  CONTESTED_BY: "contested_by",
  CONTESTS: "contests"
} as const;

export type GraphPredicate = (typeof GraphPredicate)[keyof typeof GraphPredicate] | string;

const INVERSE_GRAPH_PREDICATES: Record<string, string> = {
  [GraphPredicate.MENTIONS_ENTITY]: GraphPredicate.MENTIONED_IN,
  [GraphPredicate.MENTIONED_IN]: GraphPredicate.MENTIONS_ENTITY,
  [GraphPredicate.SUBJECT_ENTITY]: GraphPredicate.FACT_SUBJECT_OF,
  [GraphPredicate.FACT_SUBJECT_OF]: GraphPredicate.SUBJECT_ENTITY,
  [GraphPredicate.OBJECT_ENTITY]: GraphPredicate.FACT_OBJECT_OF,
  [GraphPredicate.FACT_OBJECT_OF]: GraphPredicate.OBJECT_ENTITY,
  [GraphPredicate.DERIVED_FROM]: GraphPredicate.DERIVED_SEMANTIC,
  [GraphPredicate.DERIVED_SEMANTIC]: GraphPredicate.DERIVED_FROM,
  [GraphPredicate.REFERENCES_RECORD]: GraphPredicate.REFERENCED_BY,
  [GraphPredicate.REFERENCED_BY]: GraphPredicate.REFERENCES_RECORD,
  [GraphPredicate.DEPENDS_ON]: GraphPredicate.DEPENDENCY_OF,
  [GraphPredicate.DEPENDENCY_OF]: GraphPredicate.DEPENDS_ON,
  [GraphPredicate.USES]: GraphPredicate.USED_BY,
  [GraphPredicate.USED_BY]: GraphPredicate.USES,
  [GraphPredicate.CAUSED_BY]: GraphPredicate.CAUSES,
  [GraphPredicate.CAUSES]: GraphPredicate.CAUSED_BY,
  [GraphPredicate.SUPPORTS]: GraphPredicate.SUPPORTED_BY,
  [GraphPredicate.SUPPORTED_BY]: GraphPredicate.SUPPORTS,
  [GraphPredicate.CONTRADICTS]: GraphPredicate.CONTRADICTED_BY,
  [GraphPredicate.CONTRADICTED_BY]: GraphPredicate.CONTRADICTS,
  [GraphPredicate.SUPERSEDES]: GraphPredicate.SUPERSEDED_BY,
  [GraphPredicate.SUPERSEDED_BY]: GraphPredicate.SUPERSEDES,
  [GraphPredicate.CONTESTED_BY]: GraphPredicate.CONTESTS,
  [GraphPredicate.CONTESTS]: GraphPredicate.CONTESTED_BY
};

export function normalizeGraphPredicate(predicate: string): string {
  return predicate
    .trim()
    .replace(/([A-Z]+)([A-Z][a-z])/gu, "$1_$2")
    .replace(/([a-z0-9])([A-Z])/gu, "$1_$2")
    .toLowerCase()
    .split(/[^\p{L}\p{N}]+/u)
    .filter(Boolean)
    .join("_");
}

export function normalizeSemanticPredicate(predicate: string): string {
  return normalizeGraphPredicate(predicate);
}

export function inverseGraphPredicate(predicate: string): string {
  const normalized = normalizeGraphPredicate(predicate);
  return INVERSE_GRAPH_PREDICATES[normalized] ?? `inverse_of_${normalized}`;
}

export const InterpretationStatus = {
  TENTATIVE: "tentative",
  RESOLVED: "resolved"
} as const;

export type InterpretationStatus = (typeof InterpretationStatus)[keyof typeof InterpretationStatus];

export const SourceKind = {
  EVENT: "event",
  TOOL_OUTPUT: "tool_output",
  OBSERVATION: "observation",
  WORKING_STATE: "working_state",
  AGENT_TURN: "agent_turn"
} as const;

export type SourceKind = (typeof SourceKind)[keyof typeof SourceKind];

export interface TrustContext {
  max_sensitivity: Sensitivity | string;
  authenticated: boolean;
  actor_id: string;
  scopes: string[];
}

export interface DecayProfile {
  curve: DecayCurve | string;
  half_life_seconds: number;
  min_salience?: number;
  max_age_seconds?: number;
  reinforcement_gain?: number;
}

export interface Lifecycle {
  decay: DecayProfile;
  last_reinforced_at?: string;
  pinned?: boolean;
  deletion_policy?: DeletionPolicy | string;
}

export interface ProvenanceSource {
  kind?: ProvenanceKind | string;
  ref: string;
  timestamp?: string;
  hash?: string;
  created_by?: string;
}

export interface Provenance {
  sources: ProvenanceSource[];
  created_by?: string;
}

export interface Relation {
  target_id: string;
  predicate?: string;
  kind?: string;
  weight?: number;
  created_at?: string;
}

export interface GraphEdge {
  source_id: string;
  predicate: string;
  target_id: string;
  weight?: number;
  created_at?: string;
}

export interface AuditEntry {
  action: AuditAction | string;
  actor: string;
  timestamp: string;
  rationale: string;
}

export interface MemoryRecord {
  id: string;
  type: MemoryType | string;
  sensitivity: Sensitivity | string;
  confidence: number;
  salience: number;
  scope?: string;
  tags?: string[];
  created_at?: string;
  updated_at?: string;
  lifecycle?: Lifecycle;
  provenance?: Provenance;
  relations?: Relation[];
  interpretation?: Interpretation;
  payload?: unknown;
  audit_log?: AuditEntry[];
}

export type JsonObject = Record<string, unknown>;

export interface SelectionResult {
  selected: MemoryRecord[];
  confidence: number;
  needs_more: boolean;
  scores?: Record<string, number>;
}

export interface RetrievalDiagnostic {
  code: string;
  message: string;
}

export interface RecordProjection {
  relations_omitted: boolean;
  relations_truncated: boolean;
  history_omitted: boolean;
  records_truncated: boolean;
}

export interface RetrieveResult {
  records: MemoryRecord[];
  selection?: SelectionResult;
}

export interface GraphNode {
  record: MemoryRecord;
  root: boolean;
  hop: number;
}

export interface RetrieveGraphResult {
  nodes: GraphNode[];
  edges: GraphEdge[];
  root_ids: string[];
  selection?: SelectionResult;
  diagnostics?: RetrievalDiagnostic[];
  projection?: RecordProjection;
}

export interface MetricsSnapshot extends JsonObject {
  collected_at?: string;
  total_records?: number;
  records_by_type?: Record<MemoryType, number>;
  avg_salience?: number;
  avg_confidence?: number;
  salience_distribution?: Record<string, number>;
  active_records?: number;
  pinned_records?: number;
  total_audit_entries?: number;
  embedding_model?: string;
  embedded_records?: number;
  missing_embeddings?: number;
  embedding_coverage?: number;
  memory_growth_rate?: number;
  retrieval_usefulness?: number;
  competence_success_rate?: number;
  plan_reuse_frequency?: number;
  revision_rate?: number;
}

export interface CaptureMemoryResult {
  primary_record: MemoryRecord;
  created_records: MemoryRecord[];
  edges: GraphEdge[];
}

export function createDefaultTrustContext(): TrustContext {
  return {
    max_sensitivity: Sensitivity.LOW,
    authenticated: false,
    actor_id: "",
    scopes: ["default"]
  };
}

// ---------------------------------------------------------------------------
// Constraint (RFC 15A.3, 15A.6)
// ---------------------------------------------------------------------------

export interface Constraint {
  type: string;
  key: string;
  value?: unknown;
  required?: boolean;
}

// ---------------------------------------------------------------------------
// Provenance reference and revision (RFC 15A.8)
// ---------------------------------------------------------------------------

export interface ProvenanceRef {
  source_type: string;
  source_id: string;
  timestamp: string;
}

export interface RevisionState {
  supersedes?: string;
  superseded_by?: string;
  status?: RevisionStatus | string;
}

// ---------------------------------------------------------------------------
// Validity (RFC 15A.8)
// ---------------------------------------------------------------------------

export interface Validity {
  mode: ValidityMode | string;
  conditions?: Record<string, unknown>;
  start?: string;
  end?: string;
}

// ---------------------------------------------------------------------------
// Episodic payload helpers (RFC 15A.6, 15A.2)
// ---------------------------------------------------------------------------

export interface TimelineEvent {
  t: string;
  event_kind: string;
  ref: string;
  summary?: string;
}

export interface ToolNode {
  id: string;
  tool: string;
  args?: Record<string, unknown>;
  result?: unknown;
  timestamp?: string;
  depends_on?: string[];
}

export interface EnvironmentSnapshot {
  os?: string;
  os_version?: string;
  tool_versions?: Record<string, string>;
  working_directory?: string;
  context?: Record<string, unknown>;
}

export interface Mention {
  surface: string;
  entity_kind?: EntityKind | string;
  canonical_entity_id?: string;
  confidence?: number;
  aliases?: string[];
}

export interface RelationCandidate {
  predicate: string;
  target_record_id?: string;
  target_entity_id?: string;
  confidence?: number;
  resolved?: boolean;
}

export interface ReferenceCandidate {
  ref: string;
  target_record_id?: string;
  target_entity_id?: string;
  confidence?: number;
  resolved?: boolean;
}

export interface Interpretation {
  status: InterpretationStatus | string;
  summary?: string;
  proposed_type?: MemoryType | string;
  topical_labels?: string[];
  mentions?: Mention[];
  relation_candidates?: RelationCandidate[];
  reference_candidates?: ReferenceCandidate[];
  extraction_confidence?: number;
}

// ---------------------------------------------------------------------------
// Payload types (RFC 15A.2, 15A.6 – 15A.11)
// ---------------------------------------------------------------------------

export interface EpisodicPayload {
  kind: "episodic";
  timeline: TimelineEvent[];
  tool_graph?: ToolNode[];
  environment?: EnvironmentSnapshot;
  outcome?: OutcomeStatus | string;
  artifacts?: string[];
  tool_graph_ref?: string;
}

export interface WorkingPayload {
  kind: "working";
  thread_id: string;
  state: TaskState | string;
  active_constraints?: Constraint[];
  next_actions?: string[];
  open_questions?: string[];
  context_summary?: string;
}

export interface SemanticPayload {
  kind: "semantic";
  subject: string;
  predicate: string;
  object: unknown;
  validity: Validity;
  evidence?: ProvenanceRef[];
  revision_policy?: string;
  revision?: RevisionState;
}

export interface Trigger {
  signal: string;
  conditions?: Record<string, unknown>;
}

export interface RecipeStep {
  step: string;
  tool?: string;
  args_schema?: Record<string, unknown>;
  validation?: string;
}

export interface PerformanceStats {
  success_count?: number;
  failure_count?: number;
  success_rate?: number;
  avg_latency_ms?: number;
  last_used_at?: string;
}

export interface CompetencePayload {
  kind: "competence";
  skill_name: string;
  triggers: Trigger[];
  recipe: RecipeStep[];
  required_tools?: string[];
  failure_modes?: string[];
  fallbacks?: string[];
  performance?: PerformanceStats;
  version?: string;
}

export interface PlanNode {
  id: string;
  op: string;
  params?: Record<string, unknown>;
  guards?: Record<string, unknown>;
}

export interface PlanEdge {
  from: string;
  to: string;
  kind: EdgeKind | string;
}

export interface PlanMetrics {
  avg_latency_ms?: number;
  failure_rate?: number;
  execution_count?: number;
  last_executed_at?: string;
}

export interface PlanGraphPayload {
  kind: "plan_graph";
  plan_id: string;
  version: string;
  intent?: string;
  constraints?: Record<string, unknown>;
  inputs_schema?: Record<string, unknown>;
  outputs_schema?: Record<string, unknown>;
  nodes: PlanNode[];
  edges: PlanEdge[];
  metrics?: PlanMetrics;
}

export interface EntityAlias {
  value: string;
  kind?: string;
  locale?: string;
}

export interface EntityIdentifier {
  namespace: string;
  value: string;
}

export interface EntityPayload {
  kind: "entity";
  canonical_name: string;
  primary_type?: EntityType;
  types?: EntityType[];
  aliases?: EntityAlias[];
  identifiers?: EntityIdentifier[];
  summary?: string;
}
