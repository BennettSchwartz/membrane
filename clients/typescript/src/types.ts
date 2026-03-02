export const MemoryType = {
  EPISODIC: "episodic",
  WORKING: "working",
  SEMANTIC: "semantic",
  COMPETENCE: "competence",
  PLAN_GRAPH: "plan_graph"
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
  EXPONENTIAL: "exponential",
  LINEAR: "linear",
  CUSTOM: "custom"
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
  kind?: string;
  predicate?: string;
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
  payload?: unknown;
  audit_log?: AuditEntry[];
}

export type JsonObject = Record<string, unknown>;

export interface SelectionResult {
  selected: MemoryRecord[];
  confidence: number;
  needs_more: boolean;
}

export interface RetrieveResult {
  records: MemoryRecord[];
  selection?: SelectionResult;
}

export function createDefaultTrustContext(): TrustContext {
  return {
    max_sensitivity: Sensitivity.LOW,
    authenticated: false,
    actor_id: "",
    scopes: []
  };
}
