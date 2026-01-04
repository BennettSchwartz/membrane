PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;

CREATE TABLE IF NOT EXISTS memory_records (
    id TEXT PRIMARY KEY,
    type TEXT NOT NULL CHECK(type IN ('episodic','working','semantic','competence','plan_graph')),
    sensitivity TEXT NOT NULL CHECK(sensitivity IN ('public','low','medium','high','hyper')),
    confidence REAL NOT NULL CHECK(confidence >= 0 AND confidence <= 1),
    salience REAL NOT NULL CHECK(salience >= 0),
    scope TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS decay_profiles (
    record_id TEXT PRIMARY KEY REFERENCES memory_records(id) ON DELETE CASCADE,
    curve TEXT NOT NULL CHECK(curve IN ('exponential','linear','custom')),
    half_life_seconds INTEGER NOT NULL CHECK(half_life_seconds > 0),
    min_salience REAL NOT NULL DEFAULT 0 CHECK(min_salience >= 0),
    max_age_seconds INTEGER,
    reinforcement_gain REAL NOT NULL DEFAULT 0.1,
    last_reinforced_at TEXT NOT NULL,
    pinned INTEGER NOT NULL DEFAULT 0,
    deletion_policy TEXT NOT NULL DEFAULT 'auto_prune'
);

CREATE TABLE IF NOT EXISTS payloads (
    record_id TEXT PRIMARY KEY REFERENCES memory_records(id) ON DELETE CASCADE,
    payload_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS tags (
    record_id TEXT NOT NULL REFERENCES memory_records(id) ON DELETE CASCADE,
    tag TEXT NOT NULL,
    PRIMARY KEY(record_id, tag)
);

CREATE TABLE IF NOT EXISTS provenance_sources (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    record_id TEXT NOT NULL REFERENCES memory_records(id) ON DELETE CASCADE,
    kind TEXT NOT NULL CHECK(kind IN ('event','artifact','tool_call','observation','outcome')),
    ref TEXT NOT NULL,
    hash TEXT,
    created_by TEXT,
    timestamp TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS relations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id TEXT NOT NULL REFERENCES memory_records(id) ON DELETE CASCADE,
    predicate TEXT NOT NULL,
    target_id TEXT NOT NULL REFERENCES memory_records(id) ON DELETE CASCADE,
    weight REAL DEFAULT 1.0 CHECK(weight >= 0 AND weight <= 1),
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    record_id TEXT NOT NULL REFERENCES memory_records(id) ON DELETE CASCADE,
    action TEXT NOT NULL CHECK(action IN ('create','revise','fork','merge','delete','reinforce','decay')),
    actor TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    rationale TEXT NOT NULL,
    previous_state_json TEXT
);

CREATE TABLE IF NOT EXISTS competence_stats (
    record_id TEXT PRIMARY KEY REFERENCES memory_records(id) ON DELETE CASCADE,
    success_count INTEGER NOT NULL DEFAULT 0,
    failure_count INTEGER NOT NULL DEFAULT 0
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_records_type ON memory_records(type);
CREATE INDEX IF NOT EXISTS idx_records_salience ON memory_records(salience DESC);
CREATE INDEX IF NOT EXISTS idx_records_sensitivity ON memory_records(sensitivity);
CREATE INDEX IF NOT EXISTS idx_records_scope ON memory_records(scope);
CREATE INDEX IF NOT EXISTS idx_records_created ON memory_records(created_at);
CREATE INDEX IF NOT EXISTS idx_relations_source ON relations(source_id);
CREATE INDEX IF NOT EXISTS idx_relations_target ON relations(target_id);
CREATE INDEX IF NOT EXISTS idx_relations_predicate ON relations(predicate);
CREATE INDEX IF NOT EXISTS idx_audit_record ON audit_log(record_id);
CREATE INDEX IF NOT EXISTS idx_audit_timestamp ON audit_log(timestamp);
CREATE INDEX IF NOT EXISTS idx_tags_tag ON tags(tag);
CREATE INDEX IF NOT EXISTS idx_provenance_record ON provenance_sources(record_id);
