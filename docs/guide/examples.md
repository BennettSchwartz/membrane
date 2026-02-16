# Examples & Recipes

This page provides end-to-end examples that demonstrate real-world usage patterns with Membrane. Each recipe shows both the **Go SDK** (embedded) approach and the equivalent **gRPC** call where applicable. All payload structures match the types defined in `pkg/schema/`.

[[toc]]

## Agent Conversation Memory

Store episodic memories of agent interactions and retrieve context for ongoing conversations.

### Ingesting Conversation Events

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/GustyCube/membrane/pkg/ingestion"
    "github.com/GustyCube/membrane/pkg/membrane"
    "github.com/GustyCube/membrane/pkg/retrieval"
    "github.com/GustyCube/membrane/pkg/schema"
)

func main() {
    cfg := membrane.DefaultConfig()
    cfg.DBPath = "agent-memory.db"
    m, err := membrane.New(cfg)
    if err != nil {
        log.Fatal(err)
    }

    ctx := context.Background()
    if err := m.Start(ctx); err != nil {
        log.Fatal(err)
    }
    defer m.Stop()

    // Ingest a user message as an episodic event
    userMsg, err := m.IngestEvent(ctx, ingestion.IngestEventRequest{
        Source:    "user-alice",
        EventKind: "user_input",
        Ref:       "conv-001-msg-001",
        Summary:   "User asked how to deploy a Go service to Kubernetes",
        Tags:      []string{"conversation", "kubernetes", "deployment"},
        Scope:     "project-acme",
    })
    if err != nil {
        log.Fatal(err)
    }

    // Ingest the agent's tool call
    toolRec, err := m.IngestToolOutput(ctx, ingestion.IngestToolOutputRequest{
        Source:   "agent-v1",
        ToolName: "kubectl",
        Args:     map[string]any{"cmd": "apply -f deployment.yaml"},
        Result:   "deployment.apps/myservice configured",
        Tags:     []string{"conversation", "kubernetes", "tool_call"},
        Scope:    "project-acme",
    })
    if err != nil {
        log.Fatal(err)
    }

    // Record the outcome of the episode
    _, err = m.IngestOutcome(ctx, ingestion.IngestOutcomeRequest{
        Source:         "agent-v1",
        TargetRecordID: toolRec.ID,
        OutcomeStatus:  schema.OutcomeStatusSuccess,
    })
    if err != nil {
        log.Fatal(err)
    }

    fmt.Printf("Stored conversation event: %s\n", userMsg.ID)
}
```

### Retrieving Conversation Context

Retrieve recent episodic memories to build context for the next turn:

```go
// Retrieve the last 10 episodic records for this project scope
trust := retrieval.NewTrustContext(
    schema.SensitivityMedium, // max sensitivity we can see
    true,                     // authenticated
    "agent-v1",               // actor making the request
    []string{"project-acme"}, // scopes we have access to
)

resp, err := m.Retrieve(ctx, &retrieval.RetrieveRequest{
    Trust:       trust,
    MemoryTypes: []schema.MemoryType{schema.MemoryTypeEpisodic},
    Limit:       10,
})
if err != nil {
    log.Fatal(err)
}

for _, rec := range resp.Records {
    ep := rec.Payload.(*schema.EpisodicPayload)
    for _, event := range ep.Timeline {
        fmt.Printf("[%s] %s: %s\n", event.T.Format("15:04"), event.EventKind, event.Summary)
    }
}
```

### gRPC Equivalent

```protobuf
// IngestEvent over gRPC
rpc IngestEvent(IngestEventRequest) returns (IngestResponse);
```

```go
// Using the generated gRPC client
client := membranev1.NewMembraneServiceClient(conn)

resp, err := client.IngestEvent(ctx, &membranev1.IngestEventRequest{
    Source:    "user-alice",
    EventKind: "user_input",
    Ref:       "conv-001-msg-001",
    Summary:   "User asked how to deploy a Go service to Kubernetes",
    Tags:      []string{"conversation", "kubernetes"},
    Scope:     "project-acme",
    Sensitivity: "low",
})
```

---

## Knowledge Base Management

Ingest semantic facts, supersede outdated information, and retract incorrect facts.

### Ingesting Semantic Observations

```go
// Store a fact: "project uses Go 1.21"
fact, err := m.IngestObservation(ctx, ingestion.IngestObservationRequest{
    Source:    "agent-v1",
    Subject:   "project",
    Predicate: "go_version",
    Object:    "1.21",
    Tags:      []string{"tech", "go", "version"},
    Scope:     "project-acme",
})
if err != nil {
    log.Fatal(err)
}
fmt.Printf("Stored fact %s: project go_version = 1.21\n", fact.ID)
```

### Superseding Outdated Information

When information changes, use `Supersede` to atomically replace the old record. The old record's salience drops to zero and its revision status becomes `retracted`.

```go
// The project upgraded to Go 1.22 -- supersede the old fact.
// Build the replacement record with evidence (required by RFC Section 7).
newFact := schema.NewMemoryRecord("", schema.MemoryTypeSemantic, schema.SensitivityLow,
    &schema.SemanticPayload{
        Kind:      "semantic",
        Subject:   "project",
        Predicate: "go_version",
        Object:    "1.22",
        Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
    },
)
newFact.Provenance.Sources = append(newFact.Provenance.Sources, schema.ProvenanceSource{
    Kind: "observation",
    Ref:  "go-mod-update-2024-03",
})

updated, err := m.Supersede(ctx, fact.ID, newFact, "agent-v1", "Go version updated in go.mod")
if err != nil {
    log.Fatal(err)
}
fmt.Printf("Superseded %s with %s\n", fact.ID, updated.ID)
```

### Retracting Incorrect Facts

If a fact is simply wrong, retract it without providing a replacement:

```go
err = m.Retract(ctx, fact.ID, "agent-v1", "Observation was incorrect based on new evidence")
if err != nil {
    log.Fatal(err)
}
// The record remains in the store for auditability, but salience = 0
// and revision status = "retracted".
```

---

## Skill Learning

Store competence records as agents learn new procedures. Competence payloads capture procedural knowledge -- the "how" rather than the "what."

```go
// Record a learned skill for fixing Go build errors
skillRec := schema.NewMemoryRecord("", schema.MemoryTypeCompetence, schema.SensitivityLow,
    &schema.CompetencePayload{
        Kind:      "competence",
        SkillName: "fix-go-build-error",
        Triggers: []schema.Trigger{
            {
                Signal:     "build_failure",
                Conditions: map[string]any{"language": "go"},
            },
        },
        Recipe: []schema.RecipeStep{
            {
                Step:       "Run `go build ./...` to identify the error",
                Tool:       "bash",
                Validation: "Exit code 0 or clear error message",
            },
            {
                Step:       "Parse error output for file and line number",
                Tool:       "regex_extract",
                Validation: "Non-empty file path and line number",
            },
            {
                Step:       "Apply fix based on error type",
                Tool:       "file_edit",
                Validation: "Subsequent `go build` succeeds",
            },
        },
        RequiredTools: []string{"bash", "regex_extract", "file_edit"},
        FailureModes:  []string{"ambiguous error message", "missing dependency"},
        Fallbacks:     []string{"run go mod tidy", "ask user for clarification"},
        Performance: &schema.PerformanceStats{
            SuccessCount: 42,
            FailureCount: 3,
            SuccessRate:  0.933,
            AvgLatencyMs: 4500,
        },
        Version: "1.2",
    },
)
skillRec.Provenance.Sources = append(skillRec.Provenance.Sources, schema.ProvenanceSource{
    Kind: "observation",
    Ref:  "learned-from-episodes-batch-7",
})

// Persist via the store (competence records are typically created
// through consolidation or direct construction, not the ingestion API).
```

### Reinforcing Successful Skills

When a competence is used successfully, reinforce it so it stays salient:

```go
err := m.Reinforce(ctx, skillRec.ID, "agent-v1", "Successfully fixed build error in project-acme")
if err != nil {
    log.Fatal(err)
}
```

### Penalizing Failed Skills

When a competence leads to a failure, penalize it:

```go
err := m.Penalize(ctx, skillRec.ID, 0.2, "agent-v1", "Recipe step 3 did not resolve the error")
if err != nil {
    log.Fatal(err)
}
```

---

## Plan Tracking

Use `plan_graph` memories for multi-step task tracking. Plan graphs model actions as directed graphs with data and control dependencies.

```go
deployPlan := schema.NewMemoryRecord("", schema.MemoryTypePlanGraph, schema.SensitivityLow,
    &schema.PlanGraphPayload{
        Kind:    "plan_graph",
        PlanID:  "deploy-k8s-v2",
        Version: "2.0",
        Intent:  "deploy_to_kubernetes",
        Constraints: map[string]any{
            "cluster":   "production",
            "namespace": "default",
        },
        Nodes: []schema.PlanNode{
            {ID: "build",  Op: "docker_build", Params: map[string]any{"tag": "v2.0.0"}},
            {ID: "push",   Op: "docker_push",  Params: map[string]any{"registry": "gcr.io/myproject"}},
            {ID: "apply",  Op: "kubectl_apply", Params: map[string]any{"manifest": "k8s/deployment.yaml"}},
            {ID: "verify", Op: "kubectl_rollout_status", Params: map[string]any{"timeout": "120s"}},
        },
        Edges: []schema.PlanEdge{
            {From: "build",  To: "push",   Kind: schema.EdgeKindData},
            {From: "push",   To: "apply",  Kind: schema.EdgeKindControl},
            {From: "apply",  To: "verify", Kind: schema.EdgeKindControl},
        },
        Metrics: &schema.PlanMetrics{
            AvgLatencyMs:   180000,
            FailureRate:    0.05,
            ExecutionCount: 47,
        },
    },
)
deployPlan.Confidence = 0.92
deployPlan.Provenance.Sources = append(deployPlan.Provenance.Sources, schema.ProvenanceSource{
    Kind: "observation",
    Ref:  "derived-from-deploy-episodes",
})
```

### Selecting the Best Plan

When multiple plan graphs match, the selector ranks them by confidence, success rate, and recency:

```go
resp, err := m.Retrieve(ctx, &retrieval.RetrieveRequest{
    Trust:       trust,
    MemoryTypes: []schema.MemoryType{schema.MemoryTypePlanGraph},
})
if err != nil {
    log.Fatal(err)
}

if resp.Selection != nil {
    best := resp.Selection.Selected[0]
    plan := best.Payload.(*schema.PlanGraphPayload)
    fmt.Printf("Selected plan: %s v%s (confidence=%.2f)\n",
        plan.PlanID, plan.Version, resp.Selection.Confidence)
}
```

---

## Working Memory

Manage transient state during active tasks. Working memory captures the current thread state and is freely editable.

```go
// Capture the current state of an active task
workingRec, err := m.IngestWorkingState(ctx, ingestion.IngestWorkingStateRequest{
    Source:         "agent-v1",
    ThreadID:       "thread-abc-123",
    State:          schema.TaskStateExecuting,
    NextActions:    []string{"run test suite", "review coverage report"},
    OpenQuestions:  []string{"should we add integration tests?"},
    ContextSummary: "Refactoring auth module; unit tests passing, integration tests pending",
    ActiveConstraints: []schema.Constraint{
        {
            Type:     "deadline",
            Key:      "sprint_end",
            Value:    "2024-03-15T17:00:00Z",
            Required: true,
        },
        {
            Type:  "resource",
            Key:   "max_parallel_tests",
            Value: 4,
        },
    },
    Tags:  []string{"refactor", "auth", "testing"},
    Scope: "project-acme",
})
if err != nil {
    log.Fatal(err)
}
fmt.Printf("Working memory snapshot: %s (state=%s)\n", workingRec.ID, "executing")
```

### Retrieving Working Memory for Task Resumption

```go
// When resuming a session, load working memory first (it's the first layer in
// the retrieval order: working -> semantic -> competence -> plan_graph -> episodic)
resp, err := m.Retrieve(ctx, &retrieval.RetrieveRequest{
    Trust:       trust,
    MemoryTypes: []schema.MemoryType{schema.MemoryTypeWorking},
})
if err != nil {
    log.Fatal(err)
}

for _, rec := range resp.Records {
    wp := rec.Payload.(*schema.WorkingPayload)
    fmt.Printf("Thread %s is %s\n", wp.ThreadID, wp.State)
    fmt.Printf("  Next actions: %v\n", wp.NextActions)
    fmt.Printf("  Open questions: %v\n", wp.OpenQuestions)
    fmt.Printf("  Context: %s\n", wp.ContextSummary)
}
```

---

## Multi-Agent Setup

Use scopes and trust contexts to partition memory across multiple agents in a shared substrate.

### Scoped Ingestion

Each agent writes to its own scope. Shared facts use a common scope:

```go
// Agent A writes to its private scope
_, err = m.IngestObservation(ctx, ingestion.IngestObservationRequest{
    Source:    "agent-a",
    Subject:   "user",
    Predicate: "prefers",
    Object:    "dark mode",
    Tags:      []string{"preference"},
    Scope:     "agent-a-private",
    Sensitivity: schema.SensitivityMedium,
})

// Agent B writes to its private scope
_, err = m.IngestObservation(ctx, ingestion.IngestObservationRequest{
    Source:    "agent-b",
    Subject:   "project",
    Predicate: "framework",
    Object:    "React",
    Tags:      []string{"tech"},
    Scope:     "agent-b-private",
    Sensitivity: schema.SensitivityLow,
})

// Both agents contribute to a shared scope
_, err = m.IngestObservation(ctx, ingestion.IngestObservationRequest{
    Source:    "agent-a",
    Subject:   "deployment",
    Predicate: "target",
    Object:    "AWS us-east-1",
    Tags:      []string{"infra"},
    Scope:     "shared-workspace",
    Sensitivity: schema.SensitivityLow,
})
```

### Trust-Gated Retrieval per Agent

Each agent creates a trust context that restricts what it can see:

```go
// Agent A can see its own scope plus the shared scope
trustA := retrieval.NewTrustContext(
    schema.SensitivityMedium,
    true,
    "agent-a",
    []string{"agent-a-private", "shared-workspace"},
)

// Agent B can see its own scope plus the shared scope
trustB := retrieval.NewTrustContext(
    schema.SensitivityLow,
    true,
    "agent-b",
    []string{"agent-b-private", "shared-workspace"},
)

// Agent A retrieves -- sees its own records + shared, not Agent B's private data
respA, _ := m.Retrieve(ctx, &retrieval.RetrieveRequest{Trust: trustA})
fmt.Printf("Agent A sees %d records\n", len(respA.Records))

// Agent B retrieves -- sees its own records + shared, not Agent A's private data
respB, _ := m.Retrieve(ctx, &retrieval.RetrieveRequest{Trust: trustB})
fmt.Printf("Agent B sees %d records\n", len(respB.Records))
```

::: tip Unscoped Records
Records with an empty `Scope` field are unscoped and visible to all trust contexts regardless of their scope restrictions. Use scopes deliberately when isolation is needed.
:::

---

## Memory Consolidation

Consolidation distills episodic experiences into stable semantic knowledge. It runs on a background scheduler.

### Configuring Consolidation

```go
cfg := membrane.DefaultConfig()
cfg.DBPath = "agent-memory.db"
cfg.ConsolidationInterval = 6 * time.Hour  // run every 6 hours
cfg.DecayInterval = 1 * time.Hour          // decay salience every hour

m, err := membrane.New(cfg)
if err != nil {
    log.Fatal(err)
}

// Start kicks off both the decay and consolidation schedulers
ctx := context.Background()
if err := m.Start(ctx); err != nil {
    log.Fatal(err)
}
defer m.Stop()
```

### YAML Configuration

```yaml
# membrane.yaml
db_path: "agent-memory.db"
listen_addr: ":9090"
decay_interval: "1h"
consolidation_interval: "6h"
default_sensitivity: "low"
selection_confidence_threshold: 0.7
```

### What Consolidation Does

The consolidation scheduler periodically:

1. Scans episodic records for recurring patterns
2. Extracts semantic facts (subject-predicate-object triples)
3. Creates new semantic memory records with provenance linking back to the source episodes
4. Extracts procedural patterns into competence records

The original episodic records are **never mutated** -- episodic memory is append-only per RFC 15A.6. Instead, new semantic and competence records are created with `derived_from` relations.

---

## Fact Correction Workflow

The contest-retract-supersede pattern handles knowledge correction when conflicting evidence arises.

### Step 1: Contest the Existing Fact

Mark a fact as contested when you find conflicting evidence:

```go
// Original fact: "database type is PostgreSQL"
original, _ := m.IngestObservation(ctx, ingestion.IngestObservationRequest{
    Source:    "agent-v1",
    Subject:   "database",
    Predicate: "type",
    Object:    "PostgreSQL",
    Tags:      []string{"tech", "database"},
})

// New evidence suggests it might be SQLite in development...
// Contest the original fact
err := m.Contest(ctx, original.ID, "config-file-scan-042", "agent-v1",
    "Found SQLite config in development environment")
if err != nil {
    log.Fatal(err)
}
// The record's revision status is now "contested"
```

### Step 2: Fork for Conditional Validity

If both facts are correct under different conditions, fork instead of replacing:

```go
// Fork: SQLite is used in development
devVariant := schema.NewMemoryRecord("", schema.MemoryTypeSemantic, schema.SensitivityLow,
    &schema.SemanticPayload{
        Kind:      "semantic",
        Subject:   "database",
        Predicate: "type",
        Object:    "SQLite",
        Validity: schema.Validity{
            Mode:       schema.ValidityModeConditional,
            Conditions: map[string]any{"env": "development"},
        },
    },
)
devVariant.Provenance.Sources = append(devVariant.Provenance.Sources, schema.ProvenanceSource{
    Kind: "observation",
    Ref:  "config-file-scan-042",
})

forked, err := m.Fork(ctx, original.ID, devVariant, "agent-v1",
    "SQLite used in dev, PostgreSQL in prod")
if err != nil {
    log.Fatal(err)
}
// Both records remain active. The forked record has a "derived_from"
// relation to the original.
fmt.Printf("Forked: %s (derived from %s)\n", forked.ID, original.ID)
```

### Step 3: Supersede When One is Definitively Correct

If the old fact is simply wrong, supersede it:

```go
// After investigation, the project actually uses MySQL now
corrected := schema.NewMemoryRecord("", schema.MemoryTypeSemantic, schema.SensitivityLow,
    &schema.SemanticPayload{
        Kind:      "semantic",
        Subject:   "database",
        Predicate: "type",
        Object:    "MySQL",
        Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
    },
)
corrected.Provenance.Sources = append(corrected.Provenance.Sources, schema.ProvenanceSource{
    Kind: "observation",
    Ref:  "migration-log-2024-03",
})

superseded, err := m.Supersede(ctx, original.ID, corrected, "agent-v1",
    "Project migrated from PostgreSQL to MySQL")
if err != nil {
    log.Fatal(err)
}
// original.Salience is now 0, original.Revision.Status = "retracted"
// superseded has a "supersedes" relation to original
```

### Merging Redundant Facts

When multiple records say the same thing, merge them:

```go
rec1, _ := m.IngestObservation(ctx, ingestion.IngestObservationRequest{
    Source: "agent-v1", Subject: "tool", Predicate: "editor", Object: "vim",
})
rec2, _ := m.IngestObservation(ctx, ingestion.IngestObservationRequest{
    Source: "agent-v1", Subject: "tool", Predicate: "editor", Object: "neovim",
})

mergedRec := schema.NewMemoryRecord("", schema.MemoryTypeSemantic, schema.SensitivityLow,
    &schema.SemanticPayload{
        Kind:      "semantic",
        Subject:   "tool",
        Predicate: "editor",
        Object:    "neovim-based editor",
        Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
    },
)
mergedRec.Provenance.Sources = append(mergedRec.Provenance.Sources, schema.ProvenanceSource{
    Kind: "observation",
    Ref:  "consolidation-merge-evidence",
})

merged, err := m.Merge(ctx,
    []string{rec1.ID, rec2.ID},
    mergedRec,
    "agent-v1",
    "Consolidating editor preference records",
)
// Both source records are retracted; merged record has "derived_from" relations to each.
```

::: warning Episodic Immutability
Revision operations (`Supersede`, `Fork`, `Retract`, `Merge`) cannot target episodic records. Episodic memory is append-only. Attempting to revise an episodic record returns `revision.ErrEpisodicImmutable`.
:::

---

## Scoped Retrieval

Retrieve memories with trust context filtering. The trust context controls both sensitivity gating and scope visibility.

### Sensitivity Levels

Records are classified by sensitivity: `public` < `low` < `medium` < `high` < `hyper`. A trust context with `MaxSensitivity: "medium"` can see `public`, `low`, and `medium` records but not `high` or `hyper`.

```go
// A low-privilege trust context
restrictedTrust := retrieval.NewTrustContext(
    schema.SensitivityLow, // can only see public and low
    true,
    "readonly-agent",
    nil, // nil scopes = all scopes visible
)

// A full-access trust context
adminTrust := retrieval.NewTrustContext(
    schema.SensitivityHyper, // can see everything
    true,
    "admin-agent",
    nil,
)

// The same Retrieve call returns different results based on trust
restricted, _ := m.Retrieve(ctx, &retrieval.RetrieveRequest{Trust: restrictedTrust})
full, _ := m.Retrieve(ctx, &retrieval.RetrieveRequest{Trust: adminTrust})

fmt.Printf("Restricted agent sees %d records\n", len(restricted.Records))
fmt.Printf("Admin agent sees %d records\n", len(full.Records))
```

### Filtered Retrieval with Salience and Limits

```go
resp, err := m.Retrieve(ctx, &retrieval.RetrieveRequest{
    Trust:          trust,
    MemoryTypes:    []schema.MemoryType{schema.MemoryTypeSemantic, schema.MemoryTypeCompetence},
    MinSalience:    0.5,  // only records with salience >= 0.5
    Limit:          20,   // at most 20 results
    TaskDescriptor: "deploy Go service to Kubernetes",
})
// Results are sorted by salience descending.
```

### Retrieving a Single Record by ID

```go
record, err := m.RetrieveByID(ctx, "some-uuid-here", trust)
if err != nil {
    // Could be storage.ErrNotFound or retrieval.ErrAccessDenied
    log.Fatal(err)
}
fmt.Printf("Record type: %s, salience: %.2f\n", record.Type, record.Salience)
```

---

## gRPC Client Setup

Membrane ships a gRPC server (`membraned`) with a Protobuf service definition. Connect from any language with gRPC support.

### Go Client

```go
package main

import (
    "context"
    "encoding/json"
    "fmt"
    "log"

    membranev1 "github.com/GustyCube/membrane/api/grpc/gen/membranev1"
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
    "google.golang.org/grpc/metadata"
)

func main() {
    conn, err := grpc.NewClient("localhost:9090",
        grpc.WithTransportCredentials(insecure.NewCredentials()),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer conn.Close()

    client := membranev1.NewMembraneServiceClient(conn)

    // Attach API key via metadata (if server requires authentication)
    ctx := metadata.AppendToOutgoingContext(context.Background(),
        "authorization", "Bearer your-api-key-here",
    )

    // Ingest an event
    resp, err := client.IngestEvent(ctx, &membranev1.IngestEventRequest{
        Source:      "go-client",
        EventKind:   "user_input",
        Ref:         "ref-001",
        Summary:     "User asked about deployment",
        Tags:        []string{"conversation"},
        Scope:       "project-acme",
        Sensitivity: "low",
    })
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("Ingested record: %s\n", string(resp.Record))

    // Retrieve memories
    retrieveResp, err := client.Retrieve(ctx, &membranev1.RetrieveRequest{
        TaskDescriptor: "deployment help",
        Trust: &membranev1.TrustContext{
            MaxSensitivity: "medium",
            Authenticated:  true,
            ActorId:        "go-client",
            Scopes:         []string{"project-acme"},
        },
        MemoryTypes: []string{"semantic", "episodic"},
        MinSalience: 0.3,
        Limit:       10,
    })
    if err != nil {
        log.Fatal(err)
    }

    for _, raw := range retrieveResp.Records {
        var rec map[string]any
        json.Unmarshal(raw, &rec)
        fmt.Printf("  %s [%s] salience=%.2f\n",
            rec["id"], rec["type"], rec["salience"])
    }
}
```

### Python Client

```python
from membrane import MembraneClient, Sensitivity, TrustContext

# Connect to membraned (with optional auth)
client = MembraneClient(
    "localhost:9090",
    api_key="your-api-key-here",  # omit if auth is disabled
)

# Ingest an observation
record = client.ingest_observation(
    subject="user",
    predicate="timezone",
    obj="America/New_York",
    source="python-agent",
    tags=["preference"],
    scope="user-preferences",
    sensitivity=Sensitivity.LOW,
)
print(f"Stored fact: {record.id}")

# Retrieve with trust context
trust = TrustContext(
    max_sensitivity=Sensitivity.MEDIUM,
    authenticated=True,
    actor_id="python-agent",
    scopes=["user-preferences"],
)
records = client.retrieve(
    "personalize response",
    trust=trust,
    memory_types=["semantic"],
    limit=20,
)

for rec in records:
    print(f"  [{rec.type.value}] {rec.id} (salience={rec.salience:.2f})")

client.close()
```

### TypeScript Client

```ts
import { MembraneClient, Sensitivity } from "@gustycube/membrane";

const client = new MembraneClient("localhost:9090", {
  apiKey: "your-api-key-here"
});

// Ingest an event
const created = await client.ingestEvent("user_input", "msg-001", {
  source: "ts-agent",
  summary: "User requested code review",
  tags: ["conversation", "code-review"],
  scope: "project-acme",
  sensitivity: Sensitivity.LOW
});
console.log(`Ingested: ${created.id}`);

// Retrieve memories
const records = await client.retrieve("code review context", {
  trust: {
    max_sensitivity: Sensitivity.MEDIUM,
    authenticated: true,
    actor_id: "ts-agent",
    scopes: ["project-acme"]
  },
  memoryTypes: ["semantic", "competence"],
  minSalience: 0.3,
  limit: 15
});

for (const record of records) {
  console.log(`  ${record.type}: ${record.id} (salience=${record.salience})`);
}

client.close();
```

### TypeScript + LLM Loop (OpenAI-Compatible, including OpenRouter)

```ts
import OpenAI from "openai";
import { MembraneClient, Sensitivity } from "@gustycube/membrane";

const memory = new MembraneClient("localhost:9090", {
  apiKey: process.env.MEMBRANE_API_KEY
});

const llm = new OpenAI({
  apiKey: process.env.LLM_API_KEY,
  // OpenRouter example:
  // baseURL: "https://openrouter.ai/api/v1",
  // defaultHeaders: { "HTTP-Referer": "https://your-app.example", "X-Title": "Your App" },
});

// 1) Retrieve memories for the current task
const memories = await memory.retrieve("debug flaky migration pipeline", {
  trust: {
    max_sensitivity: Sensitivity.MEDIUM,
    authenticated: true,
    actor_id: "ts-agent",
    scopes: ["project-acme"]
  },
  memoryTypes: ["semantic", "competence", "working", "episodic"],
  limit: 20
});

const memoryContext = memories
  .map((m, i) => `[${i + 1}] id=${m.id} type=${m.type} payload=${JSON.stringify(m.payload)}`)
  .join("\\n");

// 2) Ask the model with retrieved memory context
const completion = await llm.chat.completions.create({
  model: "gpt-5.2",
  messages: [
    {
      role: "system",
      content: "You are a pragmatic coding agent. Use memory context as evidence and cite memory ids."
    },
    {
      role: "user",
      content: `Task: fix flaky migration pipeline\\n\\nMemory:\\n${memoryContext}`
    }
  ]
});

const answer = completion.choices[0]?.message?.content ?? "";
console.log(answer);

// 3) Ingest model output / outcome back into memory
const rec = await memory.ingestEvent("llm_plan", "task-123", {
  source: "ts-agent",
  summary: answer.slice(0, 400),
  tags: ["llm", "plan", "migration"],
  scope: "project-acme",
  sensitivity: Sensitivity.LOW
});

await memory.reinforce(rec.id, "ts-agent", "plan used successfully");
memory.close();
```

---

## Quick Reference: Revision Operations

| Operation | Source Type | Effect on Old Record | Relation Created |
|-----------|-----------|---------------------|-----------------|
| `Supersede` | semantic, competence, plan_graph | Salience set to 0, status = `retracted` | New record has `supersedes` relation to old |
| `Fork` | semantic, competence, plan_graph | Unchanged (both remain active) | Forked record has `derived_from` relation to source |
| `Retract` | semantic, competence, plan_graph | Salience set to 0, status = `retracted` | None |
| `Merge` | semantic, competence, plan_graph | All sources retracted (salience = 0) | Merged record has `derived_from` relation to each source |
| `Contest` | semantic, competence, plan_graph | Semantic records are marked `contested` and audit is appended | Source record gets `contested_by` relation |

::: danger All revision operations are rejected on episodic records
Episodic memory is immutable. Any attempt to supersede, fork, retract, or merge an episodic record returns `ErrEpisodicImmutable`.
:::
