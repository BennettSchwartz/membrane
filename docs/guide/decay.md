---
title: Decay & Reinforcement
outline: [2, 3]
---

# Decay & Reinforcement

## Overview

Not all memories deserve to live forever. In biological systems, the brain continuously weakens neural connections that are not revisited, allowing rarely-used information to fade while reinforcing pathways that prove useful. Membrane applies this same principle to agentic memory through **salience decay** -- a mechanism that gradually reduces the importance score of memory records over time, unless they are actively reinforced.

Without decay, a memory substrate would accumulate unbounded state. Old, irrelevant memories would compete equally with fresh, critical ones during retrieval. Decay solves this by ensuring that the memory pool is self-curating: records that are never accessed or reinforced naturally lose prominence and can eventually be pruned away entirely.

The decay system in Membrane is built around three core concepts:

- **Salience** -- a numeric score representing how important a memory is right now.
- **Decay curves** -- mathematical functions that reduce salience over time.
- **Reinforcement** -- explicit actions that boost salience, resetting the decay clock.

Together, these form a feedback loop: useful memories get reinforced and persist, while stale memories fade and are eventually cleaned up.

## Salience

Salience is a floating-point value stored on every `MemoryRecord` that represents the record's current importance. It is the primary signal used by the retrieval system to rank and filter results.

```go
type MemoryRecord struct {
    // ...
    // Salience is the decay-weighted importance score.
    // Range: [0, +inf)
    Salience float64 `json:"salience"`
    // ...
}
```

### Initial Value

When a new memory record is created via `NewMemoryRecord`, salience is initialized to **1.0**:

```go
func NewMemoryRecord(...) *MemoryRecord {
    return &MemoryRecord{
        Salience:   1.0,
        Confidence: 1.0,
        // ...
    }
}
```

A salience of 1.0 means the record is at full importance. Over time, the decay system will reduce this value toward zero (or a configured floor).

### Salience Range

Unlike confidence (which is clamped to `[0, 1]`), salience has no upper bound. Repeated reinforcement can push salience above 1.0, which is useful for marking memories that have proven exceptionally relevant. The lower bound is `0`, though in practice the `MinSalience` floor in the decay profile often prevents it from reaching absolute zero.

::: tip
Records with salience above 1.0 are "super-salient" -- they have been reinforced more than they have decayed. This is a strong signal that the memory is actively useful.
:::

## Decay Curves

A decay curve is a mathematical function that determines how salience decreases over elapsed time. Each memory record carries a `DecayProfile` that specifies which curve to use and its parameters.

```go
type DecayProfile struct {
    Curve           DecayCurve `json:"curve"`
    HalfLifeSeconds int64     `json:"half_life_seconds"`
    MinSalience     float64   `json:"min_salience,omitempty"`
    MaxAgeSeconds   int64     `json:"max_age_seconds,omitempty"`
    ReinforcementGain float64 `json:"reinforcement_gain,omitempty"`
}
```

All decay functions share the same signature:

```go
type DecayFunc func(
    currentSalience float64,
    elapsedSeconds  float64,
    profile         DecayProfile,
) float64
```

### Exponential Decay

The default and most commonly used curve. Salience decays by half every `HalfLifeSeconds`:

$$
S(t) = S_0 \cdot 2^{-t / h}
$$

Where:
- `S(t)` is the salience after `t` seconds
- `S_0` is the current salience
- `h` is the half-life in seconds

Implemented as:

```go
func Exponential(currentSalience, elapsedSeconds float64, profile DecayProfile) float64 {
    halfLife := float64(profile.HalfLifeSeconds)
    if halfLife <= 0 {
        return math.Max(currentSalience, profile.MinSalience)
    }
    decayed := currentSalience * math.Exp(-elapsedSeconds * math.Log(2) / halfLife)
    return math.Max(decayed, profile.MinSalience)
}
```

Exponential decay starts fast and slows down as salience decreases -- a record loses half its salience in the first half-life, then a quarter in the next, and so on. This mirrors biological forgetting curves and is appropriate for most use cases.

**Behavior over time** (starting salience = 1.0, half-life = 24h):

| Elapsed | Salience |
|---------|----------|
| 0h      | 1.000    |
| 12h     | 0.707    |
| 24h     | 0.500    |
| 48h     | 0.250    |
| 72h     | 0.125    |
| 96h     | 0.063    |
| 168h (7d) | 0.008  |

### Linear Decay

Salience decreases at a constant rate proportional to the half-life:

$$
S(t) = S_0 - \frac{t}{h} \cdot S_0 = S_0 \left(1 - \frac{t}{h}\right)
$$

```go
func Linear(currentSalience, elapsedSeconds float64, profile DecayProfile) float64 {
    halfLife := float64(profile.HalfLifeSeconds)
    if halfLife <= 0 {
        return math.Max(currentSalience, profile.MinSalience)
    }
    decayed := currentSalience - (elapsedSeconds/halfLife)*currentSalience
    return math.Max(decayed, profile.MinSalience)
}
```

Linear decay is more aggressive in the long run: it reaches zero in exactly one half-life period (not asymptotically like exponential). Use this when you want memories to have a hard deadline.

**Behavior over time** (starting salience = 1.0, half-life = 24h):

| Elapsed | Salience |
|---------|----------|
| 0h      | 1.000    |
| 6h      | 0.750    |
| 12h     | 0.500    |
| 18h     | 0.250    |
| 24h     | 0.000    |

::: warning
With linear decay, salience reaches zero at exactly one half-life. If `MinSalience` is 0 and `DeletionPolicy` is `auto_prune`, the record will be pruned after one half-life period.
:::

### Custom Curves

The `custom` curve type is reserved for implementation-defined behavior. In the current implementation, unknown and custom curve types fall back to exponential decay:

```go
func GetDecayFunc(curve DecayCurve) DecayFunc {
    switch curve {
    case DecayCurveLinear:
        return Linear
    case DecayCurveExponential:
        return Exponential
    default:
        return Exponential // fallback
    }
}
```

### Maximum Age Hard Cutoff

In addition to curve-based decay, each record can specify a `MaxAgeSeconds`. When a record's total age (since `CreatedAt`) exceeds this value, its salience is immediately set to zero regardless of the decay curve:

```go
if profile.MaxAgeSeconds > 0 {
    ageSeconds := now.Sub(record.CreatedAt).Seconds()
    if ageSeconds >= float64(profile.MaxAgeSeconds) {
        newSalience = 0
    }
}
```

This provides a hard upper bound on record lifetime, useful for working memory or ephemeral context that should never outlive a session.

### MinSalience Floor

The `MinSalience` field in `DecayProfile` sets a floor below which decay cannot reduce salience. Every decay function respects this floor via `math.Max(decayed, profile.MinSalience)`. This is useful for ensuring that certain records never fully fade -- they become low-priority but remain retrievable.

## The Decay Scheduler

Decay does not happen in real-time. Instead, a background **scheduler** periodically sweeps all records and applies decay calculations.

### Architecture

The scheduler is a goroutine that ticks at a configurable interval:

```go
type Scheduler struct {
    service  *Service
    interval time.Duration
    stopCh   chan struct{}
    done     chan struct{}
}
```

### Tick Lifecycle

On each tick, the scheduler performs two operations in sequence:

1. **Decay sweep** -- calls `ApplyDecayAll`, which iterates every non-pinned record and recalculates its salience based on elapsed time since `LastReinforcedAt`.
2. **Prune sweep** -- calls `Prune`, which deletes records that have decayed to their floor and are eligible for auto-pruning.

```go
case <-ticker.C:
    count, err := s.service.ApplyDecayAll(ctx)
    // ... log result ...
    pruned, err := s.service.Prune(ctx)
    // ... log result ...
```

### Pinned Records

Records with `Lifecycle.Pinned = true` are completely exempt from decay. The `ApplyDecayAll` method skips them:

```go
for _, record := range records {
    if record.Lifecycle.Pinned {
        continue
    }
    // apply decay...
}
```

Pin a record when its salience should remain stable indefinitely -- for example, core system instructions or foundational knowledge that the agent must never forget.

### Start and Stop

The scheduler is started with a context and can be stopped gracefully:

```go
scheduler := decay.NewScheduler(decayService, 1*time.Hour)
scheduler.Start(ctx)

// Later, during shutdown:
scheduler.Stop()
```

`Stop()` is safe to call even if `Start()` was never called. It signals the goroutine and waits for it to finish, ensuring clean shutdown without goroutine leaks.

::: tip
The scheduler recovers from panics internally and logs them, so a bug in a single record's decay will not crash the entire scheduler loop.
:::

## Reinforcement

Reinforcement is the counterpart to decay. When a memory proves useful -- it was retrieved, acted upon, or explicitly marked as important -- its salience should be boosted.

### How Reinforcement Works

The `Reinforce` method on the decay service:

1. Reads the record's current `ReinforcementGain` from its decay profile.
2. Adds the gain to the current salience: `newSalience = salience + gain`.
3. Updates `LastReinforcedAt` to the current time, resetting the decay clock.
4. Writes an audit entry recording the reinforcement.

```go
func (s *Service) Reinforce(ctx context.Context, id, actor, rationale string) error {
    // Within a transaction:
    gain := record.Lifecycle.Decay.ReinforcementGain
    newSalience := record.Salience + gain
    // update salience, set LastReinforcedAt = now, add audit entry
}
```

Because `LastReinforcedAt` is reset, the next decay sweep will calculate elapsed time from this new timestamp, effectively restarting the decay clock from the boosted salience value.

### The Reinforce RPC

The gRPC API exposes reinforcement through the `Reinforce` RPC:

```protobuf
rpc Reinforce(ReinforceRequest) returns (ReinforceResponse);
```

The request requires:

| Field       | Type   | Description                                |
|-------------|--------|--------------------------------------------|
| `id`        | string | The memory record ID to reinforce          |
| `actor`     | string | Who is performing the reinforcement        |
| `rationale` | string | Why the reinforcement is happening         |

Both `actor` and `rationale` are validated for length and recorded in the audit log. Example usage with `grpcurl`:

```bash
grpcurl -plaintext -d '{
  "id": "mem-abc-123",
  "actor": "agent-orchestrator",
  "rationale": "Record was retrieved and used successfully in task completion"
}' localhost:9090 membrane.v1.MembraneService/Reinforce
```

### The Penalize RPC

The inverse of reinforcement is **penalization**. The `Penalize` RPC reduces a record's salience by a specified amount, floored at `MinSalience`:

```go
func (s *Service) Penalize(ctx context.Context, id string, amount float64, ...) error {
    floor := record.Lifecycle.Decay.MinSalience
    newSalience := record.Salience - amount
    if newSalience < floor {
        newSalience = floor
    }
    // update salience, add audit entry
}
```

```bash
grpcurl -plaintext -d '{
  "id": "mem-abc-123",
  "amount": 0.3,
  "actor": "feedback-loop",
  "rationale": "Memory led to incorrect tool invocation"
}' localhost:9090 membrane.v1.MembraneService/Penalize
```

::: warning
Penalize does **not** reset `LastReinforcedAt`. The decay clock continues from its previous reinforcement time. This means penalized records will continue decaying from their reduced salience at the normal rate.
:::

## Pruning

Pruning is the automatic cleanup of records that have decayed beyond usefulness.

### Prune Criteria

A record is pruned when **all** of the following conditions are met:

1. The record is **not pinned** (`Lifecycle.Pinned == false`).
2. The record's `DeletionPolicy` is `auto_prune`.
3. The record's salience is at or below its `MinSalience` floor.
4. The record's salience is below `0.001` (effectively zero).

```go
if record.Salience <= floor && record.Salience < 0.001 {
    // prune this record
}
```

### Deletion Policies

Three deletion policies control whether a record can be auto-pruned:

| Policy        | Behavior                                              |
|---------------|-------------------------------------------------------|
| `auto_prune`  | Record is deleted when salience reaches the floor     |
| `manual_only` | Record can only be deleted by explicit user action    |
| `never`       | Record cannot be deleted under any circumstances      |

The default policy for new records is `auto_prune`.

### Audit Trail

Before a record is deleted by the pruner, an audit entry is written documenting the action:

```go
entry := schema.AuditEntry{
    Action:    schema.AuditActionDelete,
    Actor:     "decay-service",
    Timestamp: now,
    Rationale: "auto-pruned: salience reached floor",
}
```

This ensures that even deleted records leave a forensic trace for debugging and compliance purposes.

## Configuration

Decay behavior is controlled at two levels: **global** server configuration and **per-record** decay profiles.

### Global Configuration

In your `membrane.yaml`:

```yaml
# How often the decay scheduler runs its sweep
decay_interval: 1h
```

| Parameter        | Type     | Default | Description                                      |
|------------------|----------|---------|--------------------------------------------------|
| `decay_interval` | duration | `1h`    | How often the scheduler applies decay and prunes |

A shorter interval means salience values are updated more frequently (more accurate) but uses more CPU. A longer interval is cheaper but means salience values can be stale between ticks.

### Per-Record Decay Profile

Each record's `Lifecycle.Decay` field contains its decay profile:

```yaml
lifecycle:
  decay:
    curve: "exponential"          # or "linear", "custom"
    half_life_seconds: 86400      # 24 hours
    min_salience: 0.0             # floor value
    max_age_seconds: 0            # 0 = no hard cutoff
    reinforcement_gain: 0.0       # how much reinforcement adds
  pinned: false
  deletion_policy: "auto_prune"
```

| Field                 | Type    | Default        | Description                                     |
|-----------------------|---------|----------------|-------------------------------------------------|
| `curve`               | string  | `exponential`  | Decay function: `exponential`, `linear`, `custom` |
| `half_life_seconds`   | int64   | `86400` (1 day)| Time parameter for the decay curve              |
| `min_salience`        | float64 | `0.0`          | Floor below which salience cannot decay          |
| `max_age_seconds`     | int64   | `0` (disabled) | Hard age cutoff; salience set to 0 when exceeded |
| `reinforcement_gain`  | float64 | `0.0`          | Salience boost applied on each reinforcement     |

::: tip
If `reinforcement_gain` is 0, calling `Reinforce` still resets `LastReinforcedAt` (restarting the decay clock) but does not increase salience. This is useful for "touch to keep alive" patterns without inflating importance.
:::

## Examples

### Exponential Decay Over One Week

A record created with default settings (exponential curve, 24h half-life, no floor):

```
Hour  0: salience = 1.000  ████████████████████
Hour 12: salience = 0.707  ██████████████
Hour 24: salience = 0.500  ██████████
Hour 48: salience = 0.250  █████
Hour 72: salience = 0.125  ██
Hour 96: salience = 0.063  █
Hour168: salience = 0.008  ▏
```

After one week the record is nearly zero and will be auto-pruned on the next scheduler tick.

### Linear Decay With Floor

A record configured with linear decay, 12h half-life, and a floor of 0.1:

```
Hour  0: salience = 1.000  ████████████████████
Hour  3: salience = 0.750  ███████████████
Hour  6: salience = 0.500  ██████████
Hour  9: salience = 0.250  █████
Hour 12: salience = 0.100  ██  (floor reached)
Hour 24: salience = 0.100  ██  (held at floor)
```

The floor prevents the record from being pruned -- it remains retrievable at low priority indefinitely.

### Reinforcement Keeping a Record Alive

A record with exponential decay (24h half-life) and `reinforcement_gain: 0.5`, reinforced every 12 hours:

```
Hour  0: salience = 1.000  (created)
Hour 12: salience = 0.707  (decayed)
         reinforce  +0.500
         salience = 1.207  (boosted, clock reset)
Hour 24: salience = 0.854  (decayed 12h from 1.207)
         reinforce  +0.500
         salience = 1.354  (boosted, clock reset)
Hour 36: salience = 0.957  (decayed 12h from 1.354)
```

Each reinforcement pushes salience higher than the last cycle, creating a "snowball" effect for frequently-used memories.

### Max Age Hard Cutoff

A working memory record with `max_age_seconds: 3600` (1 hour):

```
Min  0: salience = 1.000  (created)
Min 30: salience = 0.707  (normal exponential decay)
Min 60: salience = 0.000  (max age exceeded, forced to zero)
        --> auto-pruned on next scheduler tick
```

This is ideal for ephemeral context like active task state that should not persist beyond a session.

## Interaction with Retrieval

Salience directly affects how records are ranked and filtered during retrieval.

### Salience Filtering

The `Retrieve` RPC accepts a `min_salience` parameter. Records with salience below this threshold are excluded from results:

```go
if req.MinSalience > 0 {
    records = FilterBySalience(records, req.MinSalience)
}
```

This allows callers to control the quality floor for returned memories. A `min_salience` of `0.3` would exclude records that have decayed below 30% of their original importance.

### Salience-Based Ranking

After filtering, all results are sorted by salience in descending order:

```go
SortBySalience(allRecords)
```

This means the most recently reinforced and highest-importance records always appear first in retrieval results, giving agents the most relevant context at the top of the list.

### Practical Impact

The combination of decay + retrieval ranking creates a natural "recency and relevance" bias:

- **Fresh memories** have high salience and rank at the top.
- **Old but reinforced memories** maintain high salience through repeated reinforcement.
- **Stale memories** gradually fall in ranking and eventually drop below the `min_salience` filter entirely.

## Best Practices

### Choosing a Decay Curve

| Use Case                          | Recommended Curve | Half-Life      | Notes                                    |
|-----------------------------------|-------------------|----------------|------------------------------------------|
| General episodic memory           | `exponential`     | 24h -- 72h     | Mimics natural forgetting                |
| Session/working memory            | `linear`          | 1h -- 4h       | Hard deadline for cleanup                |
| Learned competence (skills)       | `exponential`     | 168h+ (7d+)    | Skills should persist longer             |
| Semantic facts                    | `exponential`     | 720h+ (30d+)   | Facts are valuable long-term             |
| Temporary debug context           | `linear`          | 15m -- 1h      | Clean up quickly                         |

### Setting Reinforcement Gain

- **0.0** -- Reinforcement only resets the decay clock without boosting salience. Good for "keep alive" patterns.
- **0.1 -- 0.3** -- Moderate boost. Suitable for memories that benefit from retrieval but should not grow unbounded.
- **0.5+** -- Aggressive boost. Use for memories in active feedback loops where frequent use should strongly signal importance.

### Using MinSalience Floors

Set a non-zero `min_salience` when you want records to become low-priority but never disappear:

- Compliance records that must be retained.
- Baseline knowledge that should always be retrievable if specifically queried.
- Records with `deletion_policy: manual_only` (the floor provides a safety net even if auto-prune is set).

### Pinning vs. Floors vs. Deletion Policy

Three mechanisms prevent record removal. Choose the right one:

| Mechanism             | Decay Applies? | Can Be Pruned? | Use When                                |
|-----------------------|----------------|----------------|-----------------------------------------|
| `pinned: true`        | No             | No             | Record must never lose salience          |
| `min_salience > 0`    | Yes (to floor) | No (if > 0.001)| Record should fade but never vanish      |
| `deletion_policy: never` | Yes         | No             | Record can decay but must not be deleted |

### Scheduler Interval Tuning

- **Production (typical):** `1h` -- good balance of accuracy and performance.
- **High-throughput agents:** `15m -- 30m` -- more accurate salience, higher CPU cost.
- **Low-activity systems:** `6h -- 24h` -- minimal overhead, salience updates are coarse.

The scheduler processes all non-pinned records on each tick, so the interval should be tuned based on your total record count and acceptable staleness for salience values.
