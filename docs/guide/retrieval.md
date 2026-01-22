# Retrieval

Membrane implements **layered retrieval** -- a structured approach to querying memory that mirrors how humans recall information, surfacing the most relevant and actionable knowledge first.

## Layered Retrieval Order

When a `Retrieve` request is made, the service queries each memory type in a fixed order:

```
1. Working     -- immediate task context
2. Semantic    -- known facts
3. Competence  -- procedural skills
4. Plan Graph  -- reusable action plans
5. Episodic    -- raw experience
```

You can restrict retrieval to specific memory types using the `memory_types` field in the request. When provided, only those types are queried (still in the canonical order above).

## Trust Context

Every retrieval request **must** include a trust context. The trust context gates which records are visible:

```json
{
  "max_sensitivity": "medium",
  "authenticated": true,
  "actor_id": "agent-1",
  "scopes": ["project-alpha", "global"]
}
```

### Sensitivity Filtering

Records are filtered based on a sensitivity hierarchy:

```
public < low < medium < high < hyper
```

A trust context with `max_sensitivity: "medium"` can access records classified as `public`, `low`, or `medium`, but not `high` or `hyper`.

### Scope Filtering

If the trust context specifies `scopes`, only records whose `scope` field matches one of the allowed scopes are returned. Records with an empty scope (unscoped) are always accessible regardless of the trust context's scope list.

::: tip
If the trust context has an empty `scopes` list, all scopes are allowed. This is useful for global administrative queries.
:::

## Salience Filtering

The `min_salience` parameter filters out records whose salience has decayed below a threshold. This prevents stale, low-importance memories from cluttering retrieval results.

```json
{
  "min_salience": 0.3
}
```

Records are sorted by salience in descending order, so the most important memories appear first.

## Multi-Solution Selection

When retrieval produces multiple **competence** or **plan graph** candidates, the Selector ranks them using three equally-weighted signals:

### Scoring Signals

1. **Applicability** -- how well the candidate's triggers or constraints match the current context (approximated by the record's `confidence` field)

2. **Observed success rate** -- from `performance.success_rate` (competence) or `1 - metrics.failure_rate` (plan graph)

3. **Recency of reinforcement** -- more recently reinforced records score higher, using an exponential decay with a 30-day half-life

### Selection Confidence

The selector computes a **confidence score** based on the normalized gap between the best and second-best candidates:

```
confidence = (best_score - second_best_score) / best_score
```

When confidence falls below the configured threshold (default: 0.7), the `needs_more` flag is set to `true`, signaling that the agent should seek additional information or ask for user disambiguation.

### Selection Result

The `selection` field in the response contains:

```json
{
  "selected": [ /* ranked MemoryRecords */ ],
  "confidence": 0.82,
  "needs_more": false
}
```

## Retrieve by ID

The `RetrieveByID` method fetches a single record by its UUID. The trust context is still enforced -- if the record's sensitivity exceeds the trust context's maximum, or the scope does not match, access is denied.

## Limiting Results

Use the `limit` field to cap the number of returned records:

```json
{
  "limit": 20
}
```

A limit of `0` means no limit is applied.
