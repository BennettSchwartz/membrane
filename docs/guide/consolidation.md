# Consolidation

Consolidation is the process of distilling raw episodic experience into durable, structured knowledge. It runs automatically on a configurable schedule and requires no user approval.

## Overview

The consolidation service orchestrates four sub-consolidators that run in sequence:

```
Episodic Compression -> Semantic Extraction -> Competence Extraction -> Plan Graph Synthesis
```

Each sub-consolidator reads from the store, identifies candidates for promotion, and writes new or updated records back.

## Episodic Compression

**Goal:** Reduce the salience of old episodic records so they eventually fall below the retrieval threshold and are pruned.

Episodic memories are intentionally short-lived. Compression identifies episodic records that have exceeded an age threshold and reduces their salience. The raw experience is preserved (episodic memory is append-only), but it becomes progressively harder to retrieve as it ages.

::: info
Episodic compression does not delete records. Deletion happens separately through the decay service when salience drops below the auto-prune threshold.
:::

## Semantic Extraction

**Goal:** Promote repeated observations into stable semantic facts.

When the consolidator detects that the same subject-predicate-object pattern appears across multiple episodic records, it creates a new semantic memory record. The new record:

- Links back to its episodic sources via **provenance** references
- Starts with the default decay profile (long half-life)
- Has its `revision.status` set to `active`
- Begins with `confidence` based on the number of supporting observations

If a matching semantic record already exists, the consolidator reinforces it instead of creating a duplicate.

## Competence Extraction

**Goal:** Identify successful tool-use patterns and encode them as competence records.

The competence consolidator looks for episodic records with:

- A `tool_graph` containing one or more tool calls
- An `outcome` of `success`
- Similar patterns appearing across multiple episodes

When a pattern is identified, the consolidator creates a competence record with:

- **Triggers** derived from the context that preceded the tool calls
- **Recipe steps** derived from the tool graph
- **Performance stats** initialized from the observed outcomes
- **Required tools** extracted from the tool nodes

## Plan Graph Synthesis

**Goal:** Extract reusable directed action graphs from episodic tool graphs.

Plan graph synthesis goes a step further than competence extraction by preserving the full dependency structure of tool call sequences. It creates plan graph records with:

- **Nodes** corresponding to individual tool calls
- **Edges** representing data and control dependencies between them
- **Constraints** derived from environment snapshots
- **Metrics** initialized from observed execution times

## Consolidation Results

Each consolidation run returns a summary:

```json
{
  "episodic_compressed": 15,
  "semantic_extracted": 3,
  "competence_extracted": 1,
  "plan_graphs_extracted": 0,
  "duplicates_resolved": 2
}
```

## Scheduling

Consolidation runs on a configurable interval (default: 6 hours). You can adjust this in the [configuration](/reference/configuration):

```yaml
consolidation_interval: 4h
```

Setting a shorter interval means knowledge is promoted faster but uses more CPU. Setting a longer interval reduces overhead but delays learning.

## Relationship to Decay

Consolidation and decay work together:

1. **Episodic records** are created during ingestion with high salience
2. **Decay** gradually reduces their salience over time
3. **Consolidation** extracts durable knowledge before the episodic records fade
4. **Promoted records** (semantic, competence, plan graph) start fresh with their own decay profiles
5. Records that are never consolidated eventually fall below the auto-prune threshold
