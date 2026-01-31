---
title: Metrics & Observability
outline: [2, 3]
---

# Metrics & Observability

## Overview

Membrane exposes a point-in-time metrics snapshot that captures the health, composition, and behavioral characteristics of the memory substrate. The observability system is designed around a single `Collector` struct that queries the underlying `storage.Store` and returns a `Snapshot` value -- a JSON-serializable summary of every measurable dimension of the memory pool.

Key design principles:

- **Pull-based collection** -- metrics are computed on demand when `Collect()` is called, not accumulated in background goroutines. This avoids lock contention and keeps the hot path (ingestion, retrieval) free of metric bookkeeping overhead.
- **Single snapshot struct** -- all metrics are returned in one atomic `Snapshot`, making it trivial to serialize to JSON, feed into Prometheus, or log to structured output.
- **RFC 15.10 behavioral metrics** -- in addition to basic inventory counts, the collector computes higher-order behavioral indicators such as memory growth rate, retrieval usefulness, competence success rate, plan reuse frequency, and revision rate.

The metrics endpoint is exposed through the gRPC `GetMetrics` RPC and can be polled by any external monitoring system.

## Collector Interface

The `Collector` is defined in `pkg/metrics/metrics.go` and takes a `storage.Store` at construction time:

```go
package metrics

// Collector gathers metrics from the underlying store.
type Collector struct {
    store storage.Store
}

// NewCollector creates a new Collector backed by the given store.
func NewCollector(store storage.Store) *Collector {
    return &Collector{store: store}
}

// Collect queries the store and returns a metrics Snapshot.
func (c *Collector) Collect(ctx context.Context) (*Snapshot, error)
```

The `Membrane` orchestrator instantiates the collector during initialization and delegates to it from the public `GetMetrics` method:

```go
// In pkg/membrane/membrane.go
metricsCollector := metrics.NewCollector(store)

// ...

func (m *Membrane) GetMetrics(ctx context.Context) (*metrics.Snapshot, error) {
    return m.metrics.Collect(ctx)
}
```

At the gRPC layer, the `GetMetrics` handler serializes the snapshot to JSON and returns it in a `MetricsResponse`:

```go
func (h *Handler) GetMetrics(ctx context.Context, _ *pb.GetMetricsRequest) (*pb.MetricsResponse, error) {
    snap, err := h.membrane.GetMetrics(ctx)
    if err != nil {
        return nil, internalErr(err)
    }
    data, err := json.Marshal(snap)
    if err != nil {
        return nil, internalErr(fmt.Errorf("marshal metrics: %w", err))
    }
    return &pb.MetricsResponse{Snapshot: data}, nil
}
```

## Built-in Metrics

The `Snapshot` struct contains every metric that the collector computes. The fields are grouped into inventory metrics and behavioral metrics.

### Inventory Metrics

| Field | Type | Description |
|---|---|---|
| `total_records` | `int` | Total number of memory records in the store |
| `records_by_type` | `map[string]int` | Record count broken down by memory type (`episodic`, `semantic`, `working`, `competence`, `plan_graph`) |
| `avg_salience` | `float64` | Mean salience across all records |
| `avg_confidence` | `float64` | Mean confidence across all records |
| `salience_distribution` | `map[string]int` | Histogram of salience values bucketed into five ranges |
| `active_records` | `int` | Records with salience > 0 (not fully decayed) |
| `pinned_records` | `int` | Records with `Lifecycle.Pinned == true` (exempt from decay) |
| `total_audit_entries` | `int` | Sum of audit log entries across all records |

### Salience Distribution Buckets

The salience distribution divides records into five equal-width buckets:

| Bucket | Range |
|---|---|
| `0.0-0.2` | Salience in [0.0, 0.2) |
| `0.2-0.4` | Salience in [0.2, 0.4) |
| `0.4-0.6` | Salience in [0.4, 0.6) |
| `0.6-0.8` | Salience in [0.6, 0.8) |
| `0.8-1.0` | Salience in [0.8, 1.0] |

::: tip
A healthy memory substrate should show a bell-curve distribution weighted toward the middle buckets. If the `0.0-0.2` bucket dominates, decay may be too aggressive. If `0.8-1.0` dominates, reinforcement signals may be too liberal or decay is not running.
:::

### Record Type Breakdown

The `records_by_type` map uses the `MemoryType` enum values as keys:

| Key | Memory Type |
|---|---|
| `episodic` | Raw experience -- user inputs, tool calls, events |
| `working` | Current task state snapshots |
| `semantic` | Stable knowledge -- preferences, environment facts |
| `competence` | Procedural knowledge -- how to achieve goals |
| `plan_graph` | Reusable solution structures as directed graphs |

## Behavioral Metrics

These metrics implement **RFC 15.10** and provide higher-order insight into how effectively the memory substrate supports the agent.

| Field | Type | Description | Computation |
|---|---|---|---|
| `memory_growth_rate` | `float64` | Fraction of records created in the last 24 hours | `recent_records / total_records` |
| `retrieval_usefulness` | `float64` | Fraction of audit actions that are reinforcements | `reinforce_count / total_audit_count` |
| `competence_success_rate` | `float64` | Average success rate across competence records | Mean of `CompetencePayload.Performance.SuccessRate` |
| `plan_reuse_frequency` | `float64` | Average execution count across plan graph records | Mean of `PlanGraphPayload.Metrics.ExecutionCount` |
| `revision_rate` | `float64` | Fraction of audit actions that are revisions (revise, fork, merge) | `revision_count / total_audit_count` |

### Memory Growth Rate

Computed as the ratio of records created within the last 24 hours to total records. A value approaching 1.0 means the substrate is almost entirely new memories; a value near 0.0 means the knowledge base is stable and mature.

```
memory_growth_rate = records_created_last_24h / total_records
```

::: warning
A sustained growth rate above 0.8 may indicate that the agent is not consolidating memories effectively, leading to unbounded store growth.
:::

### Retrieval Usefulness

Measures how often retrieved memories are reinforced after use. The collector scans every audit log entry across all records and counts `reinforce` actions relative to total audit entries.

```
retrieval_usefulness = reinforce_actions / total_audit_entries
```

A higher value indicates that the memories being retrieved are actually useful to the agent -- they get reinforced rather than ignored or revised.

### Competence Success Rate

The average `SuccessRate` field from all `CompetencePayload` records that have performance data. This metric is only meaningful when competence-type records are present.

### Plan Reuse Frequency

The average `ExecutionCount` from all `PlanGraphPayload` records that have metrics. A higher value means plans are being reused across tasks rather than rebuilt from scratch each time.

### Revision Rate

The fraction of audit log entries that represent structural changes to knowledge: `revise`, `fork`, or `merge` actions. A healthy revision rate indicates the agent is actively correcting and evolving its knowledge rather than accumulating stale records.

```
revision_rate = (revise + fork + merge) / total_audit_entries
```

## Integration with Prometheus

Membrane does not ship a built-in Prometheus exporter, but the `Snapshot` struct is designed to be trivially bridged. Here is a recommended adapter pattern:

```go
package promexporter

import (
    "context"
    "github.com/prometheus/client_golang/prometheus"
    "github.com/GustyCube/membrane/pkg/metrics"
)

type MembraneCollector struct {
    collector *metrics.Collector

    totalRecords    *prometheus.Desc
    activeRecords   *prometheus.Desc
    pinnedRecords   *prometheus.Desc
    avgSalience     *prometheus.Desc
    avgConfidence   *prometheus.Desc
    auditEntries    *prometheus.Desc
    recordsByType   *prometheus.Desc
    salienceBucket  *prometheus.Desc

    // RFC 15.10
    memoryGrowthRate      *prometheus.Desc
    retrievalUsefulness   *prometheus.Desc
    competenceSuccessRate *prometheus.Desc
    planReuseFrequency    *prometheus.Desc
    revisionRate          *prometheus.Desc
}

func NewMembraneCollector(c *metrics.Collector) *MembraneCollector {
    return &MembraneCollector{
        collector: c,
        totalRecords: prometheus.NewDesc(
            "membrane_records_total",
            "Total number of memory records",
            nil, nil,
        ),
        activeRecords: prometheus.NewDesc(
            "membrane_records_active",
            "Number of records with salience > 0",
            nil, nil,
        ),
        pinnedRecords: prometheus.NewDesc(
            "membrane_records_pinned",
            "Number of pinned records exempt from decay",
            nil, nil,
        ),
        avgSalience: prometheus.NewDesc(
            "membrane_salience_avg",
            "Average salience across all records",
            nil, nil,
        ),
        avgConfidence: prometheus.NewDesc(
            "membrane_confidence_avg",
            "Average confidence across all records",
            nil, nil,
        ),
        auditEntries: prometheus.NewDesc(
            "membrane_audit_entries_total",
            "Total audit log entries across all records",
            nil, nil,
        ),
        recordsByType: prometheus.NewDesc(
            "membrane_records_by_type",
            "Record count by memory type",
            []string{"memory_type"}, nil,
        ),
        salienceBucket: prometheus.NewDesc(
            "membrane_salience_distribution",
            "Number of records in each salience bucket",
            []string{"bucket"}, nil,
        ),
        memoryGrowthRate: prometheus.NewDesc(
            "membrane_memory_growth_rate",
            "Fraction of records created in the last 24h",
            nil, nil,
        ),
        retrievalUsefulness: prometheus.NewDesc(
            "membrane_retrieval_usefulness",
            "Fraction of audit actions that are reinforcements",
            nil, nil,
        ),
        competenceSuccessRate: prometheus.NewDesc(
            "membrane_competence_success_rate",
            "Average success rate across competence records",
            nil, nil,
        ),
        planReuseFrequency: prometheus.NewDesc(
            "membrane_plan_reuse_frequency",
            "Average execution count across plan graph records",
            nil, nil,
        ),
        revisionRate: prometheus.NewDesc(
            "membrane_revision_rate",
            "Fraction of audit entries that are revisions",
            nil, nil,
        ),
    }
}

func (mc *MembraneCollector) Describe(ch chan<- *prometheus.Desc) {
    ch <- mc.totalRecords
    ch <- mc.activeRecords
    ch <- mc.pinnedRecords
    ch <- mc.avgSalience
    ch <- mc.avgConfidence
    ch <- mc.auditEntries
    ch <- mc.recordsByType
    ch <- mc.salienceBucket
    ch <- mc.memoryGrowthRate
    ch <- mc.retrievalUsefulness
    ch <- mc.competenceSuccessRate
    ch <- mc.planReuseFrequency
    ch <- mc.revisionRate
}

func (mc *MembraneCollector) Collect(ch chan<- prometheus.Metric) {
    snap, err := mc.collector.Collect(context.Background())
    if err != nil {
        return
    }

    ch <- prometheus.MustNewConstMetric(mc.totalRecords, prometheus.GaugeValue, float64(snap.TotalRecords))
    ch <- prometheus.MustNewConstMetric(mc.activeRecords, prometheus.GaugeValue, float64(snap.ActiveRecords))
    ch <- prometheus.MustNewConstMetric(mc.pinnedRecords, prometheus.GaugeValue, float64(snap.PinnedRecords))
    ch <- prometheus.MustNewConstMetric(mc.avgSalience, prometheus.GaugeValue, snap.AvgSalience)
    ch <- prometheus.MustNewConstMetric(mc.avgConfidence, prometheus.GaugeValue, snap.AvgConfidence)
    ch <- prometheus.MustNewConstMetric(mc.auditEntries, prometheus.GaugeValue, float64(snap.TotalAuditEntries))

    for typ, count := range snap.RecordsByType {
        ch <- prometheus.MustNewConstMetric(mc.recordsByType, prometheus.GaugeValue, float64(count), typ)
    }
    for bucket, count := range snap.SalienceDistribution {
        ch <- prometheus.MustNewConstMetric(mc.salienceBucket, prometheus.GaugeValue, float64(count), bucket)
    }

    ch <- prometheus.MustNewConstMetric(mc.memoryGrowthRate, prometheus.GaugeValue, snap.MemoryGrowthRate)
    ch <- prometheus.MustNewConstMetric(mc.retrievalUsefulness, prometheus.GaugeValue, snap.RetrievalUsefulness)
    ch <- prometheus.MustNewConstMetric(mc.competenceSuccessRate, prometheus.GaugeValue, snap.CompetenceSuccessRate)
    ch <- prometheus.MustNewConstMetric(mc.planReuseFrequency, prometheus.GaugeValue, snap.PlanReuseFrequency)
    ch <- prometheus.MustNewConstMetric(mc.revisionRate, prometheus.GaugeValue, snap.RevisionRate)
}
```

Register the collector and expose the `/metrics` HTTP endpoint:

```go
reg := prometheus.NewRegistry()
reg.MustRegister(promexporter.NewMembraneCollector(metricsCollector))

http.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
```

## Custom Collectors

Because the `Collector` operates against the `storage.Store` interface, you can build custom metric pipelines by wrapping or replacing it. The simplest approach is to call `Collect()` and post-process the `Snapshot`:

```go
func collectAndPublish(ctx context.Context, c *metrics.Collector) error {
    snap, err := c.Collect(ctx)
    if err != nil {
        return err
    }

    // Example: push to StatsD
    statsd.Gauge("membrane.records.total", float64(snap.TotalRecords))
    statsd.Gauge("membrane.records.active", float64(snap.ActiveRecords))
    statsd.Gauge("membrane.growth_rate", snap.MemoryGrowthRate)

    for typ, count := range snap.RecordsByType {
        statsd.Gauge("membrane.records."+typ, float64(count))
    }

    return nil
}
```

For periodic collection, run the publisher on a ticker:

```go
go func() {
    ticker := time.NewTicker(30 * time.Second)
    defer ticker.Stop()
    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            if err := collectAndPublish(ctx, collector); err != nil {
                log.Printf("metrics publish error: %v", err)
            }
        }
    }
}()
```

::: tip
When polling from an external system, call the gRPC `GetMetrics` RPC rather than accessing the `Collector` directly. This ensures you go through the same authentication and rate-limiting middleware as all other requests.
:::

## Metric Dimensions

Metrics that break down by dimension use the following label sets:

### `memory_type` label

Applied to `membrane_records_by_type`. Possible values:

| Value | Description |
|---|---|
| `episodic` | Experience records (events, tool outputs) |
| `working` | Active task state snapshots |
| `semantic` | Stable knowledge and observations |
| `competence` | Procedural knowledge with performance data |
| `plan_graph` | Reusable directed-graph solution structures |

### `bucket` label

Applied to `membrane_salience_distribution`. Values: `0.0-0.2`, `0.2-0.4`, `0.4-0.6`, `0.6-0.8`, `0.8-1.0`.

### Audit action dimensions

The behavioral metrics aggregate over the following audit actions, defined in `pkg/schema/enums.go`:

| Action | Counted in |
|---|---|
| `create` | (total audit count) |
| `reinforce` | `retrieval_usefulness` numerator |
| `revise` | `revision_rate` numerator |
| `fork` | `revision_rate` numerator |
| `merge` | `revision_rate` numerator |
| `decay` | (total audit count) |
| `delete` | (total audit count) |

## Dashboard Examples

Below are PromQL queries you can use in Grafana dashboards to monitor a Membrane deployment.

### Total Records Over Time

```promql
membrane_records_total
```

### Records by Type (Stacked Area)

```promql
membrane_records_by_type
```

Group by the `memory_type` label and use a stacked area visualization.

### Salience Health

Average salience trending toward zero indicates aggressive decay or insufficient reinforcement:

```promql
membrane_salience_avg
```

### Decay Pressure

Percentage of records that have decayed to near-zero salience:

```promql
membrane_salience_distribution{bucket="0.0-0.2"} / membrane_records_total
```

### Memory Growth Rate Alert

Alert when more than 80% of records were created in the last 24 hours:

```promql
membrane_memory_growth_rate > 0.8
```

### Retrieval Effectiveness

Track whether retrieved memories are being reinforced:

```promql
membrane_retrieval_usefulness
```

::: warning
A retrieval usefulness below 0.1 over a sustained period suggests the agent is retrieving memories that are not contributing to task success. Review your ingestion quality and retrieval parameters.
:::

### Competence Performance

```promql
membrane_competence_success_rate
```

### Knowledge Stability

A high revision rate means the knowledge base is in flux. Combine with growth rate for context:

```promql
membrane_revision_rate
membrane_memory_growth_rate
```

### Pinned vs Active Records

```promql
membrane_records_pinned / membrane_records_active
```

A ratio approaching 1.0 means most surviving records are pinned and the decay system has little effect.

## Health Checks

### gRPC Health Probe

The simplest health check is to call `GetMetrics` and verify a successful response:

```bash
grpcurl -plaintext localhost:50051 membrane.v1.MembraneService/GetMetrics
```

A healthy response returns a JSON-encoded snapshot. Any gRPC error code other than `OK` indicates a problem with the store or the service.

### Programmatic Health Check

```go
func healthCheck(ctx context.Context, m *membrane.Membrane) error {
    snap, err := m.GetMetrics(ctx)
    if err != nil {
        return fmt.Errorf("health check failed: %w", err)
    }

    // Verify the store is responsive and contains data
    if snap.TotalRecords < 0 {
        return fmt.Errorf("invalid record count: %d", snap.TotalRecords)
    }

    return nil
}
```

### Recommended Alert Thresholds

| Condition | Severity | Suggested Threshold |
|---|---|---|
| `GetMetrics` returns error | **Critical** | Any error |
| `membrane_records_total` is 0 for > 5 min after startup | **Warning** | 0 records |
| `membrane_memory_growth_rate` > 0.8 sustained 1h | **Warning** | Growth rate spike |
| `membrane_retrieval_usefulness` < 0.05 sustained 1h | **Warning** | Low retrieval value |
| `membrane_salience_avg` < 0.1 | **Warning** | Over-aggressive decay |
| `membrane_records_active` / `membrane_records_total` < 0.1 | **Warning** | Most records decayed |
| `membrane_revision_rate` > 0.5 sustained 1h | **Info** | High knowledge churn |

### Kubernetes Liveness Probe

If running in Kubernetes, configure a gRPC liveness probe against the `GetMetrics` RPC:

```yaml
livenessProbe:
  grpc:
    port: 50051
  initialDelaySeconds: 10
  periodSeconds: 30
```

For a more targeted readiness probe, wrap the health check in an HTTP handler:

```go
http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
    ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
    defer cancel()

    if err := healthCheck(ctx, m); err != nil {
        http.Error(w, err.Error(), http.StatusServiceUnavailable)
        return
    }
    w.WriteHeader(http.StatusOK)
    w.Write([]byte("ok"))
})
```

```yaml
readinessProbe:
  httpGet:
    path: /healthz
    port: 8080
  initialDelaySeconds: 5
  periodSeconds: 15
```
