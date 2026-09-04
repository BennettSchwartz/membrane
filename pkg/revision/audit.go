// Package revision implements the revision layer for the Membrane memory substrate.
// All revision operations are atomic: partial revisions are never externally visible (RFC 15.7).
package revision

import (
	"time"

	"github.com/BennettSchwartz/membrane/pkg/schema"
)

// newAuditEntry creates an AuditEntry with the given action, actor, rationale, and timestamp.
func newAuditEntry(action schema.AuditAction, actor, rationale string, timestamp time.Time) schema.AuditEntry {
	return schema.AuditEntry{
		Action:    action,
		Actor:     actor,
		Timestamp: timestamp,
		Rationale: rationale,
	}
}

// normalizeNewRecordMetadata makes operation identity, lineage, time, and
// lifecycle policy authoritative while preserving the caller's attributed
// evidence kind, opaque reference, and hash.
func normalizeNewRecordMetadata(rec *schema.MemoryRecord, actor string, timestamp time.Time, lifecycle schema.Lifecycle) {
	if rec == nil {
		return
	}
	rec.AuditLog = nil
	rec.Lifecycle = lifecycle
	rec.Lifecycle.LastReinforcedAt = timestamp
	rec.Provenance.CreatedBy = actor
	for i := range rec.Provenance.Sources {
		rec.Provenance.Sources[i].CreatedBy = actor
		rec.Provenance.Sources[i].Timestamp = timestamp
	}
	for i := range rec.Relations {
		rec.Relations[i].CreatedAt = timestamp
	}
	if payload, ok := rec.Payload.(*schema.SemanticPayload); ok && payload != nil {
		if payload.Revision != nil {
			payload.Revision.Supersedes = ""
			payload.Revision.SupersededBy = ""
			payload.Revision.Status = ""
		}
		for i := range payload.Evidence {
			payload.Evidence[i].Timestamp = timestamp
		}
	}
}
