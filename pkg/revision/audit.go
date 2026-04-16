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
