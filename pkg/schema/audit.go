package schema

import "time"

// AuditEntry records a single action in a memory record's audit log.
// RFC 15A.8: Every revision MUST be auditable and traceable to evidence.
type AuditEntry struct {
	// Action is the type of action that was performed.
	// RFC 15A.8: Required field (create, revise, fork, merge, delete, reinforce, decay).
	Action AuditAction `json:"action"`

	// Actor identifies who or what performed the action.
	// RFC 15A.8: Required field (e.g., user ID, system component, consolidator).
	Actor string `json:"actor"`

	// Timestamp records when the action occurred.
	// RFC 15A.8: Required field.
	Timestamp time.Time `json:"timestamp"`

	// Rationale explains why the action was taken.
	// RFC 15A.8: Required field for traceability.
	Rationale string `json:"rationale"`
}
