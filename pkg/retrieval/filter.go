package retrieval

import (
	"sort"
	"time"

	"github.com/BennettSchwartz/membrane/pkg/schema"
)

// FilterBySalience returns only records whose salience is at or above the
// given minimum threshold.
func FilterBySalience(records []*schema.MemoryRecord, minSalience float64) []*schema.MemoryRecord {
	result := make([]*schema.MemoryRecord, 0, len(records))
	for _, r := range records {
		if r.Salience >= minSalience {
			result = append(result, r)
		}
	}
	return result
}

// FilterBySensitivity returns only records whose sensitivity level is at or
// below the given maximum sensitivity.
func FilterBySensitivity(records []*schema.MemoryRecord, maxSensitivity schema.Sensitivity) []*schema.MemoryRecord {
	maxLevel := SensitivityLevel(maxSensitivity)
	if maxLevel < 0 {
		return []*schema.MemoryRecord{}
	}
	result := make([]*schema.MemoryRecord, 0, len(records))
	for _, r := range records {
		level := SensitivityLevel(r.Sensitivity)
		if level >= 0 && level <= maxLevel {
			result = append(result, r)
		}
	}
	return result
}

// Redact creates a redacted copy of a MemoryRecord, preserving metadata while
// removing sensitive content. This implements graduated exposure per RFC Section 13.
//
// A redacted record retains:
//   - ID, Type, Sensitivity, Confidence, Salience, CreatedAt, UpdatedAt, Scope, Tags
//
// And clears:
//   - Payload (set to nil)
//   - Provenance (empty)
//   - AuditLog (empty)
//
// This gives metadata visibility without exposing sensitive content.
func Redact(record *schema.MemoryRecord) *schema.MemoryRecord {
	if record == nil {
		return nil
	}

	return &schema.MemoryRecord{
		ID:          record.ID,
		Type:        record.Type,
		Sensitivity: record.Sensitivity,
		Confidence:  record.Confidence,
		Salience:    record.Salience,
		Scope:       record.Scope,
		Tags:        append([]string(nil), record.Tags...),
		CreatedAt:   record.CreatedAt,
		UpdatedAt:   record.UpdatedAt,
		// Lifecycle is kept but zeroed out for redacted records
		Lifecycle: schema.Lifecycle{
			Decay: schema.DecayProfile{
				Curve:           schema.DecayCurveExponential,
				HalfLifeSeconds: 0,
			},
			LastReinforcedAt: time.Time{},
			DeletionPolicy:   schema.DeletionPolicyAutoPrune,
		},
		// Provenance is cleared
		Provenance: schema.Provenance{
			Sources: []schema.ProvenanceSource{},
		},
		// Relations are cleared
		Relations: []schema.Relation{},
		// Payload is set to nil
		Payload: nil,
		// Interpretation is stripped from redacted records.
		Interpretation: nil,
		// AuditLog is cleared
		AuditLog: []schema.AuditEntry{},
	}
}

// FilterByTrust returns records that the given TrustContext allows.
// Records at exactly one sensitivity level above the threshold are returned
// in redacted form (metadata only, no sensitive content).
func FilterByTrust(records []*schema.MemoryRecord, trust *TrustContext) []*schema.MemoryRecord {
	if trust == nil {
		return records
	}
	result := make([]*schema.MemoryRecord, 0, len(records))
	for _, r := range records {
		if trust.Allows(r) {
			result = append(result, r)
		} else if trust.AllowsRedacted(r) {
			result = append(result, Redact(r))
		}
	}
	return result
}

// SortBySalience sorts records by salience in descending order (highest first).
func SortBySalience(records []*schema.MemoryRecord) {
	sort.Slice(records, func(i, j int) bool {
		return records[i].Salience > records[j].Salience
	})
}
