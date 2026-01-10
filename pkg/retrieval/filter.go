package retrieval

import (
	"sort"

	"github.com/GustyCube/membrane/pkg/schema"
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
	result := make([]*schema.MemoryRecord, 0, len(records))
	for _, r := range records {
		if SensitivityLevel(r.Sensitivity) <= maxLevel {
			result = append(result, r)
		}
	}
	return result
}

// FilterByTrust returns only records that the given TrustContext allows.
func FilterByTrust(records []*schema.MemoryRecord, trust *TrustContext) []*schema.MemoryRecord {
	if trust == nil {
		return records
	}
	result := make([]*schema.MemoryRecord, 0, len(records))
	for _, r := range records {
		if trust.Allows(r) {
			result = append(result, r)
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
