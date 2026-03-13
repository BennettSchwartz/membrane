package schema

import "time"

// Lifecycle contains lifecycle metadata for a memory record.
// RFC 15A.3: Lifecycle controls decay, reinforcement, and deletion behavior.
type Lifecycle struct {
	// Decay defines the decay profile for this record's salience.
	Decay DecayProfile `json:"decay"`

	// LastReinforcedAt is the timestamp when salience was last reinforced.
	// RFC 15A.3: Required field for lifecycle tracking.
	LastReinforcedAt time.Time `json:"last_reinforced_at"`

	// Pinned indicates whether the record is protected from decay.
	// When true, salience will not decrease over time.
	Pinned bool `json:"pinned,omitempty"`

	// DeletionPolicy controls how this record may be deleted.
	// RFC 15A.3: Determines auto-prune, manual-only, or never deletion.
	DeletionPolicy DeletionPolicy `json:"deletion_policy,omitempty"`
}

// DecayProfile defines how salience decreases over time.
// RFC 15A.7: Decay profiles MUST be monotonic and reversible via reinforcement.
type DecayProfile struct {
	// Curve specifies the mathematical function for decay.
	// RFC 15A.3: Required field. The current implementation supports exponential decay only.
	Curve DecayCurve `json:"curve"`

	// HalfLifeSeconds defines the time for salience to decay by half.
	// RFC 15A.3: Required field with minimum value of 1.
	HalfLifeSeconds int64 `json:"half_life_seconds"`

	// MinSalience is the floor value below which salience cannot decay.
	// RFC 15A.7: Corresponds to the "floor" field in DecayProfile.
	// Value range: [0, 1]
	MinSalience float64 `json:"min_salience,omitempty"`

	// MaxAgeSeconds is the maximum age before the record is eligible for deletion.
	// Optional field that can trigger deletion regardless of salience.
	MaxAgeSeconds int64 `json:"max_age_seconds,omitempty"`

	// ReinforcementGain defines how much salience increases on reinforcement.
	// RFC 15A.7: Decay is reversible via reinforcement.
	ReinforcementGain float64 `json:"reinforcement_gain,omitempty"`
}
