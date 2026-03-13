// Package decay implements salience decay, reinforcement, and scheduling
// as specified in RFC 15A.7.
package decay

import (
	"math"

	"github.com/GustyCube/membrane/pkg/schema"
)

// DecayFunc takes current salience, elapsed seconds since last reinforcement,
// and the decay profile, returns new salience.
type DecayFunc func(currentSalience float64, elapsedSeconds float64, profile schema.DecayProfile) float64

// Exponential computes exponential decay: salience * 2^(-elapsed/halfLife),
// floored at MinSalience.
// RFC 15A.7: Exponential decay with half-life parameter.
func Exponential(currentSalience, elapsedSeconds float64, profile schema.DecayProfile) float64 {
	halfLife := float64(profile.HalfLifeSeconds)
	if halfLife <= 0 {
		return math.Max(currentSalience, profile.MinSalience)
	}
	// salience * 2^(-elapsed/halfLife) = salience * exp(-elapsed * ln(2) / halfLife)
	decayed := currentSalience * math.Exp(-elapsedSeconds*math.Log(2)/halfLife)
	return math.Max(decayed, profile.MinSalience)
}

// GetDecayFunc returns the decay function used by the implementation.
// Membrane currently supports exponential decay only.
func GetDecayFunc(curve schema.DecayCurve) DecayFunc {
	return Exponential
}
