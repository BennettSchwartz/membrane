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

// Linear computes linear decay: salience - (elapsed / halfLife) * salience,
// floored at MinSalience.
// RFC 15A.7: Linear decay over time.
func Linear(currentSalience, elapsedSeconds float64, profile schema.DecayProfile) float64 {
	halfLife := float64(profile.HalfLifeSeconds)
	if halfLife <= 0 {
		return math.Max(currentSalience, profile.MinSalience)
	}
	decayed := currentSalience - (elapsedSeconds/halfLife)*currentSalience
	return math.Max(decayed, profile.MinSalience)
}

// GetDecayFunc returns the appropriate decay function for a curve type.
// Falls back to Exponential for unknown or custom curve types.
func GetDecayFunc(curve schema.DecayCurve) DecayFunc {
	switch curve {
	case schema.DecayCurveLinear:
		return Linear
	case schema.DecayCurveExponential:
		return Exponential
	default:
		// Custom and unknown curves default to exponential.
		return Exponential
	}
}
