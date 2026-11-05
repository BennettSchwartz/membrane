package decay_test

import (
	"math"
	"testing"

	"github.com/BennettSchwartz/membrane/pkg/decay"
	"github.com/BennettSchwartz/membrane/pkg/schema"
)

func nearlyEqual(a, b, epsilon float64) bool {
	return math.Abs(a-b) < epsilon
}

func TestExponential(t *testing.T) {
	for _, tc := range []struct {
		name            string
		salience        float64
		elapsed         float64
		halfLifeSeconds int64
		minSalience     float64
		want            float64
	}{
		{name: "one half-life halves salience", salience: 0.8, elapsed: 3600, halfLifeSeconds: 3600, minSalience: 0, want: 0.4},
		{name: "no elapsed time keeps salience", salience: 0.8, elapsed: 0, halfLifeSeconds: 3600, minSalience: 0, want: 0.8},
		{name: "two half-lives quarter salience", salience: 0.8, elapsed: 7200, halfLifeSeconds: 3600, minSalience: 0, want: 0.2},
		{name: "half of a half-life", salience: 0.8, elapsed: 1800, halfLifeSeconds: 3600, minSalience: 0, want: 0.8 * math.Sqrt(0.5)},
		{name: "short half-life decays faster", salience: 0.8, elapsed: 30, halfLifeSeconds: 10, minSalience: 0, want: 0.1},
		{name: "decay below floor returns floor", salience: 0.8, elapsed: 36000, halfLifeSeconds: 3600, minSalience: 0.2, want: 0.2},
		{name: "decay above floor is not clamped", salience: 0.8, elapsed: 3600, halfLifeSeconds: 3600, minSalience: 0.2, want: 0.4},
		{name: "salience already below floor is raised", salience: 0.1, elapsed: 0, halfLifeSeconds: 3600, minSalience: 0.2, want: 0.2},
		{name: "zero half-life keeps salience", salience: 0.8, elapsed: 3600, halfLifeSeconds: 0, minSalience: 0.2, want: 0.8},
		{name: "negative half-life keeps salience", salience: 0.8, elapsed: 3600, halfLifeSeconds: -60, minSalience: 0.2, want: 0.8},
		{name: "zero half-life raises to floor", salience: 0.1, elapsed: 3600, halfLifeSeconds: 0, minSalience: 0.2, want: 0.2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			profile := schema.DecayProfile{
				HalfLifeSeconds: tc.halfLifeSeconds,
				MinSalience:     tc.minSalience,
			}

			got := decay.Exponential(tc.salience, tc.elapsed, profile)

			if !nearlyEqual(got, tc.want, 0.0001) {
				t.Fatalf("Exponential(%.4f, %.0f) = %.4f, want %.4f", tc.salience, tc.elapsed, got, tc.want)
			}
		})
	}
}

func TestGetDecayFunc(t *testing.T) {
	profile := schema.DecayProfile{
		HalfLifeSeconds: 3600,
		MinSalience:     0.1,
	}

	for _, tc := range []struct {
		name  string
		curve schema.DecayCurve
	}{
		{name: "known exponential curve", curve: schema.DecayCurveExponential},
		{name: "empty curve falls back to exponential", curve: schema.DecayCurve("")},
		{name: "unknown curve falls back to exponential", curve: schema.DecayCurve("unknown")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fn := decay.GetDecayFunc(tc.curve)
			if fn == nil {
				t.Fatalf("GetDecayFunc(%q) = nil, want decay function", tc.curve)
			}

			got := fn(0.8, 3600, profile)
			want := decay.Exponential(0.8, 3600, profile)

			if !nearlyEqual(got, want, 0.0001) {
				t.Fatalf("GetDecayFunc(%q)(0.8, 3600) = %.4f, want %.4f", tc.curve, got, want)
			}
		})
	}
}
