package schema

import (
	"strings"
	"testing"
)

func TestEntityPhraseBoundaryCompatibility(t *testing.T) {
	for _, tc := range []struct {
		haystack, needle string
		want             bool
	}{
		{"aaaaa", "aaa", false},
		{"aaaa aaa", "aaa", true},
		{"--a--a--", "--a--", false},
		{"debug auth-service latency", "auth-service", true},
		{"postgres", "go", false},
		{"αorchidβ", "orchid", true},
		{"a a a a a", "a a a", true},
		{"a a a a ax", "a a a ax", true},
		{"abc", "abcdef", false},
	} {
		if got := boundedPhraseContains(tc.haystack, tc.needle); got != tc.want {
			t.Errorf("phrase %q in %q = %v, want %v", tc.needle, tc.haystack, got, tc.want)
		}
	}
}

func BenchmarkEntityPhraseOverlap(b *testing.B) {
	haystack, needle := strings.Repeat("a", 16<<10), strings.Repeat("a", 256)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if boundedPhraseContains(haystack, needle) {
			b.Fatal("unexpected boundary match")
		}
	}
}

func TestEntityPhraseMatchingHasLinearWork(t *testing.T) {
	for _, n := range []int{128, 1024, 16 << 10} {
		for _, tc := range []struct{ haystack, needle string }{
			{strings.Repeat("a", n), strings.Repeat("a", n/2)},
			{strings.Repeat("ab", n), strings.Repeat("ab", n/2) + "c"},
			{strings.Repeat("a-", n), strings.Repeat("a-", n/2) + "a"},
		} {
			_, work := entityPhraseMatch(tc.haystack, tc.needle)
			if limit := 6 * (len(tc.haystack) + len(tc.needle)); work > limit {
				t.Fatalf("work = %d, linear bound = %d", work, limit)
			}
		}
	}
}

func TestEntityPhraseMatchingAgreesWithBoundaryReference(t *testing.T) {
	// Exhaustive small inputs exercise prefix fallback, punctuation and rejected
	// overlaps independently of the production search algorithm.
	words := []string{""}
	level := []string{""}
	for n := 0; n < 4; n++ {
		var next []string
		for _, word := range level {
			for _, ch := range []byte{'a', 'b', '-', ' '} {
				next = append(next, word+string(ch))
			}
		}
		words = append(words, next...)
		level = next
	}
	for _, haystack := range words {
		for _, needle := range words {
			if len(needle) > 3 {
				continue
			}
			want := false
			if needle != "" {
				for i := 0; i+len(needle) <= len(haystack); i++ {
					if strings.HasPrefix(haystack[i:], needle) && isEntityTermBoundary(haystack, i-1) && isEntityTermBoundary(haystack, i+len(needle)) {
						want = true
						break
					}
				}
			}
			if got := boundedPhraseContains(haystack, needle); got != want {
				t.Fatalf("phrase %q in %q = %v, reference = %v", needle, haystack, got, want)
			}
		}
	}
}
