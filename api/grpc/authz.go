package grpc

import (
	"context"
	"fmt"
	"net"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/BennettSchwartz/membrane/pkg/membrane"
	"github.com/BennettSchwartz/membrane/pkg/retrieval"
	"github.com/BennettSchwartz/membrane/pkg/schema"
)

type contextKey string

const principalContextKey contextKey = "membrane-grpc-principal"

type accessPolicy struct {
	readMax            schema.Sensitivity
	readScopes         []string
	writeMax           schema.Sensitivity
	writeScopes        []string
	defaultSensitivity schema.Sensitivity
}

type accessPrincipal struct {
	actorID       string
	authenticated bool
	policy        *accessPolicy
}

func newAccessPolicy(cfg *membrane.Config) *accessPolicy {
	if cfg == nil {
		cfg = membrane.DefaultConfig()
	}
	readMax := schema.Sensitivity(cfg.ReadMaxSensitivity)
	if !schema.IsValidSensitivity(readMax) {
		readMax = schema.SensitivityLow
	}
	writeMax := schema.Sensitivity(cfg.WriteMaxSensitivity)
	if !schema.IsValidSensitivity(writeMax) {
		writeMax = schema.SensitivityLow
	}
	defaultSensitivity := schema.Sensitivity(cfg.DefaultSensitivity)
	if !schema.IsValidSensitivity(defaultSensitivity) {
		defaultSensitivity = schema.SensitivityLow
	}
	return &accessPolicy{
		readMax:            readMax,
		readScopes:         normalizePolicyScopes(cfg.ReadScopes),
		writeMax:           writeMax,
		writeScopes:        normalizePolicyScopes(cfg.WriteScopes),
		defaultSensitivity: defaultSensitivity,
	}
}

func normalizePolicyScopes(scopes []string) []string {
	out := make([]string, 0, len(scopes))
	seen := make(map[string]struct{}, len(scopes))
	for _, scope := range scopes {
		scope = strings.TrimSpace(scope)
		if scope == "" {
			continue
		}
		if _, ok := seen[scope]; ok {
			continue
		}
		seen[scope] = struct{}{}
		out = append(out, scope)
	}
	if len(out) == 0 {
		return []string{"default"}
	}
	return out
}

func withAccessPrincipal(ctx context.Context, policy *accessPolicy, authenticated bool) context.Context {
	return context.WithValue(ctx, principalContextKey, &accessPrincipal{
		actorID:       "grpc",
		authenticated: authenticated,
		policy:        policy,
	})
}

func accessPrincipalFromContext(ctx context.Context) (*accessPrincipal, bool) {
	principal, ok := ctx.Value(principalContextKey).(*accessPrincipal)
	return principal, ok && principal != nil && principal.policy != nil
}

func (p *accessPolicy) readTrust(ctx context.Context, requestedMax schema.Sensitivity, requestedScopes []string) (*retrieval.TrustContext, error) {
	principal, ok := accessPrincipalFromContext(ctx)
	if !ok {
		principal = &accessPrincipal{actorID: "grpc", authenticated: false, policy: p}
	}
	scopes, err := requestedScopesWithinPolicy(requestedScopes, principal.policy.readScopes, "trust.scopes")
	if err != nil {
		return nil, err
	}
	return retrieval.NewTrustContext(
		minSensitivity(requestedMax, principal.policy.readMax),
		principal.authenticated,
		principal.actorID,
		scopes,
	), nil
}

func (p *accessPolicy) writeTrust(ctx context.Context) *retrieval.TrustContext {
	principal, ok := accessPrincipalFromContext(ctx)
	if !ok {
		principal = &accessPrincipal{actorID: "grpc", authenticated: false, policy: p}
	}
	return retrieval.NewTrustContext(principal.policy.writeMax, principal.authenticated, principal.actorID, principal.policy.writeScopes)
}

// allowsWriteRecord applies write policy semantics. Unlike read trust, writes
// never inherit the globally-readable treatment of unscoped records: every
// network mutation must name an explicitly permitted non-empty scope.
func (p *accessPolicy) allowsWriteRecord(ctx context.Context, record *schema.MemoryRecord) bool {
	if p == nil || record == nil || strings.TrimSpace(record.Scope) == "" {
		return false
	}
	principal, ok := accessPrincipalFromContext(ctx)
	if !ok {
		principal = &accessPrincipal{actorID: "grpc", authenticated: false, policy: p}
	}
	return isSensitivityAllowed(record.Sensitivity, principal.policy.writeMax) &&
		scopeAllowed(record.Scope, principal.policy.writeScopes)
}

func (p *accessPolicy) actor(ctx context.Context, fallback string) string {
	principal, ok := accessPrincipalFromContext(ctx)
	if ok && principal.actorID != "" {
		return principal.actorID
	}
	return fallback
}

func requestedScopesWithinPolicy(requested, allowed []string, field string) ([]string, error) {
	allowed = normalizePolicyScopes(allowed)
	if len(requested) == 0 {
		return allowed, nil
	}
	out := make([]string, 0, len(requested))
	seen := make(map[string]struct{}, len(requested))
	for _, scope := range requested {
		scope = strings.TrimSpace(scope)
		if scope == "" {
			continue
		}
		if !scopeAllowed(scope, allowed) {
			return nil, status.Errorf(codes.PermissionDenied, "%s includes scope %q outside the server policy", field, scope)
		}
		if _, ok := seen[scope]; ok {
			continue
		}
		seen[scope] = struct{}{}
		out = append(out, scope)
	}
	if len(out) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "%s must include at least one non-empty scope", field)
	}
	return out, nil
}

func scopeAllowed(scope string, allowed []string) bool {
	for _, candidate := range allowed {
		if candidate == "*" || candidate == scope {
			return true
		}
	}
	return false
}

func minSensitivity(a, b schema.Sensitivity) schema.Sensitivity {
	if retrieval.SensitivityLevel(a) <= retrieval.SensitivityLevel(b) {
		return a
	}
	return b
}

func isSensitivityAllowed(value, max schema.Sensitivity) bool {
	return retrieval.SensitivityLevel(value) >= 0 &&
		retrieval.SensitivityLevel(max) >= 0 &&
		retrieval.SensitivityLevel(value) <= retrieval.SensitivityLevel(max)
}

func listenAddrRequiresAPIKey(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}
	host = strings.Trim(host, "[]")
	if host == "" || host == "0.0.0.0" || host == "::" {
		return true
	}
	if strings.EqualFold(host, "localhost") {
		return false
	}
	ip := net.ParseIP(host)
	return ip == nil || !ip.IsLoopback()
}

func rejectUnauthenticatedPublicListen(cfg *membrane.Config) error {
	if cfg == nil || strings.TrimSpace(cfg.APIKey) != "" {
		return nil
	}
	if listenAddrRequiresAPIKey(cfg.ListenAddr) {
		return fmt.Errorf("grpc: api_key is required when listening on non-loopback address %q", cfg.ListenAddr)
	}
	return nil
}

func rejectInsecurePublicCredentials(cfg *membrane.Config) error {
	if cfg == nil || strings.TrimSpace(cfg.APIKey) == "" || cfg.AllowInsecureCredentials {
		return nil
	}
	if !listenAddrRequiresAPIKey(cfg.ListenAddr) {
		return nil
	}
	if cfg.TLSCertFile != "" && cfg.TLSKeyFile != "" {
		return nil
	}
	return fmt.Errorf(
		"grpc: refusing API-key authentication over plaintext on non-loopback address %q; configure tls_cert_file and tls_key_file or set allow_insecure_credentials for a trusted development network",
		cfg.ListenAddr,
	)
}
