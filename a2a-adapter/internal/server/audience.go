package server

import (
	"fmt"

	"github.com/a2aproject/a2a-go/a2a"

	"github.com/openshift/lightspeed-hub/a2a-adapter/internal/auth"
)

func audienceSet(token string) (map[string]struct{}, error) {
	audiences, err := auth.TokenAudiences(token)
	if err != nil {
		return nil, err
	}
	set := make(map[string]struct{}, len(audiences))
	for _, audience := range audiences {
		set[audience] = struct{}{}
	}
	return set, nil
}

func hasAudience(audiences map[string]struct{}, audience string) bool {
	_, ok := audiences[audience]
	return ok
}

func (h *Handler) checkRunTokenAudience(token string) error {
	if h.cfg.ExecutionTokenAudience == "" {
		return nil
	}
	audiences, err := audienceSet(token)
	if err != nil {
		return h.rejectAudienceToken("run token R", token, "missing or malformed aud claim")
	}
	if hasAudience(audiences, h.cfg.ExecutionTokenAudience) {
		return h.rejectAudienceToken("run token R", token,
			fmt.Sprintf("forbidden execution audience %q is present", h.cfg.ExecutionTokenAudience))
	}
	return nil
}

func (h *Handler) checkExecutionTokenAudience(token string) error {
	if h.cfg.ExecutionTokenAudience == "" {
		return nil
	}
	audiences, err := audienceSet(token)
	if err != nil {
		return h.rejectAudienceToken("execution token R_exec", token, "missing or malformed aud claim")
	}
	if !hasAudience(audiences, h.cfg.ExecutionTokenAudience) {
		return h.rejectAudienceToken("execution token R_exec", token,
			fmt.Sprintf("requested execution audience %q is absent", h.cfg.ExecutionTokenAudience))
	}
	if hasAudience(audiences, h.cfg.RunTokenAudience) {
		return h.rejectAudienceToken("execution token R_exec", token,
			fmt.Sprintf("forbidden run-token audience %q is present", h.cfg.RunTokenAudience))
	}
	return nil
}

func (h *Handler) rejectAudienceToken(kind, token, reason string) error {
	h.deps.Logger.Warn("rejected exchanged credential audience",
		"credential_kind", kind,
		"credential_fp", auth.Fingerprint(token))
	return fmt.Errorf("%w: Keycloak did not downscope aud for %s: %s", a2a.ErrInternalError, kind, reason)
}
