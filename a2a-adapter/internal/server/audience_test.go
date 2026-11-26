package server

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/openshift/lightspeed-hub/a2a-adapter/internal/auth"
)

func audienceToken(t *testing.T, aud any) string {
	t.Helper()
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none"}`))
	payload, err := json.Marshal(map[string]any{"aud": aud})
	if err != nil {
		t.Fatal(err)
	}
	return header + "." + base64.RawURLEncoding.EncodeToString(payload) + ".c2ln"
}

func audienceGuardHandler() *Handler {
	return &Handler{
		cfg:  Config{RunTokenAudience: "praxis-spoke-gateway", ExecutionTokenAudience: "praxis-spoke-gateway-exec"},
		deps: Deps{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))},
	}
}

func TestTokenAudienceDownscopingGuard(t *testing.T) {
	h := audienceGuardHandler()
	tests := []struct {
		name    string
		check   func(string) error
		aud     any
		wantErr string
	}{
		{
			name:  "good R string audience",
			check: h.checkRunTokenAudience,
			aud:   "praxis-spoke-gateway",
		},
		{
			name:    "R containing execution audience",
			check:   h.checkRunTokenAudience,
			aud:     []string{"praxis-spoke-gateway", "praxis-spoke-gateway-exec"},
			wantErr: "Keycloak did not downscope aud",
		},
		{
			name:  "good R_exec array audience",
			check: h.checkExecutionTokenAudience,
			aud:   []string{"praxis-spoke-gateway-exec"},
		},
		{
			name:    "R_exec missing execution audience",
			check:   h.checkExecutionTokenAudience,
			aud:     "praxis-spoke-gateway",
			wantErr: "requested execution audience",
		},
		{
			name:    "R_exec containing run audience",
			check:   h.checkExecutionTokenAudience,
			aud:     []string{"praxis-spoke-gateway-exec", "praxis-spoke-gateway"},
			wantErr: "forbidden run-token audience",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			token := audienceToken(t, tc.aud)
			err := tc.check(token)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("guard error = %v, want text %q", err, tc.wantErr)
			}
			if strings.Contains(err.Error(), token) {
				t.Fatal("guard error leaked token value")
			}
		})
	}
}

func TestTokenAudiencesRejectsMalformedClaim(t *testing.T) {
	for _, token := range []string{
		audienceToken(t, nil),
		audienceToken(t, 42),
		audienceToken(t, []any{"praxis-spoke-gateway", 42}),
	} {
		if _, err := auth.TokenAudiences(token); err == nil {
			t.Errorf("TokenAudiences(%q) succeeded, want fail-closed error", token)
		}
	}
}

func TestAudienceRejectionLogsFingerprintOnly(t *testing.T) {
	var logs bytes.Buffer
	token := audienceToken(t, []string{"praxis-spoke-gateway", "praxis-spoke-gateway-exec"})
	h := &Handler{
		cfg:  Config{RunTokenAudience: "praxis-spoke-gateway", ExecutionTokenAudience: "praxis-spoke-gateway-exec"},
		deps: Deps{Logger: slog.New(slog.NewTextHandler(&logs, nil))},
	}
	if err := h.checkRunTokenAudience(token); err == nil {
		t.Fatal("execution audience on R was accepted")
	}
	if !strings.Contains(logs.String(), auth.Fingerprint(token)) {
		t.Fatalf("rejection log lacks token fingerprint: %s", logs.String())
	}
	if strings.Contains(logs.String(), token) {
		t.Fatal("rejection log contains the token value")
	}
}
