package auth

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http/httptest"
	"testing"
)

func TestHubTokenHeaderSourceUsesInboundAsTokenH(t *testing.T) {
	tok := hubTestToken(t)
	r := httptest.NewRequest("POST", "/", nil)
	r.Header.Set("Authorization", "Bearer "+tok)
	cred, err := HubTokenHeaderSource{}.Credential(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	if cred.TokenH != tok || cred.TokenA != tok {
		t.Fatalf("TokenH/TokenA must both be the inbound Token H")
	}
	if cred.Subject != "user-1" {
		t.Errorf("subject = %q", cred.Subject)
	}
	// Plain HeaderSource leaves TokenH empty (the adapter exchanges A->H).
	plain, err := HeaderSource{}.Credential(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	if plain.TokenH != "" {
		t.Errorf("HeaderSource must not set TokenH")
	}
}

func hubTestToken(t *testing.T) string {
	t.Helper()
	enc := func(v map[string]any) string {
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		return base64.RawURLEncoding.EncodeToString(b)
	}
	return enc(map[string]any{"alg": "none"}) + "." + enc(map[string]any{"sub": "user-1", "iss": "https://kc.example.test/realms/r", "aud": "lightspeed-hub-a2a"}) + ".sig"
}
