package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/openshift/lightspeed-hub/a2a-adapter/internal/config"
	"github.com/openshift/lightspeed-hub/a2a-adapter/internal/server"
)

// writeSelfSignedCert mints a throwaway self-signed certificate + key into
// dir with the OpenShift service-serving cert's file shape (tls.crt /
// tls.key) and returns their paths. Test fixture only — the real deployment
// gets its cert from the service-ca operator via the Service annotation
// service.beta.openshift.io/serving-cert-secret-name.
func writeSelfSignedCert(t *testing.T, dir string) (certPath, keyPath string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "a2a-adapter-test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:     []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	certPath = filepath.Join(dir, "tls.crt")
	keyPath = filepath.Join(dir, "tls.key")
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	if err := os.WriteFile(certPath, certPEM, 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}
	return certPath, keyPath
}

// newTestMux is the production HTTP surface in miniature: /healthz, the
// same handler main() serves there.
func newTestMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("/healthz", server.HealthHandler())
	return mux
}

// TestServeTLS pins the production shape: with both --tls-cert-file and
// --tls-key-file set, serve() must speak TLS (an https client gets /healthz
// 200; a plain-HTTP client does NOT).
func TestServeTLS(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath := writeSelfSignedCert(t, dir)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	srv := &http.Server{Handler: newTestMux()}
	cfg := &config.Config{TLSCertFile: certPath, TLSKeyFile: keyPath}

	errCh := make(chan error, 1)
	go func() { errCh <- serve(ln, srv, cfg) }()
	defer srv.Close()

	client := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			// The fixture is self-signed; the point here is TLS
			// serving, not chain validation.
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec
		},
	}
	resp, err := client.Get("https://" + ln.Addr().String() + "/healthz")
	if err != nil {
		t.Fatalf("https request against TLS-serving adapter: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("https /healthz status = %d, want 200", resp.StatusCode)
	}

	// Plain HTTP against the TLS listener must NOT be served: Go's TLS
	// server answers the stray plaintext request with 400 ("client sent
	// an HTTP request to an HTTPS server") rather than a healthz 200.
	if resp, err := http.Get("http://" + ln.Addr().String() + "/healthz"); err == nil {
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			t.Fatal("plain-HTTP request against the TLS listener got /healthz 200")
		}
	}

	select {
	case err := <-errCh:
		t.Fatalf("serve returned early: %v", err)
	default:
	}
}

// TestServePlainHTTP pins the local-dev shape: both TLS flags empty means
// plain HTTP serving, unchanged from the pre-TLS behavior.
func TestServePlainHTTP(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	srv := &http.Server{Handler: newTestMux()}
	cfg := &config.Config{}

	errCh := make(chan error, 1)
	go func() { errCh <- serve(ln, srv, cfg) }()
	defer srv.Close()

	resp, err := http.Get("http://" + ln.Addr().String() + "/healthz")
	if err != nil {
		t.Fatalf("http request against plain-HTTP adapter: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("http /healthz status = %d, want 200", resp.StatusCode)
	}

	select {
	case err := <-errCh:
		t.Fatalf("serve returned early: %v", err)
	default:
	}
}
