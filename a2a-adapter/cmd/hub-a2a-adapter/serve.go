package main

import (
	"net"
	"net/http"

	"github.com/openshift/lightspeed-hub/a2a-adapter/internal/config"
)

// serve serves srv on ln, wrapping it in TLS when both --tls-cert-file and
// --tls-key-file are set (the production shape: praxis-proxy's a2aUpstream
// hop connects over TLS and verifies the OpenShift service-serving CA) and
// serving plain HTTP otherwise (local dev only).
func serve(ln net.Listener, srv *http.Server, cfg *config.Config) error {
	if cfg.TLSCertFile != "" && cfg.TLSKeyFile != "" {
		return srv.ServeTLS(ln, cfg.TLSCertFile, cfg.TLSKeyFile)
	}
	return srv.Serve(ln)
}
