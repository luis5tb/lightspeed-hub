// Command hub-adapter is the lightspeed-hub A2A front door (design:
// docs/LIGHTSPEED_HUB_ADAPTER_DESIGN.md, Task B Phase 1, milestones M1-M3).
//
// It terminates A2A JSON-RPC (message/send, message/stream, tasks/get,
// tasks/cancel, tasks/resubscribe) behind Praxis, maps messages onto
// AgenticRun CRs as the caller (Token H), writes the run-token Secrets
// that repo C's gateway credential mode consumes, watches AgenticRun /
// Result / Approval CRs to push task status, bridges the
// CredentialsExpired pause onto A2A auth-required with bounded renewal
// waits (design §6), and cleans run-token Secrets after terminal runs
// (§5.5). M4 (chart, admission webhook) and M5 (live acceptance matrix)
// are not in this binary.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/a2aproject/a2a-go/a2asrv"
	agenticv1alpha1 "github.com/openshift/lightspeed-agentic-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"

	"github.com/openshift/lightspeed-hub/a2a-adapter/internal/auth"
	"github.com/openshift/lightspeed-hub/a2a-adapter/internal/config"
	"github.com/openshift/lightspeed-hub/a2a-adapter/internal/run"
	"github.com/openshift/lightspeed-hub/a2a-adapter/internal/server"
	"github.com/openshift/lightspeed-hub/a2a-adapter/internal/store"
	kubeWatch "github.com/openshift/lightspeed-hub/a2a-adapter/internal/watch"
)

func main() {
	if err := mainRun(); err != nil {
		slog.Error("hub-adapter exiting", "error", err)
		os.Exit(1)
	}
}

func mainRun() error {
	cfg := &config.Config{}
	cfg.AddFlags(flag.CommandLine)
	flag.Parse()
	if err := cfg.Validate(); err != nil {
		return err
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	// Startup assertion (mirrors the operator's cmd/fips.go): refuse to run
	// without FIPS-approved crypto when the host is in FIPS mode.
	checkFIPSMode(logger.Info)

	// Kubernetes clients. The adapter SA reads runs/secrets; run CREATION
	// happens as the caller (Token H) via the per-request factory.
	restCfg, err := rest.InClusterConfig()
	if err != nil {
		return err
	}
	scheme := runtime.NewScheme()
	if err := agenticv1alpha1.AddToScheme(scheme); err != nil {
		return err
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		return err
	}

	kube, err := run.NewCtrlClient(restCfg, scheme)
	if err != nil {
		return err
	}

	// M1 auth wiring: adapter-fallback placement (design §3.2). Praxis
	// validates Token A and forwards it; the adapter exchanges A→H itself.
	// The Praxis-side placement (preferred, gated on design §10 Q2/Q3)
	// would swap HeaderSource for the protected-channel source.
	exchangeCfg := auth.ExchangerConfig{
		IssuerURL:     cfg.KeycloakIssuer,
		TokenEndpoint: cfg.KeycloakTokenEndpoint,
		ClientID:      cfg.KeycloakClientID,
		ClientSecret:  cfg.KeycloakClientSecret,
		Audience:      cfg.HubAudience,
		Logger:        logger,
	}
	if cfg.SVIDSocketPath != "" {
		exchangeCfg.SVIDSource = &auth.WorkloadAPISVIDSource{SocketPath: cfg.SVIDSocketPath}
	}
	exchanger, err := auth.NewTokenExchanger(exchangeCfg)
	if err != nil {
		return err
	}
	// Optional run-token exchanger (A -> R): same client and auth as the
	// hub exchange, different requested audience. Nil = store Token A.
	var runTokenExchanger auth.Exchanger
	if cfg.RunTokenAudience != "" {
		runCfg := exchangeCfg
		runCfg.Audience = cfg.RunTokenAudience
		rte, err := auth.NewTokenExchanger(runCfg)
		if err != nil {
			return err
		}
		runTokenExchanger = rte
	}

	handler, err := server.New(server.Config{
		Namespace:            cfg.Namespace,
		TargetHeader:         cfg.TargetHeader,
		AllowedTargets:       cfg.AllowedTargets,
		ClockSkew:            cfg.ClockSkew,
		StreamDeadline:       cfg.StreamDeadline,
		RenewalTimeout:       cfg.RenewalTimeout,
		MaxPauses:            cfg.MaxPauses,
		TerminalCleanupGrace: cfg.TerminalCleanupGrace,
		SweepInterval:        cfg.SweepInterval,
	}, server.Deps{
		Store:             store.New(),
		Credentials:       credentialSource(cfg.InboundHubToken),
		Exchanger:         exchanger,
		RunTokenExchanger: runTokenExchanger,
		Readers:           kube,
		Results:           kube,
		Submitters:        run.NewCtrlRunClientFactory(restCfg, scheme),
		Runs: &run.Creator{
			Namespace: cfg.Namespace,
			AgentName: cfg.RunAgent,
			WrittenBy: cfg.WriterIdentity,
		},
		Tokens: &run.TokenWriter{
			Secrets:   kube,
			Namespace: cfg.Namespace,
			Identity:  cfg.WriterIdentity,
		},
		Logger: logger,
	})
	if err != nil {
		return err
	}

	// Restart recovery (design §2.3): rebuild task records from
	// AgenticRun annotations before serving traffic. The M3 informers
	// rehydrate anything this list misses as their caches sync.
	//
	// Startup retry: the recovery LIST (and the informer start below)
	// can race the API server finishing CRD storage establishment —
	// the CRDs apply at Argo sync-wave -1 and this Deployment starts at
	// wave 0 — surfacing as "storage is (re)initializing" (503), or as
	// 503/504/timeouts when etcd flaps. Those are transient: retry with
	// exponential backoff for --startup-retry-timeout (default 5 m)
	// instead of crash-looping. Non-transient errors (RBAC, invalid
	// config) exit immediately.
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	startup := func() error {
		if err := handler.Recover(ctx); err != nil {
			return err
		}

		// M3 watch wiring (design §7.1): informers on AgenticRun +
		// Result CRs + AgenticRunApproval feed the watcher, which
		// projects changes into the task store; the sweeper enforces
		// the §6.4 pause deadlines and the §5.5 terminal-run Secret
		// cleanup.
		watcher, err := handler.NewWatcher()
		if err != nil {
			return err
		}
		if err := kubeWatch.Start(ctx, restCfg, scheme, cfg.Namespace, watcher, logger); err != nil {
			return fmt.Errorf("start informers: %w", err)
		}
		go watcher.Start(ctx)
		return nil
	}
	if err := server.RetryTransient(ctx, server.RetryOptions{
		Timeout: cfg.StartupRetryTimeout,
		Logger:  logger,
	}, startup); err != nil {
		return err
	}

	// HTTP surface: the agent card plus the JSON-RPC/SSE A2A endpoint.
	// TLS serving is configured via --tls-cert-file/--tls-key-file (both
	// required together): production serves TLS from the OpenShift
	// service-serving cert because praxis-proxy's a2aUpstream hop
	// connects over TLS and verifies the service-serving CA; plain HTTP
	// (both empty) is the local-dev mode.
	mux := http.NewServeMux()
	// /healthz: unauthenticated liveness/readiness — safe because the
	// Service is internal-only behind the NetworkPolicy (Praxis-namespace
	// ingress); probes must not need a token.
	mux.Handle("/healthz", server.HealthHandler())
	cardHandler := server.ServeAgentCard(server.AgentCard(server.CardConfig{
		Name:             cfg.AgentName,
		Description:      cfg.AgentDescription,
		URL:              cfg.AgentURL,
		Version:          cfg.AgentVersion,
		ProtocolVersion:  cfg.AgentProtocolVersion,
		SkillID:          cfg.SkillID,
		SkillName:        cfg.SkillName,
		SkillDescription: cfg.SkillDescription,
		Tags:             cfg.SkillTags,
	}))
	// Unauthenticated, like /healthz: the card is public discovery data.
	// See server.AgentCardPaths for why it is served under /a2a too.
	for _, p := range server.AgentCardPaths {
		mux.Handle(p, cardHandler)
	}
	mux.Handle("/", server.WithRequestCapture(a2asrv.NewJSONRPCHandler(handler, a2asrv.WithKeepAlive(30*time.Second))))

	srv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	// Bind before the signal loop so a port conflict is a startup
	// error, not a goroutine error after "listening" was logged.
	ln, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		return err
	}

	errCh := make(chan error, 1)
	go func() {
		if cfg.TLSCertFile != "" && cfg.TLSKeyFile != "" {
			// TLS serving (production shape): praxis-proxy's
			// a2aUpstream connects over TLS and verifies the
			// service-serving CA. Paths only are logged — never
			// key material.
			logger.Info("hub-adapter listening (TLS)", "addr", cfg.ListenAddr, "namespace", cfg.Namespace, "tls-cert-file", cfg.TLSCertFile)
		} else {
			logger.Info("hub-adapter listening (plain HTTP — local dev only; production must set --tls-cert-file/--tls-key-file)", "addr", cfg.ListenAddr, "namespace", cfg.Namespace)
		}
		errCh <- serve(ln, srv, cfg)
	}()

	select {
	case <-ctx.Done():
		logger.Info("shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	case err := <-errCh:
		return err
	}
}

// credentialSource picks how the inbound bearer is read: Token A (the
// adapter exchanges A->H itself) or, with --inbound-hub-token, Token H
// already exchanged by the Praxis hub gateway.
func credentialSource(inboundHubToken bool) auth.CredentialSource {
	if inboundHubToken {
		return auth.HubTokenHeaderSource{}
	}
	return auth.HeaderSource{}
}
