// Package config loads the hub adapter's deployment configuration from
// flags (with env fallbacks for values that ride better in env, like
// secrets and the namespace).
package config

import (
	"flag"
	"fmt"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/openshift/lightspeed-hub/a2a-adapter/internal/run"
)

// requireHTTPS rejects non-https URLs on the credential-exchange endpoints.
// The issuer and token endpoint carry (and authenticate the exchange of)
// bearer tokens; a plaintext leg would expose Token A/H to anyone on the
// path. Garbage that does not parse as a URL at all is rejected with the
// same clarity.
func requireHTTPS(name, raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("%s must be a valid URL, got %q", name, raw)
	}
	if u.Scheme != "https" {
		return fmt.Errorf("%s must be an https:// URL, got %q (plaintext token endpoints would expose credentials in transit)", name, raw)
	}
	return nil
}

// Config is the fully-resolved adapter configuration.
type Config struct {
	ListenAddr string

	// TLSCertFile/TLSKeyFile enable TLS serving when BOTH are set (the
	// OpenShift service-serving cert shape: tls.crt/tls.key). Both empty
	// = plain HTTP, which is for LOCAL DEV ONLY — in production the
	// adapter serves TLS because praxis-proxy's a2aUpstream connects
	// over TLS and verifies the service-serving CA.
	TLSCertFile string
	TLSKeyFile  string

	// Namespace: the repo C operator namespace where AgenticRuns and
	// run-token Secrets live (design §8.3 / Q4).
	Namespace string

	// Keycloak settings for the adapter-fallback A→H exchange (design
	// §3.2). Client auth is EITHER secret OR SVID (socket path set).
	KeycloakIssuer        string
	KeycloakTokenEndpoint string // empty => <issuer>/protocol/openid-connect/token
	KeycloakClientID      string
	KeycloakClientSecret  string
	SVIDSocketPath        string

	// HubAudience is the audience requested on Token H.
	HubAudience string

	// RunTokenAudience, when set, makes the adapter store a RUN TOKEN R in
	// the run-token Secret instead of the caller's raw Token A: R is the
	// exchange of A for this audience (the spoke gateway's exchanger client,
	// e.g. praxis-spoke-gateway). R's lifespan is the exchange client's
	// access-token lifespan, so it can outlive a short-lived A and cover a
	// whole AgenticRun step, and it is usable only at the spoke gateway.
	// Empty (default) keeps storing Token A.
	RunTokenAudience string

	// InboundHubToken: the inbound bearer is already Token H (the Praxis hub
	// gateway exchanged A->H). The adapter uses it directly to create runs
	// and, with RunTokenAudience, exchanges it (not A) into the run token R.
	InboundHubToken bool

	// TargetHeader is the hub-leg target transport header (design §4.2).
	TargetHeader string

	// AllowedTargetsRaw is the raw --allowed-targets flag value
	// (comma-separated spoke names). Validate parses it into
	// AllowedTargets; it is kept only so flag round-trips and error
	// messages can quote what the operator actually passed.
	AllowedTargetsRaw string

	// AllowedTargets is the parsed, de-duplicated, sorted allow-list of
	// spoke names. EMPTY = no allow-list enforcement: any well-formed
	// target (per run.ResolveTarget's DNS-subdomain rule) is accepted
	// and the admission policy on the kube API remains the only target
	// gate. When non-empty, a fresh submit whose resolved target is not
	// in the list fails fast BEFORE any AgenticRun or run-token Secret
	// is created, with a status message naming the valid clusters so the
	// caller loop can self-correct on its next message.
	AllowedTargets []string

	// RunAgent is spec.analysis.agent on created runs (the CRD requires
	// the analysis step to be non-empty; "default" is the CRD's own
	// documented default agent).
	RunAgent string

	// WriterIdentity stamps a2a.lightspeed.io/written-by on run-token
	// Secrets (design §5.6 provenance).
	WriterIdentity string

	ClockSkew          time.Duration
	StreamDeadline     time.Duration
	StreamPollInterval time.Duration // retained for rollback compat; unused since M3 (watch-driven streams)

	// M3 renewal-bridge / cleanup bounds (design §6.4, §5.5).
	RenewalTimeout       time.Duration // per-pause auth-required window before the task fails
	MaxPauses            int           // total credential pauses per run before the task fails
	TerminalCleanupGrace time.Duration // grace after terminal phase before the run-token Secret is deleted
	SweepInterval        time.Duration // watcher sweeper cadence (deadlines + cleanup due-times)

	// StartupRetryTimeout bounds how long startup (the recovery LIST +
	// informer start) retries TRANSIENT API/storage errors — the CRD
	// sync-wave race ("storage is (re)initializing") and flapping etcd —
	// with exponential backoff, instead of crash-looping. 0 = fail fast
	// (the pre-retry behavior, for tests). Non-transient errors (RBAC,
	// bad config) always exit immediately regardless of this value.
	StartupRetryTimeout time.Duration

	// Agent card fields (design §2.1).
	AgentName            string
	AgentDescription     string
	AgentURL             string
	AgentVersion         string
	AgentProtocolVersion string
	SkillID              string
	SkillName            string
	SkillDescription     string
	SkillTags            []string
}

// AddFlags registers the flags on the given FlagSet.
func (c *Config) AddFlags(fs *flag.FlagSet) {
	fs.StringVar(&c.ListenAddr, "listen", ":8443", "HTTP listen address (internal-only Service)")
	fs.StringVar(&c.TLSCertFile, "tls-cert-file", "", "TLS serving certificate, PEM (empty = plain HTTP, local dev only). Production must set both --tls-cert-file and --tls-key-file — typically the OpenShift service-serving cert's tls.crt (Service annotation service.beta.openshift.io/serving-cert-secret-name)")
	fs.StringVar(&c.TLSKeyFile, "tls-key-file", "", "TLS serving key, PEM (empty = plain HTTP, local dev only). Must be set together with --tls-cert-file — typically the service-serving cert's tls.key")
	fs.StringVar(&c.Namespace, "namespace", envOr("NAMESPACE", ""), "namespace for AgenticRuns and run-token Secrets (env NAMESPACE; defaults to the pod's namespace)")
	fs.StringVar(&c.KeycloakIssuer, "keycloak-issuer", "", "Keycloak realm issuer URL (also the SVID client-assertion audience)")
	fs.StringVar(&c.KeycloakTokenEndpoint, "keycloak-token-endpoint", "", "full token endpoint URL (default: <issuer>/protocol/openid-connect/token)")
	fs.StringVar(&c.KeycloakClientID, "keycloak-client-id", "", "confidential exchange client id (secret mode; leave empty in SVID mode)")
	fs.StringVar(&c.KeycloakClientSecret, "keycloak-client-secret", envOr("KEYCLOAK_CLIENT_SECRET", ""), "confidential exchange client secret (env KEYCLOAK_CLIENT_SECRET)")
	fs.StringVar(&c.SVIDSocketPath, "svid-socket", envOr("SPIFFE_SOCKET", ""), "SPIFFE Workload API unix socket (SVID client-assertion mode; env SPIFFE_SOCKET)")
	fs.StringVar(&c.HubAudience, "hub-audience", "", "requested audience for the exchanged hub token (Token H)")
	fs.StringVar(&c.RunTokenAudience, "run-token-audience", "", "when set, store a run token R (exchange of the inbound token for this audience, e.g. the spoke gateway's exchanger client) in the run-token Secret instead of the inbound token; empty = store it as-is")
	fs.BoolVar(&c.InboundHubToken, "inbound-hub-token", false, "the inbound bearer is already Token H (exchanged by the Praxis hub gateway): use it directly for kube calls, no A->H exchange in the adapter")
	fs.StringVar(&c.TargetHeader, "target-header", "X-Hub-Cluster", "inbound transport header carrying the target cluster (design §4.2)")
	fs.StringVar(&c.AllowedTargetsRaw, "allowed-targets", "", "comma-separated allow-list of spoke (target cluster) names, e.g. east,west. Empty (default) = no allow-list enforcement: any well-formed target is accepted and the admission policy remains the hard gate; when set, a fresh submit with an unlisted target fails fast before any AgenticRun or run-token Secret is created")
	fs.StringVar(&c.RunAgent, "run-agent", "default", "spec.analysis.agent on created AgenticRuns")
	fs.StringVar(&c.WriterIdentity, "writer-identity", "", "value for the a2a.lightspeed.io/written-by provenance annotation (default: adapter-sa)")
	fs.DurationVar(&c.ClockSkew, "clock-skew", 30*time.Second, "grace after exp in the pre-start Token A expiry check")
	fs.DurationVar(&c.StreamDeadline, "stream-deadline", 30*time.Minute, "wall-clock bound on message/stream (design §2.3; renewal pauses do not count against it)")
	fs.DurationVar(&c.StreamPollInterval, "stream-poll-interval", 5*time.Second, "deprecated: streams are watch-driven since M3; kept for config compatibility, unused")
	fs.DurationVar(&c.RenewalTimeout, "renewal-timeout", 900*time.Second, "how long a task may stay auth-required before failing (design §6.4; aligned with MAS's worker-side 900 s bound)")
	fs.IntVar(&c.MaxPauses, "max-credential-pauses", 3, "total credential pauses per run before the task fails (design §6.4)")
	fs.DurationVar(&c.TerminalCleanupGrace, "terminal-cleanup-grace", time.Minute, "grace after a terminal phase before ls-run-token-{runUID} is deleted (design §5.5)")
	fs.DurationVar(&c.SweepInterval, "sweep-interval", 5*time.Second, "watcher sweeper cadence for pause deadlines and Secret cleanup")
	fs.DurationVar(&c.StartupRetryTimeout, "startup-retry-timeout", 5*time.Minute, "how long startup (recovery LIST + informer start) retries transient API/storage errors (CRD storage still initializing, 503/504, timeouts, connection refused, flapping etcd) with exponential backoff before exiting; 0 = fail fast")

	fs.StringVar(&c.AgentName, "card-name", "lightspeed-hub", "agent card name")
	fs.StringVar(&c.AgentDescription, "card-description", "Red Hat cluster investigation via lightspeed-hub", "agent card description")
	fs.StringVar(&c.AgentURL, "card-url", "", "agent card URL (the Praxis-fronted origin callers use)")
	fs.StringVar(&c.AgentVersion, "card-version", "0.1.0", "agent card version")
	fs.StringVar(&c.AgentProtocolVersion, "card-protocol-version", "0.3.0", "A2A protocol version advertised in the card")
	fs.StringVar(&c.SkillID, "card-skill-id", "cluster-investigation", "agent card skill id")
	fs.StringVar(&c.SkillName, "card-skill-name", "Red Hat cluster investigation", "agent card skill name")
	fs.StringVar(&c.SkillDescription, "card-skill-description", "Investigate a Red Hat cluster (or spoke) via the lightspeed-hub agentic stack: analysis-only AgenticRuns with per-run requester credentials.", "agent card skill description")
}

// Validate fills defaults and checks required values.
func (c *Config) Validate() error {
	if c.Namespace == "" {
		ns, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
		if err != nil {
			return fmt.Errorf("namespace is required (flag --namespace or env NAMESPACE)")
		}
		c.Namespace = strings.TrimSpace(string(ns))
	}
	if c.KeycloakIssuer == "" {
		return fmt.Errorf("--keycloak-issuer is required")
	}
	if err := requireHTTPS("--keycloak-issuer", c.KeycloakIssuer); err != nil {
		return err
	}
	if c.KeycloakTokenEndpoint != "" {
		if err := requireHTTPS("--keycloak-token-endpoint", c.KeycloakTokenEndpoint); err != nil {
			return err
		}
	}
	if c.HubAudience == "" {
		return fmt.Errorf("--hub-audience is required")
	}
	if c.SVIDSocketPath == "" && (c.KeycloakClientID == "" || c.KeycloakClientSecret == "") {
		return fmt.Errorf("exchange client auth requires either --keycloak-client-id+secret or --svid-socket")
	}
	if (c.TLSCertFile == "") != (c.TLSKeyFile == "") {
		return fmt.Errorf("--tls-cert-file and --tls-key-file must be set together (both set = TLS serving, both empty = plain HTTP for local dev only — production must set both)")
	}
	if c.WriterIdentity == "" {
		c.WriterIdentity = "adapter-sa"
	}
	if c.StartupRetryTimeout < 0 {
		return fmt.Errorf("--startup-retry-timeout must be >= 0 (0 = fail fast)")
	}

	// Parse --allowed-targets into the sorted, de-duplicated allow-list.
	// Entries must be single DNS labels (the shape a spoke name can
	// take): a typo like "east west" or "East" must fail at startup, not
	// silently never match. EMPTY stays empty — no enforcement, any
	// well-formed target accepted (admission policy is the hard gate).
	c.AllowedTargets = nil
	if strings.TrimSpace(c.AllowedTargetsRaw) != "" {
		seen := map[string]bool{}
		for _, entry := range strings.Split(c.AllowedTargetsRaw, ",") {
			entry = strings.TrimSpace(entry)
			if entry == "" {
				continue // tolerate trailing/doubled commas
			}
			if !run.IsDNSLabel(entry) {
				return fmt.Errorf("--allowed-targets entry %q is not a valid DNS label (lowercase alphanumeric, internal hyphens, no dots)", entry)
			}
			if !seen[entry] {
				seen[entry] = true
				c.AllowedTargets = append(c.AllowedTargets, entry)
			}
		}
		if len(c.AllowedTargets) == 0 {
			return fmt.Errorf("--allowed-targets was set but contained no names")
		}
		sort.Strings(c.AllowedTargets)
	}
	return nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
