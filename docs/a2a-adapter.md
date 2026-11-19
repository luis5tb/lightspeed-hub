# The hub A2A adapter (optional component)

The **hub A2A adapter** is an optional component of lightspeed-hub: a Go
service that gives the hub an [A2A protocol](https://a2a-protocol.org/)
front door and bridges A2A task semantics onto the `AgenticRun` machinery
of [lightspeed-agentic-operator](https://github.com/openshift/lightspeed-agentic-operator)
(branch `a2a-support`). It accepts A2A `message/send` / `message/stream` /
`tasks/get` / `tasks/cancel` / `tasks/resubscribe`, maps messages onto
`AgenticRun` CRs **as the caller**, owns the run-token Secret lifecycle
(`ls-run-token-{runUID}`) that repo C's gateway credential mode consumes,
watches AgenticRun/Result/Approval CRs to push task status, and bridges
the `CredentialsExpired` pause onto A2A `auth-required` with bounded
renewal waits.

**Authoritative design**: `docs/LIGHTSPEED_HUB_ADAPTER_DESIGN.md` in the
[multicloud-gitops](https://github.com/luis5tb/multicloud-gitops) repo
(branch `rca-mao-praxis-hub`). That document is the source of truth for
the token model, placement, and the §-references used throughout the
adapter's code comments and README.

## Token model (summary)

- **Token A** — the caller's original token (no groups, by contract).
- **Token H** — hub-audience exchange of A (RFC 8693 token exchange
  against Keycloak); the adapter impersonates the requester with H when
  creating AgenticRuns and deciding approvals, so repo C's admission
  webhooks see the true requester/approver.
- **Token U** — spoke-audience exchange of A, minted per call by the
  hub-hosted spoke-facing PPE (NOT by this adapter in Phase 1).

The adapter's own Keycloak client auth for the A→H exchange is either a
confidential client secret or a SPIFFE JWT-SVID client assertion
(`SPIFFE_SOCKET` / `--svid-socket`).

## Placement: internal-only, behind Praxis

The adapter's Service is internal-only (ClusterIP, no Route, ever) with a
NetworkPolicy admitting ingress solely from the Praxis policy gateway's
namespace. Praxis is the caller allow-list; the adapter trusts the token
Praxis forwards. The adapter itself serves **TLS on port 8443** from an
OpenShift service-serving cert (the Service's
`service.beta.openshift.io/serving-cert-secret-name` annotation mints the
Secret the adapter mounts for `--tls-cert-file`/`--tls-key-file`), because
praxis-proxy's `a2aUpstream` hop connects over TLS and verifies the
service-serving CA; plain HTTP (both TLS flags empty) is a local-dev
shape only. See the design doc §1.

**Placement note for the gitops pattern:** when the adapter is deployed
via THIS component, the Service is named `lightspeed-hub-a2a-adapter`
(the kustomize `namePrefix`) in the `openshift-lightspeed` namespace,
serving TLS on port 8443 — set the gitops praxis-proxy chart's
`a2aUpstream.service=lightspeed-hub-a2a-adapter`,
`a2aUpstream.namespace=openshift-lightspeed` and `a2aUpstream.port=8443`
accordingly, instead of that chart's defaults (which point at the
lightspeed-hub Helm chart's own Service `lightspeed-hub` in the
`lightspeed-hub` namespace).

## Building with and without the A2A surface

The adapter lives in its own **nested Go module** (`a2a-adapter/`) because
its dependency set (k8s 0.35.x / controller-runtime 0.23.x, the A2A SDK,
go-spiffe, and a `replace` on the unpublished
`lightspeed-agentic-operator/api` module pinned to the sibling
`a2a-support` checkout) diverges from the operator's.

- **Without A2A (default)**: nothing to do. `make build` / `make test` /
  `go vet ./...` on the root module do not include the adapter; the stock
  `config/default` deployment does not contain it.
- **With A2A**, from the repo root:

```bash
make build-a2a-adapter    # binary into bin/hub-a2a-adapter
make test-a2a-adapter     # vet + unit tests (with coverage)
make image-a2a-adapter    # container image (vendored staging build context,
                          # --target a2a-adapter of the root Dockerfile)
```

## Deploying

`config/a2a-adapter/` is a kustomize component kept out of
`config/default`:

```bash
make deploy-a2a-adapter
```

Required per-cluster patches (Keycloak issuer, hub audience, exchange
client secret or SVID socket, Praxis namespace for the NetworkPolicy) are
documented in `config/a2a-adapter/README.md`. The fully-wired deployment
for the Red Hat Validated Pattern lives in the gitops repo's
`charts/all/lightspeed-hub` Helm chart (disabled by default there too).

## Target cluster validation (who rejects a bad spoke, in order of speed)

1. **MAS orchestrator prompt (assist)** — the caller-facing prompt carries
   the closed list of cluster names, so the right target is usually chosen
   in the first place. Advisory: prompts are not enforcement.
2. **Adapter fail-fast, `--allowed-targets`** — comma-separated spoke
   names (e.g. `east,west`); empty (default) means no enforcement: any
   well-formed target is accepted and the admission policy remains the
   only target gate. When set, a fresh submit whose resolved target is not
   listed fails BEFORE any AgenticRun or run-token Secret is created: the
   task parks adapter-terminally failed (the same pre-create convention as
   the expired-token path) with a self-correcting status message, e.g.
   `unknown target "est"; available clusters: east, west` — the caller's
   retry loop re-sends on the same task with a corrected target and a new
   messageId, and the parked original question is the one submitted. That
   retry MUST carry the corrected target: a target-less retry on a parked
   record re-fails with the same message (the park is adapter-terminal,
   not resumed).
   Entries must be single DNS labels; malformed entries fail adapter
   startup. Validation layer, not a security boundary.
3. **Admission policy (hard gate)** — the kube API admission policy on
   AgenticRun creation (design §4.3) actually enforces which targets a
   caller may run against. The allow-list just converts "run dies at
   admission" into a fast, actionable task failure.
4. **Praxis router allow-list** — the Praxis policy gateway in front of
   the adapter routes only approved callers/paths to it at all: the
   outermost ring of the same story.

Hub-local runs (no target given) are not spoke targets and are not
filtered by the allow-list.

## Startup resilience (`--startup-retry-timeout`)

**Live incident (the motivation):** the adapter crash-looped fatally
with `hub-adapter exiting: list agenticruns for recovery: storage is
(re)initializing`. The AgenticRun CRDs are applied at Argo sync-wave -1
while the adapter Deployment starts at wave 0, so the startup recovery
LIST can race the API server finishing CRD storage establishment; the
same 503 appears when etcd flaps. Both are transient — exiting only
widens the race window via CrashLoopBackOff.

Startup (the recovery LIST plus the informer start) retries transient
errors — `storage is (re)initializing`, HTTP 503/504, server timeouts,
connection refused, deadline exceeded — with exponential backoff (2 s,
doubling, capped at 30 s, jittered) for up to `--startup-retry-timeout`
(default 5 m; `0` = fail fast). Each retry logs the error, attempt
count, and next delay. Non-transient errors (Forbidden, NotFound,
invalid config) exit immediately; on exhaustion the process exits with
the original error. The HTTP listener still starts only after
successful recovery. Details: `a2a-adapter/README.md` → "Startup
resilience".

## Where things are

| What | Where |
| --- | --- |
| Adapter source | `a2a-adapter/` (nested module; `cmd/hub-a2a-adapter/`, `internal/{auth,config,run,server,store,watch}/`) |
| Adapter README (built surface, decisions, security invariants) | `a2a-adapter/README.md` |
| Deployment component | `config/a2a-adapter/` |
| Container image | `Dockerfile`, target `a2a-adapter` (UBI9 go-toolset, `strictfipsruntime`, mirroring the operator image) |
| Design doc (authoritative) | gitops repo, `docs/LIGHTSPEED_HUB_ADAPTER_DESIGN.md` |
