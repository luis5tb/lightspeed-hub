# hub A2A adapter (optional component of lightspeed-hub)

The lightspeed-hub **A2A front door** (Task B, Phase 1): a net-new Go
service that gives lightspeed-hub (this repo) an A2A protocol surface and
bridges A2A task semantics onto the `AgenticRun` machinery of
lightspeed-agentic-operator (branch `a2a-support`). It is an **optional
component** of this repo: it lives in its own nested Go module
(`a2a-adapter/`) with its own image and deployment component, so the stock
operator build and `config/default` deployment are completely unaffected by
it — "building lightspeed-hub without the A2A surface" is simply not
building/deploying the `*-a2a-adapter` targets.

Authoritative design: `docs/LIGHTSPEED_HUB_ADAPTER_DESIGN.md` in the
**multicloud-gitops** repo (branch `rca-mao-praxis-hub`) — the token model
(Token A = caller's original token, Token H = hub-audience exchange of A,
Token U = spoke-audience exchange minted per call by the spoke-facing PPE),
the placement decision (internal-only Service behind the Praxis policy
gateway), and the §-references used throughout this README live there.
This README covers what is **built** (M1+M2+M3), the decisions taken where
the design left freedom, and what is explicitly deferred to M4/M5.

## What is here (M1+M2+M3)

- **A2A server**: `message/send`, `message/stream` (SSE, push-driven),
  `tasks/get`, `tasks/cancel`, `tasks/resubscribe`, and
  `/.well-known/agent-card.json`.
- **ID mapping** (design §2.2): contextId/taskId/messageId records;
  deterministic AgenticRun naming as the REJECT_DUPLICATE substitute;
  duplicate messageId returns the existing record without re-submitting
  (including duplicate renewals and duplicate approval decisions);
  restart recovery rehydrates task records from AgenticRun annotations.
- **Adapter-fallback exchange** (design §3.2): RFC 8693 A→H against
  Keycloak, in Go, as plain HTTP form POSTs — a port of the external
  a2a-split fork's `KeycloakTokenExchanger` logic. Client auth is either a
  confidential-client secret or a SPIFFE JWT-SVID client assertion
  (audience = realm issuer; `client_id` omitted in SVID mode).
- **Protected Token A handoff interface** (design §3.3):
  `auth.CredentialSource` with `HeaderSource` (adapter-fallback) and a
  documented placeholder for the Praxis-side protected channel.
- **Message → CR mapping** (design §4): every run has analysis, execution,
  and verification steps (same configured `--run-agent`); text parts →
  `spec.request`, principal annotation, deterministic target rules (§4.2:
  structured data only, duplicates/conflicts rejected), `revisionFeedback`
  as the `revise` DataPart follow-up.
- **Run-token Secret writer** (design §5): repo C's exact contract
  (`ls-run-token-{runUID}`, key `token-a`, labels
  `hub.openshift.io/agentic-run` + `hub.openshift.io/spoke-cluster`
  (truncated with repo C's exact truncation function), non-empty value,
  provenance annotation, renewal preserves labels, foreign secrets are
  overwritten not consumed). **M3 adds deletion** (§5.5, see below).
- **Pre-start expiry check** (design §6.2, write side): Token A's `exp` is
  checked locally (unverified decode — cheap pre-check; authoritative
  validation is Praxis/Keycloak's) before any CR/Secret is written; an
  expired A parks the task in `auth-required` with **no run created** and
  the §6.4 pause clock running. A dead renewal token on an existing run is
  never written either — the task stays wherever it was.
- **Watches** (design §7.1, M3): controller-runtime informers on
  AgenticRun, the four Result CR kinds, and AgenticRunApproval in the run
  namespace (adapter SA), feeding a watch controller that projects CR
  state into the task store; `message/stream` and `tasks/resubscribe` wait
  on per-task store signals — **push, not poll** (M2's CR polling is
  gone). Watch drop/reconnect is the informer's relist; projections are
  idempotent and the store dedupes unchanged projections, so a reconnect
  neither duplicates nor loses task updates (tested).
- **Phase mapping**: Pending→submitted;
  Analyzing/Executing/Verifying/Escalating→working; Proposed→
  **input-required** (human TextPart + `approvalRequest` DataPart below);
  Completed→completed; Failed/Denied/Escalated→failed (condition
  reason/message surfaced), except Execution denial after successful
  analysis, which completes with the analysis artifact; EmergencyStopped→canceled;
  Stopping→canceled; the auth-required pause is a record-side overlay on
  the existing task (CredentialsExpired does not participate in
  DerivePhase, per repo C's contract). Artifacts: one per step kind from
  the newest `Succeeded` Result CR referenced in `status.steps`, ID
  derived from the Result CR's UID (stable across restarts), text +
  structured DataPart payload; token usage aggregated into task metadata
  `tokenUsage` when Result CRs expose it.
- **Approval surface** (design §7.1): a pending approval surfaces
  `input-required` naming the pending stage (derived from the run's
  AgenticRunApproval: first workflow stage absent from `spec.stages`). The
  status message has a human TextPart and a DataPart carrying the
  `approvalRequest` contract below. `message/send` returns this task
  immediately. `message/stream` emits the input-required status update with
  `Final: true` and ends (the task remains resumable). A decision follow-up
  on `message/stream` waits past the unchanged Proposed phase and streams the
  continued run until terminal or another checkpoint. Auth-required retains
  its existing renewal-bound stream behavior. The decision is a DataPart on
  the same task, new messageId:
  `{"approve":{"stage":"Execution","option":0}}` or
  `{"reject":{"stage":"..."}}` — forwarded to the AgenticRunApproval CR
  **as the caller** (Token H), so repo C's mutating admission webhook
  stamps the true approver. Approval, rejection, and revision commands are
  mutually exclusive within one message and validated before writes. Idempotent
  replays are per stage; conflicting re-decisions are rejected without changing
  credentials or approval state.
- **Approval-gated workflow**: cluster policy is expected to configure
  Analysis=Automatic, Execution=Manual, Verification=Automatic, and
  Escalation=Automatic. The adapter always creates all three run steps;
  the operator parks at Proposed until Execution receives an approval
  stage. The cross-service request shape is:

  ```json
  {"approvalRequest":{"stage":"Execution","run":"a2a-...","targetCluster":"east","options":[{"index":0,"number":1,"title":"Restart the service","summary":"Restart the workload safely."}],"decisions":["approve","reject"]}}
  ```

  Option `index` is the operator's zero-based ExecutionApproval index;
  `number` is the corresponding human one-based choice. Options are read in
  order from the newest successful `AnalysisResult.Status.Options`; `title`
  and `summary` map directly from each option, with `summary` truncated to
  500 Unicode runes. An unavailable or unreadable AnalysisResult produces
  an empty options list. Approval follow-ups carry a DataPart on the same
  task with a new messageId, e.g. `{"approve":{"stage":"Execution","option":0}}`
  or `{"reject":{"stage":"Execution"}}`. The option value is the
  zero-based `index`, not the displayed `number`.
- **Per-step spoke credentials** (optional): `--run-token-audience` mints
  the reader-only run token R stored under `token-a`. When
  `--execution-token-audience` is set, approving Execution first writes a
  fresh R and an execution-only `token-execution` into the same Secret,
  then patches the approval as the caller. The execution key is removed
  once execution finishes; `token-a` stays for verification and terminal
  grace cleanup. Empty execution audience keeps the legacy behavior.
- **Renewal bridge** (design §6, the core — M3): see below.
- **Terminal-run Secret cleanup** (design §5.5, M3): when a run reaches a
  terminal phase, `ls-run-token-{runUID}` is deleted (never blanked) after
  a configurable grace period (default 60 s); a terminal→non-terminal
  revision re-open within the grace cancels the deletion. The delayed delete
  re-reads the current run under its per-run lock and requires the same UID,
  task binding, terminal state, run generation, and analysis generation; a
  transient read/delete failure retains a retry schedule. A deleted run's
  Secret is also tied to its AgenticRun by a non-controller, non-blocking
  owner reference, so Kubernetes garbage collection covers run deletion.

### The renewal bridge in one paragraph

The watch controller observes `CredentialsExpired=True` (reason
`CredentialsExpired`) on a non-terminal run and flips the EXISTING task to
`auth-required` (record overlay — no new task, context, or message
semantics; the pause counter and per-pause deadline start). The caller's
follow-up arrives on the same task/context with a new messageId and fresh
Token A′: the adapter validates ownership (principal fingerprint),
context, and target immutability — both against the task record AND the
bound run's actual `spec.targetCluster` (CEL-immutable in the CRD, but
verified, not trusted) — rewrites `token-a` in
`ls-run-token-{runUID}` via the existing TokenWriter (labels untouched;
the resourceVersion change is repo C's resume signal, acted on within its
30 s `credentialPauseRequeueAfter`), and returns the task to its in-flight
state immediately, without waiting for repo C to clear the condition. The
adapter never writes conditions on the run (§6.1: only the Secret write
triggers resume). 403-style authorization failures are task failures and
never enter the bridge (§6.3). Terminal tasks are never revived by a fresh
A′ (§6.3): a follow-up on a terminal run applies revisions but does not
rewrite the Secret.

`token-execution` is valid only when Execution is approved, the run is not
terminal, the newest successful AnalysisResult belongs to the current
AgenticRun generation, and the run's `a2a.lightspeed.io/approved-analysis`
annotation matches that result's UID/generation. Approval writes that
identity before atomically storing fresh `token-a` + `token-execution` and
then applying the caller-authenticated approval patch. Renewal of an existing
approval repeats the same checks; if analysis moved on, it strips
`token-execution` and renews only `token-a`. A periodic/startup reconciliation
derives each Secret name from the already-listed AgenticRun UID and removes
credentials that fail the durable checks; failed removals retry on the next
pass. An uncertain approval-update response is resolved from the durable
approval before cleanup, and an unreadable state is left for reconciliation
rather than risking removal of a credential for an approval that committed.
An adapter-terminal renewal timeout/cap is authoritative even if a follow-up
loaded its task record before the terminal transition; the record is reloaded
under the run lock before approval or credential work. A successful execution
approval renewal clears the adapter's auth-required overlay immediately.
Every present approval stage is immutable: an omitted decision has the
operator's effective `Approved` meaning, and an omitted Execution decision
cannot be changed, adopted, or used to mint credentials. Scoped execution
credentials are renewed only for an explicitly approved stage already bound to
the current analysis.

Approval/revision operations, watch lifecycle projection, and reconciliation
use a keyed, process-local per-run mutex to serialize decision, credential,
pause-deadline, and terminal-cleanup state changes. The periodic sweep
reconciles current run state so lifecycle events skipped during lock contention
are retried. **Single replica
and `Recreate` are required** (`replicas: 1`, Deployment strategy
`Recreate`): the mutex is not distributed, and a default RollingUpdate could
overlap old/new adapters so one prepares `token-execution` while the other's
reconciler removes it before the approval write commits. Reconciliation also
preserves an execution credential whose Secret preparation timestamp is
within a two-minute grace window, unless Execution has finished or the run
is terminal. Keep both deployment requirements until coordination is
replaced by a cross-replica mechanism.

Same-run revision feedback is written by the adapter service account after
task ownership validation, with `a2a.lightspeed.io/revised-by` attribution;
caller-authenticated writes remain in use for run creation and approval
decisions. A revision stream remains `working` until analysis for the new
run generation is observed, rather than replaying the stale Proposed gate.
The same durable generation check applies to message/send, tasks/get,
resubscribe, and restart recovery; current-generation failures and
auth-required pauses remain visible.

### Bounded waits (§6.4)

- `--renewal-timeout` (default 900 s): how long a task may sit
  auth-required before the adapter fails it with a credential-timeout
  reason. Aligned with MAS's worker-side 900 s bound: ACME gives up after
  2 renewals, so a pause older than one worker bound can no longer be
  answered upstream — failing the task then is honest. **Note the
  asymmetry**: repo C itself has NO bound on a credential pause (it
  requeues every 30 s indefinitely), so an adapter-side timeout leaves the
  AgenticRun paused on the cluster until its TTL/`deleteAfter` GC. Until
  design Q6 (stop semantics) resolves, the adapter deliberately does not
  touch the run; the task-level failure is the caller-visible bound.
- `--max-credential-pauses` (default 3): total pauses per run; exceeding
  the cap fails the task instead of pause-looping forever.

**Both bounds are durable across adapter restarts (the §6.4 contract):**

- The pause deadline is anchored to the `CredentialsExpired` condition's
  `lastTransitionTime` when a task is rehydrated from the CR
  (`recordFromRun`) — NOT to the restart time — so a restart (or a
  crash-looping adapter) can never extend the renewal window. A
  rehydrated pause whose anchor is already past the timeout fails
  immediately on the rehydration event, not on the next sweeper tick.
- The pause counter is persisted on the run CR as the
  **`a2a.lightspeed.io/credential-pauses`** annotation (same annotation
  namespace as the other adapter annotations; written by the watch
  controller on every increment, restored on rehydration) so the
  max-pause cap cannot be reset by a restart. This costs the adapter SA
  one extra verb: `patch`/`update` on `agenticruns` (annotations only —
  the adapter still never touches `status`; see the RBAC notes).
- Tasks parked auth-required BEFORE their first submit have no CR and are
  legitimately unrecoverable (the caller re-sends) — their pause state is
  in-memory only, unchanged.

- Stream deadlines (§2.3) apply independently, and a renewal pause does
  NOT count against the stream deadline: while auth-required the deadline
  is pushed out and the pause's own bound ends the stream through the
  normal terminal path. A client that disconnects during a pause
  reattaches with `tasks/resubscribe`.

### Target validation story (`--allowed-targets`)

Who rejects a bad target cluster, in order of speed:

1. **MAS orchestrator prompt (assist)**: the caller-facing prompt lists the
   closed set of clusters, so the right target is chosen in the first place.
   Advisory only — prompts are not enforcement.
2. **Adapter fail-fast (`--allowed-targets`)**: comma-separated spoke names
   (e.g. `east,west`). Empty (default) = no enforcement — any well-formed
   target is accepted and the admission policy remains the only target
   gate. When set, a fresh submit whose resolved target is not listed
   fails BEFORE any AgenticRun or run-token Secret is created: the task is
   parked adapter-terminally failed (same pre-create convention as the
   expired-A path) with a self-correcting status message, e.g.
   `unknown target "est"; available clusters: east, west`. The caller's
   retry loop reads the message and re-sends on the same task with a
   corrected target and a new messageId — the pre-submit record has no run
   to be immutable about, and the original question is parked with it.
   Entries must be single DNS labels (`run.IsDNSLabel`); a typo like
   `East` or `east.lab` fails adapter startup, not a request later. This
   is a UX/validation layer, NOT a security boundary.
3. **Admission policy (hard gate)**: the kube API admission policy on
   AgenticRun creation (design §4.3) is what actually enforces which
   targets a caller may run against. The adapter's allow-list failing
   fast just gives the caller an actionable error instead of a run that
   dies at admission.
4. **Praxis router allow-list**: the Praxis policy gateway in front of the
   adapter only routes approved callers/paths to it at all — the outermost
   ring of the same story.

Hub-local runs (no target given) are not spoke targets and are not
filtered by the allow-list.

### Startup resilience (`--startup-retry-timeout`)

**Live incident (the motivation, hard-won):** the adapter crash-looped
fatally with

```
hub-adapter exiting: list agenticruns for recovery: storage is (re)initializing
```

The AgenticRun CRDs are applied at Argo sync-wave -1 while the adapter
Deployment starts at wave 0, so the startup recovery LIST can race the
API server finishing CRD storage establishment; the same 503 also
appears when etcd flaps. Both are **transient** — exiting only widens
the race window via CrashLoopBackOff. Nothing caught it sooner because
every test fake answers the LIST immediately; the race only exists
against a real API server with freshly-applied CRDs.

Startup (the recovery LIST plus the informer start) now retries
transient errors — `storage is (re)initializing`, HTTP 503/504, server
timeouts, connection refused, deadline exceeded — with exponential
backoff (2 s, doubling, capped at 30 s, jittered) for up to
`--startup-retry-timeout` (default 5 m; `0` = fail fast, the old
behavior, for tests). Each retry logs the error, attempt count, and
next delay. Non-transient errors (Forbidden, NotFound, invalid config)
exit immediately — retrying an RBAC mistake for five minutes would only
hide it from the operator. On exhaustion the process exits with the
ORIGINAL error, so the log says the real thing. The HTTP listener still
only starts serving after successful recovery (unchanged order).
Classification lives in `internal/server/retry.go`
(`IsTransientAPIError`); the retry loop is `server.RetryTransient`,
pinned by `internal/server/retry_test.go` with a flaky fake that fails
the LIST N times with the incident's exact error before succeeding.


## Layout

```
a2a-adapter/                        (nested Go module — optional component)
├── cmd/hub-a2a-adapter/main.go     # wiring: flags, in-cluster clients, mux, informers, graceful shutdown
├── internal/auth/
│   ├── exchange.go                 # TokenExchanger: RFC 8693 A→H, typed errors, no tokens in logs
│   ├── svid.go                     # SVIDSource: SPIFFE Workload API JWT-SVID fetch (go-spiffe)
│   ├── expiry.go                   # unverified exp check w/ skew, fingerprints, PrincipalID
│   └── handoff.go                  # CredentialSource: HeaderSource + protected-channel placeholder
├── internal/server/
│   ├── handler.go                  # a2asrv.RequestHandler: send/stream/get/cancel/resubscribe,
│   │                               #   projection (state+artifacts+usage), renewal + approval follow-ups
│   ├── state.go                    # §7.2 phase mapping, terminal/credentials-expired predicates, status messages
│   ├── watch.go                    # watch controller: pause bridge, pause deadlines, §5.5 cleanup, sweeper
│   ├── retry.go                    # startup retry: transient API/storage errors (CRD sync-wave race, etcd flap)
│   └── card.go                     # agent card from config + well-known route
├── internal/watch/ctrl.go          # controller-runtime informer glue (thin, no logic; feeds server.Watcher)
├── internal/store/store.go         # task/context/message records + per-task subscriber signals (deduped)
├── internal/run/
│   ├── client.go                   # narrow RunClient/ResultClient/SecretClient interfaces + factory
│   ├── client_ctrl.go              # controller-runtime implementations (SA + as-caller)
│   ├── creator.go                  # message → AgenticRun mapping, create-or-get, revision
│   ├── target.go                   # §4.2 deterministic target rules
│   └── token.go                    # run-token Secret writer + terminal cleanup delete (repo C contract)
└── internal/config/config.go       # flags/env
```

## Build / test / deploy

From the **repo root** (the Makefile drives the nested module):

```
make build-a2a-adapter   # go build ./cmd/hub-a2a-adapter (into bin/)
make test-a2a-adapter    # gofmt check + go vet + go test ./... with coverage
make image-a2a-adapter   # container image (stages a vendored build context)
make deploy-a2a-adapter  # apply config/a2a-adapter (kustomize component)
```

The deployment component is `config/a2a-adapter/` — deliberately NOT part
of `config/default`, so the stock deployment is unchanged. See its README
for the required per-cluster patches (Keycloak issuer, hub audience,
exchange client secret / SVID socket).

Building without the A2A surface requires nothing: the root Go module does
not include this directory (nested module), and `config/default` does not
reference the component.

`a2a-adapter/go.mod` has a `replace` pointing
`github.com/openshift/lightspeed-agentic-operator/api` at the sibling
checkout `../../lightspeed-agentic-operator/api` (branch `a2a-support`) —
the same relative path the adapter used in its previous home. The image
build vendors that module (`go mod vendor` into a staging context) because
the sibling checkout is not part of any container build context. Before
building images outside this workspace, pin a published version of that
module and drop the replace.

## Design decisions where the doc left freedom

- **A2A SDK vs shim: SDK.** `github.com/a2aproject/a2a-go v0.3.15` was
  fetchable (`go get` works here), so the design's Q8 spike resolved in
  favor of the SDK — but only for **transport**: the SDK's
  `a2asrv.NewJSONRPCHandler` provides JSON-RPC 2.0 framing, SSE streaming,
  keep-alives, and error-code mapping, while this repo implements the full
  `a2asrv.RequestHandler` interface itself. Task semantics (duplicate
  messageId, auth-required-before-submit, deterministic CR naming,
  ownership masking) stay in our code; if the SDK regresses, swapping the
  transport for the design's documented JSON-RPC shim is a contained
  change. `tasks/resubscribe` IS implemented (M3, watch-driven).
- **Kubernetes client: controller-runtime** behind narrow interfaces
  (`run.RunClient` / `run.ResultClient` / `run.SecretClient` /
  `run.RunClientFactory`). Matches the design's "kubebuilder-style
  controller-runtime binary" intent, and the api module already depends
  on it. Tests use hand-rolled in-memory fakes (API-server semantics:
  AlreadyExists, NotFound, assigned UIDs, RV bumps) rather than the fake
  client, for exact call-count assertions; the fake's `notify` hook is
  the fake informer, driving the real watch-controller path.
- **Watches: shared-cache informers, not a manager.** `internal/watch`
  builds a `cache.Cache` scoped to the run namespace with one informer per
  kind and forwards events to `server.Watcher` — deliberately thin and
  logic-free (all logic lives in the test-covered watcher). Result CR and
  approval events re-project the parent run (via
  `spec.agenticRunName` / the 1:1 approval name). Run deletes signal the
  task's subscribers (their next projection observes TaskNotFound, §2.3
  result retention) and delete the orphaned run-token Secret.
- **Two kube identities** (design §3.2/§3.3/§5.2): AgenticRun
  **creation/update and approval decisions happen as the caller** (Token
  H, per-request client via the factory — the adapter never creates runs
  or decides approvals as its own SA; repo C's mutating admission webhook
  attributes the approver from the request user); AgenticRun/Result/
  Approval **reads and run-token Secret writes/deletes happen as the
  adapter SA**.
- **All three step agents are set explicitly** (default `"default"`, flag
  `--run-agent`): the CRD's `MinProperties=1` rejects empty step objects,
  and the approval-gated workflow requires analysis, execution, and
  verification steps on every run.
- **Analysis output is not Minimal**: execution needs the complete option
  payload (including remediation plans); the adapter leaves
  `spec.analysisOutput` unset so the operator uses its full default schema.
- **Execution-token audience env fallback.** `--execution-token-audience`
  falls back to `EXECUTION_TOKEN_AUDIENCE`; empty disables the feature. A
  non-empty value requires `--run-token-audience`, which has no env fallback.
- **Expiry skew direction**: the pre-start check grants grace *after* exp
  (`expired = now > exp + skew`, default 30 s) — it is a pre-check, not
  the validator; see `auth.Expired` for the rationale.
- **tasks/cancel** (design §10 Q6): non-terminal tasks return A2A
  `TaskNotCancelable` (-32002) with an explanatory message — the only
  stop semantic visible in repo C's types is `EmergencyStopped`, the
  adapter must not write conditions on runs (design §6.1), and Q6 is
  explicitly unresolved. Terminal tasks return their terminal state
  (§2.3), which IS implemented — including a run already `Stopping`,
  which projects `canceled` per §7.2's "tasks/cancel during Stopping".
- **Approval decision DataPart: minimal, documented representation.**
  The design's vocabulary is approve/reject/modify/redirect, but repo C's
  AgenticRunApproval only accepts Approved/Denied per stage (plus an
  execution option index); "modify"/"redirect" have no CR-side mapping
  and are rejected as invalid params rather than silently degraded. The
  stage is REQUIRED in the DataPart — guessing it would put words in the
  approver's mouth. The pending stage named in the input-required message
  is derived from the approval CR (first workflow stage absent from
  `spec.stages`) — repo C exposes no explicit "pending stage" field.
- **Execution denial completes with the analysis result.** Design §7.2
  maps all `Denied` runs to `failed`; this adapter deliberately deviates for
  an Execution denial after `Analyzed=True`: the task is `completed` with
  the exact status message `Execution rejected by approver; analysis result
  attached.` and the analysis artifact. This preserves the successful
  analysis outcome without describing a human choice as an agent failure.
- **Renewal does not wait for repo C to clear the condition.** After the
  Secret rewrite the task immediately projects its derived (in-flight)
  phase; repo C clears `CredentialsExpired` on its own 30 s poll. The
  condition stays on the CR in the meantime — by design (§6.1 step 6:
  "the adapter does nothing to trigger resume beyond the Secret write").
- **Same-value renewal is a no-op.** `TokenWriter.Write` skips the update
  when the new Token A is byte-identical to the current Secret value:
  repo C's resume signal is the Secret's resourceVersion, which only an
  actual VALUE change bumps. A caller retrying an unchanged token gets
  success but the run does NOT resume — resuming requires a genuinely
  fresh token value.
- **Terminal follow-ups park new A on the next message** (§5.5 edge): a
  follow-up on a run whose CURRENT phase is terminal applies revisions
  but never rewrites the Secret; if the revision re-opens the run, the
  next valid follow-up renews through the normal path.
- **TLS serving on 8443 (service-serving cert)**: the adapter serves TLS
  when `--tls-cert-file` and `--tls-key-file` are both set (they must be
  set together — `config.Validate` rejects exactly-one); production wires
  them to the OpenShift service-serving cert minted by the Service
  annotation `service.beta.openshift.io/serving-cert-secret-name` (keys
  `tls.crt`/`tls.key`, rotated by the service-ca operator) on port 8443,
  because praxis-proxy's `a2aUpstream` hop connects over TLS and verifies
  the service-serving CA. Plain HTTP (both flags empty) is a **local dev
  only** shape. The outbound rule is unchanged and strict: the Keycloak
  issuer and token endpoint must be **https** (enforced at construction in
  both `internal/config` and `internal/auth` — plaintext exchange legs
  would expose Token A/H in transit).
- **`GET /healthz`**: unauthenticated 200, for kubelet
  liveness/readiness probes. Safe by construction: the Service is
  internal-only and the NetworkPolicy admits ingress solely from the
  Praxis namespace; probes must not need a token. Served on the same
  listener/protocol as the A2A surface (HTTPS when TLS is configured).
  Not part of the A2A surface.

## RBAC notes

The adapter's grants are rendered by two synchronized consumers — the
gitops repo's Helm chart (`charts/all/lightspeed-hub`,
`templates/run-token-writer-rbac.yaml`) and this repo's kustomize
component (`config/a2a-adapter/role.yaml`). Keep all three (this README
included) in agreement:

The chart's Role (`templates/run-token-writer-rbac.yaml`) is reconciled
against this implementation (M4 pass):

- **`secrets`: `create/get/update/delete`.** `delete` is the §5.5
  terminal-run cleanup and the run-deletion cleanup (`watch.go`
  `Sweep`/`RunDeleted`). Namespace isolation remains the real boundary.
- **`agenticruns`: `get/list/watch/update`.** `update` covers adapter-owned
  annotations only: the §6.4 pause counter, revision attribution, and the
  approved-analysis identity used to bind execution credentials. Revision
  feedback itself is also written by the adapter SA after ownership checks.
  The adapter NEVER writes `agenticruns/status` or conditions (design §6.1)
  — no status-subresource grant. Existing run list/get access is sufficient
  for execution-token reconciliation; no Secret `list` permission is needed.
- **`agenticrunapprovals`: adapter `get/list/watch`; caller `get/update`.**
  The adapter SA reads pending-stage state, but decision reads and patches
  happen **as the caller** (Token H). The caller's hub-authenticated group
  must have both `get` and `update` on `agenticrunapprovals` in the run
  namespace through hub Authentication/RoleBinding wiring (keycloak-oidc
  chart work).
- The result-CR `get/list/watch` grants match the implementation (reads +
  informers).

Run CREATION and approval decisions are authorized as the caller (Token H):
the hub Authentication/RoleBinding wiring that lets H-authenticated
principals create AgenticRuns and get/update AgenticRunApprovals is external
to this adapter component. The chart must also ship the §4.3 admission
policy and document the `oc auth can-i` escalation checks.

## Contract friction found against repo C (a2a-support)

Reported, not worked around silently:

1. **Repo C has no bound on a credential pause.**
   `credential_pause.go` requeues every 30 s forever; the design's
   "worker wait bounds (900s)" bound lives in MAS, not in repo C. The
   adapter's task-level `--renewal-timeout` therefore fails the A2A task
   while the AgenticRun stays paused on-cluster until TTL GC. A stop
   semantic for "caller gave up" (design Q6) would close this.
2. **Approval "pending stage" is not exposed.** The AgenticRunApproval CR
   has no field saying which stage is currently awaited. This pattern has a
   single manual checkpoint (Execution after automatic Analysis approval),
   so the adapter projects Execution only while the run is Proposed and no
   Execution decision exists; it does not guess the next absent stage in the
   append-only approval list. The caller must still name the stage explicitly
   in the decision DataPart.
3. **modify/redirect approval decisions have no CR-side mapping** (see
   design decisions above) — rejected rather than degraded.
4. **The pause condition persists after renewal** until repo C's next 30 s
   poll clears it; the adapter's projection deliberately un-pauses
   immediately. Observers reading the CR directly will see the run
   "paused" for up to 30 s after the task reports in-flight — documented,
   benign (§6.1 step 6).
5. **`AgenticRunApproval.spec.approver` is webhook-authoritative** — the
   adapter patches approvals as the caller precisely so this field is
   truthful; any deployment path that bypasses repo C's mutating webhook
   would silently drop approver attribution.

## Explicitly deferred

- **M4**: the §4.3 target-authorization admission webhook/policy, image
  pinning, and the Praxis-side protected-channel `CredentialSource`
  (design §10 Q2/Q3 gates). The M4 deployment scaffolding now exists on
  both sides — the gitops repo's Helm chart
  (`charts/all/lightspeed-hub`, disabled by default) and this repo's
  kustomize component (`config/a2a-adapter/`, not in `config/default`).
- **M5**: the live end-to-end acceptance matrix (the PPE-dependent
  spoke-leg tests need design Q1; everything CR-level is covered by the
  unit suite against the fake condition-writer/watch harness).
- **tasks/cancel for live runs**: blocked on design Q6 (stop semantics).

## Security invariants (enforced in code and tests)

- Token A/H values are never logged (fingerprints only), never in CR
  fields, never in A2A responses, never in error messages
  (`auth.ExchangeError` redacts JWT-shaped strings defensively).
- The adapter never reads run-token data bytes. Secret reads are limited to
  owned-object updates, key presence, and the execution-token preparation
  timestamp used by reconciliation; credential values are never logged or
  returned. It removes only the `token-execution` key after execution (and,
  at terminal+grace, deletes the whole Secret).
- `token-a` is the reader-only run credential. `token-execution` is minted
  only for an Execution approval bound to the current successful analysis;
  its Secret update with a fresh `token-a` is atomic and precedes the
  caller-authenticated approval patch. It is removed at execution completion
  and reconciled at startup/periodically against durable approval, run, and
  analysis identity. If an approval write reports failure, the adapter
  re-reads durable state before deciding whether cleanup is safe.
- Reconciliation conservatively retains a just-prepared execution
  credential for two minutes unless the run is terminal or Execution has
  finished, preventing a rolling/overlapping process from racing approval.
- With `--execution-token-audience` enabled, exchanged tokens are locally
  fail-closed on unverified `aud`: R must exclude the execution audience;
  R_exec must include it and exclude the reader audience before either is
  written, and rejections log only a token fingerprint. This claim check is
  not signature validation; upstream validators remain authoritative.
- Ownership is validated on EVERY operation — get, cancel, stream,
  resubscribe, duplicate-message replay, renewal, and approval decisions;
  a mismatch is `TaskNotFound` (existence is not confirmed to non-owners).
  `message/send` with a caller-supplied taskID rehydrates-and-masks the
  same way after a restart (`findTask` semantics inside `handleSend`): a
  foreign principal sending the victim's taskID with a fresh messageId
  gets `TaskNotFound` and cannot shadow the victim's task record or
  spawn a run under it.
- Multi-valued or conflicting target indicators are protocol errors, never
  OR-matched; renewal re-verifies the bound run's actual
  `spec.targetCluster` (verify, don't trust). When `--allowed-targets` is
  set, an unlisted target on a fresh submit fails before any CR or Secret
  exists (validation layer only — the admission policy is the hard gate).
- The adapter never writes conditions on AgenticRuns (§6.1) — the only
  resume-relevant write is the run-token Secret.
