# A2A adapter deployment component (optional)

Kustomize component deploying the **hub A2A adapter** (the `a2a-adapter/`
nested Go module). It is deliberately **NOT referenced from
`config/default`** — the stock lightspeed-hub deployment is unchanged;
deploying this component is the enable/disable knob at the deployment
level.

## Deploy

```bash
make deploy-a2a-adapter    # from the repo root
# or from a custom overlay:
#   resources:
#   - ../default
#   - ../a2a-adapter
```

## Required per-cluster patches

The adapter's flags (`a2a-adapter/internal/config/config.go` is the source
of truth) include required values with no defaults. The manifest defaults
carry `REPLACE-ME` placeholders that MUST be patched before deploying:

| Placeholder | Flag / field | What it is |
| --- | --- | --- |
| `keycloak.REPLACE-ME.invalid` | `--keycloak-issuer`, `--keycloak-token-endpoint` | Keycloak realm issuer + token endpoint of the A→H exchange (must be `https://`) |
| `REPLACE-ME` | `--keycloak-client-id` | confidential exchange client (drop both `--keycloak-client-*` flags and set `SPIFFE_SOCKET` instead for SVID client auth) |
| `REPLACE-ME` | `--hub-audience` | audience requested on the exchanged hub token (Token H) |
| `REPLACE-ME-keycloak-client-secret` | env `KEYCLOAK_CLIENT_SECRET` | Secret (key `client-secret`) holding the exchange client's secret |
| `REPLACE-ME-praxis-namespace` | NetworkPolicy `namespaceSelector` | namespace running the Praxis policy gateway pods |
| (commented out) | `--allowed-targets` | fail-fast allow-list of spoke names (comma-separated, e.g. `east,west`); empty (default) = no enforcement — the admission policy is the hard gate either way. MUST stay in sync with the gitops praxis-proxy chart's `spokeGateways` keys (and the lightspeed-hub chart's `adapter.allowedTargets`) |
| (commented out) | `--run-token-audience` | optional reader-token audience; required to enable the execution-token split |
| (commented out) | `--execution-token-audience` / env `EXECUTION_TOKEN_AUDIENCE` | optional execution-only token audience; when non-empty, `--run-token-audience` is required |

All other flags mirror the adapter's own defaults (`--listen=:8443`,
`--target-header=X-Hub-Cluster`, M3 bounds: `--clock-skew=30s`,
`--stream-deadline=30m`, `--renewal-timeout=15m`,
`--max-credential-pauses=3`, `--terminal-cleanup-grace=1m`,
`--sweep-interval=5s`, card flags per the config source). `--namespace`
is left at its default: the pod's own namespace.

The Deployment is deliberately **one replica with `Recreate` strategy**.
Approval, revision, and execution-token reconciliation use a process-local,
per-run mutex; RollingUpdate can overlap two adapters and let one delete
`token-execution` while the other is preparing it before approval commits.
The reconciler also honors a two-minute execution-token preparation grace
unless the run is terminal or Execution has finished. Keep both the replica
count and strategy until coordination is replaced with a distributed
mechanism.

The deployment example leaves both run-token audience flags commented out
(legacy credential behavior). To enable the per-step token split, patch both
audiences: `token-a` is exchanged for the reader gateway, while
`token-execution` is minted only on an Execution approval and removed as
soon as execution finishes. The adapter rejects
`--execution-token-audience` without `--run-token-audience` at startup.

## TLS on 8443 (service-serving cert)

The adapter serves **TLS on port 8443**: the Service carries
`service.beta.openshift.io/serving-cert-secret-name:
a2a-adapter-serving-cert`, so OpenShift's service-ca operator mints and
rotates a serving certificate into that Secret; the Deployment mounts it
at `/etc/a2a-adapter/tls` and passes `--tls-cert-file`/`--tls-key-file`.
This is required in production because praxis-proxy's `a2aUpstream` hop
connects over TLS and verifies the service-serving CA. Plain HTTP (both
TLS flags empty) is a **local dev only** shape. Note the annotation VALUE
is the literal Secret name — kustomize's `namePrefix` does not rewrite it
— so the Deployment mounts the same unprefixed name.

## Internal-only behind Praxis — do not add a Route

The Service is `ClusterIP` only and the NetworkPolicy admits ingress
solely from the Praxis namespace, over the TLS port 8443. `/healthz` is
unauthenticated (kubelet probes, HTTPS against the serving cert). This
mirrors the gitops repo's chart
(`charts/all/lightspeed-hub`), which is the fully-wired deployment for the
validated pattern — keep the RBAC of the two in sync (`role.yaml` here,
`templates/run-token-writer-rbac.yaml` there).

## RBAC

Dedicated SA (`a2a-adapter`) with a Role in the run namespace (this
component deploys everything into `openshift-lightspeed`, which doubles as
the run namespace per the design's §8.3 single-namespace shape):

- `secrets`: create/get/update/delete (run-token writer + §5.5 terminal
  cleanup)
- `agenticruns`: get/list/watch/update (annotation bookkeeping, revision
  feedback, and approved-analysis/revision attribution; never status). The
  existing list/get grants also support durable execution-token cleanup,
  without Secret `list` permission.
- `agenticrunapprovals`: get/list/watch for the adapter SA (approval-state
  derivation/reconciliation and informer watch). Separately, the caller's hub-authenticated
  group needs **get + update** on `agenticrunapprovals` in the run namespace:
  the adapter reads the approval as its SA, then patches the decision as the
  caller (Token H) so the admission webhook attributes the real approver.
  That caller-group grant is in hub Authentication/RoleBinding wiring, not
  this adapter Role.
- result CRs (`analysisresults`, `executionresults`,
  `verificationresults`, `escalationresults`): get/list/watch

Run creation and approval decisions are authorized as the caller (Token H):
the hub Authentication/RoleBinding wiring that lets H-authenticated
principals create agenticruns and get/update agenticrunapprovals is external
to this component.
