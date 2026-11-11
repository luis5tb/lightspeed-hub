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

All other flags mirror the adapter's own defaults (`--listen=:8443`,
`--target-header=X-Hub-Cluster`, M3 bounds: `--clock-skew=30s`,
`--stream-deadline=30m`, `--renewal-timeout=15m`,
`--max-credential-pauses=3`, `--terminal-cleanup-grace=1m`,
`--sweep-interval=5s`, card flags per the config source). `--namespace`
is left at its default: the pod's own namespace.

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
- `agenticruns`: get/list/watch/update (the `update` covers exactly one
  write: the §6.4 pause-counter annotation; never status)
- `agenticrunapprovals`: get/list/watch (the SA reads approvals both
  directly — pending-stage derivation — and via the M3 informer watch;
  decisions are still patched as the caller, Token H
  — hub Authentication/RoleBinding wiring, not this Role)
- result CRs (`analysisresults`, `executionresults`,
  `verificationresults`, `escalationresults`): get/list/watch

Run CREATION is authorized as the caller (Token H): the hub
Authentication/RoleBinding wiring that lets H-authenticated principals
create agenticruns and decide approvals is external to this component.
