# Lightspeed Hub

Multicluster hub operator for OpenShift Lightspeed. Go/kubebuilder.

## Specs

All specifications live in `.ai/spec/`. Start with `.ai/spec/README.md` for project overview, reading order, and structure guide.

## Commands

```bash
make build      # Build the operator binary
make test       # Run unit tests (always use make, not go test directly)
make lint       # golangci-lint
make generate   # Regenerate DeepCopy implementations
make manifests  # Regenerate CRD YAML and RBAC ClusterRole
make fmt        # Run go fmt
make vet        # Run go vet
```

The optional A2A adapter is a separate nested Go module (`a2a-adapter/`)
with its own targets: `make build-a2a-adapter`, `make test-a2a-adapter`,
`make image-a2a-adapter`, `make deploy-a2a-adapter`. See
`docs/a2a-adapter.md`. Its `go.mod` replaces
`lightspeed-agentic-operator/api` with the sibling checkout at
`../../lightspeed-agentic-operator/api` (branch `a2a-support`) — those
targets require that checkout to exist.

## Conventions

- Follow the same Go patterns as lightspeed-operator and lightspeed-agentic-operator (controller-runtime, kubebuilder markers, `fmt.Errorf("%s: %w", ErrConstant, err)` error wrapping)
- SpokeCluster CR is cluster-scoped
- Reconciler must be idempotent — re-applying a SpokeCluster must converge without side effects
- Create-only idempotency: `Create` + handle `AlreadyExists` (not Get-then-Create)

## Git and PR Workflow

### Commit Messages
- Start with the Jira ticket reference: `OLS-XXXX: description`
- Keep the first line under 72 characters
- Use imperative mood

### Pull Requests
This repo uses a **fork-based workflow**:

1. **Push to your fork**, not to `origin` (openshift/lightspeed-hub)
2. **Create the PR** against `origin/main` using your fork's branch:
   ```bash
   git push <your-fork-remote> <branch>
   gh pr create --repo openshift/lightspeed-hub --head <your-github-user>:<branch> --base main
   ```
3. **PR title** must start with the Jira reference: `OLS-XXXX: description`
4. **Squash commits** before pushing
