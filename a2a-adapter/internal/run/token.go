package run

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	agenticv1alpha1 "github.com/openshift/lightspeed-agentic-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// The run-token Secret contract, verbatim from repo C
// (lightspeed-agentic-operator, controller/agenticrun/gateway.go and
// docs/gateway-credential-mode.md). Repo C validates all of these
// fail-closed before creating the sandbox pod — the adapter must write
// exactly this shape or the run pauses (CredentialsExpired).
const (
	// RunTokenSecretPrefix + run UID = the Secret name (gateway.go:35,67-69).
	RunTokenSecretPrefix = "ls-run-token-"
	// RunTokenKey is the data key holding Token A (gateway.go:38).
	RunTokenKey = "token-a"
	// ExecutionTokenKey is the execution-only spoke credential. It is
	// written after caller approval and removed as soon as execution ends.
	ExecutionTokenKey = "token-execution"
	// LabelAgenticRun binds the Secret to the run UID (spoke.go labels).
	LabelAgenticRun = "hub.openshift.io/agentic-run"
	// LabelSpokeCluster binds the Secret to the (truncated) target
	// cluster (spoke.go labels; gateway.go:124).
	LabelSpokeCluster = "hub.openshift.io/spoke-cluster"
	// AnnotationWrittenBy is the adapter's provenance marker (design
	// §5.6): overwrite rather than consume a Secret we did not write.
	AnnotationWrittenBy = "a2a.lightspeed.io/written-by"
	// AnnotationExecutionTokenWrittenAt lets a concurrent reconciler avoid
	// deleting a just-prepared execution credential before approval commits.
	AnnotationExecutionTokenWrittenAt = "a2a.lightspeed.io/token-execution-written-at"
)

// ErrEmptyToken guards the contract's "non-empty value" requirement.
var ErrEmptyToken = errors.New("token-a value must not be empty")

// ErrEmptyExecutionToken guards the approval-only credential contract.
var ErrEmptyExecutionToken = errors.New("token-execution value must not be empty")

// ErrNoRunUID guards against writing a Secret that could never validate
// (repo C resolves the name from the run's server-assigned UID).
var ErrNoRunUID = errors.New("run has no UID (not yet created?)")

// maxLabelValueLen is the Kubernetes label-value limit (63 chars), matching
// repo C's spoke.go constant.
const maxLabelValueLen = 63

// TruncateLabelValue is an exact port of repo C's spoke.go truncateLabelValue
// (spoke.go:69-75): values within the limit pass through; longer values are
// truncated and suffixed with a 7-char SHA-256 hash for uniqueness. The
// label values the adapter writes must byte-match what repo C *expects*
// (it computes the same truncation from spec.targetCluster), so this
// function may not drift from repo C's.
func TruncateLabelValue(v string) string {
	if len(v) <= maxLabelValueLen {
		return v
	}
	h := fmt.Sprintf("%x", sha256.Sum256([]byte(v)))
	return v[:maxLabelValueLen-8] + "-" + h[:7]
}

// RunTokenSecretName returns ls-run-token-{runUID}.
func RunTokenSecretName(runUID string) string {
	return RunTokenSecretPrefix + runUID
}

// TokenWriter creates and renews run-token Secrets (design §5). It only
// ever WRITES: the token value never round-trips through the adapter's
// reads, logs, or responses (design §5.6). Renewal = rewriting the value;
// the resourceVersion then changes naturally, which is repo C's resume
// signal (gateway.go:216-236 — the controller compares the observed RV
// embedded in the CredentialsExpired condition message).
type TokenWriter struct {
	Secrets SecretClient
	// Namespace is the run's namespace — repo C fetches the Secret in
	// r.Namespace (credential_pause.go:80), so the Secret lives wherever
	// the run CR lives.
	Namespace string
	// Identity is the value stamped into AnnotationWrittenBy (the adapter
	// SA name), used to distinguish our own Secrets from foreign ones.
	Identity string
}

// Write creates or renews the run-token Secret for run with Token A.
//
// Binding (design §5.3, enforced twice): the labels are derived from the
// RUN CR's UID and spec.targetCluster — never from the inbound message,
// which could lie. Repo C independently rejects a Secret whose labels
// don't match the run.
//
// Sequencing (design §5.4): the Secret is written immediately AFTER the CR
// create (the name needs the server-assigned UID; the adapter holds A in
// memory across the two calls), so the common path never pauses.
//
// Ownership (design §5.6): an existing Secret carrying our
// AnnotationWrittenBy is renewed by updating ONLY the token-a data key —
// labels and every other field stay untouched (§5.3: a renewal must not
// touch labels). A Secret we did not write is overwritten wholesale (data
// + labels + annotation) rather than consumed.
//
// Same-value no-op: when the new Token A is byte-identical to the current
// Secret value, the write is SKIPPED (success, current state untouched).
// Repo C's resume signal is the Secret's resourceVersion, which only an
// actual VALUE change bumps — a caller retrying an unchanged token must
// not spin on a renewal that never resumes the run. Callers that need the
// run to resume must send a genuinely fresh token.
func (w *TokenWriter) Write(ctx context.Context, run *agenticv1alpha1.AgenticRun, tokenA string) error {
	return w.writeTokens(ctx, run, map[string]string{RunTokenKey: tokenA})
}

// WriteWithExecution renews the normal run token and execution token in one
// Secret update. This is used immediately before forwarding an Execution
// approval so the spoke can use write credentials only after approval.
func (w *TokenWriter) WriteWithExecution(ctx context.Context, run *agenticv1alpha1.AgenticRun, runToken, executionToken string) error {
	return w.writeTokens(ctx, run, map[string]string{
		RunTokenKey:       runToken,
		ExecutionTokenKey: executionToken,
	})
}

// WriteExecution writes or renews only the execution credential while
// preserving the run credential and all other Secret data keys.
func (w *TokenWriter) WriteExecution(ctx context.Context, run *agenticv1alpha1.AgenticRun, token string) error {
	return w.writeTokens(ctx, run, map[string]string{ExecutionTokenKey: token})
}

func (w *TokenWriter) writeTokens(ctx context.Context, run *agenticv1alpha1.AgenticRun, values map[string]string) error {
	if run == nil || run.UID == "" {
		return ErrNoRunUID
	}
	for key, value := range values {
		if value == "" {
			if key == RunTokenKey {
				return ErrEmptyToken
			}
			if key == ExecutionTokenKey {
				return ErrEmptyExecutionToken
			}
			return fmt.Errorf("%s value must not be empty", key)
		}
	}
	name := RunTokenSecretName(string(run.UID))
	ownerRef := agenticRunOwnerReference(run)
	data := make(map[string][]byte, len(values))
	for key, value := range values {
		data[key] = []byte(value)
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: w.Namespace,
			Labels: map[string]string{
				LabelAgenticRun:   string(run.UID),
				LabelSpokeCluster: TruncateLabelValue(run.Spec.TargetCluster),
			},
			Annotations: map[string]string{
				AnnotationWrittenBy: w.Identity,
			},
			OwnerReferences: []metav1.OwnerReference{ownerRef},
		},
		Data: data,
		Type: corev1.SecretTypeOpaque,
	}
	if _, writesExecution := values[ExecutionTokenKey]; writesExecution {
		secret.Annotations[AnnotationExecutionTokenWrittenAt] = time.Now().UTC().Format(time.RFC3339Nano)
	}

	if err := w.Secrets.CreateSecret(ctx, secret); err == nil {
		return nil
	} else if !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create run-token Secret %s: %w", name, err)
	}

	existing, err := w.Secrets.GetSecret(ctx, w.Namespace, name)
	if err != nil {
		return fmt.Errorf("get existing run-token Secret %s: %w", name, err)
	}
	if existing.Annotations[AnnotationWrittenBy] == w.Identity {
		// Renewal of our own Secret updates only requested keys. Labels,
		// annotations, and other credentials are preserved. Same-value
		// semantics are per key; no update occurs if every requested key
		// already has its desired value.
		changed := false
		if existing.Data == nil {
			existing.Data = map[string][]byte{}
		}
		for key, value := range data {
			if !bytes.Equal(existing.Data[key], value) {
				existing.Data[key] = value
				changed = true
			}
		}
		if _, writesExecution := values[ExecutionTokenKey]; writesExecution &&
			(changed || existing.Annotations[AnnotationExecutionTokenWrittenAt] == "") {
			if existing.Annotations == nil {
				existing.Annotations = map[string]string{}
			}
			writtenAt := time.Now().UTC().Format(time.RFC3339Nano)
			if existing.Annotations[AnnotationExecutionTokenWrittenAt] != writtenAt {
				existing.Annotations[AnnotationExecutionTokenWrittenAt] = writtenAt
				changed = true
			}
		}
		if !hasAgenticRunOwnerReference(existing.OwnerReferences, run.UID) {
			existing.OwnerReferences = append(existing.OwnerReferences, ownerRef)
			changed = true
		}
		if !changed {
			return nil
		}
	} else {
		// Foreign or pre-provenance Secret: take it over wholesale
		// (§5.6) — overwrite, never consume.
		existing.Labels = secret.Labels
		existing.Annotations = secret.Annotations
		existing.Data = secret.Data
		existing.OwnerReferences = secret.OwnerReferences
	}
	if err := w.Secrets.UpdateSecret(ctx, existing); err != nil {
		return fmt.Errorf("renew run-token Secret %s: %w", name, err)
	}
	return nil
}

func agenticRunOwnerReference(run *agenticv1alpha1.AgenticRun) metav1.OwnerReference {
	controller := false
	blockOwnerDeletion := false
	return metav1.OwnerReference{
		APIVersion:         agenticv1alpha1.GroupVersion.String(),
		Kind:               "AgenticRun",
		Name:               run.Name,
		UID:                run.UID,
		Controller:         &controller,
		BlockOwnerDeletion: &blockOwnerDeletion,
	}
}

func hasAgenticRunOwnerReference(refs []metav1.OwnerReference, uid types.UID) bool {
	for _, ref := range refs {
		if ref.UID == uid && ref.Kind == "AgenticRun" && ref.APIVersion == agenticv1alpha1.GroupVersion.String() {
			return true
		}
	}
	return false
}

// RemoveExecution removes the execution-only credential from an adapter-
// owned run-token Secret. It is idempotent and deliberately leaves token-a,
// labels, annotations, and foreign Secrets untouched.
func (w *TokenWriter) RemoveExecution(ctx context.Context, runUID string) error {
	_, err := w.removeExecution(ctx, runUID, time.Time{}, 0, true)
	return err
}

// RemoveExecutionUnlessRecentlyWritten removes token-execution only if the
// Secret does not show a fresh preparation timestamp. `force` bypasses the
// grace when the run is terminal or Execution has finished. The Get/update
// resourceVersion pair makes a concurrent token refresh conflict rather than
// deleting the freshly written value.
func (w *TokenWriter) RemoveExecutionUnlessRecentlyWritten(ctx context.Context, runUID string, now time.Time, grace time.Duration, force bool) (bool, error) {
	return w.removeExecution(ctx, runUID, now, grace, force)
}

func (w *TokenWriter) removeExecution(ctx context.Context, runUID string, now time.Time, grace time.Duration, force bool) (bool, error) {
	if runUID == "" {
		return false, ErrNoRunUID
	}
	name := RunTokenSecretName(runUID)
	secret, err := w.Secrets.GetSecret(ctx, w.Namespace, name)
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("get run-token Secret %s to remove execution token: %w", name, err)
	}
	if secret.Annotations[AnnotationWrittenBy] != w.Identity || len(secret.Data[ExecutionTokenKey]) == 0 {
		return false, nil
	}
	if !force && executionTokenRecentlyWritten(secret, now, grace) {
		return false, nil
	}
	delete(secret.Data, ExecutionTokenKey)
	delete(secret.Annotations, AnnotationExecutionTokenWrittenAt)
	if err := w.Secrets.UpdateSecret(ctx, secret); err != nil {
		return false, fmt.Errorf("remove execution token from run-token Secret %s: %w", name, err)
	}
	return true, nil
}

func executionTokenRecentlyWritten(secret *corev1.Secret, now time.Time, grace time.Duration) bool {
	if secret == nil || grace <= 0 {
		return false
	}
	stamp := secret.Annotations[AnnotationExecutionTokenWrittenAt]
	if stamp == "" {
		return false
	}
	writtenAt, err := time.Parse(time.RFC3339Nano, stamp)
	if err != nil {
		return false
	}
	return now.Sub(writtenAt) <= grace
}

// Delete removes the run-token Secret for a TERMINAL run (design §5.5).
// It deletes (never blanks — the name must not stay resolvable) the Secret
// holding a Token A that expired long ago, once the run's phase is terminal
// and the cleanup grace period has elapsed. Repo C's mode-flip behavior
// shows a deleted Secret in gateway mode simply re-pauses a run, never
// corrupts it, so deleting at-terminal is safe even if a human later
// triggers a revision re-analysis: the adapter re-writes the Secret from
// the NEXT valid follow-up's Token A through the same renewal path (§6).
// A NotFound result is success (already GC'd or already cleaned).
func (w *TokenWriter) Delete(ctx context.Context, runUID string) error {
	if runUID == "" {
		return ErrNoRunUID
	}
	name := RunTokenSecretName(runUID)
	if err := w.Secrets.DeleteSecret(ctx, w.Namespace, name); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete run-token Secret %s: %w", name, err)
	}
	return nil
}
