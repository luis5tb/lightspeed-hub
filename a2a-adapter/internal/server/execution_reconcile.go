package server

import (
	"context"
	"fmt"
	"time"

	agenticv1alpha1 "github.com/openshift/lightspeed-agentic-operator/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/openshift/lightspeed-hub/a2a-adapter/internal/run"
)

const executionTokenPrepareGrace = 2 * time.Minute

// reconcileExecutionToken serializes with approval preparation and removes
// token-execution unless the durable approval, unfinished run, and newest
// analysis identity all agree. It intentionally does not need Secret list
// permission: AgenticRuns already provide the run UID/name for GetSecret.
func (h *Handler) reconcileExecutionToken(ctx context.Context, observedRunName string) error {
	if h.deps.Tokens == nil || h.deps.Tokens.Secrets == nil {
		return nil // recovery-only harnesses may omit Secret API wiring
	}
	observed, err := h.deps.Readers.GetRun(ctx, h.cfg.Namespace, observedRunName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil // ownerReference garbage collection handles a deleted run
		}
		return fmt.Errorf("get AgenticRun for execution-token reconciliation: %w", err)
	}
	if observed.UID == "" || observed.Annotations[run.AnnotationTaskID] == "" {
		return nil
	}
	unlock := h.locks.Lock(string(observed.UID))
	defer unlock()

	current, err := h.deps.Readers.GetRun(ctx, h.cfg.Namespace, observed.Name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("refresh AgenticRun for execution-token reconciliation: %w", err)
	}
	if current.UID != observed.UID || current.Annotations[run.AnnotationTaskID] == "" {
		return nil
	}
	return h.reconcileExecutionTokenLocked(ctx, current)
}

// reconcileExecutionTokenLocked applies the durable approval/analysis check
// to a run that the caller has already reloaded while holding its per-run
// lock. It deliberately does not acquire the lock or perform another read.
func (h *Handler) reconcileExecutionTokenLocked(ctx context.Context, current *agenticv1alpha1.AgenticRun) error {
	if h.deps.Tokens == nil || h.deps.Tokens.Secrets == nil {
		return nil // recovery-only harnesses may omit Secret API wiring
	}
	if current == nil || current.UID == "" || current.Annotations[run.AnnotationTaskID] == "" {
		return nil
	}
	taskID := current.Annotations[run.AnnotationTaskID]
	rec, ok := h.deps.Store.Task(taskID)
	if !ok || rec.TaskID != taskID || rec.RunName != current.Name || rec.RunUID != string(current.UID) {
		return nil // do not authorize cleanup without the matching task binding
	}
	if h.executionCredentialAllowed(ctx, current) {
		return nil
	}
	finished := executionFinished(current) || RunTerminalPhase(current)
	if _, err := h.deps.Tokens.RemoveExecutionUnlessRecentlyWritten(
		ctx, string(current.UID), h.cfg.now(), executionTokenPrepareGrace, finished); err != nil {
		return fmt.Errorf("remove unapproved or stale execution token for run %s: %w", current.Name, err)
	}
	return nil
}
