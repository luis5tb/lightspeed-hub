package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/a2aproject/a2a-go/a2a"
	agenticv1alpha1 "github.com/openshift/lightspeed-agentic-operator/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/openshift/lightspeed-hub/a2a-adapter/internal/run"
	"github.com/openshift/lightspeed-hub/a2a-adapter/internal/store"
)

// Watcher is the M3 watch controller (design §6 renewal bridge, §7 status
// flow-back, §5.5 terminal-run Secret cleanup). It is fed by the
// controller-runtime informer glue (internal/watch) in production and
// directly by tests; every entry point re-projects the affected task into
// the store, and the store signals open SSE streams.
//
// It NEVER writes conditions on runs (design §6.1: the adapter's only
// resume-relevant write is the run-token Secret) and never touches token
// values.
type Watcher struct {
	cfg   Config
	deps  Deps
	log   *slog.Logger
	locks *runLocks

	mu sync.Mutex
	// cleanups maps runUID to a terminal snapshot plus its deletion deadline.
	// The snapshot is revalidated against the current run and task under the
	// run lock before any Secret is deleted.
	cleanups map[string]terminalCleanup
}

type terminalCleanup struct {
	runUID             string
	runName            string
	taskID             string
	generation         int64
	analysisGeneration int64
	due                time.Time
}

// NewWatcher builds the watch controller sharing this handler's config,
// collaborators, and (critically) task store — watch projections and A2A
// reads must land in the same records.
func (h *Handler) NewWatcher() (*Watcher, error) {
	if err := fillDefaults(&h.cfg, &h.deps); err != nil {
		return nil, err
	}
	return &Watcher{cfg: h.cfg, deps: h.deps, log: h.deps.Logger, locks: h.locks, cleanups: map[string]terminalCleanup{}}, nil
}

// Start runs the sweeper loop (pause deadlines, §6.4; cleanup due-times,
// §5.5) until ctx is cancelled.
func (w *Watcher) Start(ctx context.Context) {
	ticker := time.NewTicker(w.cfg.SweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.Sweep(ctx, w.cfg.now())
		}
	}
}

// RunChanged re-projects the task bound to runCR. It is the AgenticRun
// informer's (and the test harness's) entry point; it is also safe to call
// repeatedly with unchanged state — the store dedupes, so an informer
// relist after a watch drop/reconnect neither duplicates nor loses task
// updates.
func (w *Watcher) RunChanged(ctx context.Context, runCR *agenticv1alpha1.AgenticRun) {
	if runCR == nil {
		return
	}
	taskID := runCR.Annotations[run.AnnotationTaskID]
	if taskID == "" {
		return // not ours
	}
	// Informer payloads are triggers, not authority for destructive effects.
	// Fetch the current object before using its state, then repeat the read
	// under the per-run lock so a stale terminal event cannot remove current
	// credentials or arm cleanup for a newer analysis.
	current, err := w.deps.Readers.GetRun(ctx, w.cfg.Namespace, runCR.Name)
	if err != nil {
		return // transient read failures must not alter cleanup schedules
	}
	if current.UID != runCR.UID || current.Annotations[run.AnnotationTaskID] != taskID {
		return
	}
	if current.UID == "" {
		return
	}
	unlock, locked := w.locks.TryLock(string(current.UID))
	if !locked {
		// The periodic sweep re-lists runs and performs lifecycle reconciliation
		// under the same lock, so a contended informer hint is retried.
		return
	}
	defer unlock()
	w.reconcileRunLocked(ctx, runCR.Name, string(runCR.UID), taskID, w.cfg.now())
}

// reconcileRunLocked reloads the authoritative run and applies every
// adapter-side lifecycle transition while the caller owns that run's lock.
// Sweep calls it with a blocking lock so a watch hint skipped under
// contention is retried from the current CR state.
func (w *Watcher) reconcileRunLocked(ctx context.Context, runName, expectedUID, taskID string, now time.Time) {
	current, err := w.deps.Readers.GetRun(ctx, w.cfg.Namespace, runName)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			w.log.Error("reload AgenticRun under lifecycle lock", "run", runName, "error", err)
		}
		return
	}
	if string(current.UID) != expectedUID || current.Annotations[run.AnnotationTaskID] != taskID ||
		current.UID == "" || taskID == "" {
		return
	}

	rec, ok := w.deps.Store.Task(taskID)
	if !ok {
		// Lazy rehydration (design §2.3) — same shape as the handler's
		// findTask, for events that arrive before any A2A read.
		rec = recordFromRun(current)
	} else if rec.RunUID != "" && rec.RunUID != string(current.UID) {
		// Deterministic name reused by a different run: a taskId
		// collision. Do not project someone else's run onto this task.
		w.log.Warn("run UID mismatch for known task; ignoring event",
			"task_id", taskID, "record_run_uid", rec.RunUID, "event_run_uid", string(current.UID))
		return
	}
	if rec.RunName != "" && rec.RunName != current.Name {
		return
	}
	rec.RunName = current.Name
	rec.RunUID = string(current.UID)

	terminal := RunTerminalPhase(current)

	// §5.5: terminal → schedule Secret cleanup after the grace period;
	// non-terminal (including the revision re-open edge) → cancel it.
	if terminal {
		w.scheduleCleanupAt(current, rec, now)
	} else {
		w.cancelCleanup(rec.RunUID)
	}

	// current was reloaded under the run lock. Use the lock-held helper to
	// avoid a nested lock and keep lifecycle projection linearized with
	// approval/renewal handlers.
	if err := (&Handler{cfg: w.cfg, deps: w.deps, locks: w.locks}).reconcileExecutionTokenLocked(ctx, current); err != nil {
		w.log.Error("reconcile execution token after run event", "run", current.Name, "run_uid", string(current.UID), "error", err)
	}

	// §6.1 steps 1-2: CredentialsExpired=True on a non-terminal run pauses
	// the EXISTING task (auth-required overlay), never a new task.
	if !terminal && CredentialsExpired(current) {
		condition := findCondition(current, agenticv1alpha1.AgenticRunConditionCredentialsExpired)
		conditionAt := condition.LastTransitionTime.Time
		if conditionAt.IsZero() {
			conditionAt = current.CreationTimestamp.Time
		}
		if conditionAt.IsZero() {
			conditionAt = time.Unix(1, 0) // stable fallback for malformed CRs
		}
		// A successful renewal clears AuthPaused immediately, while the
		// operator may take a short time to remove CredentialsExpired. Do not
		// treat a replay of that same condition transition as a new pause.
		if !rec.AuthPaused && conditionAt.After(rec.CredentialPauseObservedAt) {
			rec.CredentialPauseObservedAt = conditionAt
			rec.PauseCount++
			// Persist the counter on the CR (§6.4 durability): a restart
			// or crash-looping adapter must not reset the pause cap. The
			// annotation is restored by recordFromRun on rehydration.
			w.persistPauseCount(ctx, current, rec.PauseCount)
			if rec.PauseCount > w.cfg.MaxPauses {
				// §6.4: total-pause cap — a flapping credential must not
				// pause-loop forever.
				rec.AdapterTerminal = true
				rec.FailureReason = fmt.Sprintf(
					"credential renewal pause cap exceeded (%d pauses, max %d); task failed",
					rec.PauseCount-1, w.cfg.MaxPauses)
				rec.AuthPaused = false
				w.log.Warn("task failed: credential pause cap exceeded",
					"task_id", taskID, "run", rec.RunName, "pauses", rec.PauseCount-1)
			} else {
				rec.AuthPaused = true
				rec.AuthPausedAt = now
				w.log.Info("task paused for credential renewal",
					"task_id", taskID, "run", rec.RunName, "pause_count", rec.PauseCount)
			}
		}
	} else if rec.AuthPaused && (terminal || (rec.HasRun && !CredentialsExpired(current))) {
		// A terminal phase wins over a lingering pause overlay. For a
		// run-backed task, the CR condition is also authoritative when the
		// operator clears it (for example after switching to spoke-SA mode):
		// do not let the old pause deadline fail a healthy run. Pre-submit
		// pauses have no CR and remain governed by the branch above in Sweep.
		rec.AuthPaused = false
		rec.AuthPausedAt = time.Time{}
	}

	// §6.4, restart-durable: a rehydrated pause whose ORIGINAL anchor
	// (the condition's lastTransitionTime, restored by recordFromRun) is
	// already older than the renewal timeout fails immediately — not on
	// the next sweeper tick. Without this, a restart mid-pause would
	// grant the task a fresh window.
	w.expirePauseIfDueAt(rec, now)

	rec, _, err = w.projectTask(ctx, rec)
	if err != nil {
		if errors.Is(err, a2a.ErrTaskNotFound) {
			return // run CR gone (TTL GC); reads observe TaskNotFound
		}
		w.log.Error("project task from run event", "task_id", taskID, "error", err)
		return
	}
	w.deps.Store.PutTask(rec)
}

func executionFinished(runCR *agenticv1alpha1.AgenticRun) bool {
	if cond := findCondition(runCR, agenticv1alpha1.AgenticRunConditionExecuted); cond != nil &&
		(cond.Status == metav1.ConditionTrue || cond.Status == metav1.ConditionFalse) {
		return true
	}
	switch agenticv1alpha1.DerivePhase(runCR.Status.Conditions) {
	case agenticv1alpha1.AgenticRunPhaseVerifying, agenticv1alpha1.AgenticRunPhaseEscalating,
		agenticv1alpha1.AgenticRunPhaseCompleted, agenticv1alpha1.AgenticRunPhaseFailed,
		agenticv1alpha1.AgenticRunPhaseDenied, agenticv1alpha1.AgenticRunPhaseEscalated,
		agenticv1alpha1.AgenticRunPhaseEmergencyStopped:
		return true
	default:
		return false
	}
}

// RunDeleted handles the AgenticRun informer's delete events: the task's
// subscribers must wake (their next projection observes TaskNotFound per
// design §2.3 result retention), and any run-token Secret still standing
// is removed — the adapter-written Secret carries no owner ref, so it
// would otherwise outlive the run.
func (w *Watcher) RunDeleted(ctx context.Context, runCR *agenticv1alpha1.AgenticRun) {
	if runCR == nil {
		return
	}
	w.cancelCleanup(string(runCR.UID))
	if runCR.UID != "" {
		if err := w.deps.Tokens.Delete(ctx, string(runCR.UID)); err != nil {
			w.log.Error("delete run-token Secret for deleted run", "run_uid", string(runCR.UID), "error", err)
		}
	}
	if taskID := runCR.Annotations[run.AnnotationTaskID]; taskID != "" {
		w.deps.Store.Signal(taskID)
	}
}

// ObjectChanged fans a Result CR or AgenticRunApproval event onto the
// parent run's task (design §7.1: results and approvals are watched to
// re-project artifacts and approval state). AgenticRun events are
// dispatched straight to RunChanged.
func (w *Watcher) ObjectChanged(ctx context.Context, obj interface{}) {
	switch o := obj.(type) {
	case *agenticv1alpha1.AgenticRun:
		w.RunChanged(ctx, o)
		return
	case *agenticv1alpha1.AnalysisResult:
		w.runChangedByName(ctx, o.Spec.AgenticRunName)
	case *agenticv1alpha1.ExecutionResult:
		w.runChangedByName(ctx, o.Spec.AgenticRunName)
	case *agenticv1alpha1.VerificationResult:
		w.runChangedByName(ctx, o.Spec.AgenticRunName)
	case *agenticv1alpha1.EscalationResult:
		w.runChangedByName(ctx, o.Spec.AgenticRunName)
	case *agenticv1alpha1.AgenticRunApproval:
		// 1:1 with the run — same name, same namespace (repo C
		// agenticrunapproval_types.go).
		w.runChangedByName(ctx, o.Name)
	}
}

func (w *Watcher) runChangedByName(ctx context.Context, runName string) {
	if runName == "" {
		return
	}
	runCR, err := w.deps.Readers.GetRun(ctx, w.cfg.Namespace, runName)
	if err != nil {
		return // run gone or not yet visible; the run's own informer covers it
	}
	w.RunChanged(ctx, runCR)
}

// expirePauseIfDueAt fails a task whose renewal-pause deadline (§6.4) has
// passed: the pause's ORIGINAL anchor (AuthPausedAt — condition
// lastTransitionTime when rehydrated) plus RenewalTimeout. Shared by the
// locked lifecycle reconciler and Sweep with their authoritative clock.
func (w *Watcher) expirePauseIfDueAt(rec *store.TaskRecord, now time.Time) bool {
	if !rec.AuthPaused || rec.AdapterTerminal {
		return false
	}
	if !now.After(rec.AuthPausedAt.Add(w.cfg.RenewalTimeout)) {
		return false
	}
	rec.AdapterTerminal = true
	rec.AuthPaused = false
	rec.FailureReason = fmt.Sprintf(
		"credential renewal deadline exceeded (%s pause started %s ago); task failed",
		w.cfg.RenewalTimeout, now.Sub(rec.AuthPausedAt).Round(time.Second))
	w.log.Warn("task failed: credential renewal deadline exceeded",
		"task_id", rec.TaskID, "run", rec.RunName)
	return true
}

// persistPauseCount stamps the §6.4 pause counter onto the run CR as the
// a2a.lightspeed.io/credential-pauses annotation (see server.recordFromRun
// for the read-back). Runs as the adapter SA (RBAC: patch on agenticruns).
// Best-effort on failure: the in-memory count still applies until the
// next event re-delivers the CR, and the sweeper bound does not depend on
// it — only the cap's restart durability does.
func (w *Watcher) persistPauseCount(ctx context.Context, runCR *agenticv1alpha1.AgenticRun, count int) {
	if runCR == nil || runCR.Name == "" {
		return // parked pre-submit task: no CR to persist on (documented unrecoverable)
	}
	if s := strconv.Itoa(count); runCR.Annotations[run.AnnotationCredentialPauses] != s {
		updated := runCR.DeepCopy()
		if updated.Annotations == nil {
			updated.Annotations = map[string]string{}
		}
		updated.Annotations[run.AnnotationCredentialPauses] = s
		if err := w.deps.Readers.UpdateRun(ctx, updated); err != nil {
			w.log.Error("persist credential pause count on run CR",
				"run", runCR.Name, "error", err)
		}
	}
}

// Sweep enforces the §6.4 pause deadlines, execution-token reconciliation,
// and the §5.5 cleanup due-times.
// It is exported so tests can drive it deterministically with a controlled
// clock; Start drives it on a ticker in production.
func (w *Watcher) Sweep(ctx context.Context, now time.Time) {
	for _, rec := range w.deps.Store.All() {
		// Run-backed lifecycle state is reconciled below under its per-run
		// lock from a fresh CR read. Only pre-submit parked tasks have no
		// run lock or durable run state to reload here.
		if !rec.HasRun && w.expirePauseIfDueAt(rec, now) {
			w.deps.Store.PutTask(rec)
		}
	}

	// Durable lifecycle and credential cleanup: list AgenticRuns (already
	// granted), then serialize each current projection with handlers. This
	// retries watch hints dropped on lock contention and makes pause deadline
	// terminalization linearizable with renewals. Failed Secret removals are
	// naturally retried on the next sweep.
	if runs, err := w.deps.Readers.ListRuns(ctx, w.cfg.Namespace); err != nil {
		w.log.Error("list AgenticRuns for lifecycle reconciliation", "error", err)
	} else {
		for i := range runs.Items {
			runCR := &runs.Items[i]
			taskID := runCR.Annotations[run.AnnotationTaskID]
			if runCR.UID == "" || taskID == "" {
				continue
			}
			unlock := w.locks.Lock(string(runCR.UID))
			w.reconcileRunLocked(ctx, runCR.Name, string(runCR.UID), taskID, now)
			unlock()
		}
	}

	w.mu.Lock()
	var due []terminalCleanup
	for _, cleanup := range w.cleanups {
		if !now.Before(cleanup.due) {
			due = append(due, cleanup)
		}
	}
	w.mu.Unlock()
	for _, cleanup := range due {
		w.deleteTerminalSecret(ctx, cleanup, now)
	}
}

func (w *Watcher) cancelCleanup(runUID string) {
	if runUID == "" {
		return
	}
	w.mu.Lock()
	delete(w.cleanups, runUID)
	w.mu.Unlock()
}

func (w *Watcher) scheduleCleanupAt(runCR *agenticv1alpha1.AgenticRun, rec *store.TaskRecord, now time.Time) {
	if runCR == nil || rec == nil || runCR.UID == "" || rec.RunUID != string(runCR.UID) || rec.TaskID == "" {
		return
	}
	cleanup := terminalCleanup{
		runUID:             string(runCR.UID),
		runName:            runCR.Name,
		taskID:             rec.TaskID,
		generation:         runCR.Generation,
		analysisGeneration: analysisGeneration(runCR),
		due:                now.Add(w.cfg.TerminalCleanupGrace),
	}
	w.mu.Lock()
	if previous, ok := w.cleanups[cleanup.runUID]; !ok ||
		previous.runName != cleanup.runName || previous.taskID != cleanup.taskID ||
		previous.generation != cleanup.generation || previous.analysisGeneration != cleanup.analysisGeneration {
		w.cleanups[cleanup.runUID] = cleanup
	}
	w.mu.Unlock()
}

func analysisGeneration(runCR *agenticv1alpha1.AgenticRun) int64 {
	if cond := findCondition(runCR, agenticv1alpha1.AgenticRunConditionAnalyzed); cond != nil {
		return cond.ObservedGeneration
	}
	return 0
}

// deleteTerminalSecret validates the scheduled terminal snapshot against the
// current run, UID, task record, phase, and analysis generation under the run
// lock. Read errors retain the schedule for retry; stale snapshots are simply
// discarded and can be replaced by a later current terminal event.
func (w *Watcher) deleteTerminalSecret(ctx context.Context, scheduled terminalCleanup, now time.Time) {
	unlock := w.locks.Lock(scheduled.runUID)
	defer unlock()

	w.mu.Lock()
	currentSchedule, exists := w.cleanups[scheduled.runUID]
	w.mu.Unlock()
	if !exists || !sameCleanup(currentSchedule, scheduled) {
		return // a reopen or newer terminal generation superseded this work
	}

	current, err := w.deps.Readers.GetRun(ctx, w.cfg.Namespace, scheduled.runName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			w.cancelCleanupIfCurrent(scheduled)
			return // no current run can authorize deletion; GC handles its Secret
		}
		w.retryCleanup(scheduled, now)
		w.log.Error("read run before terminal Secret cleanup; retaining retry schedule",
			"run_uid", scheduled.runUID, "error", err)
		return
	}
	if string(current.UID) != scheduled.runUID || current.Annotations[run.AnnotationTaskID] != scheduled.taskID {
		w.cancelCleanupIfCurrent(scheduled)
		return
	}
	rec, ok := w.deps.Store.Task(scheduled.taskID)
	if !ok {
		// A cache miss may be a transient recovery ordering issue. Do not
		// authorize deletion without the corresponding task binding.
		w.retryCleanup(scheduled, now)
		return
	}
	if rec.TaskID != scheduled.taskID || rec.RunName != scheduled.runName || rec.RunUID != scheduled.runUID {
		w.cancelCleanupIfCurrent(scheduled)
		return
	}
	if !RunTerminalPhase(current) || current.Generation != scheduled.generation ||
		analysisGeneration(current) != scheduled.analysisGeneration {
		w.cancelCleanupIfCurrent(scheduled)
		return
	}
	if err := w.deps.Tokens.Delete(ctx, scheduled.runUID); err != nil {
		w.retryCleanup(scheduled, now)
		w.log.Error("delete run-token Secret at terminal; retaining retry schedule",
			"run_uid", scheduled.runUID, "error", err)
		return
	}
	w.cancelCleanupIfCurrent(scheduled)
	w.log.Info("deleted run-token Secret after terminal grace", "run_uid", scheduled.runUID)
}

func (w *Watcher) retryCleanup(scheduled terminalCleanup, now time.Time) {
	w.mu.Lock()
	if current, ok := w.cleanups[scheduled.runUID]; ok && sameCleanup(current, scheduled) {
		current.due = now.Add(w.cfg.SweepInterval)
		w.cleanups[scheduled.runUID] = current
	}
	w.mu.Unlock()
}

func (w *Watcher) cancelCleanupIfCurrent(scheduled terminalCleanup) {
	w.mu.Lock()
	if current, ok := w.cleanups[scheduled.runUID]; ok && sameCleanup(current, scheduled) {
		delete(w.cleanups, scheduled.runUID)
	}
	w.mu.Unlock()
}

func sameCleanup(a, b terminalCleanup) bool {
	return a.runUID == b.runUID && a.runName == b.runName && a.taskID == b.taskID &&
		a.generation == b.generation && a.analysisGeneration == b.analysisGeneration && a.due.Equal(b.due)
}

// projectTask runs the shared projection (Handler.project) so watch events
// and A2A reads can never disagree about a task's state.
func (w *Watcher) projectTask(ctx context.Context, rec *store.TaskRecord) (*store.TaskRecord, *a2a.Task, error) {
	h := Handler{cfg: w.cfg, deps: w.deps}
	return h.project(ctx, rec)
}
