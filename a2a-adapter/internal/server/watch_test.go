package server

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/a2a"
	agenticv1alpha1 "github.com/openshift/lightspeed-agentic-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/openshift/lightspeed-hub/a2a-adapter/internal/run"
	"github.com/openshift/lightspeed-hub/a2a-adapter/internal/store"
)

// ---- helpers ---------------------------------------------------------------

func submit(t *testing.T, h *harness, taskID, contextID, text string) map[string]any {
	t.Helper()
	return resultTask(t, sendMessage(t, h,
		userMessage("m-"+taskID, taskID, contextID, text,
			map[string]any{"targetCluster": "spoke-a"}), freshToken, nil))
}

func getTask(t *testing.T, h *harness, taskID string) map[string]any {
	t.Helper()
	decoded, _ := h.rpc(t, "tasks/get", map[string]any{"id": taskID}, freshToken, nil)
	return resultTask(t, decoded)
}

func followUp(t *testing.T, h *harness, taskID, contextID, msgID, text string, token string, parts any) map[string]any {
	t.Helper()
	msg := userMessage(msgID, taskID, contextID, text, map[string]any{"targetCluster": "spoke-a"})
	if parts != nil {
		msg["parts"] = parts
	}
	return sendMessage(t, h, msg, token, nil)
}

// pauseRun simulates repo C's controller writing the CredentialsExpired
// pause condition (credential_pause.go:30-46) and the informer delivering
// it.
func pauseRun(t *testing.T, h *harness, runName, message string) {
	t.Helper()
	h.kube.setConditions(runName, metav1.Condition{
		Type:    agenticv1alpha1.AgenticRunConditionCredentialsExpired,
		Status:  metav1.ConditionTrue,
		Reason:  agenticv1alpha1.ReasonCredentialsExpired,
		Message: message,
	})
}

func clearPauseRun(t *testing.T, h *harness, runName string) {
	t.Helper()
	h.kube.mutateRun(runName, func(current *agenticv1alpha1.AgenticRun) {
		conditions := current.Status.Conditions[:0]
		for _, cond := range current.Status.Conditions {
			if cond.Type != agenticv1alpha1.AgenticRunConditionCredentialsExpired {
				conditions = append(conditions, cond)
			}
		}
		current.Status.Conditions = conditions
	})
}

// pauseRunAt is pauseRun with an explicit LastTransitionTime — the
// wall-clock anchor recordFromRun restores on rehydration (§6.4 restart
// durability). meta.SetStatusCondition keeps a provided transition time
// when the condition is new.
func pauseRunAt(t *testing.T, h *harness, runName, message string, at time.Time) {
	t.Helper()
	h.kube.setConditions(runName, metav1.Condition{
		Type:               agenticv1alpha1.AgenticRunConditionCredentialsExpired,
		Status:             metav1.ConditionTrue,
		Reason:             agenticv1alpha1.ReasonCredentialsExpired,
		Message:            message,
		LastTransitionTime: metav1.NewTime(at),
	})
}

func workingRun(t *testing.T, h *harness, runName string) {
	t.Helper()
	h.kube.setConditions(runName, metav1.Condition{
		Type: agenticv1alpha1.AgenticRunConditionAnalyzed, Status: metav1.ConditionUnknown,
		Reason: "InProgress", Message: "analysis running",
	})
}

func terminalRun(t *testing.T, h *harness, runName string) {
	t.Helper()
	h.kube.setConditions(runName, metav1.Condition{
		Type: agenticv1alpha1.AgenticRunConditionVerified, Status: metav1.ConditionTrue,
		Reason: "Verified", Message: "done",
	})
}

// syncRun re-delivers a run through the watch path (simulates an informer
// relist after a watch drop/reconnect).
func syncRun(t *testing.T, h *harness, runName string) {
	t.Helper()
	runCR := h.kube.runByName(runName)
	if runCR == nil {
		t.Fatalf("run %s not found for sync", runName)
	}
	h.watcher.RunChanged(context.Background(), runCR)
}

var renewToken = makeToken("acme-agent", testNow.Add(2*time.Hour).Unix())

// ---- renewal bridge (design §6) ---------------------------------------------

// CredentialsExpired on a live run pauses the EXISTING task: same taskId,
// same run, no new CR — auth-required is a state, not a new task (§6.1
// step 2).
func TestBridgePauseSetsAuthRequiredOnExistingTask(t *testing.T) {
	h := newHarness(t)
	submit(t, h, "t1", "c1", "question")
	runName := store.RunName("c1", "t1")
	workingRun(t, h, runName)
	if got := taskState(getTask(t, h, "t1")); got != "working" {
		t.Fatalf("pre-pause state = %q, want working", got)
	}

	pauseRun(t, h, runName, `run credential Secret ls-run-token-x not ready: token rejected (observed resourceVersion "5")`)

	task := getTask(t, h, "t1")
	if got := taskState(task); got != "auth-required" {
		t.Fatalf("post-pause state = %q, want auth-required", got)
	}
	if task["id"] != "t1" || task["contextId"] != "c1" {
		t.Errorf("pause must not create a new task: %v", task)
	}
	if h.kube.created != 1 {
		t.Errorf("creates = %d, want 1 (pause is not a new run)", h.kube.created)
	}
	// The run itself is untouched — the adapter never writes conditions
	// (§6.1: only the Secret write triggers resume).
	runCR := h.kube.runByName(runName)
	if len(runCR.Status.Conditions) != 2 { // Analyzed=Unknown + CredentialsExpired
		t.Errorf("run conditions mutated: %v", runCR.Status.Conditions)
	}
}

// Renewal with valid ownership + fresh A: the Secret is rewritten (value,
// labels preserved, resourceVersion changed — repo C's resume signal) and
// the task returns to its in-flight state (§6.1 steps 4-7).
func TestBridgeRenewalRewritesSecretAndResumes(t *testing.T) {
	h := newHarness(t)
	submit(t, h, "t1", "c1", "question")
	runName := store.RunName("c1", "t1")
	workingRun(t, h, runName)
	pauseRun(t, h, runName, `not ready (observed resourceVersion "1")`)

	runUID := string(h.kube.runByName(runName).UID)
	secretName := run.RunTokenSecretName(runUID)
	before := h.kube.secret(secretName)
	beforeRV := before.ResourceVersion

	task := resultTask(t, followUp(t, h, "t1", "c1", "m-renew", "", renewToken, nil))
	if got := taskState(task); got != "working" {
		t.Fatalf("post-renewal state = %q, want working (in-flight)", got)
	}

	after := h.kube.secret(secretName)
	if after == nil {
		t.Fatal("run-token Secret vanished on renewal")
	}
	if got := string(after.Data[run.RunTokenKey]); got != renewToken {
		t.Errorf("token-a not rewritten (len %d vs %d)", len(got), len(renewToken))
	}
	if after.Labels[run.LabelAgenticRun] != runUID {
		t.Errorf("label agentic-run not preserved: %v", after.Labels)
	}
	if after.Labels[run.LabelSpokeCluster] != "spoke-a" {
		t.Errorf("label spoke-cluster not preserved: %v", after.Labels)
	}
	if after.ResourceVersion == beforeRV {
		t.Errorf("resourceVersion unchanged (%s) — repo C would not resume", after.ResourceVersion)
	}

	// The pause condition still sits on the run (repo C clears it on its
	// own 30 s poll); the adapter must not wait for that to un-pause the
	// task projection.
	if got := taskState(getTask(t, h, "t1")); got != "working" {
		t.Errorf("tasks/get after renewal = %q, want working", got)
	}
}

func TestExecutionCredentialCleanupIsIdempotentAndKeepsRunToken(t *testing.T) {
	h := newHarness(t)
	submit(t, h, "t-clean-exec", "c-clean-exec", "question")
	runName := store.RunName("c-clean-exec", "t-clean-exec")
	runCR := h.kube.runByName(runName)
	secretName := run.RunTokenSecretName(string(runCR.UID))
	writer := &run.TokenWriter{Secrets: h.kube, Namespace: "test-ns", Identity: "adapter-sa"}
	if err := writer.WriteExecution(context.Background(), runCR, "execution-token" /* never logged */); err != nil {
		t.Fatal(err)
	}
	before := h.kube.secret(secretName)
	if string(before.Data[run.ExecutionTokenKey]) != "execution-token" {
		t.Fatal("test setup did not write token-execution")
	}
	writesBeforeFinish := h.kube.secWritt

	// Executed=False is still a completed execution attempt; verification
	// must not retain the write-capable credential.
	h.kube.setConditions(runName, metav1.Condition{
		Type: agenticv1alpha1.AgenticRunConditionExecuted, Status: metav1.ConditionFalse,
		Reason: "Failed", Message: "execution failed",
	})
	after := h.kube.secret(secretName)
	if _, exists := after.Data[run.ExecutionTokenKey]; exists {
		t.Fatal("token-execution remains after Executed=False")
	}
	if len(after.Data[run.RunTokenKey]) == 0 {
		t.Fatal("execution cleanup removed token-a")
	}
	writesAfterFirstCleanup := h.kube.secWritt
	if writesAfterFirstCleanup != writesBeforeFinish+1 {
		t.Fatalf("cleanup writes = %d, want exactly one update", writesAfterFirstCleanup-writesBeforeFinish)
	}

	// The same durable CR can be replayed after restart; cleanup recomputes
	// from its conditions and is an idempotent no-op on the already-clean key.
	syncRun(t, h, runName)
	syncRun(t, h, runName)
	if got := h.kube.secWritt; got != writesAfterFirstCleanup {
		t.Errorf("idempotent execution cleanup updated Secret again: writes %d -> %d", writesAfterFirstCleanup, got)
	}
}

func TestStartupRecoveryRemovesCrashBeforeApprovalExecutionToken(t *testing.T) {
	h := newHarnessWithConfig(t, func(c *Config) { c.SweepInterval = time.Hour })
	runCR := prepareProposed(t, h, "t-crash-approval", "c-crash-approval", []agenticv1alpha1.RemediationOption{{Title: "Restart"}}, true)
	secretName := run.RunTokenSecretName(string(runCR.UID))
	if err := h.handler.deps.Tokens.WriteWithExecution(context.Background(), runCR, "reader", "write"); err != nil {
		t.Fatal(err)
	}
	if got := h.kube.secret(secretName); len(got.Data[run.ExecutionTokenKey]) == 0 {
		t.Fatal("test setup did not simulate a crash after writing token-execution")
	}
	h.kube.mu.Lock()
	h.kube.secrets["test-ns/"+secretName].Annotations[run.AnnotationExecutionTokenWrittenAt] =
		h.clock.Now().Add(-executionTokenPrepareGrace - time.Second).Format(time.RFC3339Nano)
	h.kube.mu.Unlock()
	// No Execution stage was recorded: the process died between credential
	// preparation and the caller-authenticated approval update.
	if err := h.handler.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := h.kube.secret(secretName); len(got.Data[run.ExecutionTokenKey]) != 0 {
		t.Fatal("startup recovery retained token-execution without an approved stage")
	}
}

func TestExecutionReconcilerPreservesFreshPreparationThenRemovesStaleKey(t *testing.T) {
	h := newHarnessWithConfig(t, func(c *Config) { c.SweepInterval = time.Hour })
	runCR := prepareProposed(t, h, "t-fresh-execution-key", "c-fresh-execution-key", []agenticv1alpha1.RemediationOption{{Title: "Restart"}}, true)
	secretName := run.RunTokenSecretName(string(runCR.UID))
	if err := h.handler.deps.Tokens.WriteWithExecution(context.Background(), runCR, "reader", "newly-prepared"); err != nil {
		t.Fatal(err)
	}
	h.kube.mu.Lock()
	h.kube.secrets["test-ns/"+secretName].Annotations[run.AnnotationExecutionTokenWrittenAt] = h.clock.Now().Format(time.RFC3339Nano)
	h.kube.mu.Unlock()

	// No approval stage is committed yet, but this could be the other
	// process reconciling while the first one is between Secret and approval writes.
	if err := h.handler.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := h.kube.secret(secretName); len(got.Data[run.ExecutionTokenKey]) == 0 {
		t.Fatal("reconciler removed token-execution inside the preparation grace window")
	}

	h.kube.mu.Lock()
	h.kube.secrets["test-ns/"+secretName].Annotations[run.AnnotationExecutionTokenWrittenAt] =
		h.clock.Now().Add(-executionTokenPrepareGrace - time.Second).Format(time.RFC3339Nano)
	h.kube.mu.Unlock()
	h.watcher.Sweep(context.Background(), h.clock.Now())
	if got := h.kube.secret(secretName); len(got.Data[run.ExecutionTokenKey]) != 0 {
		t.Fatal("reconciler retained token-execution after the preparation grace expired")
	}
}

func TestExecutionTokenRemovalFailureIsRetriedBySweep(t *testing.T) {
	h := newHarnessWithConfig(t, func(c *Config) { c.SweepInterval = time.Hour })
	runCR := prepareProposed(t, h, "t-remove-retry", "c-remove-retry", []agenticv1alpha1.RemediationOption{{Title: "Restart"}}, true)
	secretName := run.RunTokenSecretName(string(runCR.UID))
	if err := h.handler.deps.Tokens.WriteWithExecution(context.Background(), runCR, "reader", "write"); err != nil {
		t.Fatal(err)
	}
	h.kube.mu.Lock()
	// This test targets the transient Secret-update failure/retry path, not
	// the reconciler's recent-preparation grace period.
	h.kube.secrets["test-ns/"+secretName].Annotations[run.AnnotationExecutionTokenWrittenAt] =
		h.clock.Now().Add(-executionTokenPrepareGrace - time.Second).Format(time.RFC3339Nano)
	h.kube.failRemoveExecution = 1
	h.kube.mu.Unlock()
	h.watcher.Sweep(context.Background(), h.clock.Now())
	if got := h.kube.secret(secretName); len(got.Data[run.ExecutionTokenKey]) == 0 {
		t.Fatal("injected first removal failure unexpectedly removed token-execution")
	}
	h.watcher.Sweep(context.Background(), h.clock.Now())
	if got := h.kube.secret(secretName); len(got.Data[run.ExecutionTokenKey]) != 0 {
		t.Fatal("subsequent sweep did not retry failed token-execution removal")
	}
}

// Wrong-owner renewal: a different principal's follow-up is masked as
// TaskNotFound and the Secret is NOT rewritten (§5.6/§6.1 step 4).
func TestBridgeRenewalWrongOwnerRejected(t *testing.T) {
	h := newHarness(t)
	submit(t, h, "t1", "c1", "question")
	runName := store.RunName("c1", "t1")
	workingRun(t, h, runName)
	pauseRun(t, h, runName, `not ready (observed resourceVersion "1")`)
	runUID := string(h.kube.runByName(runName).UID)
	before := h.kube.secret(run.RunTokenSecretName(runUID))

	otherToken := makeToken("someone-else", testNow.Add(time.Hour).Unix())
	decoded := followUp(t, h, "t1", "c1", "m-renew", "", otherToken, nil)
	errObj, ok := decoded["error"].(map[string]any)
	if !ok {
		t.Fatalf("want TaskNotFound for wrong-owner renewal, got %v", decoded)
	}
	if code, _ := errObj["code"].(float64); code != -32001 {
		t.Errorf("code = %v, want -32001 (task not found)", errObj["code"])
	}

	after := h.kube.secret(run.RunTokenSecretName(runUID))
	if string(after.Data[run.RunTokenKey]) != string(before.Data[run.RunTokenKey]) {
		t.Error("secret rewritten for a non-owner renewal")
	}
	if taskState(getTask(t, h, "t1")) != "auth-required" {
		t.Error("task left the paused state on a rejected renewal")
	}
}

// Wrong target on the renewal follow-up is rejected (target immutability,
// §5.6); changed target ON THE CR is rejected too — verify, don't trust
// the record (spec.targetCluster is CEL-immutable, but the adapter checks).
func TestBridgeRenewalTargetRejected(t *testing.T) {
	h := newHarness(t)
	submit(t, h, "t1", "c1", "question")
	runName := store.RunName("c1", "t1")
	workingRun(t, h, runName)
	pauseRun(t, h, runName, `not ready (observed resourceVersion "1")`)

	// Wrong target in the follow-up's metadata.
	msg := userMessage("m-renew", "t1", "c1", "", map[string]any{"targetCluster": "spoke-B"})
	decoded := sendMessage(t, h, msg, renewToken, nil)
	if _, ok := decoded["error"]; !ok {
		t.Fatalf("want error for mismatched renewal target, got %v", decoded)
	}

	// Changed target on the CR itself (bypassing the adapter record).
	h.kube.mutateRun(runName, func(r *agenticv1alpha1.AgenticRun) {
		r.Spec.TargetCluster = "spoke-Evil"
	})
	decoded = followUp(t, h, "t1", "c1", "m-renew2", "", renewToken, nil)
	if _, ok := decoded["error"]; !ok {
		t.Fatalf("want error for changed CR target, got %v", decoded)
	}
	if h.kube.secWritt != 1 {
		t.Errorf("secret writes = %d, want 1 (no renewal applied)", h.kube.secWritt)
	}
}

// An expired renewal token re-pauses: nothing is written, the task stays
// auth-required; and a second pause after a successful renewal surfaces
// auth-required again (§6.1; expired-again).
func TestBridgeExpiredAgainRenewal(t *testing.T) {
	h := newHarness(t)
	submit(t, h, "t1", "c1", "question")
	runName := store.RunName("c1", "t1")
	workingRun(t, h, runName)
	pauseRun(t, h, runName, `not ready (observed resourceVersion "1")`)

	// Dead A' on the paused task: no write, still paused.
	resultTask(t, followUp(t, h, "t1", "c1", "m-dead", "", expiredToken, nil))
	if h.kube.secWritt != 1 {
		t.Fatalf("secret writes = %d, want 1 (dead token must not be written)", h.kube.secWritt)
	}
	if got := taskState(getTask(t, h, "t1")); got != "auth-required" {
		t.Errorf("state after dead renewal = %q, want auth-required", got)
	}

	// Valid renewal resumes...
	resultTask(t, followUp(t, h, "t1", "c1", "m-good", "", renewToken, nil))
	if got := taskState(getTask(t, h, "t1")); got != "working" {
		t.Fatalf("state after renewal = %q, want working", got)
	}
	// The old condition may remain briefly after renewal. Reconciliation must
	// not re-apply that same pause while waiting for the operator to clear it.
	h.watcher.Sweep(context.Background(), h.clock.Now())
	if got := taskState(getTask(t, h, "t1")); got != "working" {
		t.Fatalf("state after sweep replayed the pre-renewal pause = %q, want working", got)
	}
	// ...and a later re-pause surfaces auth-required again on the same task.
	clearPauseRun(t, h, runName)
	pauseRun(t, h, runName, `not ready again (observed resourceVersion "2")`)
	if got := taskState(getTask(t, h, "t1")); got != "auth-required" {
		t.Errorf("state after re-pause = %q, want auth-required", got)
	}
}

// 403 ≠ renewal (§6.3): an authorization failure surfaced through the
// step condition projects the task as FAILED, never auth-required, and
// never enters the bridge.
func TestBridge403FailureNeverTriggers(t *testing.T) {
	h := newHarness(t)
	submit(t, h, "t1", "c1", "question")
	runName := store.RunName("c1", "t1")

	h.kube.setConditions(runName, metav1.Condition{
		Type: agenticv1alpha1.AgenticRunConditionAnalyzed, Status: metav1.ConditionFalse,
		Reason: "SpokeForbidden", Message: "spoke API returned 403 Forbidden for the sandbox call",
	})
	task := getTask(t, h, "t1")
	if got := taskState(task); got != "failed" {
		t.Fatalf("403 failure maps to %q, want failed", got)
	}
	status, _ := task["status"].(map[string]any)
	if msg, _ := status["message"].(map[string]any); msg == nil {
		t.Error("failed task carries no status message (reason must surface)")
	}

	// A renewal follow-up on the adapter-visible terminal run must not
	// rewrite the Secret (freshness does not revive terminal tasks).
	decoded := followUp(t, h, "t1", "c1", "m-renew", "", renewToken, nil)
	task = resultTask(t, decoded)
	if got := taskState(task); got != "failed" {
		t.Errorf("renewal on failed task = %q, want failed", got)
	}
	if h.kube.secWritt != 1 {
		t.Errorf("secret writes = %d, want 1 (no renewal on failed run)", h.kube.secWritt)
	}
}

// Deadline exhaustion (§6.4): a pause older than RenewalTimeout fails the
// task with a clear credential-timeout reason; a later renewal cannot
// revive it.
func TestBridgeDeadlineExhaustionFailsTask(t *testing.T) {
	h := newHarnessWithConfig(t, func(c *Config) { c.RenewalTimeout = 10 * time.Minute })
	submit(t, h, "t1", "c1", "question")
	runName := store.RunName("c1", "t1")
	workingRun(t, h, runName)
	pauseRun(t, h, runName, `not ready (observed resourceVersion "1")`)
	if got := taskState(getTask(t, h, "t1")); got != "auth-required" {
		t.Fatalf("pre-deadline state = %q", got)
	}

	h.clock.Advance(11 * time.Minute)
	h.watcher.Sweep(context.Background(), h.clock.Now())

	task := getTask(t, h, "t1")
	if got := taskState(task); got != "failed" {
		t.Fatalf("post-deadline state = %q, want failed", got)
	}
	status, _ := task["status"].(map[string]any)
	msg, _ := status["message"].(map[string]any)
	parts, _ := msg["parts"].([]any)
	if len(parts) == 0 || !strings.Contains(fmt.Sprint(parts), "deadline exceeded") {
		t.Errorf("failure reason = %v, want credential-timeout reason", msg)
	}

	// Terminal: renewal is rejected with the task state, not a resume.
	task = resultTask(t, followUp(t, h, "t1", "c1", "m-late", "", renewToken, nil))
	if got := taskState(task); got != "failed" {
		t.Errorf("late renewal = %q, want failed (deadline is terminal)", got)
	}
}

// Pause cap (§6.4): more than MaxPauses total pauses fails the task
// instead of pause-looping forever.
func TestBridgePauseCapExceeded(t *testing.T) {
	h := newHarnessWithConfig(t, func(c *Config) { c.MaxPauses = 1 })
	submit(t, h, "t1", "c1", "question")
	runName := store.RunName("c1", "t1")
	workingRun(t, h, runName)

	pauseRun(t, h, runName, `not ready (observed resourceVersion "1")`)
	if got := taskState(getTask(t, h, "t1")); got != "auth-required" {
		t.Fatalf("first pause = %q, want auth-required", got)
	}
	resultTask(t, followUp(t, h, "t1", "c1", "m-r1", "", renewToken, nil))
	if got := taskState(getTask(t, h, "t1")); got != "working" {
		t.Fatalf("renewal = %q, want working", got)
	}

	clearPauseRun(t, h, runName)
	pauseRun(t, h, runName, `not ready again (observed resourceVersion "2")`)
	task := getTask(t, h, "t1")
	if got := taskState(task); got != "failed" {
		t.Fatalf("second pause (cap 1) = %q, want failed", got)
	}
	status, _ := task["status"].(map[string]any)
	msg, _ := status["message"].(map[string]any)
	if !strings.Contains(fmt.Sprint(msg), "pause cap exceeded") {
		t.Errorf("failure reason = %v, want pause-cap reason", msg)
	}
}

// Duplicate delivery of the renewal message is idempotent: same record,
// exactly one Secret rewrite (§2.2 duplicate messageId semantics).
func TestBridgeDuplicateRenewalIdempotent(t *testing.T) {
	h := newHarness(t)
	submit(t, h, "t1", "c1", "question")
	runName := store.RunName("c1", "t1")
	workingRun(t, h, runName)
	pauseRun(t, h, runName, `not ready (observed resourceVersion "1")`)

	first := resultTask(t, followUp(t, h, "t1", "c1", "m-renew", "", renewToken, nil))
	second := resultTask(t, followUp(t, h, "t1", "c1", "m-renew", "", renewToken, nil))
	if first["id"] != second["id"] {
		t.Errorf("duplicate renewal returned different tasks")
	}
	if h.kube.created != 1 {
		t.Errorf("creates = %d, want 1", h.kube.created)
	}
	// create(1) + renewal(2: AlreadyExists create attempt + update — the
	// fake counts both, like the API server would see them): a duplicate
	// delivery must not add any further write.
	if h.kube.secWritt != 3 {
		t.Errorf("secret writes = %d, want 3 (create + exactly one renewal)", h.kube.secWritt)
	}
}

// ---- terminal-run Secret cleanup (design §5.5) ------------------------------

func TestTerminalCleanupAfterGrace(t *testing.T) {
	h := newHarness(t)
	submit(t, h, "t1", "c1", "question")
	runName := store.RunName("c1", "t1")
	runUID := string(h.kube.runByName(runName).UID)
	secretName := run.RunTokenSecretName(runUID)

	terminalRun(t, h, runName)
	// Terminal, but inside the grace period: the Secret survives.
	h.clock.Advance(10 * time.Second)
	h.watcher.Sweep(context.Background(), h.clock.Now())
	if h.kube.secret(secretName) == nil {
		t.Fatal("Secret deleted before the grace period elapsed")
	}

	// Past the grace: deleted (so the name cannot be re-resolved), and
	// the deletion is not retried.
	h.clock.Advance(30 * time.Second)
	h.watcher.Sweep(context.Background(), h.clock.Now())
	if s := h.kube.secret(secretName); s != nil {
		t.Fatalf("Secret survived terminal+grace: %v", s)
	}
	if h.kube.secDel != 1 {
		t.Errorf("secret deletions = %d, want exactly 1", h.kube.secDel)
	}
	h.watcher.Sweep(context.Background(), h.clock.Now())
	if h.kube.secDel != 1 {
		t.Errorf("secret deletions = %d after re-sweep, want 1 (no retry spam)", h.kube.secDel)
	}
}

// The revision re-open edge: a run observed terminal and then non-terminal
// before the grace elapses keeps its Secret (the §5.5 edge — park new A on
// the next message, never auto-resurrect).
func TestTerminalCleanupCancelledByReopen(t *testing.T) {
	h := newHarness(t)
	submit(t, h, "t1", "c1", "question")
	runName := store.RunName("c1", "t1")
	runUID := string(h.kube.runByName(runName).UID)
	secretName := run.RunTokenSecretName(runUID)

	terminalRun(t, h, runName)
	// Re-open before the grace elapses (revision feedback re-analysis):
	// replace the conditions wholesale — repo C's re-open clears the
	// terminal state, so the run is Analyzing again.
	h.kube.mutateRun(runName, func(r *agenticv1alpha1.AgenticRun) {
		r.Status.Conditions = []metav1.Condition{{
			Type: agenticv1alpha1.AgenticRunConditionAnalyzed, Status: metav1.ConditionUnknown,
			Reason: "InProgress", Message: "re-analyzing with feedback",
		}}
	})
	h.clock.Advance(5 * time.Minute)
	h.watcher.Sweep(context.Background(), h.clock.Now())
	if h.kube.secret(secretName) == nil {
		t.Fatal("Secret deleted although the run re-opened within the grace period")
	}
}

func TestStaleTerminalEventDoesNotRemoveCurrentExecutionCredential(t *testing.T) {
	runEx := &sequenceExchanger{tokens: []string{"run-initial", "run-approved"}}
	executionEx := &sequenceExchanger{tokens: []string{"execution-approved"}}
	h := newHarnessFull(t, newFakeKube(), &testClock{t: testNow}, nil, func(d *Deps) {
		d.RunTokenExchanger = runEx
		d.ExecutionTokenExchanger = executionEx
	})
	runCR := prepareProposed(t, h, "t-stale-terminal-event", "c-stale-terminal-event", []agenticv1alpha1.RemediationOption{{Title: "Restart"}}, true)
	resultTask(t, sendMessage(t, h, approvalMessage("m-approve-current", "t-stale-terminal-event", "c-stale-terminal-event", "approve", map[string]any{
		"stage": "Execution", "option": float64(0),
	}), freshToken, nil))
	secretName := run.RunTokenSecretName(string(runCR.UID))
	if got := h.kube.secret(secretName); len(got.Data[run.ExecutionTokenKey]) == 0 {
		t.Fatal("test setup did not create current execution credential")
	}
	staleTerminal := h.kube.runByName(runCR.Name)
	staleTerminal.Status.Conditions = []metav1.Condition{
		{Type: agenticv1alpha1.AgenticRunConditionAnalyzed, Status: metav1.ConditionTrue, ObservedGeneration: staleTerminal.Generation},
		{Type: agenticv1alpha1.AgenticRunConditionExecuted, Status: metav1.ConditionTrue, ObservedGeneration: staleTerminal.Generation},
		{Type: agenticv1alpha1.AgenticRunConditionVerified, Status: metav1.ConditionTrue, ObservedGeneration: staleTerminal.Generation},
	}
	if !RunTerminalPhase(staleTerminal) {
		t.Fatal("test setup event is not terminal")
	}
	// The API server has already reopened the same UID for a current approved
	// execution; only the informer payload is stale.
	h.watcher.RunChanged(context.Background(), staleTerminal)
	secret := h.kube.secret(secretName)
	if secret == nil || string(secret.Data[run.ExecutionTokenKey]) != "execution-approved" {
		t.Fatal("stale terminal event removed the current execution credential")
	}
	h.clock.Advance(time.Minute)
	h.watcher.Sweep(context.Background(), h.clock.Now())
	if h.kube.secret(secretName) == nil {
		t.Fatal("stale terminal event scheduled whole-Secret deletion for the reopened run")
	}
}

func TestTerminalCleanupRevalidatesReopenedGenerationBeforeDelete(t *testing.T) {
	h := newHarness(t)
	submit(t, h, "t-terminal-generation", "c-terminal-generation", "question")
	runName := store.RunName("c-terminal-generation", "t-terminal-generation")
	runCR := h.kube.runByName(runName)
	secretName := run.RunTokenSecretName(string(runCR.UID))
	terminalRun(t, h, runName)

	// Simulate the current object reopening into a new analysis generation
	// without delivering its watch event. The old terminal schedule must not
	// authorize deletion of the run-token Secret.
	h.kube.mu.Lock()
	current := h.kube.runs["test-ns/"+runName]
	current.Generation++
	current.Status.Conditions = []metav1.Condition{{
		Type: agenticv1alpha1.AgenticRunConditionAnalyzed, Status: metav1.ConditionUnknown,
		Reason: "InProgress", Message: "re-analysis running", ObservedGeneration: current.Generation,
	}}
	h.kube.mu.Unlock()
	h.clock.Advance(time.Minute)
	h.watcher.Sweep(context.Background(), h.clock.Now())
	if secret := h.kube.secret(secretName); secret == nil || len(secret.Data[run.RunTokenKey]) == 0 {
		t.Fatal("old terminal cleanup deleted the reopened generation's run-token Secret")
	}
}

func TestTerminalCleanupRetainsScheduleAfterTransientRunReadFailure(t *testing.T) {
	h := newHarness(t)
	submit(t, h, "t-terminal-read-retry", "c-terminal-read-retry", "question")
	runName := store.RunName("c-terminal-read-retry", "t-terminal-read-retry")
	runUID := string(h.kube.runByName(runName).UID)
	secretName := run.RunTokenSecretName(runUID)
	terminalRun(t, h, runName)
	h.clock.Advance(time.Minute)

	// The sweeper's execution reconciler consumes the first injected failure;
	// terminal cleanup consumes the second. It must preserve/rearm the due
	// schedule rather than treating an unreadable run as authorization.
	h.kube.mu.Lock()
	h.kube.failGetRun = 2
	h.kube.mu.Unlock()
	h.watcher.Sweep(context.Background(), h.clock.Now())
	if h.kube.secret(secretName) == nil {
		t.Fatal("transient run read failure led to terminal Secret deletion")
	}
	h.clock.Advance(time.Second)
	h.watcher.Sweep(context.Background(), h.clock.Now())
	if h.kube.secret(secretName) != nil {
		t.Fatal("terminal cleanup schedule was lost instead of retrying after transient read failure")
	}
}

// A deleted run's Secret goes too (the adapter-written Secret carries no
// owner ref), and subscribers wake to observe TaskNotFound.
func TestRunDeletedCleansSecretAndSignals(t *testing.T) {
	h := newHarness(t)
	submit(t, h, "t1", "c1", "question")
	runName := store.RunName("c1", "t1")
	runUID := string(h.kube.runByName(runName).UID)

	runCR := h.kube.runByName(runName)
	h.kube.deleteRun(runName)                         // the API object disappears…
	h.watcher.RunDeleted(context.Background(), runCR) // …and the informer delivers the delete
	if h.kube.secret(run.RunTokenSecretName(runUID)) != nil {
		t.Error("run-token Secret survived run deletion")
	}

	decoded, _ := h.rpc(t, "tasks/get", map[string]any{"id": "t1"}, freshToken, nil)
	errObj, ok := decoded["error"].(map[string]any)
	if !ok {
		t.Fatalf("want TaskNotFound after run GC, got %v", decoded)
	}
	if code, _ := errObj["code"].(float64); code != -32001 {
		t.Errorf("code = %v, want -32001", errObj["code"])
	}
}

// ---- watch reconnect (design §7.1) ------------------------------------------

// An informer relist (watch drop → reconnect) re-delivers every object;
// unchanged projections are silent, changed ones are delivered exactly
// once — no duplicates, no losses.
func TestWatchReconnectNoDuplicateNoLoss(t *testing.T) {
	h := newHarness(t)
	submit(t, h, "t1", "c1", "question")
	runName := store.RunName("c1", "t1")

	signals, cancel := h.watcher.deps.Store.Subscribe("t1")
	defer cancel()
	drain := func() int {
		n := 0
		for {
			select {
			case <-signals:
				n++
			default:
				return n
			}
		}
	}

	drain()
	// Reconnect-style relist: same object delivered twice.
	syncRun(t, h, runName)
	syncRun(t, h, runName)
	if n := drain(); n != 0 {
		t.Errorf("unchanged relist signaled %d times, want 0 (dedupe)", n)
	}

	workingRun(t, h, runName) // one real change → one signal
	if n := drain(); n != 1 {
		t.Errorf("changed run signaled %d times, want 1", n)
	}
	// And the update itself is not lost: the projection reflects it.
	if got := taskState(getTask(t, h, "t1")); got != "working" {
		t.Errorf("state after change = %q, want working", got)
	}
}

// ---- phase mapping table (design §7.2) ---------------------------------------

func TestPhaseMappingTable(t *testing.T) {
	cases := []struct {
		name       string
		conditions []metav1.Condition
		want       string
	}{
		{"pending", nil, "submitted"},
		{"analyzing", []metav1.Condition{{Type: "Analyzed", Status: metav1.ConditionUnknown}}, "working"},
		{"executing", []metav1.Condition{
			{Type: "Analyzed", Status: metav1.ConditionTrue},
			{Type: "Executed", Status: metav1.ConditionUnknown}}, "working"},
		{"verifying", []metav1.Condition{
			{Type: "Analyzed", Status: metav1.ConditionTrue},
			{Type: "Executed", Status: metav1.ConditionTrue},
			{Type: "Verified", Status: metav1.ConditionUnknown}}, "working"},
		{"proposed", []metav1.Condition{
			{Type: "Analyzed", Status: metav1.ConditionTrue, Reason: "AnalysisComplete"}}, "input-required"},
		{"completed-no-action", []metav1.Condition{
			{Type: "Analyzed", Status: metav1.ConditionTrue, Reason: agenticv1alpha1.ReasonNoActionRequired}}, "completed"},
		{"completed-verified", []metav1.Condition{
			{Type: "Analyzed", Status: metav1.ConditionTrue},
			{Type: "Executed", Status: metav1.ConditionTrue},
			{Type: "Verified", Status: metav1.ConditionTrue}}, "completed"},
		{"failed", []metav1.Condition{
			{Type: "Analyzed", Status: metav1.ConditionFalse, Reason: "AgentTimeout"}}, "failed"},
		{"denied", []metav1.Condition{{Type: "Denied", Status: metav1.ConditionTrue}}, "failed"},
		{"escalating", []metav1.Condition{
			{Type: "Analyzed", Status: metav1.ConditionTrue},
			{Type: "Executed", Status: metav1.ConditionTrue},
			{Type: "Verified", Status: metav1.ConditionFalse},
			{Type: "Escalated", Status: metav1.ConditionUnknown}}, "working"},
		{"escalated", []metav1.Condition{
			{Type: "Escalated", Status: metav1.ConditionTrue}}, "failed"},
		{"emergency-stopped", []metav1.Condition{
			{Type: "EmergencyStopped", Status: metav1.ConditionTrue}}, "canceled"},
		{"stopping", []metav1.Condition{
			{Type: "Stopping", Status: metav1.ConditionTrue},
			{Type: "Analyzed", Status: metav1.ConditionUnknown}}, "canceled"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runCR := &agenticv1alpha1.AgenticRun{}
			runCR.Status.Conditions = tc.conditions
			if got := string(TaskStateFor(runCR)); got != tc.want {
				t.Errorf("TaskStateFor(%s) = %q, want %q", tc.name, got, tc.want)
			}
		})
	}

	// The CredentialsExpired overlay is applied by the projection on top
	// of any non-terminal phase (it does not participate in DerivePhase).
	runCR := &agenticv1alpha1.AgenticRun{}
	runCR.Status.Conditions = []metav1.Condition{
		{Type: "Analyzed", Status: metav1.ConditionUnknown},
		{Type: "CredentialsExpired", Status: metav1.ConditionTrue, Reason: "CredentialsExpired"},
	}
	if got := TaskStateFor(runCR); got != a2a.TaskStateWorking {
		t.Errorf("paused run derives %q, want working (overlay is record-side)", got)
	}
}

// ---- approvals (design §7.1/§7.2) ---------------------------------------------

// Proposed → input-required, with the status message naming the pending
// stage and the decision DataPart shape; the decision is forwarded to the
// AgenticRunApproval CR as the caller.
func TestApprovalInputRequiredAndDecision(t *testing.T) {
	h := newHarness(t)
	submit(t, h, "t1", "c1", "question")
	runName := store.RunName("c1", "t1")

	// Analysis proposes options → Proposed → input-required.
	h.kube.setConditions(runName, metav1.Condition{
		Type: agenticv1alpha1.AgenticRunConditionAnalyzed, Status: metav1.ConditionTrue,
		Reason: "AnalysisComplete", Message: "options ready",
	})
	task := getTask(t, h, "t1")
	if got := taskState(task); got != "input-required" {
		t.Fatalf("proposed run = %q, want input-required", got)
	}
	status, _ := task["status"].(map[string]any)
	msg, _ := status["message"].(map[string]any)
	if !strings.Contains(fmt.Sprint(msg), "approve") {
		t.Errorf("input-required message does not describe the decision DataPart: %v", msg)
	}

	// The approval CR exists (repo C creates it with the run, 1:1);
	// initially no stages — the decision is the caller's to append.
	h.kube.putObject(&agenticv1alpha1.AgenticRunApproval{
		ObjectMeta: metav1.ObjectMeta{Name: runName, Namespace: "test-ns"},
	})

	// Decision follow-up: {"approve": {"stage": "Execution", "option": 0}}.
	parts := []map[string]any{
		{"kind": "data", "data": map[string]any{
			"approve": map[string]any{"stage": "Execution", "option": 0}}},
	}
	task = resultTask(t, followUp(t, h, "t1", "c1", "m-approve", "", renewToken, parts))
	approval, err := h.kube.GetApproval(context.Background(), "test-ns", runName)
	if err != nil {
		t.Fatalf("approval CR not written: %v", err)
	}
	if len(approval.Spec.Stages) != 1 {
		t.Fatalf("approval stages = %v", approval.Spec.Stages)
	}
	stage := approval.Spec.Stages[0]
	if stage.Type != agenticv1alpha1.ApprovalStageExecution ||
		stage.Decision != agenticv1alpha1.ApprovalDecisionApproved ||
		stage.Execution == nil || stage.Execution.Option == nil || *stage.Execution.Option != 0 {
		t.Errorf("approval stage = %+v", stage)
	}

	// Duplicate delivery of the same decision is a no-op.
	resultTask(t, followUp(t, h, "t1", "c1", "m-approve", "", renewToken, parts))
	if approval, _ := h.kube.GetApproval(context.Background(), "test-ns", runName); len(approval.Spec.Stages) != 1 {
		t.Errorf("duplicate decision appended a second stage: %+v", approval.Spec.Stages)
	}

	// An invalid stage is a protocol error, never a guess.
	bad := []map[string]any{
		{"kind": "data", "data": map[string]any{"approve": map[string]any{"stage": "Bogus"}}},
	}
	if decoded := followUp(t, h, "t1", "c1", "m-bad", "", renewToken, bad); decoded["error"] == nil {
		t.Error("invalid approval stage accepted")
	}
}

// ---- artifacts + usage (design §7.2) -------------------------------------------

func TestArtifactsAndTokenUsage(t *testing.T) {
	h := newHarness(t)
	submit(t, h, "t1", "c1", "question")
	runName := store.RunName("c1", "t1")

	result := &agenticv1alpha1.AnalysisResult{
		ObjectMeta: metav1.ObjectMeta{Name: "ar-1", Namespace: "test-ns", UID: "result-uid-1"},
		Spec:       agenticv1alpha1.AnalysisResultSpec{AgenticRunName: runName},
		Status: agenticv1alpha1.AnalysisResultStatus{
			Options: []agenticv1alpha1.RemediationOption{{Title: "Increase memory limit"}},
			TokenUsage: agenticv1alpha1.TokenUsage{
				InputTokens: ptr(int64(1200)), OutputTokens: ptr(int64(300)),
			},
		},
	}
	h.kube.putObject(result)
	// Reference it from the run's step status (repo C writes this ref).
	h.kube.mutateRun(runName, func(r *agenticv1alpha1.AgenticRun) {
		r.Status.Steps.Analysis.Results = []agenticv1alpha1.StepResultRef{
			{Name: "ar-1", Outcome: agenticv1alpha1.ActionOutcomeSucceeded},
		}
	})

	task := getTask(t, h, "t1")
	artifacts, ok := task["artifacts"].([]any)
	if !ok || len(artifacts) != 1 {
		t.Fatalf("artifacts = %v", task["artifacts"])
	}
	art, _ := artifacts[0].(map[string]any)
	if art["artifactId"] != "result-result-uid-1" {
		t.Errorf("artifact ID = %v, want derived from the Result CR UID (stable)", art["artifactId"])
	}
	if art["name"] != "analysis" {
		t.Errorf("artifact name = %v", art["name"])
	}
	meta, _ := task["metadata"].(map[string]any)
	usage, _ := meta["tokenUsage"].(map[string]any)
	if usage == nil || usage["inputTokens"] != float64(1200) || usage["outputTokens"] != float64(300) {
		t.Errorf("token usage metadata = %v", meta)
	}
}

// ---- resubscribe + stream push (design §2.3/§7.1) -------------------------------

// tasks/resubscribe: an owner reattaching receives push updates to
// terminal; a non-owner is masked with TaskNotFound (§5.6 ownership on
// every operation).
func TestResubscribe(t *testing.T) {
	h := newHarness(t)
	submit(t, h, "t1", "c1", "question")
	runName := store.RunName("c1", "t1")
	workingRun(t, h, runName)

	// Non-owner: masked — the stream carries a TaskNotFound error, never
	// the task or its updates.
	otherToken := makeToken("someone-else", testNow.Add(time.Hour).Unix())
	body := map[string]any{"jsonrpc": "2.0", "id": 3, "method": "tasks/resubscribe", "params": map[string]any{"id": "t1"}}
	raw, _ := json.Marshal(body)
	req, _ := http.NewRequest(http.MethodPost, h.server.URL+"/", strings.NewReader(string(raw)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+otherToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	nonOwner := scanSSE(t, resp.Body, false)
	if nonOwner == nil {
		t.Fatal("non-owner resubscribe produced no events")
	}
	if _, isErr := nonOwner["error"]; !isErr {
		t.Fatalf("non-owner resubscribe must error, got %v", nonOwner)
	}

	// Owner: push updates to terminal via the watch signal.
	req2, _ := http.NewRequest(http.MethodPost, h.server.URL+"/", strings.NewReader(string(raw)))
	req2.Header.Set("Content-Type", "application/json")
	req2.Header.Set("Authorization", "Bearer "+freshToken)
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	go func() {
		time.Sleep(50 * time.Millisecond)
		terminalRun(t, h, runName)
	}()
	last := scanSSE(t, resp2.Body, true)
	if last == nil {
		t.Fatal("no events on resubscribe")
	}
	result, _ := last["result"].(map[string]any)
	status, _ := result["status"].(map[string]any)
	if status["state"] != "completed" {
		t.Errorf("final resubscribe state = %v, want completed (push, not poll)", status["state"])
	}
}

// scanSSE reads a text/event-stream body and returns the last decoded
// data event; stops after a final event when wantFinal is set.
func scanSSE(t *testing.T, r io.ReadCloser, wantFinal bool) map[string]any {
	t.Helper()
	defer r.Close()
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 1<<20), 1<<20)
	var last map[string]any
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		var ev map[string]any
		if err := json.Unmarshal([]byte(line[len("data: "):]), &ev); err != nil {
			t.Fatalf("bad SSE data: %q", line)
		}
		last = ev
		if result, ok := ev["result"].(map[string]any); ok && wantFinal {
			if final, _ := result["final"].(bool); final {
				break
			}
		}
	}
	return last
}

// ---- cancel semantics unchanged (§2.3) -------------------------------------------

func TestCancelStoppingMapsCanceled(t *testing.T) {
	h := newHarness(t)
	submit(t, h, "t1", "c1", "question")
	runName := store.RunName("c1", "t1")

	// Live task: still not cancelable (Q6 unresolved).
	decoded, _ := h.rpc(t, "tasks/cancel", map[string]any{"id": "t1"}, freshToken, nil)
	if _, ok := decoded["error"]; !ok {
		t.Fatal("live task cancel should fail (Q6)")
	}

	// A run already Stopping projects canceled, so cancel returns it.
	h.kube.setConditions(runName, metav1.Condition{
		Type: agenticv1alpha1.AgenticRunConditionStopping, Status: metav1.ConditionTrue,
		Reason: "Stopping", Message: "stop accepted",
	})
	decoded, _ = h.rpc(t, "tasks/cancel", map[string]any{"id": "t1"}, freshToken, nil)
	if got := taskState(resultTask(t, decoded)); got != "canceled" {
		t.Errorf("cancel during Stopping = %q, want canceled (§7.2)", got)
	}
}

// The §2.3/§6.4 interaction, end-to-end on a real clock: while a task
// sits auth-required the stream deadline is NOT consumed — the stream
// outlives it and ends only when the renewal deadline fails the task.
func TestStreamPauseDoesNotConsumeDeadline(t *testing.T) {
	h := newHarnessWithConfig(t, func(c *Config) {
		c.Now = nil // real clock
		c.StreamDeadline = 150 * time.Millisecond
		c.RenewalTimeout = 400 * time.Millisecond
		c.SweepInterval = 10 * time.Millisecond
	})
	liveToken := makeToken("acme-agent", time.Now().Add(time.Hour).Unix())
	msg := userMessage("m1", "t1", "c1", "q", map[string]any{"targetCluster": "spoke-a"})
	body := map[string]any{"jsonrpc": "2.0", "id": 9, "method": "message/stream",
		"params": map[string]any{"message": msg}}
	raw, _ := json.Marshal(body)
	req, _ := http.NewRequest(http.MethodPost, h.server.URL+"/", strings.NewReader(string(raw)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+liveToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}

	runName := store.RunName("c1", "t1")
	// Pause immediately (before the 150 ms stream deadline elapses).
	time.Sleep(30 * time.Millisecond)
	pauseRun(t, h, runName, `not ready (observed resourceVersion "1")`)

	last := scanSSE(t, resp.Body, true)
	result, ok := last["result"].(map[string]any)
	if !ok {
		t.Fatalf("last event = %v", last)
	}
	status, _ := result["status"].(map[string]any)
	if status["state"] != "failed" {
		t.Fatalf("final state = %v, want failed (renewal deadline, not stream deadline)", status["state"])
	}
	smsg, _ := status["message"].(map[string]any)
	if !strings.Contains(fmt.Sprint(smsg), "renewal deadline exceeded") {
		t.Errorf("final message = %v, want the credential-timeout reason (pause must not consume the stream deadline)", smsg)
	}
}

func ptr[T any](v T) *T { return &v }

// ---- §6.4 restart durability --------------------------------------------------

// clearPause simulates repo C's 30 s poll clearing the CredentialsExpired
// condition after a successful renewal (the fake API server never clears
// it on its own).
func clearPause(t *testing.T, h *harness, runName string) {
	t.Helper()
	h.kube.mutateRun(runName, func(r *agenticv1alpha1.AgenticRun) {
		kept := r.Status.Conditions[:0]
		for _, c := range r.Status.Conditions {
			if c.Type != agenticv1alpha1.AgenticRunConditionCredentialsExpired {
				kept = append(kept, c)
			}
		}
		r.Status.Conditions = kept
	})
}

// restartHarness simulates an adapter restart: a fresh store, handler and
// watcher over the SAME fake kube and clock (the API server state and the
// time domain survive; the adapter's memory does not).
func restartHarness(t *testing.T, h *harness, tune func(*Config)) *harness {
	t.Helper()
	return newHarnessSharing(t, h.kube, h.clock, tune)
}

// A restart mid-pause must not extend the §6.4 deadline: the pause anchor
// is the CredentialsExpired condition's lastTransitionTime (restored by
// recordFromRun), not the restart time. The deadline fires at the ORIGINAL
// wall-clock.
func TestRestartMidPauseDeadlineDurable(t *testing.T) {
	h := newHarnessWithConfig(t, func(c *Config) { c.RenewalTimeout = 10 * time.Minute })
	submit(t, h, "t1", "c1", "question")
	runName := store.RunName("c1", "t1")
	workingRun(t, h, runName)
	pauseRunAt(t, h, runName, `not ready (observed resourceVersion "1")`, testNow)
	if got := taskState(getTask(t, h, "t1")); got != "auth-required" {
		t.Fatalf("pre-restart state = %q, want auth-required", got)
	}

	// "Restart" two minutes into the pause.
	h.clock.Advance(2 * time.Minute)
	h2 := restartHarness(t, h, func(c *Config) { c.RenewalTimeout = 10 * time.Minute })
	syncRun(t, h2, runName)
	if got := taskState(getTask(t, h2, "t1")); got != "auth-required" {
		t.Fatalf("post-restart state = %q, want auth-required (pause survives)", got)
	}

	// 9 minutes after the ORIGINAL pause (7 after the restart): still
	// paused — a re-anchored deadline (restart+10m) would not have fired
	// here either, so this only pins no early failure.
	h.clock.Advance(7 * time.Minute)
	h2.watcher.Sweep(context.Background(), h.clock.Now())
	if got := taskState(getTask(t, h2, "t1")); got != "auth-required" {
		t.Fatalf("state at T0+9m = %q, want auth-required (deadline is T0+10m)", got)
	}

	// Past the ORIGINAL deadline (T0+11m, only 9m after the restart): the
	// task must fail — a restart-anchored deadline would still have a
	// minute left.
	h.clock.Advance(2 * time.Minute)
	h2.watcher.Sweep(context.Background(), h.clock.Now())
	task := getTask(t, h2, "t1")
	if got := taskState(task); got != "failed" {
		t.Fatalf("state at T0+11m = %q, want failed (original wall-clock deadline)", got)
	}
	status, _ := task["status"].(map[string]any)
	if msg, _ := status["message"].(map[string]any); msg == nil {
		t.Error("failed task carries no status message (deadline reason must surface)")
	}
}

// The §6.4 pause cap survives a restart: the counter is persisted on the
// run CR (a2a.lightspeed.io/credential-pauses) and restored on
// rehydration, so a crash-looping adapter cannot reset it.
func TestRestartPauseCountDurable(t *testing.T) {
	h := newHarnessWithConfig(t, func(c *Config) { c.MaxPauses = 2 })
	submit(t, h, "t1", "c1", "question")
	runName := store.RunName("c1", "t1")
	workingRun(t, h, runName)

	// First pause: count 1, persisted on the CR.
	pauseRunAt(t, h, runName, `not ready (observed resourceVersion "1")`, testNow)
	if got := h.kube.runByName(runName).Annotations[run.AnnotationCredentialPauses]; got != "1" {
		t.Fatalf("credential-pauses annotation = %q, want 1 after first pause", got)
	}
	resultTask(t, followUp(t, h, "t1", "c1", "m-r1", "", renewToken, nil))
	clearPause(t, h, runName) // repo C's 30 s poll clears the condition

	// Restart: fresh store, same CRs.
	h2 := restartHarness(t, h, func(c *Config) { c.MaxPauses = 2 })
	syncRun(t, h2, runName)

	// Second pause after the restart: count continues at 2 (not reset to
	// 1) — still within the cap of 2.
	pauseRun(t, h2, runName, `not ready again (observed resourceVersion "2")`)
	if got := taskState(getTask(t, h2, "t1")); got != "auth-required" {
		t.Fatalf("second pause after restart = %q, want auth-required (count 2 <= cap 2)", got)
	}
	if got := h2.kube.runByName(runName).Annotations[run.AnnotationCredentialPauses]; got != "2" {
		t.Fatalf("credential-pauses annotation = %q, want 2 (count must survive the restart)", got)
	}
	resultTask(t, followUp(t, h2, "t1", "c1", "m-r2", "", renewToken, nil))
	clearPause(t, h2, runName)

	// Third pause: 3 > cap 2 → failed. Without persistence the restarted
	// counter would sit at 2 and the task would pause-loop forever.
	pauseRun(t, h2, runName, `not ready again (observed resourceVersion "3")`)
	task := getTask(t, h2, "t1")
	if got := taskState(task); got != "failed" {
		t.Fatalf("third pause after restart = %q, want failed (durable cap)", got)
	}
	status, _ := task["status"].(map[string]any)
	if msg, _ := status["message"].(map[string]any); !strings.Contains(fmt.Sprint(msg), "pause cap exceeded") {
		t.Errorf("failure reason = %v, want pause-cap reason", msg)
	}
}

// A rehydrated pause whose condition lastTransitionTime is ALREADY older
// than the renewal timeout fails immediately on rehydration — not on the
// next sweeper tick, and never with a fresh window.
func TestRestartExpiredConditionFailsImmediately(t *testing.T) {
	h := newHarness(t) // RenewalTimeout = 1h (frozen clock)
	submit(t, h, "t1", "c1", "question")
	runName := store.RunName("c1", "t1")
	workingRun(t, h, runName)
	// The pause started two hours before the adapter's "now".
	pauseRunAt(t, h, runName, `not ready (observed resourceVersion "1")`, testNow.Add(-2*time.Hour))

	h2 := restartHarness(t, h, nil)
	syncRun(t, h2, runName) // rehydration event — must fail the task inline

	task := getTask(t, h2, "t1")
	if got := taskState(task); got != "failed" {
		t.Fatalf("post-rehydration state = %q, want failed immediately (no sweep, no fresh window)", got)
	}
	status, _ := task["status"].(map[string]any)
	msg, _ := status["message"].(map[string]any)
	if !strings.Contains(fmt.Sprint(msg), "deadline exceeded") {
		t.Errorf("failure reason = %v, want credential-deadline reason", msg)
	}
}

// ---- store collision after restart (design §5.6) -------------------------------

// After a restart the in-memory store is empty; a foreign principal
// sending the victim's taskID with a fresh messageId must get TaskNotFound
// (masked) and must NOT shadow the victim's task record or create a new
// run — handleSend rehydrates-and-masks exactly like findTask.
func TestRestartForeignPrincipalTaskIDMasked(t *testing.T) {
	h := newHarness(t)
	submit(t, h, "t1", "c1", "question")
	runName := store.RunName("c1", "t1")

	h2 := restartHarness(t, h, nil)
	otherToken := makeToken("someone-else", testNow.Add(time.Hour).Unix())
	msg := userMessage("m-evil", "t1", "c-evil", "hostile question",
		map[string]any{"targetCluster": "spoke-a"})
	decoded := sendMessage(t, h2, msg, otherToken, nil)

	errObj, ok := decoded["error"].(map[string]any)
	if !ok {
		t.Fatalf("want TaskNotFound for foreign principal on victim taskID, got %v", decoded)
	}
	if code, _ := errObj["code"].(float64); code != -32001 {
		t.Errorf("code = %v, want -32001 (masked TaskNotFound)", errObj["code"])
	}

	// No shadowing record was created and no new run submitted.
	if h2.kube.created != 1 {
		t.Errorf("AgenticRun creates = %d, want 1 (no shadowing run)", h2.kube.created)
	}
	// The victim still owns and can read their task through the restarted
	// adapter (the store holds the VICTIM's rehydrated record, not the
	// attacker's).
	task := getTask(t, h2, "t1")
	if task["id"] != "t1" {
		t.Errorf("victim task no longer readable after the foreign send: %v", task)
	}
	// And the attacker's context never landed on the run.
	if got := h2.kube.runByName(runName).Annotations[run.AnnotationContextID]; got != "c1" {
		t.Errorf("context-id annotation = %q, want c1 (run must be untouched)", got)
	}
}

// ---- /healthz -------------------------------------------------------------------

func TestHealthz(t *testing.T) {
	h := newHarness(t)
	req, err := http.NewRequest(http.MethodGet, h.server.URL+"/healthz", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req) // NO Authorization header
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /healthz without credentials = %d, want 200", resp.StatusCode)
	}
}

type blockingExecutionExchanger struct {
	mu      sync.Mutex
	calls   int
	entered chan struct{}
	release chan struct{}
}

func (e *blockingExecutionExchanger) Exchange(context.Context, string) (string, error) {
	e.mu.Lock()
	e.calls++
	call := e.calls
	e.mu.Unlock()
	if call == 2 {
		close(e.entered)
		<-e.release
	}
	return fmt.Sprintf("execution-token-%d", call), nil
}

type sweepListSignalReader struct {
	run.RunClient
	listed chan struct{}
	once   sync.Once
}

func (r *sweepListSignalReader) ListRuns(ctx context.Context, namespace string) (*agenticv1alpha1.AgenticRunList, error) {
	list, err := r.RunClient.ListRuns(ctx, namespace)
	r.once.Do(func() { close(r.listed) })
	return list, err
}

type runReadBarrierReader struct {
	run.RunClient
	mu      sync.Mutex
	calls   int
	entered chan struct{}
	proceed chan struct{}
}

func (r *runReadBarrierReader) GetRun(ctx context.Context, namespace, name string) (*agenticv1alpha1.AgenticRun, error) {
	r.mu.Lock()
	r.calls++
	call := r.calls
	r.mu.Unlock()
	if call == 2 {
		close(r.entered)
		<-r.proceed
	}
	return r.RunClient.GetRun(ctx, namespace, name)
}

func TestSweepRetriesPauseEventSkippedUnderRunLock(t *testing.T) {
	h := newHarness(t)
	submit(t, h, "t-sweep-pause", "c-sweep-pause", "question")
	rec, _ := h.handler.deps.Store.Task("t-sweep-pause")
	unlock := h.handler.locks.Lock(rec.RunUID)
	pauseRun(t, h, rec.RunName, "expired while handler lock held")
	unlock()

	h.watcher.Sweep(context.Background(), h.clock.Now())
	if got := taskState(getTask(t, h, rec.TaskID)); got != "auth-required" {
		t.Fatalf("sweeper did not retry contended pause event: state=%s", got)
	}
}

func TestSweepRetriesTerminalEventSkippedUnderRunLock(t *testing.T) {
	h := newHarness(t)
	submit(t, h, "t-sweep-terminal", "c-sweep-terminal", "question")
	rec, _ := h.handler.deps.Store.Task("t-sweep-terminal")
	secretName := run.RunTokenSecretName(rec.RunUID)
	unlock := h.handler.locks.Lock(rec.RunUID)
	terminalRun(t, h, rec.RunName)
	unlock()

	h.watcher.Sweep(context.Background(), h.clock.Now())
	if h.kube.secret(secretName) == nil {
		t.Fatal("sweeper deleted terminal Secret before grace period")
	}
	h.clock.Advance(h.handler.cfg.TerminalCleanupGrace + time.Second)
	h.watcher.Sweep(context.Background(), h.clock.Now())
	if h.kube.secret(secretName) != nil {
		t.Fatal("sweeper did not schedule and complete cleanup for contended terminal event")
	}
}

func TestSweepDeadlineSerializesWithApprovalRenewal(t *testing.T) {
	ex := &blockingExecutionExchanger{entered: make(chan struct{}), release: make(chan struct{})}
	runEx := &sequenceExchanger{tokens: []string{"run-initial", "run-approved", "run-renewed"}}
	// The explicit Sweep below is the only one allowed to race renewal; a
	// background tick after the clock advance could terminalize first.
	h := newHarnessFull(t, newFakeKube(), &testClock{t: testNow}, func(c *Config) {
		c.SweepInterval = time.Hour
	}, func(d *Deps) {
		d.RunTokenExchanger = runEx
		d.ExecutionTokenExchanger = ex
	})
	runCR := prepareProposed(t, h, "t-sweep-renewal", "c-sweep-renewal", []agenticv1alpha1.RemediationOption{{Title: "Restart"}}, true)
	decision := approvalMessage("m-sweep-approval", "t-sweep-renewal", "c-sweep-renewal", "approve", map[string]any{
		"stage": "Execution", "option": float64(0),
	})
	resultTask(t, sendMessage(t, h, decision, freshToken, nil))
	h.kube.setConditions(runCR.Name, metav1.Condition{
		Type: agenticv1alpha1.AgenticRunConditionExecuted, Status: metav1.ConditionUnknown, Reason: "InProgress",
	})
	pauseRun(t, h, runCR.Name, "expired before renewal")
	h.clock.Advance(h.handler.cfg.RenewalTimeout + time.Second)

	renewalDone := make(chan map[string]any, 1)
	go func() {
		renewalDone <- sendMessage(t, h, approvalMessage("m-sweep-renew", "t-sweep-renewal", "c-sweep-renewal", "approve", map[string]any{
			"stage": "Execution", "option": float64(0),
		}), renewToken, nil)
	}()
	select {
	case <-ex.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("renewal did not enter the blocked execution-token exchange")
	}

	listed := make(chan struct{})
	h.watcher.deps.Readers = &sweepListSignalReader{RunClient: h.kube, listed: listed}
	sweepDone := make(chan struct{})
	go func() {
		h.watcher.Sweep(context.Background(), h.clock.Now())
		close(sweepDone)
	}()
	select {
	case <-listed:
	case <-time.After(2 * time.Second):
		t.Fatal("sweeper did not list current runs")
	}
	if rec, _ := h.handler.deps.Store.Task("t-sweep-renewal"); rec.AdapterTerminal {
		t.Fatal("sweeper terminalized a run-backed task while renewal held the run lock")
	}

	close(ex.release)
	resultTask(t, <-renewalDone)
	<-sweepDone
	if rec, _ := h.handler.deps.Store.Task("t-sweep-renewal"); rec.AdapterTerminal {
		t.Fatalf("deadline sweep overwrote successful renewal: %+v", rec)
	}
	if got := taskState(getTask(t, h, "t-sweep-renewal")); got != "working" {
		t.Fatalf("post-renewal sweep state = %q, want working", got)
	}
	secret := h.kube.secret(run.RunTokenSecretName(string(runCR.UID)))
	if string(secret.Data[run.ExecutionTokenKey]) != "execution-token-2" {
		t.Fatalf("renewal execution credential = %q, want second exchanged token", secret.Data[run.ExecutionTokenKey])
	}
}

func TestRunChangedLifecycleLockPreventsStalePauseOverlay(t *testing.T) {
	h := newHarness(t)
	submit(t, h, "t-stale-lifecycle", "c-stale-lifecycle", "question")
	rec, _ := h.handler.deps.Store.Task("t-stale-lifecycle")
	pauseRun(t, h, rec.RunName, "credential expired")
	event := h.kube.runByName(rec.RunName)
	reader := &runReadBarrierReader{
		RunClient: h.kube,
		entered:   make(chan struct{}),
		proceed:   make(chan struct{}),
	}
	h.watcher.deps.Readers = reader
	watchDone := make(chan struct{})
	go func() {
		h.watcher.RunChanged(context.Background(), event)
		close(watchDone)
	}()
	select {
	case <-reader.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("RunChanged did not reach its locked authoritative read")
	}

	renewalDone := make(chan map[string]any, 1)
	go func() {
		renewalDone <- followUp(t, h, rec.TaskID, rec.ContextID, "m-stale-lifecycle-renew", "", renewToken, nil)
	}()

	// Model the operator clearing the old condition while the watcher is
	// suspended. Its locked read must see this current state; renewal then runs
	// after the watcher and remains the final writer of the task overlay.
	h.kube.mu.Lock()
	current := h.kube.runs["test-ns/"+rec.RunName]
	conditions := current.Status.Conditions[:0]
	for _, cond := range current.Status.Conditions {
		if cond.Type != agenticv1alpha1.AgenticRunConditionCredentialsExpired {
			conditions = append(conditions, cond)
		}
	}
	current.Status.Conditions = conditions
	h.kube.mu.Unlock()
	close(reader.proceed)
	<-watchDone
	resultTask(t, <-renewalDone)
	if current, _ := h.handler.deps.Store.Task(rec.TaskID); current.AuthPaused {
		t.Fatalf("stale lifecycle projection restored AuthPaused after renewal: %+v", current)
	}
}

func TestOperatorClearedCredentialConditionRemovesPauseOverlay(t *testing.T) {
	h := newHarness(t)
	submit(t, h, "t-cleared-pause", "c-cleared-pause", "question")
	rec, _ := h.handler.deps.Store.Task("t-cleared-pause")
	workingRun(t, h, rec.RunName)
	pauseRun(t, h, rec.RunName, "gateway credential missing")
	rec, _ = h.handler.deps.Store.Task(rec.TaskID)
	if !rec.HasRun || !rec.AuthPaused {
		t.Fatalf("setup did not create a run-backed pause: %+v", rec)
	}
	pauseCount := rec.PauseCount

	// The operator clears CredentialsExpired after validating a renewed token
	// or switching from gateway credentials to spoke-SA mode. This update may
	// have no corresponding adapter follow-up to clear the local overlay.
	clearPauseRun(t, h, rec.RunName)
	rec, _ = h.handler.deps.Store.Task(rec.TaskID)
	if rec.AuthPaused || !rec.AuthPausedAt.IsZero() {
		t.Fatalf("cleared CR condition left the run-backed pause overlay active: %+v", rec)
	}
	if rec.AdapterTerminal || rec.PauseCount != pauseCount {
		t.Fatalf("condition clear changed terminality or cumulative pause count: %+v", rec)
	}
	if got := taskState(getTask(t, h, rec.TaskID)); got != "working" {
		t.Fatalf("task after condition removal = %q, want working", got)
	}
	h.clock.Advance(h.handler.cfg.RenewalTimeout + time.Second)
	h.watcher.Sweep(context.Background(), h.clock.Now())
	rec, _ = h.handler.deps.Store.Task(rec.TaskID)
	if rec.AdapterTerminal || rec.AuthPaused {
		t.Fatalf("stale pause deadline terminalized a healthy run after condition removal: %+v", rec)
	}

	// Preserve an existing adapter-owned terminal result: clearing the CR
	// pause overlay may never revive a failed task.
	rec.AdapterTerminal = true
	rec.FailureReason = "adapter terminal sentinel"
	rec.AuthPaused = true
	rec.AuthPausedAt = h.clock.Now().Add(-h.handler.cfg.RenewalTimeout - time.Second)
	h.handler.deps.Store.PutTask(rec)
	syncRun(t, h, rec.RunName)
	rec, _ = h.handler.deps.Store.Task(rec.TaskID)
	if !rec.AdapterTerminal || rec.FailureReason != "adapter terminal sentinel" {
		t.Fatalf("operator condition removal cleared adapter terminality: %+v", rec)
	}

	// Even after an old pause deadline, the adapter-owned terminal result is
	// still authoritative and cannot be cleared by this CR event.
	h.watcher.Sweep(context.Background(), h.clock.Now())
	rec, _ = h.handler.deps.Store.Task(rec.TaskID)
	if !rec.AdapterTerminal || rec.FailureReason != "adapter terminal sentinel" {
		t.Fatalf("sweep changed an existing adapter-terminal result: %+v", rec)
	}
}
