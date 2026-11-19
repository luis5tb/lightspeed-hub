package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	agenticv1alpha1 "github.com/openshift/lightspeed-agentic-operator/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/openshift/lightspeed-hub/a2a-adapter/internal/auth"
	"github.com/openshift/lightspeed-hub/a2a-adapter/internal/run"
	"github.com/openshift/lightspeed-hub/a2a-adapter/internal/store"
)

// The startup-retry tests pin the behavior demanded by the live incident
// (see retry.go's header comment): a transient "storage is (re)initializing"
// LIST failure must be retried, not fatal; a non-transient failure (RBAC)
// must exit immediately; exhausting --startup-retry-timeout exits with the
// ORIGINAL error so the log says the real thing.

// flakyKube wraps fakeKube with a ListRuns that fails the first failN
// calls with err before delegating (the CRD sync-wave race: N failed
// attempts before the API server finishes establishing CRD storage).
type flakyKube struct {
	*fakeKube
	mu    sync.Mutex
	failN int
	calls int
	err   error
}

func (f *flakyKube) ListRuns(ctx context.Context, ns string) (*agenticv1alpha1.AgenticRunList, error) {
	f.mu.Lock()
	f.calls++
	n := f.calls
	f.mu.Unlock()
	if n <= f.failN {
		return nil, f.err
	}
	return f.fakeKube.ListRuns(ctx, ns)
}

func (f *flakyKube) listCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// iotimeoutErr is a transport timeout as client-go surfaces it (a
// net.Error whose Timeout() is true), not an apierrors.StatusError.
type iotimeoutErr struct{}

func (iotimeoutErr) Error() string   { return "i/o timeout" }
func (iotimeoutErr) Timeout() bool   { return true }
func (iotimeoutErr) Temporary() bool { return true }

// storageInitializingErr mints the incident's exact error shape: the API
// server answers the LIST with 503 ServiceUnavailable, "storage is
// (re)initializing".
func storageInitializingErr() error {
	return apierrors.NewServiceUnavailable("storage is (re)initializing")
}

// virtualRetryOpts returns RetryOptions on a virtual clock: Sleep
// advances the clock instead of wall-clock waiting and records the
// delays, so backoff shape is asserted without real sleeping.
func virtualRetryOpts(timeout time.Duration) (RetryOptions, func() []time.Duration) {
	var (
		mu     sync.Mutex
		t      = time.Unix(0, 0)
		delays []time.Duration
	)
	opts := RetryOptions{
		Timeout: timeout,
		Logger:  slog.New(slog.DiscardHandler),
		Now: func() time.Time {
			mu.Lock()
			defer mu.Unlock()
			return t
		},
		Sleep: func(ctx context.Context, d time.Duration) error {
			mu.Lock()
			defer mu.Unlock()
			delays = append(delays, d)
			t = t.Add(d)
			return nil
		},
	}
	return opts, func() []time.Duration {
		mu.Lock()
		defer mu.Unlock()
		return append([]time.Duration(nil), delays...)
	}
}

// newRecoveryHandler builds a Handler whose only interesting dependency
// is the (flaky) RunClient, mirroring the restart path in main.go: an
// empty store rehydrated by the recovery LIST.
func newRecoveryHandler(t *testing.T, readers interface {
	run.RunClient
	run.ResultClient
}) *Handler {
	t.Helper()
	h, err := New(Config{Namespace: "test-ns"}, Deps{
		Store:       store.New(),
		Credentials: auth.HeaderSource{},
		Readers:     readers,
		Results:     readers,
		Submitters:  func(context.Context, string) (run.RunClient, error) { return readers, nil },
		Runs:        &run.Creator{Namespace: "test-ns", AgentName: "default"},
		Tokens:      &run.TokenWriter{Secrets: nil, Namespace: "test-ns", Identity: "adapter-sa"},
		Logger:      slog.New(slog.DiscardHandler),
	})
	if err != nil {
		t.Fatal(err)
	}
	return h
}

// TestStartupRetrySurvivesStorageInitializing is the incident, replayed:
// the recovery LIST fails three times with "storage is (re)initializing"
// (503), then succeeds. The adapter must proceed after 4 attempts with
// the task store rehydrated.
func TestStartupRetrySurvivesStorageInitializing(t *testing.T) {
	base := newFakeKube()
	// One run carrying a task annotation, so recovery has something to
	// rehydrate (same CR shape the other restart tests use).
	base.mu.Lock()
	base.runs["test-ns/run-1"] = &agenticv1alpha1.AgenticRun{
		ObjectMeta: metav1.ObjectMeta{
			Name: "run-1", Namespace: "test-ns", UID: "run-uid-1",
			Annotations: map[string]string{"a2a.lightspeed.io/task-id": "t1"},
		},
	}
	base.mu.Unlock()

	flaky := &flakyKube{fakeKube: base, failN: 3, err: storageInitializingErr()}
	h := newRecoveryHandler(t, flaky)
	st := h.deps.Store

	opts, delays := virtualRetryOpts(5 * time.Minute)
	if err := h.RecoverWithRetry(context.Background(), opts); err != nil {
		t.Fatalf("RecoverWithRetry: %v", err)
	}
	if got := flaky.listCalls(); got != 4 {
		t.Errorf("ListRuns calls = %d, want 4 (3 transient failures + 1 success)", got)
	}
	got := delays()
	if len(got) != 3 {
		t.Fatalf("retry sleeps = %d, want 3", len(got))
	}
	for i, d := range got {
		if d <= 0 || d > 30*time.Second {
			t.Errorf("delay[%d] = %v, want in (0, 30s]", i, d)
		}
	}
	if rec, ok := st.Task("t1"); !ok || rec.RunName != "run-1" {
		t.Errorf("recovered task after retries = %+v, want run-1", rec)
	}
}

// TestStartupRetryBackoffShape: exponential growth (2s, 4s, 8s, 16s)
// capped at 30s, with half-jitter keeping each delay in [d/2, d).
func TestStartupRetryBackoffShape(t *testing.T) {
	opts, delays := virtualRetryOpts(time.Hour)
	attempts := 0
	err := RetryTransient(context.Background(), opts, func() error {
		attempts++
		if attempts <= 6 {
			return storageInitializingErr()
		}
		return nil
	})
	if err != nil {
		t.Fatalf("RetryTransient: %v", err)
	}
	if attempts != 7 {
		t.Errorf("attempts = %d, want 7", attempts)
	}
	got := delays()
	if len(got) != 6 {
		t.Fatalf("retry sleeps = %d, want 6", len(got))
	}
	wantMax := []time.Duration{2, 4, 8, 16, 30, 30}
	for i, d := range got {
		max := wantMax[i] * time.Second
		if d < max/2 || d >= max {
			t.Errorf("delay[%d] = %v, want in [%v, %v) (exponential, capped, half-jittered)", i, d, max/2, max)
		}
	}
}

// TestStartupRetryNonTransientFailsFast: an RBAC failure (Forbidden) is
// not retried — one attempt, no sleeps, the original error returned.
func TestStartupRetryNonTransientFailsFast(t *testing.T) {
	forbidden := apierrors.NewForbidden(
		schema.GroupResource{Group: "agentic.openshift.io", Resource: "agenticruns"},
		"", errors.New("adapter-sa cannot list"))
	flaky := &flakyKube{fakeKube: newFakeKube(), failN: 1000, err: forbidden}
	h := newRecoveryHandler(t, flaky)

	opts, delays := virtualRetryOpts(5 * time.Minute)
	err := h.RecoverWithRetry(context.Background(), opts)
	if err == nil {
		t.Fatal("RecoverWithRetry succeeded on a Forbidden LIST, want immediate failure")
	}
	if !apierrors.IsForbidden(err) {
		t.Errorf("returned error = %v, want the original Forbidden", err)
	}
	if got := flaky.listCalls(); got != 1 {
		t.Errorf("ListRuns calls = %d, want 1 (no retries on non-transient)", got)
	}
	if len(delays()) != 0 {
		t.Errorf("retry sleeps = %d, want 0", len(delays()))
	}
}

// TestStartupRetryTimeoutExhaustion: a transient error that never heals
// is retried until the --startup-retry-timeout budget is spent, then the
// ORIGINAL error is returned.
func TestStartupRetryTimeoutExhaustion(t *testing.T) {
	flaky := &flakyKube{fakeKube: newFakeKube(), failN: 1 << 20, err: storageInitializingErr()}
	h := newRecoveryHandler(t, flaky)

	opts, delays := virtualRetryOpts(10 * time.Second)
	err := h.RecoverWithRetry(context.Background(), opts)
	if err == nil {
		t.Fatal("RecoverWithRetry succeeded on permanent storage failure, want exhaustion")
	}
	if !strings.Contains(err.Error(), storageInitializingMsg) {
		t.Errorf("exhausted error = %v, want the original storage-(re)initializing error", err)
	}
	if got := flaky.listCalls(); got < 2 {
		t.Errorf("ListRuns calls = %d, want retries before exhaustion", got)
	}
	// The sleeps must not exceed the total budget.
	total := time.Duration(0)
	for _, d := range delays() {
		total += d
	}
	if total > 10*time.Second {
		t.Errorf("total slept = %v, want <= the 10s budget", total)
	}
}

// TestStartupRetryFailFast: Timeout 0 must not retry at all (the
// pre-retry behavior, for tests).
func TestStartupRetryFailFast(t *testing.T) {
	flaky := &flakyKube{fakeKube: newFakeKube(), failN: 1, err: storageInitializingErr()}
	h := newRecoveryHandler(t, flaky)

	if err := h.RecoverWithRetry(context.Background(), RetryOptions{
		Timeout: 0, Logger: slog.New(slog.DiscardHandler),
	}); err == nil {
		t.Fatal("fail-fast mode succeeded, want the single-attempt error")
	}
	if got := flaky.listCalls(); got != 1 {
		t.Errorf("ListRuns calls = %d, want 1", got)
	}
}

// TestIsTransientAPIError pins the classifier: everything the incident
// and its neighbors can produce is transient; RBAC/config errors are not.
func TestIsTransientAPIError(t *testing.T) {
	gr := schema.GroupResource{Group: "agentic.openshift.io", Resource: "agenticruns"}
	transient := []error{
		storageInitializingErr(),
		apierrors.NewServiceUnavailable("etcd is not healthy"),
		apierrors.NewServerTimeout(gr, "list", 1),
		apierrors.NewTimeoutError("timed out", 1),
		apierrors.NewGenericServerResponse(500, "list", schema.GroupResource{Group: "agentic.openshift.io", Resource: "agenticruns"}, "", "", 0, false), // etcd flap
		context.DeadlineExceeded,
		&url.Error{Op: "Get", URL: "https://kube/apis", Err: context.DeadlineExceeded},
		iotimeoutErr{},
		fmt.Errorf("list agenticruns for recovery: %w", storageInitializingErr()),
		fmt.Errorf("wrap: %w", &url.Error{Op: "Get", URL: "https://kube", Err: errors.New("dial tcp 10.0.0.1:443: connect: connection refused")}),
	}
	for i, err := range transient {
		if !IsTransientAPIError(err) {
			t.Errorf("transient[%d] (%v) classified non-transient", i, err)
		}
	}
	nonTransient := []error{
		apierrors.NewForbidden(gr, "", errors.New("no")),
		apierrors.NewNotFound(gr, "x"),
		apierrors.NewBadRequest("bad"),
		errors.New("invalid config"),
		fmt.Errorf("list agenticruns for recovery: %w", apierrors.NewForbidden(gr, "", errors.New("no"))),
	}
	for i, err := range nonTransient {
		if IsTransientAPIError(err) {
			t.Errorf("nonTransient[%d] (%v) classified transient", i, err)
		}
	}
	if IsTransientAPIError(nil) {
		t.Error("nil error classified transient")
	}
}
