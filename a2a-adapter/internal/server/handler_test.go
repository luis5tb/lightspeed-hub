package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/a2a"
	"github.com/a2aproject/a2a-go/a2asrv"
	agenticv1alpha1 "github.com/openshift/lightspeed-agentic-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/openshift/lightspeed-hub/a2a-adapter/internal/auth"
	"github.com/openshift/lightspeed-hub/a2a-adapter/internal/run"
	"github.com/openshift/lightspeed-hub/a2a-adapter/internal/store"
)

// ---- test doubles ----------------------------------------------------------

// fakeKube is a combined in-memory RunClient + SecretClient + ResultClient
// with API-server semantics (AlreadyExists, NotFound, server-assigned UIDs,
// RV bumps). Its `notify` hook simulates the informer: every mutation that
// the production watch would observe calls back into the watcher, so tests
// exercise the real watch path (projection → store → subscriber signal).
type fakeKube struct {
	mu                       sync.Mutex
	runs                     map[string]*agenticv1alpha1.AgenticRun
	secrets                  map[string]*corev1.Secret
	objects                  map[string]ctrlclient.Object // Result CRs + approvals, keyed ns/name
	uid, rv                  int
	created                  int // AgenticRun create calls
	secWritt                 int // Secret create+update calls
	secDel                   int // Secret delete calls (§5.5 cleanup)
	order                    []string
	failApproval             bool
	failAfterApprovalPersist bool
	failRemoveExecution      int
	failGetRun               int
	approvalPatched          chan struct{}
	// notify, when set, is invoked AFTER each observable mutation
	// (outside the mutex) — the fake watch delivery.
	notify func()
}

func newFakeKube() *fakeKube {
	return &fakeKube{
		runs:            map[string]*agenticv1alpha1.AgenticRun{},
		secrets:         map[string]*corev1.Secret{},
		objects:         map[string]ctrlclient.Object{},
		approvalPatched: make(chan struct{}, 8),
	}
}

// changed delivers the mutation to the fake watch. Callers must NOT hold
// f.mu (RunChanged reads back through the same mutex).
func (f *fakeKube) changed() {
	if f.notify != nil {
		f.notify()
	}
}

func (f *fakeKube) CreateRun(_ context.Context, r *agenticv1alpha1.AgenticRun) error {
	f.mu.Lock()
	k := r.Namespace + "/" + r.Name
	if _, ok := f.runs[k]; ok {
		f.mu.Unlock()
		return apierrors.NewAlreadyExists(schema.GroupResource{Group: "agentic.openshift.io", Resource: "agenticruns"}, r.Name)
	}
	f.created++
	f.uid++
	stored := r.DeepCopy()
	stored.UID = types.UID(fmt.Sprintf("run-uid-%d", f.uid))
	stored.Generation = 1
	f.runs[k] = stored
	*r = *stored
	f.mu.Unlock()
	f.changed()
	return nil
}

func (f *fakeKube) GetRun(_ context.Context, ns, name string) (*agenticv1alpha1.AgenticRun, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failGetRun > 0 {
		f.failGetRun--
		return nil, fmt.Errorf("injected transient AgenticRun read failure")
	}
	r, ok := f.runs[ns+"/"+name]
	if !ok {
		return nil, apierrors.NewNotFound(schema.GroupResource{Group: "agentic.openshift.io", Resource: "agenticruns"}, name)
	}
	return r.DeepCopy(), nil
}

func (f *fakeKube) UpdateRun(_ context.Context, r *agenticv1alpha1.AgenticRun) error {
	f.mu.Lock()
	key := r.Namespace + "/" + r.Name
	stored := r.DeepCopy()
	if existing := f.runs[key]; existing != nil {
		stored.Generation = existing.Generation
		if !reflect.DeepEqual(existing.Spec, r.Spec) {
			stored.Generation++
		}
	}
	f.runs[key] = stored
	f.mu.Unlock()
	f.changed()
	return nil
}

func (f *fakeKube) ListRuns(_ context.Context, ns string) (*agenticv1alpha1.AgenticRunList, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	list := &agenticv1alpha1.AgenticRunList{}
	for _, r := range f.runs {
		if r.Namespace == ns {
			list.Items = append(list.Items, *r.DeepCopy())
		}
	}
	return list, nil
}

func (f *fakeKube) UpdateApproval(_ context.Context, a *agenticv1alpha1.AgenticRunApproval) error {
	f.mu.Lock()
	f.order = append(f.order, "approval-update")
	fail := f.failApproval
	failAfterPersist := f.failAfterApprovalPersist
	if fail {
		f.mu.Unlock()
		select {
		case f.approvalPatched <- struct{}{}:
		default:
		}
		return fmt.Errorf("injected approval update failure")
	}
	f.objects[a.Namespace+"/"+a.Name] = a.DeepCopy()
	f.mu.Unlock()
	if failAfterPersist {
		select {
		case f.approvalPatched <- struct{}{}:
		default:
		}
		return fmt.Errorf("injected uncertain approval update response")
	}
	select {
	case f.approvalPatched <- struct{}{}:
	default:
	}
	f.changed()
	return nil
}

func (f *fakeKube) setConditions(name string, conds ...metav1.Condition) {
	f.mu.Lock()
	r := f.runs["test-ns/"+name]
	for i := range conds {
		if conds[i].ObservedGeneration == 0 {
			conds[i].ObservedGeneration = r.Generation
		}
		meta.SetStatusCondition(&r.Status.Conditions, conds[i])
	}
	f.mu.Unlock()
	f.changed()
}

// mutateRun alters a stored run in place (test driver for verify-don't-
// trust checks, e.g. changing spec.targetCluster directly on the CR).
func (f *fakeKube) mutateRun(name string, fn func(*agenticv1alpha1.AgenticRun)) {
	f.mu.Lock()
	fn(f.runs["test-ns/"+name])
	f.mu.Unlock()
	f.changed()
}

// deleteRun removes a run (TTL GC / informer delete path).
func (f *fakeKube) deleteRun(name string) {
	f.mu.Lock()
	delete(f.runs, "test-ns/"+name)
	f.mu.Unlock()
	f.changed()
}

// putObject stores a Result CR or AgenticRunApproval (server-assigned UID
// if empty) and delivers the watch event.
func (f *fakeKube) putObject(obj ctrlclient.Object) {
	f.mu.Lock()
	if obj.GetUID() == "" {
		f.uid++
		obj.SetUID(types.UID(fmt.Sprintf("obj-uid-%d", f.uid)))
	}
	f.objects["test-ns/"+obj.GetName()] = obj.DeepCopyObject().(ctrlclient.Object)
	f.mu.Unlock()
	f.changed()
}

func (f *fakeKube) getObject(name string) (ctrlclient.Object, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	o, ok := f.objects["test-ns/"+name]
	if !ok {
		return nil, apierrors.NewNotFound(schema.GroupResource{Group: "agentic.openshift.io"}, name)
	}
	return o.DeepCopyObject().(ctrlclient.Object), nil
}

func (f *fakeKube) mutateObject(name string, fn func(ctrlclient.Object)) {
	f.mu.Lock()
	obj := f.objects["test-ns/"+name]
	if obj == nil {
		f.mu.Unlock()
		return
	}
	updated := obj.DeepCopyObject().(ctrlclient.Object)
	fn(updated)
	f.objects["test-ns/"+name] = updated
	f.mu.Unlock()
	f.changed()
}
func (f *fakeKube) GetAnalysisResult(_ context.Context, _, name string) (*agenticv1alpha1.AnalysisResult, error) {
	o, err := f.getObject(name)
	if err != nil {
		return nil, err
	}
	return o.(*agenticv1alpha1.AnalysisResult), nil
}

func (f *fakeKube) GetExecutionResult(_ context.Context, _, name string) (*agenticv1alpha1.ExecutionResult, error) {
	o, err := f.getObject(name)
	if err != nil {
		return nil, err
	}
	return o.(*agenticv1alpha1.ExecutionResult), nil
}

func (f *fakeKube) GetVerificationResult(_ context.Context, _, name string) (*agenticv1alpha1.VerificationResult, error) {
	o, err := f.getObject(name)
	if err != nil {
		return nil, err
	}
	return o.(*agenticv1alpha1.VerificationResult), nil
}

func (f *fakeKube) GetEscalationResult(_ context.Context, _, name string) (*agenticv1alpha1.EscalationResult, error) {
	o, err := f.getObject(name)
	if err != nil {
		return nil, err
	}
	return o.(*agenticv1alpha1.EscalationResult), nil
}

func (f *fakeKube) GetApproval(_ context.Context, _, name string) (*agenticv1alpha1.AgenticRunApproval, error) {
	o, err := f.getObject(name)
	if err != nil {
		return nil, err
	}
	return o.(*agenticv1alpha1.AgenticRunApproval), nil
}

func (f *fakeKube) CreateSecret(_ context.Context, s *corev1.Secret) error {
	f.mu.Lock()
	f.secWritt++ // counts API calls (including AlreadyExists create-or-update attempts)
	k := s.Namespace + "/" + s.Name
	if _, ok := f.secrets[k]; ok {
		f.mu.Unlock()
		return apierrors.NewAlreadyExists(schema.GroupResource{Resource: "secrets"}, s.Name)
	}
	f.order = append(f.order, "secret-create")
	f.rv++
	stored := s.DeepCopy()
	stored.ResourceVersion = fmt.Sprintf("%d", f.rv)
	f.secrets[k] = stored
	f.mu.Unlock()
	return nil
}

func (f *fakeKube) GetSecret(_ context.Context, ns, name string) (*corev1.Secret, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	s, ok := f.secrets[ns+"/"+name]
	if !ok {
		return nil, apierrors.NewNotFound(schema.GroupResource{Resource: "secrets"}, name)
	}
	return s.DeepCopy(), nil
}

func (f *fakeKube) UpdateSecret(_ context.Context, s *corev1.Secret) error {
	f.mu.Lock()
	key := s.Namespace + "/" + s.Name
	if f.failRemoveExecution > 0 {
		if existing := f.secrets[key]; existing != nil && len(existing.Data[run.ExecutionTokenKey]) > 0 && len(s.Data[run.ExecutionTokenKey]) == 0 {
			f.failRemoveExecution--
			f.mu.Unlock()
			return fmt.Errorf("injected execution-token removal failure")
		}
	}
	f.secWritt++
	f.order = append(f.order, "secret-update")
	f.rv++
	stored := s.DeepCopy()
	stored.ResourceVersion = fmt.Sprintf("%d", f.rv)
	f.secrets[key] = stored
	f.mu.Unlock()
	return nil
}

func (f *fakeKube) DeleteSecret(_ context.Context, ns, name string) error {
	f.mu.Lock()
	k := ns + "/" + name
	if _, ok := f.secrets[k]; !ok {
		f.mu.Unlock()
		return apierrors.NewNotFound(schema.GroupResource{Resource: "secrets"}, name)
	}
	f.secDel++
	delete(f.secrets, k)
	f.mu.Unlock()
	return nil
}

func (f *fakeKube) secret(name string) *corev1.Secret {
	f.mu.Lock()
	defer f.mu.Unlock()
	s, ok := f.secrets["test-ns/"+name]
	if !ok {
		return nil
	}
	return s.DeepCopy()
}

func (f *fakeKube) runByName(name string) *agenticv1alpha1.AgenticRun {
	f.mu.Lock()
	defer f.mu.Unlock()
	r, ok := f.runs["test-ns/"+name]
	if !ok {
		return nil
	}
	return r.DeepCopy()
}

// stubExchanger returns a fixed Token H.
type stubExchanger struct{ token string }

func (s stubExchanger) Exchange(context.Context, string) (string, error) { return s.token, nil }

type sequenceExchanger struct {
	tokens []string
	calls  int
}

func (s *sequenceExchanger) Exchange(context.Context, string) (string, error) {
	if len(s.tokens) == 0 {
		s.calls++
		return "sequence-token", nil
	}
	index := s.calls
	s.calls++
	if index >= len(s.tokens) {
		index = len(s.tokens) - 1
	}
	return s.tokens[index], nil
}

// ---- harness ---------------------------------------------------------------

// testClock is a controllable clock: frozen by default (deterministic
// projections), advanceable for deadline/grace tests.
type testClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *testClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

var testNow = time.Unix(1_750_000_000, 0)

func makeToken(sub string, exp int64) string {
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none"}`))
	claims := map[string]any{"sub": sub, "iss": "https://kc/realms/hub"}
	if exp != 0 {
		claims["exp"] = exp
	}
	payload, _ := json.Marshal(claims)
	return header + "." + base64.RawURLEncoding.EncodeToString(payload) + ".sig"
}

type harness struct {
	kube    *fakeKube
	server  *httptest.Server
	handler *Handler
	watcher *Watcher
	clock   *testClock
}

// newHarness builds the full M3 stack against the fake kube: HTTP handler
// + watch controller sharing one store, with the fake informer delivery
// wired through fakeKube.notify.
func newHarness(t *testing.T) *harness {
	t.Helper()
	return newHarnessWithConfig(t, nil)
}

func newHarnessWithConfig(t *testing.T, tune func(*Config)) *harness {
	t.Helper()
	return newHarnessSharing(t, newFakeKube(), &testClock{t: testNow}, tune)
}

// newHarnessSharing builds a full harness on top of an EXISTING fake kube
// and clock — the adapter-restart shape: fresh store, fresh handler and
// watcher, same API server state and same clock domain.
func newHarnessSharing(t *testing.T, kube *fakeKube, clock *testClock, tune func(*Config)) *harness {
	t.Helper()
	return newHarnessFull(t, kube, clock, tune, nil)
}

// newHarnessFull is newHarnessSharing plus a hook to adjust Deps (e.g. a
// RunTokenExchanger).
func newHarnessFull(t *testing.T, kube *fakeKube, clock *testClock, tune func(*Config), tuneDeps func(*Deps)) *harness {
	t.Helper()
	cfg := Config{
		Namespace:            "test-ns",
		TargetHeader:         "X-Hub-Cluster",
		ClockSkew:            30 * time.Second,
		StreamDeadline:       5 * time.Second,
		RenewalTimeout:       time.Hour, // frozen clock: never fires unless advanced
		MaxPauses:            3,
		TerminalCleanupGrace: 30 * time.Second,
		SweepInterval:        10 * time.Millisecond,
		Now:                  clock.Now,
	}
	if tune != nil {
		tune(&cfg)
	}
	deps := Deps{
		Store:       store.New(),
		Credentials: auth.HeaderSource{},
		Exchanger:   stubExchanger{token: "token-h-value"},
		Readers:     kube,
		Results:     kube,
		Submitters:  func(context.Context, string) (run.RunClient, error) { return kube, nil },
		Runs:        &run.Creator{Namespace: "test-ns", AgentName: "default", WrittenBy: "adapter-sa"},
		Tokens:      &run.TokenWriter{Secrets: kube, Namespace: "test-ns", Identity: "adapter-sa"},
	}
	if tuneDeps != nil {
		tuneDeps(&deps)
	}
	handler, err := New(cfg, deps)
	if err != nil {
		t.Fatal(err)
	}
	watcher, err := handler.NewWatcher()
	if err != nil {
		t.Fatal(err)
	}
	// Fake informer delivery: every observable kube mutation re-projects
	// through the real watch path.
	kube.notify = func() {
		ctx := context.Background()
		list, err := kube.ListRuns(ctx, "test-ns")
		if err != nil {
			return
		}
		for i := range list.Items {
			watcher.RunChanged(ctx, &list.Items[i])
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go watcher.Start(ctx)

	mux := http.NewServeMux()
	mux.Handle("/healthz", HealthHandler())
	cardHandler := ServeAgentCard(AgentCard(CardConfig{
		Name: "lightspeed-hub", URL: "https://praxis/hub-adapter", Version: "0.1.0",
		SkillID: "cluster-investigation", SkillName: "investigate",
	}))
	for _, p := range AgentCardPaths {
		mux.Handle(p, cardHandler)
	}
	mux.Handle("/", WithRequestCapture(a2asrv.NewJSONRPCHandler(handler)))

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return &harness{kube: kube, server: srv, handler: handler, watcher: watcher, clock: clock}
}

// rpc posts a JSON-RPC request. extraHeaders carries X-Hub-Cluster etc.
func (h *harness) rpc(t *testing.T, method string, params map[string]any, token string, extraHeaders map[string]string) (map[string]any, *http.Response) {
	t.Helper()
	body := map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
	raw, _ := json.Marshal(body)
	req, err := http.NewRequest(http.MethodPost, h.server.URL+"/", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	for k, v := range extraHeaders {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	var decoded map[string]any
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("response not JSON: %s", data)
	}
	return decoded, resp
}

func sendMessage(t *testing.T, h *harness, msg map[string]any, token string, headers map[string]string) map[string]any {
	t.Helper()
	decoded, _ := h.rpc(t, "message/send", map[string]any{"message": msg}, token, headers)
	return decoded
}

func resultTask(t *testing.T, decoded map[string]any) map[string]any {
	t.Helper()
	if e, ok := decoded["error"].(map[string]any); ok {
		t.Fatalf("unexpected JSON-RPC error: %v", e)
	}
	task, ok := decoded["result"].(map[string]any)
	if !ok || task["kind"] != "task" {
		t.Fatalf("result is not a task: %v", decoded)
	}
	return task
}

func taskState(task map[string]any) string {
	status, _ := task["status"].(map[string]any)
	state, _ := status["state"].(string)
	return state
}

func firstSSETaskState(t *testing.T, h *harness, method string, params map[string]any) string {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "id": 1, "method": method, "params": params,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.server.URL+"/", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+freshToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		var event map[string]any
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &event); err != nil {
			t.Fatalf("invalid SSE event %q: %v", line, err)
		}
		result, _ := event["result"].(map[string]any)
		if result["kind"] != "task" {
			continue
		}
		status, _ := result["status"].(map[string]any)
		state, _ := status["state"].(string)
		return state
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("read first %s task event: %v", method, err)
	}
	t.Fatalf("%s stream ended before an initial task event", method)
	return ""
}

func userMessage(id, taskID, contextID, text string, metadata map[string]any) map[string]any {
	msg := map[string]any{
		"role":      "user",
		"messageId": id,
		"taskId":    taskID,
		"contextId": contextID,
		"parts":     []map[string]any{{"kind": "text", "text": text}},
	}
	if metadata != nil {
		msg["metadata"] = metadata
	}
	return msg
}

func approvalMessage(id, taskID, contextID, action string, fields map[string]any) map[string]any {
	return map[string]any{
		"role":      "user",
		"messageId": id,
		"taskId":    taskID,
		"contextId": contextID,
		"parts": []map[string]any{{"kind": "data", "data": map[string]any{
			action: fields,
		}}},
	}
}

func prepareProposed(t *testing.T, h *harness, taskID, contextID string, options []agenticv1alpha1.RemediationOption, withResult bool) *agenticv1alpha1.AgenticRun {
	t.Helper()
	submit(t, h, taskID, contextID, "investigate and remediate")
	runName := store.RunName(contextID, taskID)
	if withResult {
		resultName := "analysis-" + taskID
		h.kube.mutateRun(runName, func(r *agenticv1alpha1.AgenticRun) {
			r.Status.Steps.Analysis.Results = []agenticv1alpha1.StepResultRef{{
				Name: resultName, Outcome: agenticv1alpha1.ActionOutcomeSucceeded,
			}}
		})
		h.kube.putObject(&agenticv1alpha1.AnalysisResult{
			ObjectMeta: metav1.ObjectMeta{Name: resultName, Namespace: "test-ns"},
			Spec:       agenticv1alpha1.AnalysisResultSpec{AgenticRunName: runName},
			Status:     agenticv1alpha1.AnalysisResultStatus{Options: options},
		})
	}
	h.kube.putObject(&agenticv1alpha1.AgenticRunApproval{
		ObjectMeta: metav1.ObjectMeta{Name: runName, Namespace: "test-ns"},
		Spec: agenticv1alpha1.AgenticRunApprovalSpec{Stages: []agenticv1alpha1.ApprovalStage{
			agenticv1alpha1.NewApprovalStage(agenticv1alpha1.ApprovalStageAnalysis, agenticv1alpha1.ApprovalDecisionApproved, "", nil),
		}},
	})
	h.kube.setConditions(runName, metav1.Condition{
		Type: agenticv1alpha1.AgenticRunConditionAnalyzed, Status: metav1.ConditionTrue,
		Reason: "AnalysisComplete", Message: "analysis ready",
	})
	return h.kube.runByName(runName)
}

var freshToken = makeToken("acme-agent", testNow.Add(time.Hour).Unix())
var expiredToken = makeToken("acme-agent", testNow.Add(-time.Hour).Unix())

// ---- tests -----------------------------------------------------------------

// Happy path: message/send creates EXACTLY ONE AgenticRun and ONE run-token
// Secret with the exact repo C contract, and returns a submitted task.
func TestMessageSendHappyPath(t *testing.T) {
	h := newHarness(t)

	decoded := sendMessage(t, h, userMessage("m1", "t1", "c1", "why is pod foo crashlooping?",
		map[string]any{"targetCluster": "spoke-a"}), freshToken, nil)
	task := resultTask(t, decoded)
	if taskState(task) != "submitted" {
		t.Errorf("state = %q, want submitted", taskState(task))
	}
	if task["id"] != "t1" || task["contextId"] != "c1" {
		t.Errorf("task id mapping = %v", task)
	}

	if h.kube.created != 1 {
		t.Errorf("AgenticRun creates = %d, want 1", h.kube.created)
	}
	runCR := h.kube.runByName(store.RunName("c1", "t1"))
	if runCR.Spec.Request != "why is pod foo crashlooping?" {
		t.Errorf("spec.request = %q", runCR.Spec.Request)
	}
	if runCR.Spec.TargetCluster != "spoke-a" {
		t.Errorf("spec.targetCluster = %q", runCR.Spec.TargetCluster)
	}
	if runCR.Annotations[run.AnnotationTaskID] != "t1" {
		t.Errorf("task-id annotation = %v", runCR.Annotations)
	}

	// The run-token Secret contract, exactly (gateway.go:35,38,117-130).
	secretName := run.RunTokenSecretName(string(runCR.UID))
	s := h.kube.secret(secretName)
	if s == nil {
		t.Fatalf("secret %s not written", secretName)
	}
	if got := string(s.Data[run.RunTokenKey]); got != freshToken {
		t.Errorf("token-a value mismatch (len %d vs %d)", len(got), len(freshToken))
	}
	if s.Labels[run.LabelAgenticRun] != string(runCR.UID) {
		t.Errorf("label agentic-run = %q, want %q", s.Labels[run.LabelAgenticRun], runCR.UID)
	}
	if s.Labels[run.LabelSpokeCluster] != "spoke-a" {
		t.Errorf("label spoke-cluster = %q", s.Labels[run.LabelSpokeCluster])
	}
	if h.kube.secWritt != 1 {
		t.Errorf("secret writes = %d, want 1", h.kube.secWritt)
	}
}

// Duplicate messageId: same record returned, NO second run submitted.
func TestDuplicateMessageID(t *testing.T) {
	h := newHarness(t)
	msg := userMessage("m-dup", "t1", "c1", "question", map[string]any{"targetCluster": "spoke-a"})

	first := resultTask(t, sendMessage(t, h, msg, freshToken, nil))
	second := resultTask(t, sendMessage(t, h, msg, freshToken, nil))

	if first["id"] != second["id"] {
		t.Errorf("duplicate delivery returned different tasks: %v vs %v", first["id"], second["id"])
	}
	if h.kube.created != 1 {
		t.Errorf("AgenticRun creates = %d, want 1 (no re-submission)", h.kube.created)
	}
	if h.kube.secWritt != 1 {
		t.Errorf("secret writes = %d, want 1", h.kube.secWritt)
	}
}

// Expired Token A: task parks in auth-required, NO CR, NO secret.
func TestExpiredTokenParksAuthRequired(t *testing.T) {
	h := newHarness(t)
	decoded := sendMessage(t, h, userMessage("m1", "t1", "c1", "question",
		map[string]any{"targetCluster": "spoke-a"}), expiredToken, nil)
	task := resultTask(t, decoded)
	if taskState(task) != "auth-required" {
		t.Fatalf("state = %q, want auth-required", taskState(task))
	}
	if h.kube.created != 0 {
		t.Errorf("AgenticRun creates = %d, want 0", h.kube.created)
	}
	if h.kube.secWritt != 0 {
		t.Errorf("secret writes = %d, want 0 (never write a dead token)", h.kube.secWritt)
	}
}

func TestExpiredCredentialFollowUpPreservesSweepTerminalState(t *testing.T) {
	executionEx := &sequenceExchanger{tokens: []string{"execution-approved", "must-not-mint-after-timeout"}}
	h := newHarnessFull(t, newFakeKube(), &testClock{t: testNow}, func(c *Config) {
		c.SweepInterval = time.Hour
	}, func(d *Deps) {
		d.RunTokenExchanger = &sequenceExchanger{tokens: []string{"run-initial", "run-approved", "must-not-renew-after-timeout"}}
		d.ExecutionTokenExchanger = executionEx
	})
	runCR := prepareProposed(t, h, "t-expired-copy", "c-expired-copy", []agenticv1alpha1.RemediationOption{{Title: "First"}}, true)
	resultTask(t, sendMessage(t, h, approvalMessage("m-expired-approve", "t-expired-copy", "c-expired-copy", "approve", map[string]any{
		"stage": "Execution", "option": float64(0),
	}), freshToken, nil))
	pauseRun(t, h, runCR.Name, "expired")
	h.clock.Advance(h.handler.cfg.RenewalTimeout + time.Second)

	// Hold the expired request after it copies the task record but before it
	// enters the run-backed expiry branch. Sweep must be able to terminalize
	// that run first; the resumed request must reload rather than PutTask its
	// stale copy back over the terminal record.
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	h.handler.cfg.Now = func() time.Time {
		once.Do(func() {
			close(entered)
			<-release
		})
		return h.clock.Now()
	}
	done := make(chan map[string]any, 1)
	go func() {
		done <- followUp(t, h, "t-expired-copy", "c-expired-copy", "m-expired-arrival", "", expiredToken, nil)
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("expired follow-up did not reach the expiry check")
	}

	h.watcher.Sweep(context.Background(), h.clock.Now())
	before, _ := h.handler.deps.Store.Task("t-expired-copy")
	if !before.AdapterTerminal {
		t.Fatal("sweep did not terminalize the expired paused task")
	}
	writesBefore, executionExchangesBefore := h.kube.secWritt, executionEx.calls
	close(release)
	resultTask(t, <-done)
	after, _ := h.handler.deps.Store.Task("t-expired-copy")
	if !after.AdapterTerminal || after.LastMessageID != "m-expired-arrival" {
		t.Fatalf("expired follow-up did not preserve terminality and record its message: %+v", after)
	}

	// A subsequent live approval/renewal attempt must see the preserved
	// adapter-terminal state and must not mint either credential.
	resultTask(t, sendMessage(t, h, approvalMessage("m-expired-then-renew", "t-expired-copy", "c-expired-copy", "approve", map[string]any{
		"stage": "Execution", "option": float64(0),
	}), renewToken, nil))
	if h.kube.secWritt != writesBefore || executionEx.calls != executionExchangesBefore {
		t.Fatalf("post-timeout credentials minted: Secret writes %d->%d, execution exchanges %d->%d",
			writesBefore, h.kube.secWritt, executionExchangesBefore, executionEx.calls)
	}
}

// Renewal follow-up on a parked task: fresh A creates the run with the
// ORIGINAL question (not the credential-only follow-up text).
func TestRenewalFollowUpCreatesRunWithOriginalRequest(t *testing.T) {
	h := newHarness(t)
	sendMessage(t, h, userMessage("m1", "t1", "c1", "the original question",
		map[string]any{"targetCluster": "spoke-a"}), expiredToken, nil)

	decoded := sendMessage(t, h, userMessage("m2", "t1", "c1", "",
		map[string]any{"targetCluster": "spoke-a"}), freshToken, nil)
	task := resultTask(t, decoded)
	if taskState(task) != "submitted" {
		t.Fatalf("state = %q, want submitted after renewal", taskState(task))
	}
	if h.kube.created != 1 {
		t.Fatalf("creates = %d, want 1", h.kube.created)
	}
	runCR := h.kube.runByName(store.RunName("c1", "t1"))
	if runCR.Spec.Request != "the original question" {
		t.Errorf("spec.request = %q, want the ORIGINAL question", runCR.Spec.Request)
	}
	// The fresh token must be in the Secret.
	s := h.kube.secret(run.RunTokenSecretName(string(runCR.UID)))
	if string(s.Data[run.RunTokenKey]) != freshToken {
		t.Error("renewed token not written")
	}
}

// Target rules (design §4.2): conflicts are rejected, no CR.
func TestTargetRejections(t *testing.T) {
	h := newHarness(t)
	// Conflict: header says spoke-a, metadata says spoke-b.
	decoded := sendMessage(t, h, userMessage("m1", "t1", "c1", "q",
		map[string]any{"targetCluster": "spoke-b"}), freshToken,
		map[string]string{"X-Hub-Cluster": "spoke-a"})
	errObj, ok := decoded["error"].(map[string]any)
	if !ok {
		t.Fatalf("want error, got %v", decoded)
	}
	if code, _ := errObj["code"].(float64); code != -32602 {
		t.Errorf("code = %v, want -32602 (invalid params)", errObj["code"])
	}
	if h.kube.created != 0 {
		t.Errorf("creates = %d, want 0 on target conflict", h.kube.created)
	}
}

// The HTTP layer joins repeated headers; Go's http.Header.Values exposes
// them separately, and the handler must see ALL values (OR-match hazard).
func TestTargetDuplicateHeaderValues(t *testing.T) {
	h := newHarness(t)

	body := map[string]any{"jsonrpc": "2.0", "id": 1, "method": "message/send",
		"params": map[string]any{"message": userMessage("m1", "t1", "c1", "q", nil)}}
	raw, _ := json.Marshal(body)
	req, _ := http.NewRequest(http.MethodPost, h.server.URL+"/", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+freshToken)
	req.Header.Add("X-Hub-Cluster", "spoke-a")
	req.Header.Add("X-Hub-Cluster", "spoke-a")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var decoded map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&decoded)
	if _, ok := decoded["error"]; !ok {
		t.Fatalf("want error for duplicate target header, got %v", decoded)
	}
	if h.kube.created != 0 {
		t.Errorf("creates = %d, want 0", h.kube.created)
	}
}

// Target allow-list (--allowed-targets): fail-fast BEFORE any AgenticRun
// or run-token Secret, with a self-correcting status message.
func statusMessageText(t *testing.T, task map[string]any) string {
	t.Helper()
	status, _ := task["status"].(map[string]any)
	message, _ := status["message"].(map[string]any)
	parts, _ := message["parts"].([]any)
	if len(parts) == 0 {
		return ""
	}
	first, _ := parts[0].(map[string]any)
	text, _ := first["text"].(string)
	return text
}

func allowedListHarness(t *testing.T) *harness {
	t.Helper()
	return newHarnessWithConfig(t, func(c *Config) {
		c.AllowedTargets = []string{"east", "west"}
	})
}

func TestTargetAllowList(t *testing.T) {
	t.Run("empty allow-list accepts any well-formed target", func(t *testing.T) {
		h := newHarness(t) // default: no AllowedTargets
		task := resultTask(t, sendMessage(t, h, userMessage("m1", "t1", "c1", "q",
			map[string]any{"targetCluster": "some-unlisted-spoke"}), freshToken, nil))
		if taskState(task) != "submitted" {
			t.Fatalf("state = %q, want submitted (no enforcement by default)", taskState(task))
		}
		if h.kube.created != 1 {
			t.Errorf("creates = %d, want 1", h.kube.created)
		}
	})

	t.Run("listed target accepted", func(t *testing.T) {
		h := allowedListHarness(t)
		task := resultTask(t, sendMessage(t, h, userMessage("m1", "t1", "c1", "q",
			map[string]any{"targetCluster": "east"}), freshToken, nil))
		if taskState(task) != "submitted" {
			t.Fatalf("state = %q, want submitted", taskState(task))
		}
		if h.kube.created != 1 || h.kube.secWritt != 1 {
			t.Errorf("creates = %d, secret writes = %d; want 1/1", h.kube.created, h.kube.secWritt)
		}
	})

	t.Run("unlisted target fails fast, no CR, no Secret, exact message", func(t *testing.T) {
		h := allowedListHarness(t)
		task := resultTask(t, sendMessage(t, h, userMessage("m1", "t1", "c1", "why is pod foo down?",
			map[string]any{"targetCluster": "est"}), freshToken, nil))
		if taskState(task) != "failed" {
			t.Fatalf("state = %q, want failed", taskState(task))
		}
		want := `unknown target "est"; available clusters: east, west`
		if got := statusMessageText(t, task); got != want {
			t.Errorf("status message = %q, want %q", got, want)
		}
		if h.kube.created != 0 {
			t.Errorf("AgenticRun creates = %d, want 0 (fail before creation)", h.kube.created)
		}
		if h.kube.secWritt != 0 {
			t.Errorf("secret writes = %d, want 0 (no run-token Secret)", h.kube.secWritt)
		}
	})

	t.Run("caller self-corrects on the next message with the parked question", func(t *testing.T) {
		h := allowedListHarness(t)
		sendMessage(t, h, userMessage("m1", "t1", "c1", "why is pod foo down?",
			map[string]any{"targetCluster": "est"}), freshToken, nil)

		// Next message, same task, corrected target: the pre-submit
		// failure leaves nothing immutable, and the ORIGINAL question is
		// the one submitted.
		task := resultTask(t, sendMessage(t, h, userMessage("m2", "t1", "c1", "",
			map[string]any{"targetCluster": "east"}), freshToken, nil))
		if taskState(task) != "submitted" {
			t.Fatalf("state = %q, want submitted after self-correction", taskState(task))
		}
		if h.kube.created != 1 {
			t.Fatalf("creates = %d, want 1", h.kube.created)
		}
		runCR := h.kube.runByName(store.RunName("c1", "t1"))
		if runCR.Spec.Request != "why is pod foo down?" {
			t.Errorf("spec.request = %q, want the parked original question", runCR.Spec.Request)
		}
		if runCR.Spec.TargetCluster != "east" {
			t.Errorf("spec.targetCluster = %q, want east", runCR.Spec.TargetCluster)
		}
	})

	t.Run("parked auth-required record with a bad target fails at renewal, still no CR", func(t *testing.T) {
		h := allowedListHarness(t)
		// Expired A parks the task first (target unvalidated at park time).
		sendMessage(t, h, userMessage("m1", "t1", "c1", "q",
			map[string]any{"targetCluster": "est"}), expiredToken, nil)

		task := resultTask(t, sendMessage(t, h, userMessage("m2", "t1", "c1", "",
			map[string]any{"targetCluster": "est"}), freshToken, nil))
		if taskState(task) != "failed" {
			t.Fatalf("state = %q, want failed", taskState(task))
		}
		want := `unknown target "est"; available clusters: east, west`
		if got := statusMessageText(t, task); got != want {
			t.Errorf("status message = %q, want %q", got, want)
		}
		if h.kube.created != 0 || h.kube.secWritt != 0 {
			t.Errorf("creates = %d, secret writes = %d; want 0/0", h.kube.created, h.kube.secWritt)
		}
	})
}

// Agent card served at every well-known path, including the /a2a-prefixed
// one praxis-proxy's pass-through route forwards unchanged.
func TestAgentCardServed(t *testing.T) {
	h := newHarness(t)
	for _, path := range []string{"/.well-known/agent-card.json", "/a2a/.well-known/agent-card.json"} {
		t.Run(path, func(t *testing.T) {
			resp, err := http.Get(h.server.URL + path)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d", resp.StatusCode)
			}
			var card map[string]any
			if err := json.NewDecoder(resp.Body).Decode(&card); err != nil {
				t.Fatal(err)
			}
			if card["name"] != "lightspeed-hub" {
				t.Errorf("card name = %v", card["name"])
			}
			// Clients pick their transport from this card: protocolVersion
			// must stay 0.3.x (the a2a-go v0.3 server only speaks
			// message/send|stream) and url must be non-empty (a2a-sdk only
			// maps a legacy card's protocolVersion onto an interface when
			// url is set).
			if card["protocolVersion"] != "0.3.0" {
				t.Errorf("protocolVersion = %v, want 0.3.0", card["protocolVersion"])
			}
			if s, _ := card["url"].(string); s == "" {
				t.Errorf("url is empty")
			}
			caps, _ := card["capabilities"].(map[string]any)
			if caps["streaming"] != true {
				t.Errorf("capabilities = %v, want streaming=true", caps)
			}
		})
	}
}

func TestAgentCardDescribesApprovalRequestContract(t *testing.T) {
	card := AgentCard(CardConfig{
		Description:      "Investigation with an approval checkpoint before cluster changes.",
		SkillDescription: "Investigate a cluster; execution waits at an approval checkpoint.",
	})
	const required = "input-required with an approvalRequest DataPart"
	if !strings.Contains(card.Description, required) {
		t.Errorf("card description %q does not describe the approvalRequest checkpoint", card.Description)
	}
	if got := card.Skills[0].Description; !strings.Contains(got, required) {
		t.Errorf("skill description %q does not describe the approvalRequest checkpoint", got)
	}
}

// tasks/get reflects the CR's status via the (minimal M2) mapping.
func TestTasksGetReflectsCRStatus(t *testing.T) {
	h := newHarness(t)
	resultTask(t, sendMessage(t, h, userMessage("m1", "t1", "c1", "q",
		map[string]any{"targetCluster": "spoke-a"}), freshToken, nil))

	runName := store.RunName("c1", "t1")

	// No conditions → Pending → submitted.
	decoded, _ := h.rpc(t, "tasks/get", map[string]any{"id": "t1"}, freshToken, nil)
	if taskState(resultTask(t, decoded)) != "submitted" {
		t.Errorf("fresh run should map to submitted")
	}

	// Analyzed=Unknown → Analyzing → working.
	h.kube.setConditions(runName, metav1.Condition{
		Type: agenticv1alpha1.AgenticRunConditionAnalyzed, Status: metav1.ConditionUnknown,
		Reason: "InProgress", Message: "analysis running",
	})
	decoded, _ = h.rpc(t, "tasks/get", map[string]any{"id": "t1"}, freshToken, nil)
	if taskState(resultTask(t, decoded)) != "working" {
		t.Errorf("analyzing run should map to working, got %q", taskState(resultTask(t, decoded)))
	}

	// Proposed (Analyzed=True) → input-required: the approval-decision
	// DataPart path answers it (design §7.2, M3).
	h.kube.setConditions(runName, metav1.Condition{
		Type: agenticv1alpha1.AgenticRunConditionAnalyzed, Status: metav1.ConditionTrue,
		Reason: "AnalysisComplete", Message: "options ready",
	})
	decoded, _ = h.rpc(t, "tasks/get", map[string]any{"id": "t1"}, freshToken, nil)
	if got := taskState(resultTask(t, decoded)); got != "input-required" {
		t.Errorf("proposed run maps to %q, want input-required (M3 §7.2 mapping)", got)
	}
}

func TestApprovalRequestProjectionAndOptionValidation(t *testing.T) {
	options := []agenticv1alpha1.RemediationOption{
		{Title: "Restart the service", Summary: strings.Repeat("s", 510)},
		{Title: "Increase memory"},
	}
	t.Run("approval request contract", func(t *testing.T) {
		h := newHarness(t)
		prepareProposed(t, h, "t-approval", "c-approval", options, true)
		task := getTask(t, h, "t-approval")
		status := task["status"].(map[string]any)
		message := status["message"].(map[string]any)
		parts := message["parts"].([]any)
		if len(parts) != 2 {
			t.Fatalf("approval status has %d parts, want TextPart + DataPart: %v", len(parts), parts)
		}
		text := parts[0].(map[string]any)["text"].(string)
		for _, want := range []string{"Analysis complete.", "Execution stage", "spoke-a", "1) Restart the service", "2) Increase memory", "Reply approve", "or reject"} {
			if !strings.Contains(text, want) {
				t.Errorf("human approval summary %q missing %q", text, want)
			}
		}
		partData := parts[1].(map[string]any)["data"].(map[string]any)
		if len(partData) != 1 {
			t.Fatalf("approval DataPart outer shape = %v", partData)
		}
		request := partData["approvalRequest"].(map[string]any)
		if len(request) != 5 || request["stage"] != "Execution" || request["run"] != store.RunName("c-approval", "t-approval") || request["targetCluster"] != "spoke-a" {
			t.Fatalf("approvalRequest identity/shape = %v", request)
		}
		if got := request["decisions"].([]any); len(got) != 2 || got[0] != "approve" || got[1] != "reject" {
			t.Errorf("decisions = %v", request["decisions"])
		}
		mapped := request["options"].([]any)
		if len(mapped) != 2 {
			t.Fatalf("mapped options = %v", mapped)
		}
		first := mapped[0].(map[string]any)
		if len(first) != 4 || first["index"] != float64(0) || first["number"] != float64(1) || first["title"] != "Restart the service" || len(first["summary"].(string)) != 500 {
			t.Errorf("first option mapping = %v", first)
		}
		second := mapped[1].(map[string]any)
		if second["index"] != float64(1) || second["number"] != float64(2) || second["title"] != "Increase memory" {
			t.Errorf("second option mapping = %v", second)
		}
	})

	t.Run("unreadable analysis result falls back to empty options", func(t *testing.T) {
		h := newHarness(t)
		prepareProposed(t, h, "t-no-options", "c-no-options", nil, false)
		parts := getTask(t, h, "t-no-options")["status"].(map[string]any)["message"].(map[string]any)["parts"].([]any)
		request := parts[1].(map[string]any)["data"].(map[string]any)["approvalRequest"].(map[string]any)
		if options, ok := request["options"].([]any); !ok || len(options) != 0 {
			t.Fatalf("fallback options = %#v, want []", request["options"])
		}
	})

	t.Run("out of range option is a self-correcting InvalidParams", func(t *testing.T) {
		h := newHarness(t)
		prepareProposed(t, h, "t-bad-option", "c-bad-option", options, true)
		decoded := sendMessage(t, h, approvalMessage("decision-bad", "t-bad-option", "c-bad-option", "approve", map[string]any{
			"stage": "Execution", "option": 2,
		}), freshToken, nil)
		errObj, ok := decoded["error"].(map[string]any)
		data := map[string]any{}
		if ok {
			data, _ = errObj["data"].(map[string]any)
		}
		detail, _ := data["error"].(string)
		if !ok || errObj["code"] != float64(-32602) || !strings.Contains(detail, "choose an option index") {
			t.Fatalf("out-of-range option response = %v", decoded)
		}
	})
}

func TestMessageSendReturnsInputRequiredWithoutWaiting(t *testing.T) {
	h := newHarness(t)
	prepareProposed(t, h, "t-send-input", "c-send-input", nil, false)
	// message/send is a single response and must return the checkpoint task,
	// not wait for an operator transition.
	got := resultTask(t, sendMessage(t, h, userMessage("m-input", "t-send-input", "c-send-input", "", nil), freshToken, nil))
	if state := taskState(got); state != "input-required" {
		t.Fatalf("message/send state = %q, want input-required", state)
	}
}

// tasks/cancel: not-supported for live tasks (design Q6), terminal state
// returned for finished ones (design §2.3).
func TestTasksCancel(t *testing.T) {
	h := newHarness(t)
	resultTask(t, sendMessage(t, h, userMessage("m1", "t1", "c1", "q",
		map[string]any{"targetCluster": "spoke-a"}), freshToken, nil))

	decoded, _ := h.rpc(t, "tasks/cancel", map[string]any{"id": "t1"}, freshToken, nil)
	errObj, ok := decoded["error"].(map[string]any)
	if !ok {
		t.Fatalf("want not-cancelable error, got %v", decoded)
	}
	if code, _ := errObj["code"].(float64); code != -32002 {
		t.Errorf("code = %v, want -32002 (task not cancelable)", errObj["code"])
	}

	// Terminal run: cancel returns the terminal state, not an error.
	runName := store.RunName("c1", "t1")
	h.kube.setConditions(runName, metav1.Condition{
		Type: agenticv1alpha1.AgenticRunConditionVerified, Status: metav1.ConditionTrue,
		Reason: "Verified", Message: "done",
	})
	decoded, _ = h.rpc(t, "tasks/cancel", map[string]any{"id": "t1"}, freshToken, nil)
	if got := taskState(resultTask(t, decoded)); got != "completed" {
		t.Errorf("terminal cancel = %q, want completed", got)
	}
}

// Ownership (design §5.6): a different principal gets TaskNotFound, never
// the task and never AccessDenied.
func TestOwnershipMasking(t *testing.T) {
	h := newHarness(t)
	resultTask(t, sendMessage(t, h, userMessage("m1", "t1", "c1", "q",
		map[string]any{"targetCluster": "spoke-a"}), freshToken, nil))

	otherToken := makeToken("someone-else", testNow.Add(time.Hour).Unix())
	decoded, _ := h.rpc(t, "tasks/get", map[string]any{"id": "t1"}, otherToken, nil)
	errObj, ok := decoded["error"].(map[string]any)
	if !ok {
		t.Fatalf("want TaskNotFound for non-owner, got %v", decoded)
	}
	if code, _ := errObj["code"].(float64); code != -32001 {
		t.Errorf("code = %v, want -32001 (task not found)", errObj["code"])
	}
}

// Unknown task: TaskNotFound.
func TestTasksGetUnknown(t *testing.T) {
	h := newHarness(t)
	decoded, _ := h.rpc(t, "tasks/get", map[string]any{"id": "nope"}, freshToken, nil)
	if _, ok := decoded["error"]; !ok {
		t.Fatalf("want error, got %v", decoded)
	}
}

// No credential: fail closed.
func TestNoCredentialRejected(t *testing.T) {
	h := newHarness(t)
	decoded := sendMessage(t, h, userMessage("m1", "t1", "c1", "q", nil), "", nil)
	if _, ok := decoded["error"]; !ok {
		t.Fatalf("want error without Authorization, got %v", decoded)
	}
	if h.kube.created != 0 {
		t.Errorf("creates = %d, want 0", h.kube.created)
	}
}

// message/stream: initial task event, then progress to terminal with
// Final=true.
func TestMessageStream(t *testing.T) {
	h := newHarness(t)

	body := map[string]any{"jsonrpc": "2.0", "id": 7, "method": "message/stream",
		"params": map[string]any{"message": userMessage("m1", "t1", "c1", "q",
			map[string]any{"targetCluster": "spoke-a"})}}
	raw, _ := json.Marshal(body)
	req, _ := http.NewRequest(http.MethodPost, h.server.URL+"/", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+freshToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("content-type = %q", ct)
	}

	// Advance the run to terminal while the stream is open.
	go func() {
		time.Sleep(50 * time.Millisecond)
		h.kube.setConditions(store.RunName("c1", "t1"), metav1.Condition{
			Type: agenticv1alpha1.AgenticRunConditionVerified, Status: metav1.ConditionTrue,
			Reason: "Verified", Message: "done",
		})
	}()

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 1<<20), 1<<20)
	var events []map[string]any
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		var ev map[string]any
		if err := json.Unmarshal([]byte(line[len("data: "):]), &ev); err != nil {
			t.Fatalf("bad SSE data: %q", line)
		}
		events = append(events, ev)
		result, ok := ev["result"].(map[string]any)
		if !ok {
			continue
		}
		if final, _ := result["final"].(bool); final {
			break
		}
	}
	if len(events) < 2 {
		t.Fatalf("want at least initial + final events, got %d", len(events))
	}
	first := events[0]["result"].(map[string]any)
	if first["kind"] != "task" {
		t.Errorf("first event kind = %v", first["kind"])
	}
	last := events[len(events)-1]["result"].(map[string]any)
	if last["kind"] != "status-update" || last["final"] != true {
		t.Fatalf("last event = %v", last)
	}
	status, _ := last["status"].(map[string]any)
	if status["state"] != "completed" {
		t.Errorf("final state = %v", status["state"])
	}
}

func TestInputRequiredEndsMessageStreamWithApprovalRequest(t *testing.T) {
	h := newHarnessWithConfig(t, func(c *Config) { c.StreamDeadline = 300 * time.Millisecond })
	prepareProposed(t, h, "t-stream-approval", "c-stream-approval", []agenticv1alpha1.RemediationOption{{Title: "Restart"}}, true)
	msg := userMessage("m-stream-approval", "t-stream-approval", "c-stream-approval", "follow up", nil)
	body := map[string]any{"jsonrpc": "2.0", "id": 8, "method": "message/stream", "params": map[string]any{"message": msg}}
	raw, _ := json.Marshal(body)
	req, _ := http.NewRequest(http.MethodPost, h.server.URL+"/", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+freshToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	var statusEvent map[string]any
	for _, line := range strings.Split(string(data), "\n") {
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		var event map[string]any
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &event); err != nil {
			t.Fatal(err)
		}
		result, _ := event["result"].(map[string]any)
		if result["kind"] == "status-update" {
			statusEvent = result
		}
	}
	if statusEvent == nil || statusEvent["final"] != true {
		t.Fatalf("input-required stream did not finish with Final=true: %s", data)
	}
	status := statusEvent["status"].(map[string]any)
	if status["state"] != "input-required" {
		t.Fatalf("final state = %v, want input-required", status["state"])
	}
	parts := status["message"].(map[string]any)["parts"].([]any)
	if len(parts) != 2 || parts[0].(map[string]any)["kind"] != "text" || parts[1].(map[string]any)["kind"] != "data" {
		t.Fatalf("final approval status message parts = %v", parts)
	}
	dataPart := parts[1].(map[string]any)["data"].(map[string]any)
	if _, ok := dataPart["approvalRequest"]; !ok {
		t.Errorf("Final status update lost approvalRequest DataPart: %v", dataPart)
	}
}

func TestApprovalDecisionMessageStreamContinuesPastCurrentCheckpoint(t *testing.T) {
	h := newHarnessWithConfig(t, func(c *Config) { c.StreamDeadline = 2 * time.Second })
	prepareProposed(t, h, "t-continue-approval", "c-continue-approval", []agenticv1alpha1.RemediationOption{{Title: "Restart"}}, true)
	msg := approvalMessage("m-approve-stream", "t-continue-approval", "c-continue-approval", "approve", map[string]any{
		"stage": "Execution", "option": 0,
	})
	body := map[string]any{"jsonrpc": "2.0", "id": 9, "method": "message/stream", "params": map[string]any{"message": msg}}
	raw, _ := json.Marshal(body)
	requestDone := make(chan []byte, 1)
	requestErr := make(chan error, 1)
	go func() {
		req, err := http.NewRequest(http.MethodPost, h.server.URL+"/", bytes.NewReader(raw))
		if err != nil {
			requestErr <- err
			return
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+freshToken)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			requestErr <- err
			return
		}
		defer resp.Body.Close()
		data, err := io.ReadAll(resp.Body)
		if err != nil {
			requestErr <- err
			return
		}
		requestDone <- data
	}()
	go func() {
		<-h.kube.approvalPatched
		time.Sleep(75 * time.Millisecond)
		h.kube.setConditions(store.RunName("c-continue-approval", "t-continue-approval"), metav1.Condition{
			Type: agenticv1alpha1.AgenticRunConditionExecuted, Status: metav1.ConditionUnknown,
			Reason: "InProgress", Message: "execution running",
		})
		time.Sleep(75 * time.Millisecond)
		h.kube.setConditions(store.RunName("c-continue-approval", "t-continue-approval"), metav1.Condition{
			Type: agenticv1alpha1.AgenticRunConditionVerified, Status: metav1.ConditionTrue,
			Reason: "Verified", Message: "done",
		})
	}()
	var data []byte
	select {
	case err := <-requestErr:
		t.Fatal(err)
	case data = <-requestDone:
	case <-time.After(3 * time.Second):
		t.Fatal("approval follow-up stream did not reach a terminal state")
	}
	var states []string
	for _, line := range strings.Split(string(data), "\n") {
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		var event map[string]any
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &event); err != nil {
			t.Fatal(err)
		}
		result, _ := event["result"].(map[string]any)
		if result["kind"] == "status-update" {
			status := result["status"].(map[string]any)
			states = append(states, status["state"].(string))
			if status["state"] == "input-required" && result["final"] == true {
				t.Fatalf("approval follow-up stream ended at the old checkpoint: %v", states)
			}
		}
	}
	if len(states) == 0 || states[len(states)-1] != "completed" {
		t.Fatalf("approval follow-up stream states = %v; response: %s", states, data)
	}
}

// Revise follow-up: a DataPart {"revise": "..."} patches
// spec.revisionFeedback on the existing run, no new run.
func TestReviseFollowUp(t *testing.T) {
	h := newHarness(t)
	resultTask(t, sendMessage(t, h, userMessage("m1", "t1", "c1", "first question",
		map[string]any{"targetCluster": "spoke-a"}), freshToken, nil))

	msg := userMessage("m2", "t1", "c1", "", map[string]any{"targetCluster": "spoke-a"})
	msg["parts"] = []map[string]any{
		{"kind": "data", "data": map[string]any{"revise": "focus on networking"}},
	}
	decoded := sendMessage(t, h, msg, freshToken, nil)
	if _, ok := decoded["error"]; ok {
		t.Fatalf("revise follow-up failed: %v", decoded["error"])
	}
	if h.kube.created != 1 {
		t.Errorf("creates = %d, want 1 (revision is not a new run)", h.kube.created)
	}
	runCR := h.kube.runByName(store.RunName("c1", "t1"))
	if runCR.Spec.RevisionFeedback != "focus on networking" {
		t.Errorf("revisionFeedback = %q", runCR.Spec.RevisionFeedback)
	}
	if runCR.Spec.Request != "first question" {
		t.Errorf("request must stay immutable, got %q", runCR.Spec.Request)
	}
	if runCR.Annotations[run.AnnotationRevisedBy] != auth.PrincipalID("acme-agent", "https://kc/realms/hub") {
		t.Errorf("revised-by annotation = %q", runCR.Annotations[run.AnnotationRevisedBy])
	}
}

func revisionMessage(messageID, taskID, contextID string) map[string]any {
	msg := userMessage(messageID, taskID, contextID, "", map[string]any{"targetCluster": "spoke-a"})
	msg["parts"] = []map[string]any{{"kind": "data", "data": map[string]any{"revise": "focus the analysis"}}}
	return msg
}

func TestRevisionPendingProjectionIsDurableAcrossSendGetRetryRecoveryAndResubscribe(t *testing.T) {
	h := newHarness(t)
	prepareProposed(t, h, "t-revision-durable", "c-revision-durable", []agenticv1alpha1.RemediationOption{{Title: "Old option"}}, true)
	msg := revisionMessage("m-revision-durable", "t-revision-durable", "c-revision-durable")
	first := resultTask(t, sendMessage(t, h, msg, freshToken, nil))
	if got := taskState(first); got != "working" {
		t.Fatalf("message/send after accepted revision = %q, want working", got)
	}
	if got := taskState(getTask(t, h, "t-revision-durable")); got != "working" {
		t.Fatalf("tasks/get while revised analysis is pending = %q, want working", got)
	}
	retry := resultTask(t, sendMessage(t, h, msg, freshToken, nil))
	if got := taskState(retry); got != "working" {
		t.Fatalf("duplicate revision message replay = %q, want working", got)
	}

	// A fresh handler recovers only from AgenticRun annotations. The
	// generation/observedGeneration projection must still suppress the old
	// Proposed checkpoint and resubscribe must use the same projection.
	h2 := newHarnessSharing(t, h.kube, h.clock, nil)
	if err := h2.handler.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := taskState(getTask(t, h2, "t-revision-durable")); got != "working" {
		t.Fatalf("tasks/get after recovery while revision pending = %q, want working", got)
	}
	if got := firstSSETaskState(t, h2, "tasks/resubscribe", map[string]any{"id": "t-revision-durable"}); got != "working" {
		t.Fatalf("tasks/resubscribe while revision pending = %q, want working", got)
	}
}

func TestRevisionFailureProjectsFailedInsteadOfStaleWorkingCheckpoint(t *testing.T) {
	h := newHarness(t)
	runCR := prepareProposed(t, h, "t-revision-failed", "c-revision-failed", []agenticv1alpha1.RemediationOption{{Title: "Old option"}}, true)
	resultTask(t, sendMessage(t, h, revisionMessage("m-revision-failed", "t-revision-failed", "c-revision-failed"), freshToken, nil))
	generation := h.kube.runByName(runCR.Name).Generation
	h.kube.setConditions(runCR.Name, metav1.Condition{
		Type: agenticv1alpha1.AgenticRunConditionAnalyzed, Status: metav1.ConditionFalse,
		Reason: "RevisionFailed", Message: "revised analysis failed", ObservedGeneration: generation,
	})
	if got := taskState(getTask(t, h, "t-revision-failed")); got != "failed" {
		t.Fatalf("tasks/get after current-generation revision failure = %q, want failed", got)
	}
	if got := firstSSETaskState(t, h, "tasks/resubscribe", map[string]any{"id": "t-revision-failed"}); got != "failed" {
		t.Fatalf("tasks/resubscribe after revision failure = %q, want failed", got)
	}
}

func TestCredentialPauseDuringRevisionProjectsAuthRequired(t *testing.T) {
	h := newHarness(t)
	runCR := prepareProposed(t, h, "t-revision-paused", "c-revision-paused", []agenticv1alpha1.RemediationOption{{Title: "Old option"}}, true)
	resultTask(t, sendMessage(t, h, revisionMessage("m-revision-paused", "t-revision-paused", "c-revision-paused"), freshToken, nil))
	pauseRun(t, h, runCR.Name, "credential expired during revised analysis")
	if got := taskState(getTask(t, h, "t-revision-paused")); got != "auth-required" {
		t.Fatalf("tasks/get during paused revision = %q, want auth-required", got)
	}
	if got := firstSSETaskState(t, h, "tasks/resubscribe", map[string]any{"id": "t-revision-paused"}); got != "auth-required" {
		t.Fatalf("tasks/resubscribe during paused revision = %q, want auth-required", got)
	}
}

type callerUpdateRejector struct {
	run.RunClient
	updates int
}

func (c *callerUpdateRejector) UpdateRun(context.Context, *agenticv1alpha1.AgenticRun) error {
	c.updates++
	return fmt.Errorf("caller must not update AgenticRuns")
}

func TestRevisionUsesAdapterClientAndForeignPrincipalIsTaskNotFound(t *testing.T) {
	kube := newFakeKube()
	caller := &callerUpdateRejector{RunClient: kube}
	h := newHarnessFull(t, kube, &testClock{t: testNow}, nil, func(d *Deps) {
		d.Submitters = func(context.Context, string) (run.RunClient, error) { return caller, nil }
	})
	submit(t, h, "t-revision-sa", "c-revision-sa", "question")
	msg := userMessage("m-revision-sa", "t-revision-sa", "c-revision-sa", "", nil)
	msg["parts"] = []map[string]any{{"kind": "data", "data": map[string]any{"revise": "narrow the scope"}}}
	decoded := sendMessage(t, h, msg, freshToken, nil)
	if _, ok := decoded["error"]; ok {
		t.Fatalf("adapter-SA revision failed: %v", decoded["error"])
	}
	if caller.updates != 0 {
		t.Fatalf("caller UpdateRun calls = %d, want 0", caller.updates)
	}

	foreign := makeToken("someone-else", testNow.Add(time.Hour).Unix())
	foreignMsg := userMessage("m-foreign-revision", "t-revision-sa", "c-revision-sa", "", nil)
	foreignMsg["parts"] = []map[string]any{{"kind": "data", "data": map[string]any{"revise": "steal ownership"}}}
	foreignResult := sendMessage(t, h, foreignMsg, foreign, nil)
	errObj, ok := foreignResult["error"].(map[string]any)
	if !ok || errObj["code"] != float64(-32001) {
		t.Fatalf("foreign revision response = %v, want TaskNotFound", foreignResult)
	}
	if caller.updates != 0 {
		t.Fatalf("foreign caller UpdateRun calls = %d, want 0", caller.updates)
	}
}

func TestRevisionStreamWaitsForRevisedAnalysisGeneration(t *testing.T) {
	h := newHarnessWithConfig(t, func(c *Config) { c.StreamDeadline = 2 * time.Second })
	runCR := prepareProposed(t, h, "t-revision-stream", "c-revision-stream", []agenticv1alpha1.RemediationOption{{Title: "Old option"}}, true)
	msg := userMessage("m-revision-stream", "t-revision-stream", "c-revision-stream", "", nil)
	msg["parts"] = []map[string]any{{"kind": "data", "data": map[string]any{"revise": "prefer a safer remediation"}}}
	body, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 10, "method": "message/stream", "params": map[string]any{"message": msg}})
	requestDone := make(chan []byte, 1)
	requestErr := make(chan error, 1)
	go func() {
		req, err := http.NewRequest(http.MethodPost, h.server.URL+"/", bytes.NewReader(body))
		if err != nil {
			requestErr <- err
			return
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+freshToken)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			requestErr <- err
			return
		}
		defer resp.Body.Close()
		data, err := io.ReadAll(resp.Body)
		if err != nil {
			requestErr <- err
			return
		}
		requestDone <- data
	}()

	go func() {
		time.Sleep(100 * time.Millisecond) // leave the old Proposed checkpoint observable first
		newResult := "analysis-revised-t-revision-stream"
		h.kube.mutateRun(runCR.Name, func(current *agenticv1alpha1.AgenticRun) {
			current.Status.Steps.Analysis.Results = append(current.Status.Steps.Analysis.Results, agenticv1alpha1.StepResultRef{
				Name: newResult, Outcome: agenticv1alpha1.ActionOutcomeSucceeded,
			})
		})
		h.kube.putObject(&agenticv1alpha1.AnalysisResult{
			ObjectMeta: metav1.ObjectMeta{Name: newResult, Namespace: "test-ns"},
			Spec:       agenticv1alpha1.AnalysisResultSpec{AgenticRunName: runCR.Name},
			Status:     agenticv1alpha1.AnalysisResultStatus{Options: []agenticv1alpha1.RemediationOption{{Title: "New revised option"}}},
		})
		generation := h.kube.runByName(runCR.Name).Generation
		h.kube.setConditions(runCR.Name, metav1.Condition{
			Type: agenticv1alpha1.AgenticRunConditionAnalyzed, Status: metav1.ConditionTrue,
			Reason: "RevisionComplete", Message: "revised analysis ready", ObservedGeneration: generation,
		})
	}()

	var data []byte
	select {
	case err := <-requestErr:
		t.Fatal(err)
	case data = <-requestDone:
	case <-time.After(3 * time.Second):
		t.Fatal("revision follow-up stream did not reach the revised checkpoint")
	}
	var firstState string
	var statusStates []string
	var finalApproval map[string]any
	for _, line := range strings.Split(string(data), "\n") {
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		var event map[string]any
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &event); err != nil {
			t.Fatal(err)
		}
		result, _ := event["result"].(map[string]any)
		if result["kind"] == "task" && firstState == "" {
			status := result["status"].(map[string]any)
			firstState = status["state"].(string)
		}
		if result["kind"] != "status-update" {
			continue
		}
		status := result["status"].(map[string]any)
		state := status["state"].(string)
		statusStates = append(statusStates, state)
		if state == "input-required" && result["final"] == true {
			parts := status["message"].(map[string]any)["parts"].([]any)
			finalApproval = parts[1].(map[string]any)["data"].(map[string]any)["approvalRequest"].(map[string]any)
		}
	}
	if firstState != "working" {
		t.Fatalf("initial revision-stream task state = %q, want working (no stale Proposed checkpoint)", firstState)
	}
	if len(statusStates) == 0 || statusStates[len(statusStates)-1] != "input-required" || finalApproval == nil {
		t.Fatalf("revision stream did not finish at new checkpoint: states=%v response=%s", statusStates, data)
	}
	options := finalApproval["options"].([]any)
	if len(options) != 1 || options[0].(map[string]any)["title"] != "New revised option" {
		t.Fatalf("revised approval options = %#v, want new analysis option", options)
	}
}

func TestExecutionDecisionSuppressesStaleProposedGate(t *testing.T) {
	for _, action := range []string{"approve", "reject"} {
		t.Run(action, func(t *testing.T) {
			h := newHarness(t)
			prepareProposed(t, h, "t-stale-"+action, "c-stale-"+action, []agenticv1alpha1.RemediationOption{{Title: "Restart"}}, true)
			resultTask(t, sendMessage(t, h, approvalMessage("m-stale-"+action, "t-stale-"+action, "c-stale-"+action, action, map[string]any{
				"stage": "Execution", "option": 0,
			}), freshToken, nil))
			task := getTask(t, h, "t-stale-"+action)
			if state := taskState(task); state != "working" {
				t.Fatalf("tasks/get after %s before reconciliation = %q, want working", action, state)
			}
			status, _ := task["status"].(map[string]any)
			message, _ := status["message"].(map[string]any)
			if parts, _ := message["parts"].([]any); len(parts) > 0 {
				for _, rawPart := range parts {
					part, _ := rawPart.(map[string]any)
					data, _ := part["data"].(map[string]any)
					if _, hasGate := data["approvalRequest"]; hasGate {
						t.Fatalf("stale decision still projected a manual approval gate: %v", message)
					}
				}
			}
		})
	}
}

// Target immutability on follow-up: a mismatched target is rejected, not
// OR-matched (design §5.6).
func TestFollowUpTargetImmutable(t *testing.T) {
	h := newHarness(t)
	resultTask(t, sendMessage(t, h, userMessage("m1", "t1", "c1", "q",
		map[string]any{"targetCluster": "spoke-a"}), freshToken, nil))

	decoded := sendMessage(t, h, userMessage("m2", "t1", "c1", "more",
		map[string]any{"targetCluster": "spoke-B"}), freshToken, nil)
	if _, ok := decoded["error"]; !ok {
		t.Fatalf("want error for changed target, got %v", decoded)
	}
}

// Restart recovery: a fresh handler (empty store) rebuilds the task record
// from AgenticRun annotations on tasks/get.
func TestRestartRecovery(t *testing.T) {
	h := newHarness(t)
	resultTask(t, sendMessage(t, h, userMessage("m1", "t1", "c1", "q",
		map[string]any{"targetCluster": "spoke-a"}), freshToken, nil))

	// Second harness sharing the same fake kube = adapter restart.
	handler, err := New(Config{
		Namespace: "test-ns", ClockSkew: 30 * time.Second,
		StreamDeadline: 5 * time.Second, RenewalTimeout: time.Hour,
		TerminalCleanupGrace: 30 * time.Second, SweepInterval: 10 * time.Millisecond,
		Now: func() time.Time { return testNow },
	}, Deps{
		Store:       store.New(), // empty — restart
		Credentials: auth.HeaderSource{},
		Exchanger:   stubExchanger{token: "token-h"},
		Readers:     h.kube,
		Results:     h.kube,
		Submitters:  func(context.Context, string) (run.RunClient, error) { return h.kube, nil },
		Runs:        &run.Creator{Namespace: "test-ns", AgentName: "default"},
		Tokens:      &run.TokenWriter{Secrets: h.kube, Namespace: "test-ns", Identity: "adapter-sa"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := handler.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}

	decoded, resp := h.rpc(t, "tasks/get", map[string]any{"id": "t1"}, freshToken, nil)
	_ = resp
	task := resultTask(t, decoded)
	if task["id"] != "t1" {
		t.Errorf("recovered task = %v", task)
	}
}

// TaskStateFor spot-checks for phases not reachable through the fakes.
func TestTaskStateForPhases(t *testing.T) {
	runCR := &agenticv1alpha1.AgenticRun{}
	if got := TaskStateFor(runCR); got != a2a.TaskStateSubmitted {
		t.Errorf("empty run = %v", got)
	}
	runCR.Status.Conditions = []metav1.Condition{{
		Type: agenticv1alpha1.AgenticRunConditionDenied, Status: metav1.ConditionTrue,
	}}
	if got := TaskStateFor(runCR); got != a2a.TaskStateFailed {
		t.Errorf("denied = %v", got)
	}
	runCR.Status.Conditions = []metav1.Condition{{
		Type: agenticv1alpha1.AgenticRunConditionEmergencyStopped, Status: metav1.ConditionTrue,
	}}
	if got := TaskStateFor(runCR); got != a2a.TaskStateCanceled {
		t.Errorf("emergency-stopped = %v", got)
	}
}

// With a RunTokenExchanger the run-token Secret carries the run token R
// (exchange of A for the spoke gateway's audience), never the caller's raw
// Token A: R outlives a short-lived A and is usable only at the spoke
// gateway (docs: GATEWAY_MODE_SCOPED_TOKEN_EXCHANGE.md option 2). The
// operator contract (name, key, labels) is unchanged.
func TestRunTokenExchangerStoresRunToken(t *testing.T) {
	h := newHarnessFull(t, newFakeKube(), &testClock{t: testNow}, nil, func(d *Deps) {
		d.RunTokenExchanger = stubExchanger{token: "token-r-value"}
	})
	resultTask(t, sendMessage(t, h, userMessage("m1", "t1", "c1", "q",
		map[string]any{"targetCluster": "spoke-a"}), freshToken, nil))

	runCR := h.kube.runByName(store.RunName("c1", "t1"))
	s := h.kube.secret(run.RunTokenSecretName(string(runCR.UID)))
	if s == nil {
		t.Fatal("run-token Secret not written")
	}
	if got := string(s.Data[run.RunTokenKey]); got != "token-r-value" {
		if got == freshToken {
			t.Fatal("run-token Secret holds the raw Token A, want the run token R")
		}
		t.Fatalf("run-token value = %q, want the run token R", got)
	}
	if s.Labels[run.LabelAgenticRun] != string(runCR.UID) || s.Labels[run.LabelSpokeCluster] != "spoke-a" {
		t.Errorf("run-token labels changed: %v", s.Labels)
	}
}

func TestRunTokenAudienceGuardRejectsExecutionAudienceBeforeInitialWrite(t *testing.T) {
	const readAudience = "praxis-spoke-gateway"
	const execAudience = "praxis-spoke-gateway-exec"
	badRunToken := audienceToken(t, []string{readAudience, execAudience})
	h := newHarnessFull(t, newFakeKube(), &testClock{t: testNow}, func(c *Config) {
		c.RunTokenAudience = readAudience
		c.ExecutionTokenAudience = execAudience
	}, func(d *Deps) {
		d.RunTokenExchanger = stubExchanger{token: badRunToken}
		d.ExecutionTokenExchanger = stubExchanger{token: audienceToken(t, execAudience)}
	})

	decoded := sendMessage(t, h, userMessage("m-aud-initial", "t-aud-initial", "c-aud-initial", "inspect",
		map[string]any{"targetCluster": "spoke-a"}), freshToken, nil)
	if errObj, ok := decoded["error"]; !ok || !strings.Contains(fmt.Sprint(errObj), "Keycloak did not downscope aud") {
		t.Fatalf("initial run-token audience error = %v", errObj)
	}
	if strings.Contains(fmt.Sprint(decoded["error"]), badRunToken) {
		t.Fatal("caller error leaked exchanged token")
	}
	if h.kube.secWritt != 0 {
		t.Fatalf("Secret writes = %d, want 0", h.kube.secWritt)
	}
	runCR := h.kube.runByName(store.RunName("c-aud-initial", "t-aud-initial"))
	if runCR == nil || h.kube.secret(run.RunTokenSecretName(string(runCR.UID))) != nil {
		t.Fatal("rejected run token was written to a Secret")
	}
}

func TestRunTokenAudienceGuardRejectsExecutionAudienceBeforeRenewalWrite(t *testing.T) {
	const readAudience = "praxis-spoke-gateway"
	const execAudience = "praxis-spoke-gateway-exec"
	goodRunToken := audienceToken(t, readAudience)
	badRunToken := audienceToken(t, []string{readAudience, execAudience})
	runEx := &sequenceExchanger{tokens: []string{goodRunToken, badRunToken}}
	h := newHarnessFull(t, newFakeKube(), &testClock{t: testNow}, func(c *Config) {
		c.RunTokenAudience = readAudience
		c.ExecutionTokenAudience = execAudience
	}, func(d *Deps) {
		d.RunTokenExchanger = runEx
		d.ExecutionTokenExchanger = stubExchanger{token: audienceToken(t, execAudience)}
	})

	submit(t, h, "t-aud-renew", "c-aud-renew", "start")
	runCR := h.kube.runByName(store.RunName("c-aud-renew", "t-aud-renew"))
	secretName := run.RunTokenSecretName(string(runCR.UID))
	originalSecret := h.kube.secret(secretName)
	if originalSecret == nil || string(originalSecret.Data[run.RunTokenKey]) != goodRunToken {
		t.Fatalf("initial run token Secret = %v", originalSecret)
	}
	writesBefore := h.kube.secWritt
	pauseRun(t, h, runCR.Name, "test scoped token renewal")
	decoded := followUp(t, h, "t-aud-renew", "c-aud-renew", "m-aud-renew", "", renewToken, nil)
	if errObj, ok := decoded["error"]; !ok || !strings.Contains(fmt.Sprint(errObj), "Keycloak did not downscope aud") {
		t.Fatalf("renewal run-token audience error = %v", errObj)
	}
	if h.kube.secWritt != writesBefore {
		t.Fatalf("Secret writes after rejected renewal = %d, want unchanged %d", h.kube.secWritt, writesBefore)
	}
	got := h.kube.secret(secretName)
	if string(got.Data[run.RunTokenKey]) != goodRunToken {
		t.Fatal("rejected renewal changed token-a")
	}
}

func TestExecutionApprovalWritesBothCredentialsBeforeCallerPatch(t *testing.T) {
	runEx := &sequenceExchanger{tokens: []string{"run-initial", "run-approved"}}
	executionEx := &sequenceExchanger{tokens: []string{"execution-approved"}}
	h := newHarnessFull(t, newFakeKube(), &testClock{t: testNow}, nil, func(d *Deps) {
		d.RunTokenExchanger = runEx
		d.ExecutionTokenExchanger = executionEx
	})
	runCR := prepareProposed(t, h, "t-exec-approve", "c-exec-approve", []agenticv1alpha1.RemediationOption{{Title: "Restart"}}, true)
	h.kube.mu.Lock()
	h.kube.order = nil
	h.kube.mu.Unlock()
	resultTask(t, sendMessage(t, h, approvalMessage("m-approve-exec", "t-exec-approve", "c-exec-approve", "approve", map[string]any{
		"stage": "Execution", "option": 0,
	}), freshToken, nil))

	h.kube.mu.Lock()
	order := append([]string(nil), h.kube.order...)
	h.kube.mu.Unlock()
	if len(order) != 2 || order[0] != "secret-update" || order[1] != "approval-update" {
		t.Fatalf("approval side-effect order = %v, want one Secret update before caller approval patch", order)
	}
	secret := h.kube.secret(run.RunTokenSecretName(string(runCR.UID)))
	if got := string(secret.Data[run.RunTokenKey]); got != "run-approved" {
		t.Errorf("token-a = %q, want refreshed run token", got)
	}
	if got := string(secret.Data[run.ExecutionTokenKey]); got != "execution-approved" {
		t.Errorf("token-execution = %q, want approval-only execution token", got)
	}
	approval, err := h.kube.GetApproval(context.Background(), "test-ns", runCR.Name)
	if err != nil {
		t.Fatal(err)
	}
	if len(approval.Spec.Stages) != 2 || approval.Spec.Stages[1].Type != agenticv1alpha1.ApprovalStageExecution || approval.Spec.Stages[1].Decision != agenticv1alpha1.ApprovalDecisionApproved {
		t.Fatalf("caller approval patch = %#v", approval.Spec.Stages)
	}
	if got := h.kube.runByName(runCR.Name).Annotations[run.AnnotationApprovedAnalysis]; got == "" {
		t.Fatal("adapter SA did not bind the approval to an analysis identity")
	}
}

func TestExecutionApprovalReplayAfterOptionTrimmingAndRecovery(t *testing.T) {
	runEx := &sequenceExchanger{tokens: []string{"run-initial", "run-approved", "run-replay-1", "run-replay-2"}}
	executionEx := &sequenceExchanger{tokens: []string{"exec-approved", "exec-replay-1", "exec-replay-2"}}
	kube := newFakeKube()
	h := newHarnessFull(t, kube, &testClock{t: testNow}, nil, func(d *Deps) {
		d.RunTokenExchanger = runEx
		d.ExecutionTokenExchanger = executionEx
	})
	runCR := prepareProposed(t, h, "t-replay-trim", "c-replay-trim", []agenticv1alpha1.RemediationOption{
		{Title: "First"}, {Title: "Selected second"},
	}, true)
	decision := func(id string) map[string]any {
		return approvalMessage(id, "t-replay-trim", "c-replay-trim", "approve", map[string]any{
			"stage": "Execution", "option": 1,
		})
	}
	resultTask(t, sendMessage(t, h, decision("m-first-approval"), freshToken, nil))

	// Repo C trims all options except the selected one after it observes the
	// approval. A replay still carries the original option index (1).
	h.kube.mutateObject("analysis-t-replay-trim", func(obj ctrlclient.Object) {
		analysis := obj.(*agenticv1alpha1.AnalysisResult)
		analysis.Status.Options = []agenticv1alpha1.RemediationOption{{Title: "Selected second"}}
	})
	resultTask(t, sendMessage(t, h, decision("m-replay-after-trim"), freshToken, nil))

	// Rebuild the handler/store as after process restart, then replay again.
	h2 := newHarnessFull(t, kube, &testClock{t: testNow}, nil, func(d *Deps) {
		d.RunTokenExchanger = runEx
		d.ExecutionTokenExchanger = executionEx
	})
	if err := h2.handler.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	resultTask(t, sendMessage(t, h2, decision("m-replay-after-recovery"), freshToken, nil))

	secret := kube.secret(run.RunTokenSecretName(string(runCR.UID)))
	if got := string(secret.Data[run.RunTokenKey]); got != "run-replay-2" {
		t.Errorf("final token-a = %q, want last idempotent replay renewal", got)
	}
	if got := string(secret.Data[run.ExecutionTokenKey]); got != "exec-replay-2" {
		t.Errorf("final token-execution = %q, want last idempotent replay renewal", got)
	}
	approval, err := kube.GetApproval(context.Background(), "test-ns", runCR.Name)
	if err != nil {
		t.Fatal(err)
	}
	stage := executionStage(approval)
	if stage == nil || stage.Execution == nil || stage.Execution.Option == nil || *stage.Execution.Option != 1 {
		t.Fatalf("replayed approval stage = %+v, want immutable option index 1", stage)
	}
}

func TestExecutionApprovalReplayAfterExecutionFinishedIsNoOp(t *testing.T) {
	for _, tc := range []struct {
		name       string
		condition  metav1.Condition
		recoverRun bool
	}{
		{
			name: "verification",
			condition: metav1.Condition{
				Type: agenticv1alpha1.AgenticRunConditionExecuted, Status: metav1.ConditionTrue,
				Reason: "Executed", Message: "execution finished",
			},
		},
		{
			name: "completed",
			condition: metav1.Condition{
				Type: agenticv1alpha1.AgenticRunConditionVerified, Status: metav1.ConditionTrue,
				Reason: "Verified", Message: "run completed",
			},
		},
		{
			name:       "completed after recovery",
			recoverRun: true,
			condition: metav1.Condition{
				Type: agenticv1alpha1.AgenticRunConditionVerified, Status: metav1.ConditionTrue,
				Reason: "Verified", Message: "run completed",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runEx := &sequenceExchanger{tokens: []string{"run-initial", "run-approved", "must-not-renew"}}
			executionEx := &sequenceExchanger{tokens: []string{"exec-approved", "must-not-remint"}}
			kube := newFakeKube()
			h := newHarnessFull(t, kube, &testClock{t: testNow}, nil, func(d *Deps) {
				d.RunTokenExchanger = runEx
				d.ExecutionTokenExchanger = executionEx
			})
			runCR := prepareProposed(t, h, "t-replay-finished-"+strings.ReplaceAll(tc.name, " ", "-"), "c-replay-finished-"+strings.ReplaceAll(tc.name, " ", "-"), []agenticv1alpha1.RemediationOption{{Title: "Restart"}}, true)
			taskID := runCR.Annotations[run.AnnotationTaskID]
			contextID := runCR.Annotations[run.AnnotationContextID]
			resultTask(t, sendMessage(t, h, approvalMessage("m-first-finished-"+tc.name, taskID, contextID, "approve", map[string]any{
				"stage": "Execution", "option": 0,
			}), freshToken, nil))
			runExCalls, executionExCalls := runEx.calls, executionEx.calls

			h.kube.setConditions(runCR.Name, tc.condition)
			if tc.recoverRun {
				h = newHarnessFull(t, kube, &testClock{t: testNow}, nil, func(d *Deps) {
					d.RunTokenExchanger = runEx
					d.ExecutionTokenExchanger = executionEx
				})
				if err := h.handler.Recover(context.Background()); err != nil {
					t.Fatal(err)
				}
			}

			resultTask(t, sendMessage(t, h, approvalMessage("m-replay-finished-"+tc.name, taskID, contextID, "approve", map[string]any{
				"stage": "Execution", "option": 0,
			}), freshToken, nil))
			if runEx.calls != runExCalls || executionEx.calls != executionExCalls {
				t.Fatalf("finished Execution replay exchanged credentials: run %d->%d, execution %d->%d",
					runExCalls, runEx.calls, executionExCalls, executionEx.calls)
			}
		})
	}
}

func TestExecutionTokenRenewalRejectsMismatchedAnalysisIdentity(t *testing.T) {
	runEx := &sequenceExchanger{tokens: []string{"run-initial", "run-approved", "run-renewed"}}
	executionEx := &sequenceExchanger{tokens: []string{"exec-approved", "must-not-mint"}}
	h := newHarnessFull(t, newFakeKube(), &testClock{t: testNow}, nil, func(d *Deps) {
		d.RunTokenExchanger = runEx
		d.ExecutionTokenExchanger = executionEx
	})
	runCR := prepareProposed(t, h, "t-analysis-mismatch", "c-analysis-mismatch", []agenticv1alpha1.RemediationOption{{Title: "Restart"}}, true)
	resultTask(t, sendMessage(t, h, approvalMessage("m-approve-analysis-mismatch", "t-analysis-mismatch", "c-analysis-mismatch", "approve", map[string]any{
		"stage": "Execution", "option": 0,
	}), freshToken, nil))

	newAnalysis := "analysis-new-generation"
	h.kube.mutateRun(runCR.Name, func(current *agenticv1alpha1.AgenticRun) {
		current.Generation++
		current.Status.Steps.Analysis.Results = append(current.Status.Steps.Analysis.Results, agenticv1alpha1.StepResultRef{
			Name: newAnalysis, Outcome: agenticv1alpha1.ActionOutcomeSucceeded,
		})
	})
	h.kube.putObject(&agenticv1alpha1.AnalysisResult{
		ObjectMeta: metav1.ObjectMeta{Name: newAnalysis, Namespace: "test-ns"},
		Spec:       agenticv1alpha1.AnalysisResultSpec{AgenticRunName: runCR.Name},
		Status:     agenticv1alpha1.AnalysisResultStatus{Options: []agenticv1alpha1.RemediationOption{{Title: "Different analysis"}}},
	})
	generation := h.kube.runByName(runCR.Name).Generation
	h.kube.setConditions(runCR.Name, metav1.Condition{
		Type: agenticv1alpha1.AgenticRunConditionAnalyzed, Status: metav1.ConditionTrue,
		Reason: "RevisionComplete", ObservedGeneration: generation,
	})
	pauseRun(t, h, runCR.Name, "analysis identity changed before credential renewal")
	resultTask(t, followUp(t, h, "t-analysis-mismatch", "c-analysis-mismatch", "m-renew-analysis-mismatch", "", renewToken, nil))

	secret := h.kube.secret(run.RunTokenSecretName(string(runCR.UID)))
	if _, exists := secret.Data[run.ExecutionTokenKey]; exists {
		t.Fatal("stale token-execution remained after analysis identity changed")
	}
	if got := string(secret.Data[run.RunTokenKey]); got != "run-renewed" {
		t.Errorf("token-a renewal = %q", got)
	}
	if executionEx.calls != 1 {
		t.Fatalf("execution-token exchanges = %d, want only initial approval mint", executionEx.calls)
	}
}

func TestReviseAfterExecutionDecisionRejected(t *testing.T) {
	h := newHarness(t)
	runCR := prepareProposed(t, h, "t-revise-after-decision", "c-revise-after-decision", []agenticv1alpha1.RemediationOption{{Title: "Restart"}}, true)
	resultTask(t, sendMessage(t, h, approvalMessage("m-exec-decided", "t-revise-after-decision", "c-revise-after-decision", "approve", map[string]any{
		"stage": "Execution", "option": 0,
	}), freshToken, nil))
	msg := userMessage("m-revise-decided", "t-revise-after-decision", "c-revise-after-decision", "", nil)
	msg["parts"] = []map[string]any{{"kind": "data", "data": map[string]any{"revise": "request a different remediation"}}}
	decoded := sendMessage(t, h, msg, freshToken, nil)
	errObj, ok := decoded["error"].(map[string]any)
	const want = "execution was already decided for this run; start a new task to request a different remediation"
	if !ok || errObj["code"] != float64(-32602) || errObj["message"] != want {
		t.Fatalf("revise-after-decision response = %v, want InvalidParams with %q", decoded, want)
	}
	current := h.kube.runByName(runCR.Name)
	if current.Spec.RevisionFeedback != "" {
		t.Fatalf("revision was written after immutable Execution decision: %q", current.Spec.RevisionFeedback)
	}
}

func TestMixedFollowUpCommandsAreRejectedWithoutWrites(t *testing.T) {
	decision := map[string]any{"stage": "Execution", "option": float64(0)}
	tests := []struct {
		name  string
		parts a2a.ContentParts
	}{
		{
			name: "approve and revise in one DataPart",
			parts: a2a.ContentParts{a2a.DataPart{Data: map[string]any{
				"approve": decision, "revise": "change the plan",
			}}},
		},
		{
			name: "reject and revise across DataParts",
			parts: a2a.ContentParts{
				a2a.DataPart{Data: map[string]any{"reject": map[string]any{"stage": "Execution"}}},
				a2a.DataPart{Data: map[string]any{"revise": "change the plan"}},
			},
		},
		{
			name: "approve and reject",
			parts: a2a.ContentParts{a2a.DataPart{Data: map[string]any{
				"approve": decision, "reject": map[string]any{"stage": "Execution"},
			}}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarnessFull(t, newFakeKube(), &testClock{t: testNow}, nil, func(d *Deps) {
				d.RunTokenExchanger = &sequenceExchanger{tokens: []string{"run-initial", "must-not-write"}}
				d.ExecutionTokenExchanger = &sequenceExchanger{tokens: []string{"must-not-write"}}
			})
			runCR := prepareProposed(t, h, "t-mixed", "c-mixed", []agenticv1alpha1.RemediationOption{{Title: "Restart"}}, true)
			beforeRun := h.kube.runByName(runCR.Name)
			beforeApproval, err := h.kube.GetApproval(context.Background(), "test-ns", runCR.Name)
			if err != nil {
				t.Fatal(err)
			}
			secretName := run.RunTokenSecretName(string(runCR.UID))
			beforeSecret := h.kube.secret(secretName)
			writesBefore := h.kube.secWritt

			msg := userMessage("m-mixed", "t-mixed", "c-mixed", "", nil)
			msg["parts"] = tt.parts
			decoded := sendMessage(t, h, msg, freshToken, nil)
			if errObj, ok := decoded["error"].(map[string]any); !ok || errObj["code"] != float64(-32602) {
				t.Fatalf("mixed command response = %v, want InvalidParams", decoded)
			}
			if h.kube.secWritt != writesBefore {
				t.Fatalf("Secret writes changed %d -> %d for invalid command", writesBefore, h.kube.secWritt)
			}
			afterRun := h.kube.runByName(runCR.Name)
			if afterRun.Spec.RevisionFeedback != beforeRun.Spec.RevisionFeedback ||
				!reflect.DeepEqual(afterRun.Annotations, beforeRun.Annotations) {
				t.Fatalf("run revision/annotations mutated for invalid command: before=%+v after=%+v", beforeRun, afterRun)
			}
			afterApproval, err := h.kube.GetApproval(context.Background(), "test-ns", runCR.Name)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(afterApproval.Spec, beforeApproval.Spec) {
				t.Fatalf("approval mutated for invalid command: before=%+v after=%+v", beforeApproval.Spec, afterApproval.Spec)
			}
			afterSecret := h.kube.secret(secretName)
			if !reflect.DeepEqual(afterSecret.Data, beforeSecret.Data) {
				t.Fatal("run-token Secret data changed for invalid command")
			}
		})
	}
}

func TestAdapterTerminalRecordReloadPreventsStaleExecutionApprovalReplay(t *testing.T) {
	runEx := &sequenceExchanger{tokens: []string{"run-initial", "must-not-renew"}}
	executionEx := &sequenceExchanger{tokens: []string{"must-not-mint"}}
	h := newHarnessFull(t, newFakeKube(), &testClock{t: testNow}, nil, func(d *Deps) {
		d.RunTokenExchanger = runEx
		d.ExecutionTokenExchanger = executionEx
	})
	runCR := prepareProposed(t, h, "t-terminal-replay", "c-terminal-replay", []agenticv1alpha1.RemediationOption{{Title: "Restart"}}, true)
	stale, ok := h.handler.deps.Store.Task("t-terminal-replay")
	if !ok {
		t.Fatal("task record missing")
	}
	current, _ := h.handler.deps.Store.Task("t-terminal-replay")
	current.AdapterTerminal = true
	current.FailureReason = "credential renewal pause cap exceeded; task failed"
	current.AuthPaused = false
	current.State = string(a2a.TaskStateFailed)
	h.handler.deps.Store.PutTask(current)
	secretName := run.RunTokenSecretName(string(runCR.UID))
	beforeSecret := h.kube.secret(secretName)
	writesBefore := h.kube.secWritt
	approvalBefore, err := h.kube.GetApproval(context.Background(), "test-ns", runCR.Name)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/", nil)
	request.Header.Set("Authorization", "Bearer "+renewToken)
	cred, err := h.handler.deps.Credentials.Credential(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	msg := &a2a.Message{
		ID: "m-stale-terminal-replay", TaskID: a2a.TaskID(stale.TaskID), ContextID: stale.ContextID,
		Role: a2a.MessageRoleUser,
		Parts: a2a.ContentParts{a2a.DataPart{Data: map[string]any{
			"approve": map[string]any{"stage": "Execution", "option": float64(0)},
		}}},
	}
	if _, err := h.handler.handleFollowUp(context.Background(), stale, msg, cred, "spoke-a"); err != nil {
		t.Fatalf("stale execution approval replay: %v", err)
	}
	if h.kube.secWritt != writesBefore || runEx.calls != 1 || executionEx.calls != 0 {
		t.Fatalf("stale terminal replay wrote credentials: Secret writes %d->%d, run exchanges=%d execution exchanges=%d",
			writesBefore, h.kube.secWritt, runEx.calls, executionEx.calls)
	}
	if after := h.kube.secret(secretName); !reflect.DeepEqual(after.Data, beforeSecret.Data) {
		t.Fatal("stale adapter-terminal replay changed run-token Secret")
	}
	approvalAfter, err := h.kube.GetApproval(context.Background(), "test-ns", runCR.Name)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(approvalAfter.Spec, approvalBefore.Spec) {
		t.Fatal("stale adapter-terminal replay changed approval")
	}
	stored, _ := h.handler.deps.Store.Task(stale.TaskID)
	if !stored.AdapterTerminal || stored.FailureReason == "" {
		t.Fatalf("authoritative terminal record was overwritten: %+v", stored)
	}
}

func TestOmittedApprovalStagesAreImmutableAndNeverAdopted(t *testing.T) {
	runEx := &sequenceExchanger{tokens: []string{"run-initial", "run-should-not-mint"}}
	executionEx := &sequenceExchanger{tokens: []string{"exec-should-not-mint"}}
	h := newHarnessFull(t, newFakeKube(), &testClock{t: testNow}, nil, func(d *Deps) {
		d.RunTokenExchanger = runEx
		d.ExecutionTokenExchanger = executionEx
	})
	runCR := prepareProposed(t, h, "t-omitted-decision", "c-omitted-decision", []agenticv1alpha1.RemediationOption{{Title: "Restart"}, {Title: "Stop"}}, true)
	zero := int32(0)
	approval, err := h.kube.GetApproval(context.Background(), "test-ns", runCR.Name)
	if err != nil {
		t.Fatal(err)
	}
	approval.Spec.Stages = []agenticv1alpha1.ApprovalStage{
		{Type: agenticv1alpha1.ApprovalStageAnalysis}, // omitted means effectively Approved
		{Type: agenticv1alpha1.ApprovalStageExecution, Execution: &agenticv1alpha1.ExecutionApproval{Option: &zero}},
	}
	h.kube.putObject(approval)
	approvalBefore, err := h.kube.GetApproval(context.Background(), "test-ns", runCR.Name)
	if err != nil {
		t.Fatal(err)
	}
	runBefore := h.kube.runByName(runCR.Name)
	secretName := run.RunTokenSecretName(string(runCR.UID))
	secretBefore := h.kube.secret(secretName)
	writesBefore, runExchangesBefore, executionExchangesBefore := h.kube.secWritt, runEx.calls, executionEx.calls
	if !executionWasDecided(approval) || !executionStageApproved(approval) {
		t.Fatal("omitted Execution decision must match the operator's approved semantics")
	}
	if got := taskState(getTask(t, h, "t-omitted-decision")); got != "working" {
		t.Fatalf("omitted Execution stage projected as %q, want working (already decided)", got)
	}

	for _, tc := range []struct {
		name   string
		stage  string
		action string
		option any
	}{
		{name: "matching omitted Analysis replay", stage: "Analysis", action: "approve"},
		{name: "matching omitted Execution replay", stage: "Execution", action: "approve", option: float64(0)},
	} {
		fields := map[string]any{"stage": tc.stage}
		if tc.option != nil {
			fields["option"] = tc.option
		}
		resultTask(t, sendMessage(t, h, approvalMessage("m-"+strings.ReplaceAll(tc.name, " ", "-"), "t-omitted-decision", "c-omitted-decision", tc.action, fields), freshToken, nil))
	}
	for _, tc := range []struct {
		name   string
		stage  string
		action string
		option any
	}{
		{name: "reject omitted Analysis", stage: "Analysis", action: "reject"},
		{name: "reject omitted Execution", stage: "Execution", action: "reject"},
		{name: "change omitted Execution option", stage: "Execution", action: "approve", option: float64(1)},
	} {
		fields := map[string]any{"stage": tc.stage}
		if tc.option != nil {
			fields["option"] = tc.option
		}
		decoded := sendMessage(t, h, approvalMessage("m-"+strings.ReplaceAll(tc.name, " ", "-"), "t-omitted-decision", "c-omitted-decision", tc.action, fields), freshToken, nil)
		if _, ok := decoded["error"]; !ok {
			t.Fatalf("%s was accepted: %v", tc.name, decoded)
		}
	}
	msg := userMessage("m-revise-omitted", "t-omitted-decision", "c-omitted-decision", "", nil)
	msg["parts"] = []map[string]any{{"kind": "data", "data": map[string]any{"revise": "change the plan"}}}
	decoded := sendMessage(t, h, msg, freshToken, nil)
	if errObj, ok := decoded["error"].(map[string]any); !ok || !strings.Contains(fmt.Sprint(errObj["message"]), "execution was already decided") {
		t.Fatalf("revision after omitted Execution decision = %v, want rejection", decoded)
	}

	assertUnchanged := func(when string) {
		t.Helper()
		after, err := h.kube.GetApproval(context.Background(), "test-ns", runCR.Name)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(after.Spec, approvalBefore.Spec) {
			t.Fatalf("%s changed omitted approval stages: before=%+v after=%+v", when, approvalBefore.Spec.Stages, after.Spec.Stages)
		}
		runAfter := h.kube.runByName(runCR.Name)
		if !reflect.DeepEqual(runAfter.Annotations, runBefore.Annotations) {
			t.Fatalf("%s changed run annotations: before=%v after=%v", when, runBefore.Annotations, runAfter.Annotations)
		}
		secretAfter := h.kube.secret(secretName)
		if !reflect.DeepEqual(secretAfter.Data, secretBefore.Data) || h.kube.secWritt != writesBefore {
			t.Fatalf("%s changed Secret: writes %d->%d data before=%v after=%v", when, writesBefore, h.kube.secWritt, secretBefore.Data, secretAfter.Data)
		}
		if runEx.calls != runExchangesBefore || executionEx.calls != executionExchangesBefore {
			t.Fatalf("%s exchanged credentials: run %d->%d, execution %d->%d", when, runExchangesBefore, runEx.calls, executionExchangesBefore, executionEx.calls)
		}
	}
	assertUnchanged("matching/conflicting replays")

	// A finished omitted Execution stage is also a no-op and cannot mint a
	// token after execution completion.
	h.kube.setConditions(runCR.Name, metav1.Condition{
		Type: agenticv1alpha1.AgenticRunConditionExecuted, Status: metav1.ConditionTrue, Reason: "Done",
	})
	resultTask(t, sendMessage(t, h, approvalMessage("m-finished-omitted-replay", "t-omitted-decision", "c-omitted-decision", "approve", map[string]any{
		"stage": "Execution", "option": float64(0),
	}), freshToken, nil))
	assertUnchanged("finished omitted Execution replay")
	if got := h.kube.secret(secretName); len(got.Data[run.ExecutionTokenKey]) != 0 {
		t.Fatal("unbound omitted approval unexpectedly had a scoped execution credential")
	}
}

func TestOmittedExecutionObjectMatchesOptionlessReplayWithoutMutation(t *testing.T) {
	for _, tc := range []struct {
		name      string
		execution *agenticv1alpha1.ExecutionApproval
	}{
		{name: "nil Execution object"},
		{name: "empty Execution object", execution: &agenticv1alpha1.ExecutionApproval{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runEx := &sequenceExchanger{tokens: []string{"run-initial", "run-should-not-mint"}}
			executionEx := &sequenceExchanger{tokens: []string{"exec-should-not-mint"}}
			h := newHarnessFull(t, newFakeKube(), &testClock{t: testNow}, nil, func(d *Deps) {
				d.RunTokenExchanger = runEx
				d.ExecutionTokenExchanger = executionEx
			})
			runCR := prepareProposed(t, h, "t-optionless-replay", "c-optionless-replay", []agenticv1alpha1.RemediationOption{{Title: "Restart"}}, true)
			approval, err := h.kube.GetApproval(context.Background(), "test-ns", runCR.Name)
			if err != nil {
				t.Fatal(err)
			}
			approval.Spec.Stages = append(approval.Spec.Stages, agenticv1alpha1.ApprovalStage{
				Type:      agenticv1alpha1.ApprovalStageExecution,
				Execution: tc.execution,
			})
			h.kube.putObject(approval)

			approvalBefore, err := h.kube.GetApproval(context.Background(), "test-ns", runCR.Name)
			if err != nil {
				t.Fatal(err)
			}
			runBefore := h.kube.runByName(runCR.Name)
			secretName := run.RunTokenSecretName(string(runCR.UID))
			secretBefore := h.kube.secret(secretName)
			writesBefore, runExchangesBefore, executionExchangesBefore := h.kube.secWritt, runEx.calls, executionEx.calls

			assertUnchanged := func(when string) {
				t.Helper()
				after, err := h.kube.GetApproval(context.Background(), "test-ns", runCR.Name)
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(after.Spec, approvalBefore.Spec) {
					t.Fatalf("%s changed omitted approval stages", when)
				}
				if afterRun := h.kube.runByName(runCR.Name); !reflect.DeepEqual(afterRun.Annotations, runBefore.Annotations) {
					t.Fatalf("%s changed run annotations", when)
				}
				if afterSecret := h.kube.secret(secretName); !reflect.DeepEqual(afterSecret.Data, secretBefore.Data) || h.kube.secWritt != writesBefore {
					t.Fatalf("%s changed Secret: writes %d->%d", when, writesBefore, h.kube.secWritt)
				}
				if runEx.calls != runExchangesBefore || executionEx.calls != executionExchangesBefore {
					t.Fatalf("%s exchanged credentials: run %d->%d, execution %d->%d", when, runExchangesBefore, runEx.calls, executionExchangesBefore, executionEx.calls)
				}
			}

			resultTask(t, sendMessage(t, h, approvalMessage("m-optionless-replay", "t-optionless-replay", "c-optionless-replay", "approve", map[string]any{
				"stage": "Execution",
			}), freshToken, nil))
			assertUnchanged("optionless matching replay")
			if got := h.kube.secret(secretName); len(got.Data[run.ExecutionTokenKey]) != 0 {
				t.Fatal("unbound omitted approval unexpectedly minted an execution credential")
			}

			decoded := sendMessage(t, h, approvalMessage("m-option-zero-conflict", "t-optionless-replay", "c-optionless-replay", "approve", map[string]any{
				"stage": "Execution", "option": float64(0),
			}), freshToken, nil)
			if _, ok := decoded["error"]; !ok {
				t.Fatalf("explicit option 0 was adopted by an omitted option stage: %v", decoded)
			}
			assertUnchanged("conflicting explicit option replay")
		})
	}
}

func TestExecutionApprovalRenewalClearsAdapterPauseOverlay(t *testing.T) {
	runEx := &sequenceExchanger{tokens: []string{"run-initial", "run-approved", "run-renewed"}}
	executionEx := &sequenceExchanger{tokens: []string{"exec-approved", "exec-renewed"}}
	h := newHarnessFull(t, newFakeKube(), &testClock{t: testNow}, nil, func(d *Deps) {
		d.RunTokenExchanger = runEx
		d.ExecutionTokenExchanger = executionEx
	})
	runCR := prepareProposed(t, h, "t-approval-pause", "c-approval-pause", []agenticv1alpha1.RemediationOption{{Title: "Restart"}}, true)
	decision := approvalMessage("m-approval-pause", "t-approval-pause", "c-approval-pause", "approve", map[string]any{
		"stage": "Execution", "option": float64(0),
	})
	resultTask(t, sendMessage(t, h, decision, freshToken, nil))
	h.kube.setConditions(runCR.Name, metav1.Condition{
		Type: agenticv1alpha1.AgenticRunConditionExecuted, Status: metav1.ConditionUnknown,
		Reason: "InProgress", Message: "execution running",
	})
	pauseRun(t, h, runCR.Name, "execution credential paused")
	if got := taskState(getTask(t, h, "t-approval-pause")); got != "auth-required" {
		t.Fatalf("pre-renewal state = %q, want auth-required", got)
	}
	decision = approvalMessage("m-approval-pause-replay", "t-approval-pause", "c-approval-pause", "approve", map[string]any{
		"stage": "Execution", "option": float64(0),
	})
	resultTask(t, sendMessage(t, h, decision, renewToken, nil))
	if got := taskState(getTask(t, h, "t-approval-pause")); got == "auth-required" {
		t.Fatal("successful approval credential renewal retained auth-required overlay")
	}
	h.kube.mutateRun(runCR.Name, func(current *agenticv1alpha1.AgenticRun) {
		conditions := current.Status.Conditions[:0]
		for _, cond := range current.Status.Conditions {
			if cond.Type != agenticv1alpha1.AgenticRunConditionCredentialsExpired {
				conditions = append(conditions, cond)
			}
		}
		current.Status.Conditions = conditions
	})
	if got := taskState(getTask(t, h, "t-approval-pause")); got == "auth-required" {
		t.Fatal("operator condition removal restored stale auth-required overlay")
	}
}

func TestExecutionApprovalAudienceGuardWritesNeitherTokenNorApproval(t *testing.T) {
	const readAudience = "praxis-spoke-gateway"
	const execAudience = "praxis-spoke-gateway-exec"
	goodRunToken := audienceToken(t, readAudience)
	badExecutionToken := audienceToken(t, []string{execAudience, readAudience})
	runEx := &sequenceExchanger{tokens: []string{goodRunToken, audienceToken(t, readAudience)}}
	h := newHarnessFull(t, newFakeKube(), &testClock{t: testNow}, func(c *Config) {
		c.RunTokenAudience = readAudience
		c.ExecutionTokenAudience = execAudience
	}, func(d *Deps) {
		d.RunTokenExchanger = runEx
		d.ExecutionTokenExchanger = stubExchanger{token: badExecutionToken}
	})
	runCR := prepareProposed(t, h, "t-aud-approval", "c-aud-approval", []agenticv1alpha1.RemediationOption{{Title: "Restart"}}, true)
	secretName := run.RunTokenSecretName(string(runCR.UID))
	secretBefore := h.kube.secret(secretName)
	writesBefore := h.kube.secWritt
	h.kube.mu.Lock()
	h.kube.order = nil
	h.kube.mu.Unlock()

	decoded := sendMessage(t, h, approvalMessage("m-aud-approval", "t-aud-approval", "c-aud-approval", "approve", map[string]any{
		"stage": "Execution", "option": 0,
	}), freshToken, nil)
	if errObj, ok := decoded["error"]; !ok || !strings.Contains(fmt.Sprint(errObj), "Keycloak did not downscope aud") {
		t.Fatalf("approval execution-token audience error = %v", errObj)
	}
	if strings.Contains(fmt.Sprint(decoded["error"]), badExecutionToken) {
		t.Fatal("caller error leaked exchanged token")
	}
	secretAfter := h.kube.secret(secretName)
	if h.kube.secWritt != writesBefore || string(secretAfter.Data[run.RunTokenKey]) != string(secretBefore.Data[run.RunTokenKey]) {
		t.Fatal("rejected execution token caused a run-token Secret write")
	}
	if _, exists := secretAfter.Data[run.ExecutionTokenKey]; exists {
		t.Fatal("rejected execution token was written")
	}
	h.kube.mu.Lock()
	order := append([]string(nil), h.kube.order...)
	h.kube.mu.Unlock()
	if len(order) != 0 {
		t.Fatalf("API mutations after rejected execution token = %v, want none", order)
	}
	approval, err := h.kube.GetApproval(context.Background(), "test-ns", runCR.Name)
	if err != nil {
		t.Fatal(err)
	}
	if len(approval.Spec.Stages) != 1 {
		t.Fatalf("approval stages = %#v, want unchanged Analysis-only approval", approval.Spec.Stages)
	}
}

func TestExecutionTokenRemovedWhenCallerApprovalPatchFails(t *testing.T) {
	runEx := &sequenceExchanger{tokens: []string{"run-initial", "run-approved"}}
	executionEx := &sequenceExchanger{tokens: []string{"execution-approved"}}
	h := newHarnessFull(t, newFakeKube(), &testClock{t: testNow}, nil, func(d *Deps) {
		d.RunTokenExchanger = runEx
		d.ExecutionTokenExchanger = executionEx
	})
	runCR := prepareProposed(t, h, "t-exec-fail", "c-exec-fail", []agenticv1alpha1.RemediationOption{{Title: "Restart"}}, true)
	h.kube.mu.Lock()
	h.kube.failApproval = true
	h.kube.order = nil
	h.kube.mu.Unlock()
	decoded := sendMessage(t, h, approvalMessage("m-approve-fail", "t-exec-fail", "c-exec-fail", "approve", map[string]any{
		"stage": "Execution", "option": 0,
	}), freshToken, nil)
	if _, ok := decoded["error"]; !ok {
		t.Fatalf("injected approval patch failure was hidden: %v", decoded)
	}
	secret := h.kube.secret(run.RunTokenSecretName(string(runCR.UID)))
	if _, exists := secret.Data[run.ExecutionTokenKey]; exists {
		t.Fatal("token-execution remained after caller approval patch failure")
	}
	if string(secret.Data[run.RunTokenKey]) != "run-approved" {
		t.Errorf("token-a = %q, expected run token to remain after cleanup", secret.Data[run.RunTokenKey])
	}
	h.kube.mu.Lock()
	order := append([]string(nil), h.kube.order...)
	h.kube.mu.Unlock()
	if len(order) != 3 || order[0] != "secret-update" || order[1] != "approval-update" || order[2] != "secret-update" {
		t.Errorf("write/patch/cleanup order = %v", order)
	}
}

func TestApprovalUpdateUncertainResponseReplayKeepsAndRenewsCredentials(t *testing.T) {
	runEx := &sequenceExchanger{tokens: []string{"run-initial", "run-first-approval", "run-replayed"}}
	executionEx := &sequenceExchanger{tokens: []string{"exec-first-approval", "exec-replayed"}}
	h := newHarnessFull(t, newFakeKube(), &testClock{t: testNow}, nil, func(d *Deps) {
		d.RunTokenExchanger = runEx
		d.ExecutionTokenExchanger = executionEx
	})
	runCR := prepareProposed(t, h, "t-uncertain-approval", "c-uncertain-approval", []agenticv1alpha1.RemediationOption{{Title: "Restart"}}, true)
	h.kube.mu.Lock()
	h.kube.failAfterApprovalPersist = true
	h.kube.mu.Unlock()
	first := sendMessage(t, h, approvalMessage("m-uncertain-first", "t-uncertain-approval", "c-uncertain-approval", "approve", map[string]any{
		"stage": "Execution", "option": 0,
	}), freshToken, nil)
	if _, ok := first["error"]; !ok {
		t.Fatalf("uncertain update response was hidden: %v", first)
	}
	secret := h.kube.secret(run.RunTokenSecretName(string(runCR.UID)))
	if got := string(secret.Data[run.ExecutionTokenKey]); got != "exec-first-approval" {
		t.Fatalf("execution credential removed despite committed approval: %q", got)
	}
	h.kube.mu.Lock()
	h.kube.failAfterApprovalPersist = false
	h.kube.mu.Unlock()
	resultTask(t, sendMessage(t, h, approvalMessage("m-uncertain-retry", "t-uncertain-approval", "c-uncertain-approval", "approve", map[string]any{
		"stage": "Execution", "option": 0,
	}), freshToken, nil))
	secret = h.kube.secret(run.RunTokenSecretName(string(runCR.UID)))
	if got := string(secret.Data[run.ExecutionTokenKey]); got != "exec-replayed" {
		t.Fatalf("idempotent replay did not refresh token-execution: %q", got)
	}
}

func TestConcurrentIdenticalApprovalsSerializeAndRefreshCredentials(t *testing.T) {
	runEx := &sequenceExchanger{tokens: []string{"run-initial", "run-approval-one", "run-approval-two"}}
	executionEx := &sequenceExchanger{tokens: []string{"exec-approval-one", "exec-approval-two"}}
	h := newHarnessFull(t, newFakeKube(), &testClock{t: testNow}, nil, func(d *Deps) {
		d.RunTokenExchanger = runEx
		d.ExecutionTokenExchanger = executionEx
	})
	runCR := prepareProposed(t, h, "t-concurrent-approval", "c-concurrent-approval", []agenticv1alpha1.RemediationOption{{Title: "Restart"}}, true)
	barrier := make(chan struct{})
	results := make(chan map[string]any, 2)
	for _, messageID := range []string{"m-concurrent-one", "m-concurrent-two"} {
		message := approvalMessage(messageID, "t-concurrent-approval", "c-concurrent-approval", "approve", map[string]any{
			"stage": "Execution", "option": 0,
		})
		go func(msg map[string]any) {
			<-barrier
			results <- sendMessage(t, h, msg, freshToken, nil)
		}(message)
	}
	close(barrier)
	for range 2 {
		decoded := <-results
		resultTask(t, decoded)
	}
	secret := h.kube.secret(run.RunTokenSecretName(string(runCR.UID)))
	if got := string(secret.Data[run.RunTokenKey]); got != "run-approval-two" {
		t.Errorf("token-a after serialized duplicate approvals = %q", got)
	}
	if got := string(secret.Data[run.ExecutionTokenKey]); got != "exec-approval-two" {
		t.Errorf("token-execution after serialized duplicate approvals = %q", got)
	}
	approval, err := h.kube.GetApproval(context.Background(), "test-ns", runCR.Name)
	if err != nil {
		t.Fatal(err)
	}
	if stage := executionStage(approval); stage == nil || stage.Decision != agenticv1alpha1.ApprovalDecisionApproved {
		t.Fatalf("concurrent approval decision = %+v", stage)
	}
}

func TestRejectDoesNotMintExecutionOrRunTokens(t *testing.T) {
	runEx := &sequenceExchanger{tokens: []string{"run-initial", "should-not-be-minted"}}
	executionEx := &sequenceExchanger{tokens: []string{"should-not-be-minted"}}
	h := newHarnessFull(t, newFakeKube(), &testClock{t: testNow}, nil, func(d *Deps) {
		d.RunTokenExchanger = runEx
		d.ExecutionTokenExchanger = executionEx
	})
	runCR := prepareProposed(t, h, "t-exec-reject", "c-exec-reject", []agenticv1alpha1.RemediationOption{{Title: "Restart"}}, true)
	runEx.calls, executionEx.calls = 0, 0
	resultTask(t, sendMessage(t, h, approvalMessage("m-reject-exec", "t-exec-reject", "c-exec-reject", "reject", map[string]any{
		"stage": "Execution",
	}), freshToken, nil))
	if runEx.calls != 0 || executionEx.calls != 0 {
		t.Fatalf("reject minted credentials: run exchanges=%d execution exchanges=%d", runEx.calls, executionEx.calls)
	}
	secret := h.kube.secret(run.RunTokenSecretName(string(runCR.UID)))
	if _, exists := secret.Data[run.ExecutionTokenKey]; exists {
		t.Fatal("reject wrote token-execution")
	}
}

func TestRenewalDuringApprovedExecutionRenewsBothKeys(t *testing.T) {
	runEx := &sequenceExchanger{tokens: []string{"run-initial", "run-approved", "run-renewed"}}
	executionEx := &sequenceExchanger{tokens: []string{"execution-approved", "execution-renewed"}}
	h := newHarnessFull(t, newFakeKube(), &testClock{t: testNow}, nil, func(d *Deps) {
		d.RunTokenExchanger = runEx
		d.ExecutionTokenExchanger = executionEx
	})
	runCR := prepareProposed(t, h, "t-exec-renew", "c-exec-renew", []agenticv1alpha1.RemediationOption{{Title: "Restart"}}, true)
	resultTask(t, sendMessage(t, h, approvalMessage("m-approve-renew", "t-exec-renew", "c-exec-renew", "approve", map[string]any{
		"stage": "Execution", "option": 0,
	}), freshToken, nil))
	runName := runCR.Name
	h.kube.setConditions(runName, metav1.Condition{
		Type: agenticv1alpha1.AgenticRunConditionExecuted, Status: metav1.ConditionUnknown,
		Reason: "InProgress", Message: "execution running",
	})
	pauseRun(t, h, runName, "renew both scoped credentials")
	resultTask(t, followUp(t, h, "t-exec-renew", "c-exec-renew", "m-renew-exec", "", renewToken, nil))
	secret := h.kube.secret(run.RunTokenSecretName(string(runCR.UID)))
	if got := string(secret.Data[run.RunTokenKey]); got != "run-renewed" {
		t.Errorf("renewed token-a = %q", got)
	}
	if got := string(secret.Data[run.ExecutionTokenKey]); got != "execution-renewed" {
		t.Errorf("renewed token-execution = %q", got)
	}
}

func TestExecutionDenialCompletesWithAnalysisArtifact(t *testing.T) {
	h := newHarness(t)
	runCR := prepareProposed(t, h, "t-denied-exec", "c-denied-exec", []agenticv1alpha1.RemediationOption{{Title: "Restart"}}, true)
	h.kube.putObject(&agenticv1alpha1.AgenticRunApproval{
		ObjectMeta: metav1.ObjectMeta{Name: runCR.Name, Namespace: "test-ns"},
		Spec: agenticv1alpha1.AgenticRunApprovalSpec{Stages: []agenticv1alpha1.ApprovalStage{
			agenticv1alpha1.NewApprovalStage(agenticv1alpha1.ApprovalStageAnalysis, agenticv1alpha1.ApprovalDecisionApproved, "", nil),
			agenticv1alpha1.NewApprovalStage(agenticv1alpha1.ApprovalStageExecution, agenticv1alpha1.ApprovalDecisionDenied, "", nil),
		}},
	})
	h.kube.setConditions(runCR.Name, metav1.Condition{
		Type: agenticv1alpha1.AgenticRunConditionDenied, Status: metav1.ConditionTrue,
		Reason: "Denied", Message: "execution declined",
	})
	task := getTask(t, h, "t-denied-exec")
	if got := taskState(task); got != "completed" {
		t.Fatalf("execution denial state = %q, want completed", got)
	}
	text := task["status"].(map[string]any)["message"].(map[string]any)["parts"].([]any)[0].(map[string]any)["text"]
	if text != "Execution rejected by approver; analysis result attached." {
		t.Errorf("denial status message = %q", text)
	}
	artifacts := task["artifacts"].([]any)
	if len(artifacts) == 0 || artifacts[0].(map[string]any)["name"] != "analysis" {
		t.Fatalf("execution rejection did not retain analysis artifact: %v", artifacts)
	}
}
