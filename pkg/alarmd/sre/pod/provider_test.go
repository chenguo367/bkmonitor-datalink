// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package pod

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
)

type fixtureClient struct {
	mu          sync.Mutex
	pod         corev1.Pod
	owners      map[string]OwnerResource
	getCount    int
	changeAt    int
	change      func(*corev1.Pod)
	listOptions metav1.ListOptions
	logOptions  corev1.PodLogOptions
	logs        string
	listError   error
	ownerError  error
}

func (f *fixtureClient) ListPods(ctx context.Context, namespace string, options metav1.ListOptions) (*corev1.PodList, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.listOptions = options
	return &corev1.PodList{Items: []corev1.Pod{*f.pod.DeepCopy()}}, f.listError
}
func (f *fixtureClient) GetPod(ctx context.Context, namespace, name string) (*corev1.Pod, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.getCount++
	if f.changeAt > 0 && f.getCount >= f.changeAt {
		f.change(&f.pod)
	}
	return f.pod.DeepCopy(), nil
}
func (f *fixtureClient) GetOwner(ctx context.Context, namespace string, owner metav1.OwnerReference) (OwnerResource, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.owners[owner.Name], f.ownerError
}
func (f *fixtureClient) OpenLogs(ctx context.Context, namespace, name string, options corev1.PodLogOptions) (io.ReadCloser, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.logOptions = options
	return io.NopCloser(strings.NewReader(f.logs)), nil
}

func controllerRef(kind, name, uid string) metav1.OwnerReference {
	yes := true
	group := "apps/v1"
	if kind == "Job" || kind == "CronJob" {
		group = "batch/v1"
	}
	return metav1.OwnerReference{APIVersion: group, Kind: kind, Name: name, UID: types.UID(uid), Controller: &yes}
}
func fixture() (*fixtureClient, Scope, Target) {
	labels := map[string]string{"app": "web", "app.kubernetes.io/instance": "release", "app.kubernetes.io/component": "fusion"}
	client := &fixtureClient{pod: corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "tenant", Name: "web-abc", UID: "pod-uid", Labels: labels, OwnerReferences: []metav1.OwnerReference{controllerRef("ReplicaSet", "web-rs", "rs-uid")}}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "web", Image: "web:v1", Env: []corev1.EnvVar{{Name: "PASSWORD", Value: "never-return-env"}}}}}, Status: corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{{Name: "web", ContainerID: "containerd://cid", ImageID: "sha256:image", Ready: true, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}}}, owners: map[string]OwnerResource{}, logs: "hello"}
	client.owners["web-rs"] = OwnerResource{Kind: "ReplicaSet", Namespace: "tenant", Name: "web-rs", UID: "rs-uid", Labels: labels, Owners: []metav1.OwnerReference{controllerRef("Deployment", "web", "deployment-uid")}, Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}}}
	client.owners["web"] = OwnerResource{Kind: "Deployment", Namespace: "tenant", Name: "web", UID: "deployment-uid", Labels: labels, Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}}}
	scope := Scope{Namespace: "tenant", MatchLabels: map[string]string{"app": "web"}, Release: "release", Module: "fusion", Workloads: []Workload{{Kind: "Deployment", Name: "web"}}}
	target := Target{Namespace: "tenant", Pod: "web-abc", UID: "pod-uid", Container: "web", ContainerID: "containerd://cid", ImageID: "sha256:image"}
	return client, scope, target
}
func requireCode(t *testing.T, err error, code string) {
	t.Helper()
	var named *Error
	if !errors.As(err, &named) || named.Code != code {
		t.Fatalf("error=%v, want %s", err, code)
	}
}
func mustProvider(t *testing.T, options Options) *Provider {
	t.Helper()
	provider, err := New(options)
	if err != nil {
		t.Fatal(err)
	}
	return provider
}

func TestScopeValidationAndYAML(t *testing.T) {
	var scopes []Scope
	if err := yaml.Unmarshal([]byte("- namespace: tenant\n  match_labels:\n    app: web\n  timeout:\n    contract: python3_process_group_v1\n    python_path: /usr/bin/python3\n  workloads:\n  - kind: Deployment\n    name: web\n"), &scopes); err != nil {
		t.Fatal(err)
	}
	if err := ValidateScopes(scopes); err != nil {
		t.Fatal(err)
	}
	if scopes[0].Timeout.PythonPath != "/usr/bin/python3" {
		t.Fatalf("yaml tags not applied: %+v", scopes)
	}
	if err := ValidateScopes(nil); err != nil {
		t.Fatal(err)
	}
	for _, scope := range []Scope{{Namespace: "tenant"}, {Namespace: "../tenant", MatchLabels: map[string]string{"app": "web"}}, {Namespace: "tenant", MatchLabels: map[string]string{"app": "web"}, Timeout: RuntimeContract{Contract: TimeoutContract, PythonPath: "python3"}}, {Namespace: "tenant", MatchLabels: map[string]string{"app": "web"}, Timeout: RuntimeContract{PythonPath: "/python3"}}} {
		requireCode(t, ValidateScopes([]Scope{scope}), CodeInvalidRequest)
	}
}

func TestDiscoverInspectOnlySafeFields(t *testing.T) {
	client, scope, target := fixture()
	provider := mustProvider(t, Options{Client: client, Scopes: []Scope{scope}})
	result, err := provider.Discover(context.Background(), DiscoverRequest{Namespace: "tenant", Release: "release", Module: "fusion", Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Targets) != 1 || result.Targets[0].Target != target || result.Targets[0].Workload.Name != "web" || len(result.Targets[0].OwnerChain) != 2 {
		t.Fatalf("discover: %+v", result)
	}
	if !strings.Contains(client.listOptions.LabelSelector, "app=web") || client.listOptions.Limit != 1 {
		t.Fatalf("unbounded selector: %+v", client.listOptions)
	}
	encoded, _ := json.Marshal(result)
	if strings.Contains(string(encoded), "never-return-env") || strings.Contains(string(encoded), "PASSWORD") {
		t.Fatalf("pod environment leaked: %s", encoded)
	}
	observation, err := provider.Inspect(context.Background(), TargetRequest{Target: Target{Namespace: "tenant", Pod: target.Pod, Container: "web"}})
	if err != nil || observation.Target != target {
		t.Fatalf("inspect=%+v err=%v", observation, err)
	}
}

func TestRejectNamespaceLabelsOwnerAndContainer(t *testing.T) {
	tests := map[string]func(*fixtureClient, *Scope, *Target){
		"namespace":  func(c *fixtureClient, s *Scope, target *Target) { target.Namespace = "other" },
		"pod labels": func(c *fixtureClient, s *Scope, target *Target) { c.pod.Labels = map[string]string{"app": "other"} },
		"owner labels": func(c *fixtureClient, s *Scope, target *Target) {
			owner := c.owners["web"]
			owner.Labels = map[string]string{"app": "other"}
			c.owners["web"] = owner
		},
		"owner UID": func(c *fixtureClient, s *Scope, target *Target) {
			owner := c.owners["web-rs"]
			owner.UID = "replacement"
			c.owners["web-rs"] = owner
		},
		"selector": func(c *fixtureClient, s *Scope, target *Target) {
			owner := c.owners["web"]
			owner.Selector.MatchLabels = map[string]string{"app": "other"}
			c.owners["web"] = owner
		},
		"workload":      func(c *fixtureClient, s *Scope, target *Target) { s.Workloads[0].Name = "other" },
		"unknown owner": func(c *fixtureClient, s *Scope, target *Target) { c.pod.OwnerReferences[0].Kind = "CustomWorkload" },
		"container":     func(c *fixtureClient, s *Scope, target *Target) { target.Container = "sidecar" },
	}
	for name, change := range tests {
		t.Run(name, func(t *testing.T) {
			client, scope, target := fixture()
			change(client, &scope, &target)
			provider := mustProvider(t, Options{Client: client, Scopes: []Scope{scope}})
			_, err := provider.Inspect(context.Background(), TargetRequest{Target: target})
			requireCode(t, err, CodeNotInScope)
		})
	}
}

func TestRejectStaleIdentityBeforeExec(t *testing.T) {
	for _, field := range []string{"uid", "container", "image"} {
		t.Run(field, func(t *testing.T) {
			client, scope, target := fixture()
			switch field {
			case "uid":
				target.UID = "old"
			case "container":
				target.ContainerID = "old"
			case "image":
				target.ImageID = "old"
			}
			calls := 0
			provider := mustProvider(t, Options{Client: client, Scopes: []Scope{scope}, Executor: executorFunc(func(ctx context.Context, request StreamRequest) (StreamResult, error) {
				calls++
				return StreamResult{}, nil
			})})
			_, err := provider.Exec(context.Background(), ExecRequest{Target: target, Argv: []string{"true"}})
			requireCode(t, err, CodeTargetChanged)
			if calls != 0 {
				t.Fatal("executed a stale target")
			}
		})
	}
}

func TestLogsBoundRedactionAndTargetChanged(t *testing.T) {
	client, scope, target := fixture()
	client.logs = "password=visible\nsecretliteral\n" + strings.Repeat("x", 100)
	client.changeAt = 2
	client.change = func(pod *corev1.Pod) { pod.Status.ContainerStatuses[0].ContainerID = "new" }
	provider := mustProvider(t, Options{Client: client, Scopes: []Scope{scope}, LiteralSecrets: []string{"secretliteral"}})
	result, err := provider.Logs(context.Background(), LogsRequest{Target: target, Previous: true, SinceSeconds: 60, OutputBytes: 64})
	requireCode(t, err, CodeTargetChanged)
	if !result.Truncated || !result.TargetChanged || result.ObservedAfter == nil || strings.Contains(result.Output, "visible") || strings.Contains(result.Output, "secretliteral") || len(result.Output) > 64 || !client.logOptions.Previous || client.logOptions.Follow {
		t.Fatalf("logs: %+v options=%+v", result, client.logOptions)
	}
	if result.RedactionContract != RedactionContract {
		t.Fatal("missing redaction policy")
	}
}

type executorFunc func(context.Context, StreamRequest) (StreamResult, error)

func (f executorFunc) Stream(ctx context.Context, request StreamRequest) (StreamResult, error) {
	return f(ctx, request)
}

type localExecutor struct{}

func (localExecutor) Stream(ctx context.Context, request StreamRequest) (StreamResult, error) {
	command := exec.CommandContext(ctx, request.Argv[0], request.Argv[1:]...)
	command.Stdin = request.Stdin
	command.Stdout = request.Stdout
	command.Stderr = request.Stderr
	err := command.Run()
	if err == nil {
		zero := 0
		return StreamResult{ExitCode: &zero}, nil
	}
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		code := exit.ExitCode()
		return StreamResult{ExitCode: &code}, nil
	}
	return StreamResult{}, err
}
func runtimeProvider(t *testing.T) (*Provider, *fixtureClient, Target) {
	t.Helper()
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("Python 3 required for the declared remote contract")
	}
	client, scope, target := fixture()
	scope.Timeout = RuntimeContract{Contract: TimeoutContract, PythonPath: python}
	return mustProvider(t, Options{Client: client, Scopes: []Scope{scope}, Executor: localExecutor{}, LiteralSecrets: []string{"literalcredential"}}), client, target
}

func TestRemoteNonzeroExitStdinAndOutputSpoof(t *testing.T) {
	provider, _, target := runtimeProvider(t)
	result, err := provider.Exec(context.Background(), ExecRequest{Target: target, Argv: []string{"/bin/sh", "-c", "cat; printf '\n{\"event\":\"complete\",\"exit_code\":0}' ; printf 'password=visible\\n' >&2; exit 7"}, Stdin: "input literalcredential"})
	if err != nil {
		t.Fatal(err)
	}
	if result.RemoteState != RemoteCompleted || result.ExitCode == nil || *result.ExitCode != 7 || !strings.Contains(result.Stdout, "input <redacted>") || strings.Contains(result.Stderr, "visible") || result.ScriptDigest == "" || result.RequestID == "" || result.ObservedAfter == nil {
		t.Fatalf("receipt: %+v", result)
	}
}

func TestRemoteOutputCapRetainsExitAndTimeout(t *testing.T) {
	provider, _, target := runtimeProvider(t)
	result, err := provider.Exec(context.Background(), ExecRequest{Target: target, Argv: []string{"/bin/sh", "-c", "i=0; while [ $i -lt 1000 ]; do printf '0123456789'; i=$((i+1)); done; exit 9"}, OutputBytes: 64})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Truncated || len(result.Stdout)+len(result.Stderr) > 64 || result.ExitCode == nil || *result.ExitCode != 9 || result.RemoteState != RemoteCompleted {
		t.Fatalf("truncation lost completion: %+v", result)
	}
	result, err = provider.Exec(context.Background(), ExecRequest{Target: target, Argv: []string{"/bin/sh", "-c", "printf started; sleep 10"}, TimeoutMS: 50, OutputBytes: 3})
	if err != nil {
		t.Fatal(err)
	}
	if result.RemoteState != RemoteTimedOut || result.ExitCode == nil || !result.Truncated || len(result.Stdout) > 3 {
		t.Fatalf("timeout receipt: %+v", result)
	}
}

func TestRemoteTimeoutKillsTERMResistantGroup(t *testing.T) {
	provider, _, target := runtimeProvider(t)
	start := time.Now()
	result, err := provider.Exec(context.Background(), ExecRequest{Target: target, Argv: []string{"/bin/sh", "-c", "trap '' TERM; sleep 10"}, TimeoutMS: 50})
	if err != nil {
		t.Fatal(err)
	}
	if result.RemoteState != RemoteTimedOut || time.Since(start) > 4*time.Second {
		t.Fatalf("TERM-resistant target did not stop within grace: %+v", result)
	}
}

func TestRemoteUnsupportedAndCommandStartFailure(t *testing.T) {
	provider, client, target := runtimeProvider(t)
	result, err := provider.Exec(context.Background(), ExecRequest{Target: target, Argv: []string{"/no/such/command"}})
	requireCode(t, err, CodeCommandStartFailed)
	if result.RemoteState != RemoteNotStarted {
		t.Fatalf("launch failure: %+v", result)
	}
	scope := provider.options.Scopes[0]
	scope.Timeout.PythonPath = "/no/such/python3"
	missing := mustProvider(t, Options{Client: client, Scopes: []Scope{scope}, Executor: executorFunc(func(ctx context.Context, request StreamRequest) (StreamResult, error) {
		code := 127
		return StreamResult{ExitCode: &code}, nil
	})})
	result, err = missing.Exec(context.Background(), ExecRequest{Target: target, Argv: []string{"true"}})
	requireCode(t, err, CodeTimeoutUnsupported)
	if result.RemoteState != RemoteNotStarted {
		t.Fatalf("unsupported: %+v", result)
	}
	scope.Timeout = RuntimeContract{}
	missing = mustProvider(t, Options{Client: client, Scopes: []Scope{scope}, Executor: localExecutor{}})
	_, err = missing.Exec(context.Background(), ExecRequest{Target: target, Argv: []string{"true"}})
	requireCode(t, err, CodeTimeoutUnsupported)
}

func TestTargetReplacementAfterExecution(t *testing.T) {
	provider, client, target := runtimeProvider(t)
	client.changeAt = 2
	client.change = func(pod *corev1.Pod) { pod.UID = "replaced" }
	result, err := provider.Exec(context.Background(), ExecRequest{Target: target, Argv: []string{"/bin/sh", "-c", "printf evidence"}})
	requireCode(t, err, CodeTargetChanged)
	if !result.TargetChanged || result.RemoteState != RemoteCompleted || result.EvidenceScope != "target_changed" || result.Stdout != "evidence" {
		t.Fatalf("replacement receipt: %+v", result)
	}
}

func TestTransportFailureUnknownAndNoRetry(t *testing.T) {
	provider, _, target := runtimeProvider(t)
	calls := 0
	provider.options.Executor = executorFunc(func(ctx context.Context, request StreamRequest) (StreamResult, error) {
		calls++
		return StreamResult{}, errors.New("Authorization=credential")
	})
	result, err := provider.Exec(context.Background(), ExecRequest{Target: target, Argv: []string{"true"}})
	requireCode(t, err, RemoteUnknown)
	if calls != 1 || result.RemoteState != RemoteUnknown || strings.Contains(err.Error(), "credential") {
		t.Fatalf("transport result: %+v err=%v calls=%d", result, err, calls)
	}
}

func TestConcurrentRequestsAndBudgets(t *testing.T) {
	provider, _, target := runtimeProvider(t)
	entered := make(chan struct{})
	unblock := make(chan struct{})
	provider.options.Executor = executorFunc(func(ctx context.Context, request StreamRequest) (StreamResult, error) {
		close(entered)
		<-unblock
		return StreamResult{}, errors.New("lost")
	})
	done := make(chan struct{})
	go func() {
		defer close(done)
		provider.Exec(context.Background(), ExecRequest{Target: target, Argv: []string{"true"}})
	}()
	<-entered
	_, err := provider.Exec(context.Background(), ExecRequest{Target: target, Argv: []string{"true"}})
	requireCode(t, err, CodeBusy)
	close(unblock)
	<-done
	for _, request := range []ExecRequest{{Target: target}, {Target: target, Argv: []string{"true"}, Stdin: strings.Repeat("x", 32<<10+1)}, {Target: target, Argv: []string{"true"}, TimeoutMS: 30001}, {Target: target, Argv: []string{"true"}, OutputBytes: 512<<10 + 1}} {
		_, err := provider.Exec(context.Background(), request)
		requireCode(t, err, CodeInvalidRequest)
	}
}

func TestStandardExecIsPOSTNonTTYAndNeverRetries(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Method != "POST" || r.URL.Path != "/api/v1/namespaces/tenant/pods/web-abc/exec" || r.URL.Query().Get("tty") == "true" || r.URL.Query().Get("container") != "web" || strings.Join(r.URL.Query()["command"], " ") != "python3 -c script" {
			t.Errorf("incorrect standard exec request: %s %s", r.Method, r.URL)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(403)
		io.WriteString(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Forbidden","code":403}`)
	}))
	defer server.Close()
	executor, err := NewKubernetesExecutor(&rest.Config{Host: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	_, _, target := fixture()
	_, err = executor.Stream(context.Background(), StreamRequest{Target: target, Argv: []string{"python3", "-c", "script"}, Stdout: io.Discard, Stderr: io.Discard})
	if err == nil || calls != 1 {
		t.Fatalf("exec err=%v calls=%d", err, calls)
	}
}

type startedWriter struct {
	destination io.Writer
	once        sync.Once
	started     chan struct{}
}

func (w *startedWriter) Write(data []byte) (int, error) {
	n, err := w.destination.Write(data)
	if strings.Contains(string(data), `"event":"started"`) {
		w.once.Do(func() { close(w.started) })
	}
	return n, err
}

func TestCancellationReturnsUnknownAndRemoteTimerContinues(t *testing.T) {
	provider, _, target := runtimeProvider(t)
	started := make(chan struct{})
	remoteDone := make(chan error, 1)
	provider.options.Executor = executorFunc(func(ctx context.Context, request StreamRequest) (StreamResult, error) {
		command := exec.Command(request.Argv[0], request.Argv[1:]...)
		command.Stdin = request.Stdin
		command.Stdout = &startedWriter{destination: request.Stdout, started: started}
		command.Stderr = request.Stderr
		if err := command.Start(); err != nil {
			return StreamResult{}, err
		}
		go func() { remoteDone <- command.Wait() }()
		select {
		case err := <-remoteDone:
			zero := 0
			return StreamResult{ExitCode: &zero}, err
		case <-ctx.Done():
			return StreamResult{}, ctx.Err()
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan ExecReceipt, 1)
	go func() {
		result, err := provider.Exec(ctx, ExecRequest{Target: target, Argv: []string{"/bin/sh", "-c", "sleep 10"}, TimeoutMS: 200})
		if err == nil {
			t.Error("cancel did not return a named failure")
		}
		done <- result
	}()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("remote supervisor did not start")
	}
	cancel()
	select {
	case result := <-done:
		if result.RemoteState != RemoteUnknown || result.ExitCode != nil || result.EvidenceScope != "post_observation_unavailable" {
			t.Fatalf("canceled receipt: %+v", result)
		}
	case <-time.After(time.Second):
		t.Fatal("cancellation did not return promptly")
	}
	select {
	case err := <-remoteDone:
		if err != nil {
			t.Fatalf("independent remote timer failed: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("remote process outlived its independent deadline")
	}
}

func TestSupervisorBrokenOutputPipeRetainsDeadline(t *testing.T) {
	provider, _, _ := runtimeProvider(t)
	python := provider.options.Scopes[0].Timeout.PythonPath
	command := exec.Command(python, "-u", "-c", remoteSupervisor, "nonce", "0.15", "128", `["/bin/sh","-c","while true; do printf output; sleep 0.01; done"]`)
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	line, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil || !strings.Contains(line, `"event":"started"`) {
		t.Fatalf("start frame=%q err=%v", line, err)
	}
	stdout.Close()
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("broken pipe defeated deadline: %v", err)
		}
	case <-time.After(4 * time.Second):
		command.Process.Kill()
		t.Fatal("supervisor failed to finish after broken pipe")
	}
}

func TestContainerAndProbeStatusWithoutMessages(t *testing.T) {
	client, scope, target := fixture()
	client.pod.Spec.NodeName = "node-a"
	deleted := metav1.Now()
	client.pod.DeletionTimestamp = &deleted
	client.pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse, Reason: "ContainersNotReady", Message: "credential=hidden"}}
	status := &client.pod.Status.ContainerStatuses[0]
	status.RestartCount = 3
	status.State = corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff", Message: "secret=hidden"}}
	status.LastTerminationState.Terminated = &corev1.ContainerStateTerminated{Reason: "OOMKilled", ExitCode: 137, Signal: 9}
	provider := mustProvider(t, Options{Client: client, Scopes: []Scope{scope}})
	observed, err := provider.Inspect(context.Background(), TargetRequest{Target: target})
	if err != nil {
		t.Fatal(err)
	}
	if observed.RestartCount != 3 || observed.WaitingReason != "CrashLoopBackOff" || observed.LastTermination == nil || observed.LastTermination.ExitCode != 137 || len(observed.Conditions) != 1 || observed.NodeName != "node-a" || observed.DeletionTimestamp == nil || observed.Running {
		t.Fatalf("missing status: %+v", observed)
	}
	encoded, _ := json.Marshal(observed)
	if strings.Contains(string(encoded), "hidden") {
		t.Fatalf("status message leaked: %s", encoded)
	}
}

func TestKingeyeModuleScopesSelectByActualLabels(t *testing.T) {
	client, scope, _ := fixture()
	scope.Release = ""
	scope.Module = ""
	scope.MatchLabels = map[string]string{"app.kubernetes.io/instance": "release", "kingeye.app/module": "fusion"}
	client.pod.Labels["kingeye.app/module"] = "fusion"
	second := scope
	second.MatchLabels = map[string]string{"app.kubernetes.io/instance": "release", "kingeye.app/module": "uq"}
	provider := mustProvider(t, Options{Client: client, Scopes: []Scope{scope, second}})
	scopes := provider.Scopes()
	if scopes[0].Module != "fusion" || scopes[0].Release != "release" {
		t.Fatalf("scope indices: %+v", scopes)
	}
	result, err := provider.Discover(context.Background(), DiscoverRequest{Namespace: "tenant", Module: "fusion"})
	if err != nil || len(result.Targets) != 1 || result.Targets[0].Module != "fusion" {
		t.Fatalf("kingeye discovery: %+v err=%v", result, err)
	}
	if strings.Contains(client.listOptions.LabelSelector, "app.kubernetes.io/component") {
		t.Fatal("forced an undeclared component label")
	}
	_, err = provider.Discover(context.Background(), DiscoverRequest{Namespace: "tenant"})
	requireCode(t, err, CodeInvalidRequest)
}

func TestRemoteDeadlineSurvivesOutputBackpressure(t *testing.T) {
	provider, _, _ := runtimeProvider(t)
	python := provider.options.Scopes[0].Timeout.PythonPath
	pidPath := filepath.Join(t.TempDir(), "child-pid")
	argv, _ := json.Marshal([]string{"/bin/sh", "-c", "echo $$ > \"$1\"; while true; do printf 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'; done", "cmd", pidPath})
	command := exec.Command(python, "-u", "-c", remoteSupervisor, "nonce", "2.0", "524288", string(argv))
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { stdout.Close(); command.Process.Kill(); command.Wait() }()
	reader := bufio.NewReader(stdout)
	first, err := reader.ReadString('\n')
	if err != nil || !strings.Contains(first, `"event":"started"`) {
		t.Fatalf("supervisor did not start its command: first=%q err=%v", first, err)
	}
	output, err := reader.ReadString('\n')
	if err != nil || !strings.Contains(output, `"event":"output"`) {
		t.Fatalf("command produced no output before its deadline: frame=%q err=%v", output, err)
	}
	end := time.Now().Add(4 * time.Second)
	pid := 0
	for time.Now().Before(end) {
		data, err := os.ReadFile(pidPath)
		if err == nil {
			pid, _ = strconv.Atoi(strings.TrimSpace(string(data)))
			if pid > 0 {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if pid == 0 {
		diagnostics := []string{output}
		scanner := bufio.NewScanner(reader)
		for scanner.Scan() {
			var frame wireFrame
			if json.Unmarshal(scanner.Bytes(), &frame) == nil && (frame.Stream == "stderr" || frame.Event == "complete") {
				diagnostics = append(diagnostics, string(scanner.Bytes()))
			}
		}
		t.Fatalf("supervised command did not record its pid: %v", diagnostics)
	}
	for time.Now().Before(end) {
		if errors.Is(syscall.Kill(pid, 0), syscall.ESRCH) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	syscall.Kill(pid, syscall.SIGKILL)
	t.Fatal("output backpressure disabled the remote deadline")
}

func TestInvalidProtocolCannotClaimNotStarted(t *testing.T) {
	provider, _, target := runtimeProvider(t)
	provider.options.Executor = executorFunc(func(ctx context.Context, request StreamRequest) (StreamResult, error) {
		io.WriteString(request.Stdout, `{"nonce":"wrong","event":"complete","exit_code":0}`+"\n")
		zero := 0
		return StreamResult{ExitCode: &zero}, nil
	})
	result, err := provider.Exec(context.Background(), ExecRequest{Target: target, Argv: []string{"true"}})
	requireCode(t, err, RemoteUnknown)
	if result.RemoteState != RemoteUnknown {
		t.Fatalf("invalid receipt claimed no command ran: %+v", result)
	}
}
