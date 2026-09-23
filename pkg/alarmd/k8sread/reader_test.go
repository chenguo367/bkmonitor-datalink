// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package k8sread

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeAPI is an API server holding one alarmd Deployment in namespace "ns":
// two Pods of it, a Pod of another workload beside them, and events on each.
type fakeAPI struct {
	t        *testing.T
	mu       sync.Mutex
	requests []*http.Request
	// deny answers these paths with a status, to stand in for RBAC.
	deny    map[string]int
	logBody string
}

const (
	selfPod   = "alarmd-trigger-abc-1"
	otherPod  = "other-web-xyz-1"
	crashPod  = "alarmd-trigger-abc-2"
	// copycat carries alarmd's labels but belongs to another Deployment.
	copycat = "copycat-xyz-1"
	alarmdSel = "app.kubernetes.io/component=trigger,app.kubernetes.io/name=alarmd"
)

func owner(kind, name string) []map[string]any {
	return []map[string]any{{"kind": kind, "name": name, "controller": true}}
}

func podJSON(name string, labels map[string]string, restarts int, previous map[string]any) map[string]any {
	status := map[string]any{"name": "alarmd", "ready": restarts == 0, "restartCount": restarts,
		"state": map[string]any{"running": map[string]any{"startedAt": "2026-09-23T08:00:00Z"}}}
	if previous != nil {
		status["lastState"] = map[string]any{"terminated": previous}
		status["state"] = map[string]any{"waiting": map[string]any{"reason": "CrashLoopBackOff", "message": "back-off 5m0s restarting failed container"}}
	}
	return map[string]any{
		"metadata": map[string]any{"name": name, "labels": labels, "creationTimestamp": "2026-09-23T07:00:00Z", "ownerReferences": owner("ReplicaSet", "alarmd-trigger-abc")},
		"spec":     map[string]any{"nodeName": "node-1", "containers": []map[string]any{{"name": "alarmd", "image": "alarmd:1"}}},
		"status": map[string]any{"phase": "Running", "startTime": "2026-09-23T07:00:01Z",
			"conditions":        []map[string]any{{"type": "Ready", "status": map[bool]string{true: "True", false: "False"}[restarts == 0]}},
			"containerStatuses": []map[string]any{status}},
	}
}

var alarmdLabels = map[string]string{"app.kubernetes.io/name": "alarmd", "app.kubernetes.io/component": "trigger", "pod-template-hash": "abc"}

func (f *fakeAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.requests = append(f.requests, r)
	status, denied := f.deny[r.URL.Path]
	f.mu.Unlock()
	if r.Method != http.MethodGet {
		f.t.Errorf("a %s was sent to %s: only GET may be", r.Method, r.URL.Path)
	}
	if r.Header.Get("Authorization") != "Bearer sa-token" {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	if denied {
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(map[string]any{"kind": "Status", "message": `pods is forbidden: User "system:serviceaccount:ns:alarmd" cannot list resource "pods"`})
		return
	}
	write := func(v any) { _ = json.NewEncoder(w).Encode(v) }
	crashed := map[string]any{"reason": "OOMKilled", "exitCode": 137, "startedAt": "2026-09-23T07:50:00Z", "finishedAt": "2026-09-23T07:59:00Z"}
	switch path := r.URL.Path; {
	case path == "/api/v1/namespaces/ns/pods/"+selfPod:
		write(podJSON(selfPod, alarmdLabels, 0, nil))
	case path == "/api/v1/namespaces/ns/pods/"+crashPod:
		write(podJSON(crashPod, alarmdLabels, 3, crashed))
	case path == "/api/v1/namespaces/ns/pods/"+copycat:
		pod := podJSON(copycat, alarmdLabels, 0, nil)
		pod["metadata"].(map[string]any)["ownerReferences"] = owner("ReplicaSet", "copycat-xyz")
		write(pod)
	case path == "/apis/apps/v1/namespaces/ns/replicasets/copycat-xyz":
		write(map[string]any{"metadata": map[string]any{"name": "copycat-xyz", "ownerReferences": owner("Deployment", "copycat")}})
	case path == "/api/v1/namespaces/ns/pods/"+otherPod:
		write(podJSON(otherPod, map[string]string{"app.kubernetes.io/name": "web"}, 0, nil))
	case path == "/apis/apps/v1/namespaces/ns/replicasets/alarmd-trigger-abc":
		write(map[string]any{"metadata": map[string]any{"name": "alarmd-trigger-abc", "ownerReferences": owner("Deployment", "alarmd-trigger")}})
	case path == "/apis/apps/v1/namespaces/ns/deployments/alarmd-trigger":
		write(map[string]any{"metadata": map[string]any{"name": "alarmd-trigger"},
			"spec": map[string]any{"replicas": 2, "selector": map[string]any{"matchLabels": map[string]string{"app.kubernetes.io/name": "alarmd", "app.kubernetes.io/component": "trigger"}}},
			"status": map[string]any{"replicas": 2, "readyReplicas": 1, "updatedReplicas": 2, "availableReplicas": 1, "unavailableReplicas": 1,
				"conditions": []map[string]any{{"type": "Available", "status": "False", "reason": "MinimumReplicasUnavailable", "lastTransitionTime": "2026-09-23T07:59:00Z"}}}})
	case path == "/api/v1/namespaces/ns/pods":
		if r.URL.Query().Get("labelSelector") != alarmdSel {
			f.t.Errorf("pods listed with selector %q, want the Deployment's %q", r.URL.Query().Get("labelSelector"), alarmdSel)
		}
		write(map[string]any{"items": []any{podJSON(crashPod, alarmdLabels, 3, crashed), podJSON(selfPod, alarmdLabels, 0, nil)}})
	case path == "/apis/apps/v1/namespaces/ns/replicasets":
		write(map[string]any{"items": []any{
			map[string]any{"metadata": map[string]any{"name": "alarmd-trigger-abc", "creationTimestamp": "2026-09-23T07:00:00Z", "ownerReferences": owner("Deployment", "alarmd-trigger")}},
			map[string]any{"metadata": map[string]any{"name": "alarmd-trigger-old", "creationTimestamp": "2026-09-22T07:00:00Z", "ownerReferences": owner("Deployment", "alarmd-trigger")}},
		}})
	case path == "/api/v1/namespaces/ns/events":
		selector := r.URL.Query().Get("fieldSelector")
		name := selector[strings.LastIndex(selector, "=")+1:]
		write(map[string]any{"items": []any{map[string]any{"type": "Warning", "reason": "BackOff", "message": "event on " + name, "count": 4,
			"involvedObject": map[string]any{"kind": strings.TrimPrefix(strings.Split(selector, ",")[0], "involvedObject.kind="), "name": name},
			"firstTimestamp": "2026-09-23T07:10:00Z", "lastTimestamp": "2026-09-23T07:5" + string(rune('0'+len(name)%10)) + ":00Z",
			"source": map[string]any{"component": "kubelet"}}}})
	case strings.HasSuffix(path, "/log"):
		if r.URL.Query().Get("previous") == "true" && path == "/api/v1/namespaces/ns/pods/"+selfPod+"/log" {
			w.WriteHeader(http.StatusBadRequest)
			write(map[string]any{"kind": "Status", "message": `previous terminated container "alarmd" in pod "` + selfPod + `" not found`})
			return
		}
		_, _ = w.Write([]byte(f.logBody))
	default:
		w.WriteHeader(http.StatusNotFound)
		write(map[string]any{"kind": "Status", "message": "not found"})
	}
}

func (f *fakeAPI) paths() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, r := range f.requests {
		out = append(out, r.URL.Path+"?"+r.URL.RawQuery)
	}
	return out
}

// newReader serves fakeAPI over TLS and returns a Reader wired to it with
// ServiceAccount files in a temporary directory.
func newReader(t *testing.T, api *fakeAPI) *Reader {
	t.Helper()
	server := httptest.NewTLSServer(api)
	t.Cleanup(server.Close)
	host, port, _ := net.SplitHostPort(strings.TrimPrefix(server.URL, "https://"))
	dir := t.TempDir()
	must := func(name, content string) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	return New(Options{PodName: selfPod, Host: host, Port: port, Client: server.Client(),
		TokenPath: must("token", "sa-token\n"), CAPath: must("ca.crt", ""), NamespacePath: must("namespace", "ns")})
}

func codeOfErr(t *testing.T, err error) string {
	t.Helper()
	var named *Error
	if !errors.As(err, &named) {
		t.Fatalf("not a named failure: %v", err)
	}
	return named.Code
}

func TestPodsReadsTheDeploymentFoundFromThisPodsOwners(t *testing.T) {
	api := &fakeAPI{t: t}
	got, err := newReader(t, api).Pods(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got.Scope.Namespace != "ns" || got.Scope.Deployment != "alarmd-trigger" || got.Scope.Selector != alarmdSel {
		t.Fatalf("scope %+v", got.Scope)
	}
	if got.Deployment.Desired != 2 || got.Deployment.Ready != 1 || got.Deployment.Unavailable != 1 ||
		len(got.Deployment.Conditions) != 1 || got.Deployment.Conditions[0].Reason != "MinimumReplicasUnavailable" {
		t.Errorf("deployment %+v", got.Deployment)
	}
	if len(got.Pods) != 2 || got.Pods[1].Name != crashPod {
		t.Fatalf("pods %+v", got.Pods)
	}
	crash := got.Pods[1].Containers[0]
	if crash.Restarts != 3 || crash.State.State != "waiting" || crash.State.Reason != "CrashLoopBackOff" ||
		crash.LastTermination == nil || crash.LastTermination.Reason != "OOMKilled" || *crash.LastTermination.ExitCode != 137 {
		t.Errorf("the crashing container's restarts and why: %+v / %+v", crash, crash.LastTermination)
	}
	if got.Pods[1].Ready || !got.Pods[0].Ready || got.Pods[0].Containers[0].State.State != "running" || got.Pods[0].Containers[0].LastTermination != nil {
		t.Errorf("readiness and states: %+v", got.Pods)
	}
}

func TestEventsCoverTheDeploymentItsReplicaSetsAndPods(t *testing.T) {
	api := &fakeAPI{t: t}
	got, err := newReader(t, api).Events(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{"Deployment/alarmd-trigger": true, "ReplicaSet/alarmd-trigger-abc": true, "ReplicaSet/alarmd-trigger-old": true,
		"Pod/" + selfPod: true, "Pod/" + crashPod: true}
	if len(got.Objects) != len(want) || len(got.Events) != len(want) || len(got.Failed) != 0 {
		t.Fatalf("objects %v, %d events, failed %v", got.Objects, len(got.Events), got.Failed)
	}
	for _, e := range got.Events {
		if !want[e.Kind+"/"+e.Name] || e.Count != 4 || e.Component != "kubelet" {
			t.Errorf("event %+v", e)
		}
	}
	for i := 1; i < len(got.Events); i++ {
		if got.Events[i].LastAt.After(got.Events[i-1].LastAt) {
			t.Errorf("events are not newest first: %v", got.Events)
		}
	}
	for _, path := range api.paths() {
		if strings.Contains(path, otherPod) {
			t.Errorf("read outside alarmd: %s", path)
		}
	}
}

// One object's events refused is a partial answer that says which; every
// object refused is the refusal, by its name, never an empty list.
func TestEventsFailuresAreNamedNotEmpty(t *testing.T) {
	api := &fakeAPI{t: t, deny: map[string]int{}}
	reader := newReader(t, api)
	if _, err := reader.Resolve(context.Background()); err != nil {
		t.Fatal(err)
	}
	api.deny["/api/v1/namespaces/ns/events"] = http.StatusForbidden
	_, err := reader.Events(context.Background(), "")
	if code := codeOfErr(t, err); code != CodeForbidden || !strings.Contains(err.Error(), "cannot list resource") {
		t.Errorf("every event read refused: %v", err)
	}
	if _, err := reader.Events(context.Background(), copycat); codeOfErr(t, err) != CodeOutOfScope {
		t.Errorf("a Pod with alarmd's labels under another Deployment: %v", err)
	}
	_, err = reader.Events(context.Background(), otherPod)
	delete(api.deny, "/api/v1/namespaces/ns/events")
	if code := codeOfErr(t, err); code != CodeOutOfScope {
		t.Errorf("another workload's Pod: %v", err)
	}
}

func TestLogsAreScopedBoundedAndSayPreviousIsMissing(t *testing.T) {
	api := &fakeAPI{t: t, logBody: "2026-09-23T07:59:00Z panic: out of memory\n"}
	reader := newReader(t, api)
	got, err := reader.Logs(context.Background(), LogRequest{Pod: crashPod, Previous: true, Lines: 5000})
	if err != nil {
		t.Fatal(err)
	}
	if got.Container != "alarmd" || !got.Previous || got.Lines != MaxLogLines || got.Text != api.logBody || got.Truncated {
		t.Errorf("log %+v", got)
	}
	var logQuery url.Values
	for _, path := range api.paths() {
		if strings.Contains(path, "/log?") {
			logQuery, _ = url.ParseQuery(path[strings.Index(path, "?")+1:])
		}
	}
	// No limitBytes: the server would count it forward from the start of the
	// tail and drop the newest lines.
	if logQuery.Get("previous") != "true" || logQuery.Get("tailLines") != "1000" || logQuery.Has("limitBytes") || logQuery.Get("container") != "alarmd" {
		t.Errorf("the log was asked for as %v", logQuery)
	}

	_, err = reader.Logs(context.Background(), LogRequest{Pod: selfPod, Previous: true})
	if code := codeOfErr(t, err); code != CodeNotFound || !strings.Contains(err.Error(), "previous terminated container") {
		t.Errorf("a previous log never written: %v", err)
	}
	for _, request := range []LogRequest{{Pod: otherPod}, {Pod: copycat}, {Pod: selfPod, Container: "sidecar"}} {
		if _, err := reader.Logs(context.Background(), request); codeOfErr(t, err) != CodeOutOfScope {
			t.Errorf("%+v: %v", request, err)
		}
	}
	for _, path := range api.paths() {
		if strings.Contains(path, otherPod+"/log") {
			t.Errorf("another workload's log was read: %s", path)
		}
	}

	// Past the byte bound the newest lines are kept - the last one is the
	// crash - and the cut lands on a line boundary.
	var long strings.Builder
	for i := 0; long.Len() <= MaxLogBytes+4096; i++ {
		long.WriteString("2026-09-23T07:58:00Z line " + strings.Repeat("x", 100) + "\n")
	}
	long.WriteString("2026-09-23T07:59:00Z panic: the last line\n")
	api.logBody = long.String()
	got, err = reader.Logs(context.Background(), LogRequest{Pod: selfPod})
	if err != nil || !got.Truncated || got.Bytes > MaxLogBytes || got.Lines != DefaultLogLines ||
		!strings.HasSuffix(got.Text, "panic: the last line\n") || !strings.HasPrefix(got.Text, "2026-09-23T07:58:00Z line ") {
		t.Errorf("a log past the byte bound: %d bytes, truncated %v, lines %d, ends %q, starts %q, %v", got.Bytes, got.Truncated, got.Lines,
			got.Text[max(0, len(got.Text)-40):], got.Text[:min(40, len(got.Text))], err)
	}
	api.logBody = strings.Repeat("y", maxLogReadBytes+1)
	if _, err := reader.Logs(context.Background(), LogRequest{Pod: selfPod}); codeOfErr(t, err) != CodeAPIError {
		t.Errorf("a tail past the read bound is refused, not cut: %v", err)
	}
}

// Each way the read cannot happen has its own name.
func TestEveryFailureHasItsName(t *testing.T) {
	api := &fakeAPI{t: t, deny: map[string]int{"/api/v1/namespaces/ns/pods": http.StatusForbidden}}
	reader := newReader(t, api)
	if _, err := reader.Pods(context.Background()); codeOfErr(t, err) != CodeForbidden || !strings.Contains(err.Error(), "pods") {
		t.Errorf("RBAC refusal: %v", err)
	}

	missing := newReader(t, &fakeAPI{t: t})
	missing.options.TokenPath = filepath.Join(t.TempDir(), "absent")
	if _, err := missing.Pods(context.Background()); codeOfErr(t, err) != CodeNoServiceAccount {
		t.Errorf("no token: %v", err)
	}
	noNamespace := newReader(t, &fakeAPI{t: t})
	noNamespace.options.NamespacePath = filepath.Join(t.TempDir(), "absent")
	if _, err := noNamespace.Pods(context.Background()); codeOfErr(t, err) != CodeNoServiceAccount {
		t.Errorf("no namespace: %v", err)
	}
	noServer := newReader(t, &fakeAPI{t: t})
	noServer.options.Host, noServer.options.Port = "", ""
	if _, err := noServer.Pods(context.Background()); codeOfErr(t, err) != CodeNoAPIServer {
		t.Errorf("no API server: %v", err)
	}
	wrongToken := newReader(t, &fakeAPI{t: t})
	_ = os.WriteFile(wrongToken.options.TokenPath, []byte("expired"), 0o600)
	if _, err := wrongToken.Pods(context.Background()); codeOfErr(t, err) != CodeUnauthorized {
		t.Errorf("rejected token: %v", err)
	}

	closed := httptest.NewTLSServer(http.NotFoundHandler())
	host, port, _ := net.SplitHostPort(strings.TrimPrefix(closed.URL, "https://"))
	client := closed.Client()
	closed.Close()
	gone := newReader(t, &fakeAPI{t: t})
	gone.options.Host, gone.options.Port, gone.client = host, port, client
	if _, err := gone.Pods(context.Background()); codeOfErr(t, err) != CodeUnreachable {
		t.Errorf("unreachable: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	time.Sleep(time.Millisecond)
	if _, err := newReader(t, &fakeAPI{t: t}).Pods(ctx); codeOfErr(t, err) != CodeUnreachable || !strings.Contains(err.Error(), "deadline") {
		t.Errorf("past the deadline: %v", err)
	}

	// A CA file that is not a certificate is the ServiceAccount's fault, not
	// the network's.
	badCA := New(Options{PodName: selfPod, Host: "127.0.0.1", Port: "1", TokenPath: missing.options.NamespacePath, NamespacePath: noServer.options.NamespacePath,
		CAPath: noServer.options.NamespacePath})
	_ = os.WriteFile(badCA.options.TokenPath, []byte("sa-token"), 0o600)
	if _, err := badCA.Pods(context.Background()); codeOfErr(t, err) != CodeNoServiceAccount {
		t.Errorf("a CA with no certificate: %v", err)
	}
}

func TestAPodNotOwnedByADeploymentHasNoScope(t *testing.T) {
	api := &fakeAPI{t: t}
	reader := newReader(t, api)
	reader.options.PodName = "orphan"
	if _, err := reader.Pods(context.Background()); codeOfErr(t, err) != CodeNotFound {
		t.Errorf("a Pod the server does not have: %v", err)
	}
	reader.options.PodName = ""
	if _, err := reader.Pods(context.Background()); codeOfErr(t, err) != CodeScopeUnresolved {
		t.Errorf("no Pod name: %v", err)
	}
}
