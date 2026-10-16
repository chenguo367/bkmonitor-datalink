// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License.

package obchannel

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/cliauth"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/sre/pod"
)

func frontRead(t *testing.T, c *Channel, op string) (int, Response) {
	t.Helper()
	raw, _ := json.Marshal(map[string]any{"channel_version": Version, "mode": "invoke", "operation": op, "params": map[string]any{}})
	r := httptest.NewRequest(http.MethodPost, cliauth.FrontReadPath, bytes.NewReader(raw))
	w := httptest.NewRecorder()
	c.FrontReadHandler(func(*http.Request) error { return nil }).ServeHTTP(w, r)
	var out Response
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatalf("front read of %s answered %d %s", op, w.Code, w.Body.String())
	}
	return w.Code, out
}

// Front reads are never refused for a budget: the page's reads, a burst of
// list reads included, all answer. Before, past 2.5% of the CPU limit a
// minute the list reads were refused as front_rate_limited.
func TestFrontReadsAreNeverRefusedForABudget(t *testing.T) {
	read := func(id string) Operation {
		return Operation{ID: id, Summary: id, Fields: map[string]Field{}, Run: func(context.Context, Params) Outcome {
			time.Sleep(5 * time.Millisecond)
			return Outcome{Complete: true, Value: map[string]string{"read": id}}
		}}
	}
	c, err := New(Options{Auth: postObservationAuth{}, EnvironmentID: "test", Replica: "replica-1",
		Operations: []Operation{read("fleet.get"), read("overview.get"), read("runtime.get"), read("object.list")}})
	if err != nil {
		t.Fatal(err)
	}
	const together = 32
	var done sync.WaitGroup
	codes := make([]int, together)
	for i := range codes {
		done.Add(1)
		go func() {
			defer done.Done()
			codes[i], _ = frontRead(t, c, []string{"fleet.get", "object.list", "overview.get", "runtime.get"}[i%4])
		}()
	}
	done.Wait()
	for i, code := range codes {
		if code != 200 {
			t.Errorf("front read %d answered %d, want every one answered", i, code)
		}
	}
}

// The front read takes the administrator key, nothing else.
func TestTheFrontReadTakesTheAdministratorKey(t *testing.T) {
	c, err := New(Options{Auth: postObservationAuth{}, EnvironmentID: "test", Replica: "replica-1",
		Operations: []Operation{{ID: "fleet.get", Summary: "fleet", Fields: map[string]Field{}, Run: func(context.Context, Params) Outcome { return Outcome{Complete: true} }}}})
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(map[string]any{"channel_version": Version, "mode": "invoke", "operation": "fleet.get", "params": map[string]any{}})
	w := httptest.NewRecorder()
	c.FrontReadHandler(func(*http.Request) error { return errors.New("not the key") }).ServeHTTP(w, httptest.NewRequest(http.MethodPost, cliauth.FrontReadPath, bytes.NewReader(raw)))
	if w.Code != http.StatusForbidden {
		t.Fatalf("front read without the key = %d %s", w.Code, w.Body.String())
	}
}

type namedSessionAuth struct{}

func (namedSessionAuth) Authenticate(context.Context, string) (cliauth.Session, error) {
	return cliauth.Session{ID: "named-session", EnvironmentID: "test", Scope: cliauth.ScopeReadonly, ExpiresAt: time.Now().Add(time.Hour),
		Operator: "ops-a", IssuedVia: cliauth.IssuedViaFront}, nil
}

func (namedSessionAuth) Admit(_ context.Context, session cliauth.Session, _ bool) (cliauth.Session, error) {
	return session, nil
}

// An SRE action names the operator the session was issued to and the route
// that issued it.
func TestAnSREReceiptNamesTheOperator(t *testing.T) {
	controller := true
	client := &postObservationClient{target: corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "fixture", Name: "web-0", UID: "pod-uid", OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "StatefulSet", Name: "web", UID: "workload-uid", Controller: &controller}}},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "web", Image: "web:v1"}}},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{{Name: "web", ContainerID: "containerd://fixture", ImageID: "sha256:fixture", Ready: true, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}},
	}}
	provider, err := pod.New(pod.Options{Client: client, Manager: &auditManager{}, Executor: &postObservationExecutor{}, DefaultNamespace: "fixture"})
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var actions []SREAction
	c, err := New(Options{Auth: namedSessionAuth{}, EnvironmentID: "test", Replica: "replica-1", Operations: SREOperations(SREOptions{Pods: provider, Audit: func(action SREAction) {
		mu.Lock()
		defer mu.Unlock()
		actions = append(actions, action)
	}})})
	if err != nil {
		t.Fatal(err)
	}
	if _, response := call(t, c, envelope(c, "invoke", pod.OpDeletePod, Params{"target": map[string]any{"namespace": "fixture", "pod": "web-0", "uid": "pod-uid"}})); response.Error != nil {
		t.Fatalf("delete: %+v", response)
	}
	if len(actions) != 1 || actions[0].Operator != "ops-a" || actions[0].IssuedVia != cliauth.IssuedViaFront || actions[0].SessionID != "named-session" {
		t.Fatalf("actions %+v, want one naming ops-a issued through the front surface", actions)
	}
}
