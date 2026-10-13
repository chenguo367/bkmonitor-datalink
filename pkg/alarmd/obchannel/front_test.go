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

// The charged front reads' minute budget is a share of the CPU limit:
// double the cores, double the budget.
func TestTheFrontReadBudgetFollowsTheCPULimit(t *testing.T) {
	two, four := frontReadAllowance(2), frontReadAllowance(4)
	if two != 3*time.Second || four != 2*two {
		t.Fatalf("budget at 2 cores %v, at 4 %v; want 3s and double", two, four)
	}
	var budget frontBudget
	start := time.Unix(600, 0)
	if open, _ := budget.open(start, two); !open {
		t.Fatal("a fresh minute is closed")
	}
	budget.charge(start, two)
	if open, refill := budget.open(start.Add(10*time.Second), two); open || refill != 50*time.Second {
		t.Fatalf("after spending the minute: open %v, refills in %v; want closed for 50s", open, refill)
	}
	if open, _ := budget.open(start.Add(time.Minute), two); !open {
		t.Fatal("the next minute did not refill")
	}
	// A read that started in a minute already over is not charged to the next.
	budget.charge(start, time.Hour)
	if open, _ := budget.open(start.Add(time.Minute), two); !open {
		t.Fatal("a read from the previous minute was charged to this one")
	}
}

// With the budget spent the charged reads are refused with when it refills,
// and overview.get (its own cache) and runtime.get (process memory) still
// answer.
func TestOverviewAnswersWhenTheFrontBudgetIsSpent(t *testing.T) {
	read := func(id string) Operation {
		return Operation{ID: id, Summary: id, Fields: map[string]Field{}, Run: func(context.Context, Params) Outcome {
			return Outcome{Complete: true, Value: map[string]string{"read": id}}
		}}
	}
	c, err := New(Options{Auth: postObservationAuth{}, EnvironmentID: "test", Replica: "replica-1",
		Operations: []Operation{read("fleet.get"), read("overview.get"), read("runtime.get")}, FrontCores: func() int { return 0 }})
	if err != nil {
		t.Fatal(err)
	}
	if status, out := frontRead(t, c, "fleet.get"); status != 429 || out.Error == nil || out.Error.Code != "front_rate_limited" {
		t.Fatalf("charged read with no budget = %d %+v, want 429 front_rate_limited", status, out.Error)
	}
	for _, uncharged := range []string{"overview.get", "runtime.get"} {
		if status, out := frontRead(t, c, uncharged); status != 200 || out.Status != "ok" {
			t.Fatalf("%s with the budget spent = %d %+v, want answered", uncharged, status, out)
		}
	}
	c.options.FrontCores = func() int { return 2 }
	if status, out := frontRead(t, c, "fleet.get"); status != 200 || out.Status != "ok" {
		t.Fatalf("charged read within the budget = %d %+v", status, out)
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
