// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package obchannel

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

func diagnoseNative(t *testing.T, page map[string]any) http.Handler {
	t.Helper()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/diagnose":
			_ = json.NewEncoder(w).Encode(page)
		case "/api/health":
			_ = json.NewEncoder(w).Encode(map[string]any{"health": "DEGRADED", "expected": 83, "determined": 83,
				"dependencies": []any{}, "per_replica": []any{"not kept"}})
		default:
			w.WriteHeader(404)
		}
	})
}

func infoOp(servers []any) Operation {
	return Operation{ID: "store.info", Summary: "info", Run: func(context.Context, Params) Outcome {
		return Outcome{Value: map[string]any{"servers": servers, "complete": true}, Complete: true}
	}}
}

func findingsOf(t *testing.T, out Response) []DeploymentFinding {
	t.Helper()
	result, _ := out.Result.(map[string]any)
	deployment, _ := result["deployment"].(map[string]any)
	encoded, _ := json.Marshal(deployment["findings"])
	var findings []DeploymentFinding
	_ = json.Unmarshal(encoded, &findings)
	return findings
}

func hasFinding(findings []DeploymentFinding, code, detail string) bool {
	for _, f := range findings {
		if f.Code == code && strings.Contains(f.Detail, detail) {
			return true
		}
	}
	return false
}

// The first page carries the deployment as the existing reads see it: the
// kept health facts, alarmd's Pods, and the Redis servers with the two named
// findings; a part that could not be read is there with its reason. A page
// that proves its coverage is complete and points at the next page.
func TestDiagnoseEnvironmentAddsTheDeploymentToTheFirstPage(t *testing.T) {
	page := map[string]any{"universe": map[string]any{"status": "ok", "count": 83},
		"page": map[string]any{"holds": true, "ids_expected": 20, "rows": 20}, "strategies": []any{}, "next_cursor": "abc_-1"}
	pods := Operation{ID: "k8s.pods", Summary: "pods", Availability: func() Availability { return Availability{Reason: "service_account_not_mounted"} },
		Run: func(context.Context, Params) Outcome { return Outcome{} }}
	info := infoOp([]any{
		map[string]any{"roles": []any{"runtime"}, "address": "a:1", "status": "ok", "fields": map[string]any{"maxmemory_policy": "allkeys-lru", "evicted_keys": "3"}},
		map[string]any{"roles": []any{"strategy_cache"}, "address": "b:1", "status": "ok", "fields": map[string]any{"maxmemory_policy": "noeviction", "evicted_keys": "0"}},
		map[string]any{"roles": []any{"cmdb_cache"}, "address": "c:1", "status": "ok", "fields": map[string]any{}},
		map[string]any{"roles": []any{"target_group"}, "address": "d:1", "status": "dependency_unavailable"},
	})
	c := testChannel(t, &testAuth{}, DiagnoseOperation(diagnoseNative(t, page), []Operation{pods, info}))
	status, out := call(t, c, envelope(c, "invoke", "diagnose.environment", Params{"limit": 20}))
	if status != 200 || !out.Evidence.Complete || len(out.Next) != 1 || out.Next[0].Params["cursor"] != "abc_-1" {
		t.Fatalf("status %d complete %v next %+v", status, out.Evidence.Complete, out.Next)
	}
	result := out.Result.(map[string]any)
	deployment := result["deployment"].(map[string]any)
	fleetFacts := deployment["fleet"].(map[string]any)
	if fleetFacts["health"] != "DEGRADED" || fleetFacts["per_replica"] != nil {
		t.Errorf("fleet = %v, want the kept keys only", fleetFacts)
	}
	if replicas := deployment["replicas"].(map[string]any); replicas["reason"] != "service_account_not_mounted" {
		t.Errorf("replicas = %v", replicas)
	}
	findings := findingsOf(t, out)
	for _, want := range []struct{ code, detail string }{
		{FindingRedisEvictionPolicy, "allkeys-lru"}, {FindingRedisEvictedKeys, "evicted_keys=3"},
		{FindingRedisInfoUnknown, "maxmemory_policy not reported"}, {FindingRedisInfoUnknown, "evicted_keys not reported"},
		{FindingRedisInfoUnknown, "dependency_unavailable"}, {FindingPartUnreadable, "service_account_not_mounted"},
	} {
		if !hasFinding(findings, want.code, want.detail) {
			t.Errorf("findings %+v lack %s %s", findings, want.code, want.detail)
		}
	}
	if hasFinding(findings, FindingRedisEvictionPolicy, "noeviction") || hasFinding(findings, FindingRedisEvictedKeys, "evicted_keys=0") {
		t.Errorf("a noeviction server with nothing evicted was flagged: %+v", findings)
	}
}

// A later page has no deployment section; a page that does not hold, or a
// universe that could not be read, is not complete and says why.
func TestDiagnoseEnvironmentIsIncompleteWhenAPageDoesNotProveItself(t *testing.T) {
	later := map[string]any{"universe": map[string]any{"status": "ok", "count": 83},
		"page": map[string]any{"holds": false, "ids_expected": 20, "rows": 19}, "strategies": []any{}}
	c := testChannel(t, &testAuth{}, DiagnoseOperation(diagnoseNative(t, later), nil))
	_, out := call(t, c, envelope(c, "invoke", "diagnose.environment", Params{"cursor": "abc"}))
	result := out.Result.(map[string]any)
	if out.Evidence.Complete || result["deployment"] != nil || len(out.Next) != 0 {
		t.Fatalf("later page: complete %v deployment %v next %v", out.Evidence.Complete, result["deployment"], out.Next)
	}
	unreadable := map[string]any{"universe": map[string]any{"status": "unreadable", "reason": "SOURCE_INCOMPLETE"},
		"page": map[string]any{"holds": false}, "strategies": []any{}}
	c = testChannel(t, &testAuth{}, DiagnoseOperation(diagnoseNative(t, unreadable), nil))
	_, out = call(t, c, envelope(c, "invoke", "diagnose.environment", Params{}))
	if out.Evidence.Complete || !strings.Contains(out.Summary, "SOURCE_INCOMPLETE") || !strings.Contains(out.Summary, "无效") {
		t.Fatalf("unreadable universe: complete %v summary %q", out.Evidence.Complete, out.Summary)
	}
	findings := findingsOf(t, out)
	if !hasFinding(findings, FindingPartUnreadable, "operation not registered") {
		t.Errorf("unregistered parts not named: %+v", findings)
	}
}
