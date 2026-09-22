// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package openalerts

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

func testBinding() TargetBinding {
	return TargetBinding{EventSourceID: "source", HookName: "active", KeyPrefix: "test:active", Address: "redis:6379", Database: 3, Sources: []string{"source"}}
}

func reconciliationJSON() map[string]any {
	return map[string]any{
		"target": testBinding(), "tenantId": keyA.TenantID, "strategyId": keyA.StrategyID, "key": "test:active:" + keyA.TenantID + ":" + keyA.StrategyID,
		"complete": true, "redis": map[string]any{"complete": true}, "alerts": map[string]any{"complete": true},
		"rows": []any{
			map[string]any{"fingerprint": "matched", "status": "matched", "alerts": []Alert{{AlertID: "a", EventSourceID: "source", Fingerprint: "matched", Severity: "critical"}}},
			map[string]any{"fingerprint": "missing", "status": "missing_redis", "alerts": []Alert{{AlertID: "b", EventSourceID: "source", Fingerprint: "missing"}}},
			map[string]any{"fingerprint": "stale", "status": "redis_only", "alerts": []Alert{}},
		},
	}
}

func TestHTTPReconcilerChecksBindingQueryAuthAndRetainsMetadata(t *testing.T) {
	requests := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, password, ok := r.BasicAuth()
		if !ok || user != "user" || password != "secret" {
			t.Error("BasicAuth missing")
		}
		requests = append(requests, r.URL.Path)
		if r.URL.Path == "/local-api/strategy-index/targets" {
			_ = json.NewEncoder(w).Encode([]TargetBinding{testBinding()})
			return
		}
		q := r.URL.Query()
		if q.Get("event_source_id") != "source" || q.Get("hook_name") != "active" || q.Get("bk_tenant_id") != keyA.TenantID || q.Get("strategy_id") != keyA.StrategyID {
			t.Errorf("query %v", q)
		}
		_ = json.NewEncoder(w).Encode(reconciliationJSON())
	}))
	defer server.Close()
	reader, err := NewHTTPReconciler(HTTPReconcilerOptions{BaseURL: server.URL, Client: server.Client(), Username: "user", Password: "secret", Binding: testBinding(), MaxResponseBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	result, err := reader.Reconcile(context.Background(), keyA)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(result.Members, []string{"matched", "missing"}) || !reflect.DeepEqual(result.Missing, []string{"missing"}) || !reflect.DeepEqual(result.Suppressed, []string{"stale"}) {
		t.Fatalf("result %+v", result)
	}
	if len(result.Alerts) != 2 || result.Alerts[0].Severity != "critical" || result.Alerts[1].Severity != "" {
		t.Fatal("optional severity lost or fabricated")
	}
	if len(requests) != 2 {
		t.Fatalf("requests %v", requests)
	}
}

func TestHTTPReconcilerRejectsPartialAndInvalidScope(t *testing.T) {
	for name, mutate := range map[string]func(map[string]any){
		"missing rows":   func(m map[string]any) { delete(m, "rows") },
		"partial":        func(m map[string]any) { m["complete"] = false },
		"partial Redis":  func(m map[string]any) { m["redis"] = map[string]any{"complete": false} },
		"partial alerts": func(m map[string]any) { m["alerts"] = map[string]any{"complete": false} },
		"other tenant":   func(m map[string]any) { m["tenantId"] = "other" },
		"other strategy": func(m map[string]any) { m["strategyId"] = "other" },
		"wrong key":      func(m map[string]any) { m["key"] = "other" },
		"binding moved":  func(m map[string]any) { b := testBinding(); b.Database++; m["target"] = b },
		"unknown row":    func(m map[string]any) { m["rows"].([]any)[0].(map[string]any)["status"] = "unknown" },
		"mismatched fingerprint": func(m map[string]any) {
			m["rows"].([]any)[0].(map[string]any)["alerts"] = []Alert{{AlertID: "a", EventSourceID: "source", Fingerprint: "different"}}
		},
		"foreign source": func(m map[string]any) {
			m["rows"].([]any)[0].(map[string]any)["alerts"] = []Alert{{AlertID: "a", EventSourceID: "foreign", Fingerprint: "matched"}}
		},
		"empty matched": func(m map[string]any) { m["rows"].([]any)[0].(map[string]any)["alerts"] = []Alert{} },
	} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/targets") {
					_ = json.NewEncoder(w).Encode([]TargetBinding{testBinding()})
					return
				}
				m := reconciliationJSON()
				mutate(m)
				_ = json.NewEncoder(w).Encode(m)
			}))
			defer server.Close()
			reader, err := NewHTTPReconciler(HTTPReconcilerOptions{BaseURL: server.URL, Client: server.Client(), Username: "u", Password: "p", Binding: testBinding(), MaxResponseBytes: 1 << 20})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := reader.Reconcile(context.Background(), keyA); err == nil {
				t.Fatal("invalid response accepted as a baseline")
			}
		})
	}
}

func TestHTTPReconcilerDoesNotChooseFirstTargetOrLeakCredentials(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b := testBinding()
		b.EventSourceID = "other"
		_ = json.NewEncoder(w).Encode([]TargetBinding{b})
	}))
	defer server.Close()
	reader, _ := NewHTTPReconciler(HTTPReconcilerOptions{BaseURL: server.URL, Client: server.Client(), Username: "private-user", Password: "private-password", Binding: testBinding(), MaxResponseBytes: 1 << 20})
	_, err := reader.Reconcile(context.Background(), keyA)
	if err == nil || strings.Contains(err.Error(), "private") {
		t.Fatalf("binding error = %v", err)
	}
	reader.options.MaxResponseBytes = 1
	if _, err := reader.Reconcile(context.Background(), keyA); !errors.Is(err, ErrIncomplete) {
		t.Fatalf("body bound error = %v", err)
	}
}
