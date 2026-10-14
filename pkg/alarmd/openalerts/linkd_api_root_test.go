// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package openalerts

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// testLinkdAPIRoot is the API root the link's admin module serves its
// operations under; the fake servers here answer there, as the deployment does.
const testLinkdAPIRoot = "/api/modules/linkd"

// objectMissingAnswer is the link's answer for an object it does not have,
// as the admin module and a Console both write it.
func objectMissingAnswer(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusNotFound)
	_, _ = w.Write([]byte(`{"error":{"code":"not_found","message":"对象不存在","requestId":"req-1"}}`))
}

// routeMissingAnswer is the same server's answer for a path it has no route
// for: the same status and code, another message.
func routeMissingAnswer(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusNotFound)
	_, _ = w.Write([]byte(`{"error":{"code":"not_found","message":"路径不存在","requestId":"req-1"}}`))
}

// A 404 is "no such alert" only when the link says so of the alert. The same
// status for a path the server has no route for - an API root that is not
// the link's - is a failure, or a wrong URL would read as every alert gone
// and let closes through.
func TestOnlyTheLinksOwnObjectMissingAnswerIsNoSuchAlert(t *testing.T) {
	for name, answer := range map[string]http.HandlerFunc{
		"route missing": routeMissingAnswer,
		"bare 404":      func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNotFound) },
		"other code": func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":{"code":"operation_not_registered","message":"对象不存在"}}`))
		},
	} {
		t.Run(name, func(t *testing.T) {
			reader := consoleServer(t, http.NotFound, answer)
			_, err := reader.AlertRecord(context.Background(), "system", "a-1")
			if err == nil || errors.Is(err, ErrAlertNotFound) {
				t.Fatalf("a %s answer was read as no such alert: %v", name, err)
			}
			call := reader.Record().Calls[ConsoleOpAlertRecord]
			if !call.LatestFailed || call.LastFailureClass != ConsoleFailureStatus || !strings.Contains(call.LastFailure, "API root") {
				t.Fatalf("a %s answer was recorded as %+v", name, call)
			}
		})
	}
}

// A URL naming the server rather than its API root reaches the server's web
// page, which answers 200 at any path it has no route for. Discovery refuses
// it as not_api with a sentence naming the fix, instead of a garbled answer.
func TestAURLThatIsNotTheAPIRootIsRefusedAsNotAPI(t *testing.T) {
	for name, page := range map[string]http.HandlerFunc{
		"html content type": func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			_, _ = w.Write([]byte("<!doctype html><title>console</title>"))
		},
		"html without a type": func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/octet-stream")
			_, _ = w.Write([]byte("\n  <!doctype html><title>console</title>"))
		},
	} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(page)
			t.Cleanup(server.Close)
			_, err := DiscoverTarget(context.Background(), HTTPReconcilerOptions{BaseURL: server.URL, Client: server.Client(),
				Username: "user", Password: "secret", MaxResponseBytes: 1 << 20})
			if !errors.Is(err, ErrNotAPI) || ConsoleFailureClass(err) != ConsoleFailureNotAPI || !strings.Contains(err.Error(), "API root") {
				t.Fatalf("a web page at the root was read as %v (class %s)", err, ConsoleFailureClass(err))
			}
		})
	}
}

// The operations are asked for under the root the URL names, whole: nothing
// of the old Console layout is added in front of them.
func TestTheOperationsAreAskedForUnderTheRootTheURLNames(t *testing.T) {
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		routeMissingAnswer(w, r)
	}))
	t.Cleanup(server.Close)
	reader, err := NewHTTPReconciler(HTTPReconcilerOptions{BaseURL: server.URL + "/admin/api/modules/linkd/", Client: server.Client(),
		Username: "user", Password: "secret", Index: testIndex(), MaxResponseBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = reader.Roster(context.Background(), "")
	_, _ = reader.AlertRecord(context.Background(), "system", "a-1")
	want := []string{"/admin/api/modules/linkd/strategy-index/targets", "/admin/api/modules/linkd/alerts/a-1"}
	if len(paths) < 2 || paths[0] != want[0] || paths[len(paths)-1] != want[1] {
		t.Fatalf("asked for %v, want first %s and last %s", paths, want[0], want[1])
	}
}
