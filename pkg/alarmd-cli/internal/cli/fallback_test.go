// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package cli

import (
	"bytes"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// closedEntry is an entry URL nothing listens on.
func closedEntry(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	return "http://" + address
}

// alarmdEntry answers the channel as alarmd does and counts the requests
// that reach it, with the entry each named.
type alarmdEntry struct {
	*httptest.Server
	calls   atomic.Int32
	entries chan string
}

func newAlarmdEntry(t *testing.T) *alarmdEntry {
	entry := &alarmdEntry{entries: make(chan string, 8)}
	entry.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entry.calls.Add(1)
		entry.entries <- r.Header.Get(entryHeader)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(envelope(fixtureProfile("http://unused"), "ok", map[string]any{"answered": "alarmd"}))
	}))
	t.Cleanup(entry.Close)
	return entry
}

// answering is an entry that answers every request with status and body.
func answering(t *testing.T, status int, body string, calls *atomic.Int32) string {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(server.Close)
	return server.URL
}

func fallbackApp(t *testing.T) (*App, *bytes.Buffer) {
	var stdout, stderr bytes.Buffer
	a := New(strings.NewReader(""), &stdout, &stderr, "test")
	a.Store, a.HTTP = Store{t.TempDir()}, &http.Client{}
	return a, &stderr
}

// A session issued through the admin carries the deployment's own address,
// declared by the server at login. When the admin entry does not get the
// request to alarmd -- nothing listening, the admin saying it cannot reach
// alarmd, a proxy's bare 502/503 -- the request goes to that address, says
// so on the request (X-Alarmd-Entry) and in meta.client_entry, and warns.
// Every other answer is the answer: alarmd's own error, and the admin's 504,
// after which alarmd may already have run the call.
func TestARequestMovesToTheDeclaredEntryOnlyWhenTheSessionsEntryDidNotReachAlarmd(t *testing.T) {
	adminDown := `{"error":{"code":"alarmd_unreachable","message":"cannot connect","requestId":"r1"}}`
	adminTimeout := `{"error":{"code":"alarmd_timeout","message":"no answer","requestId":"r2"}}`
	alarmdRefusal := `{"status":"error","error":{"code":"business_waiting","message":"x"},"meta":{}}`
	for _, tc := range []struct {
		name       string
		status     int
		body       string // empty: nothing listens
		exec, move bool
		why        string
		hint       bool // an executing call not moved: told where it could go
	}{
		{name: "nothing listening", why: fallbackUnsent, move: true},
		{name: "nothing listening, exec", why: fallbackUnsent, exec: true, move: true},
		{name: "admin cannot reach alarmd", status: 502, body: adminDown, why: fallbackAdminUnreachable, move: true},
		{name: "proxy 503 without JSON", status: 503, body: "<html>no healthy upstream</html>", why: fallbackGateway, move: true},
		{name: "proxy 502 without JSON", status: 502, body: "bad gateway", why: fallbackGateway, move: true},
		{name: "admin cannot reach alarmd, exec", status: 502, body: adminDown, exec: true, hint: true},
		{name: "proxy 503, exec", status: 503, body: "upstream down", exec: true, hint: true},
		{name: "admin timed out", status: 504, body: adminTimeout},
		{name: "proxy 504 without JSON", status: 504, body: "gateway timeout"},
		{name: "alarmd's own error", status: 502, body: alarmdRefusal},
		{name: "another JSON 502", status: 502, body: `{"error":{"code":"not_forwarded","message":"x"}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			own := newAlarmdEntry(t)
			var adminCalls atomic.Int32
			entry := closedEntry(t)
			if tc.body != "" {
				entry = answering(t, tc.status, tc.body, &adminCalls)
			}
			a, stderr := fallbackApp(t)
			p := fixtureProfile(entry)
			p.FallbackBaseURLs = []string{own.URL + "/ops-alarmd/"}
			m, _, err := a.requestVia(p, http.MethodPost, "api/cli/channel", map[string]any{}, 5*time.Second, tc.exec)
			if moved := own.calls.Load() == 1; moved != tc.move {
				t.Fatalf("reached the declared entry %d times, want moved=%v", own.calls.Load(), tc.move)
			}
			if !tc.move {
				hinted := strings.Contains(stderr.String(), "re-run with --entry alarmd")
				// The hint is in meta when there is an answer, in the error
				// when there is none (a proxy's bare page).
				available := stringField(objectField(objectField(m, "meta"), "client_entry"), "fallback_available")
				if m == nil && err != nil && strings.Contains(err.Error(), "re-run with --entry alarmd") {
					available = "/ops-alarmd/"
				}
				if hinted != tc.hint || (tc.hint && !strings.Contains(available, "/ops-alarmd/")) || (!tc.hint && strings.Contains(stderr.String(), "Warning")) {
					t.Fatalf("hint %v (want %v), fallback_available %q, stderr %q", hinted, tc.hint, available, stderr)
				}
				return
			}
			if got := <-own.entries; got != entryAlarmd {
				t.Fatalf("the moved request named entry %q, want %q", got, entryAlarmd)
			}
			client := objectField(objectField(m, "meta"), "client_entry")
			if stringField(client, "entry") != entryAlarmd || stringField(client, "reason") != tc.why || !strings.Contains(stringField(client, "url"), "/ops-alarmd/") {
				t.Fatalf("meta.client_entry = %v, want the declared entry for %s", client, tc.why)
			}
			if !strings.Contains(stderr.String(), "Warning: the session's entry did not reach alarmd ("+tc.why+")") {
				t.Fatalf("no warning: %q", stderr)
			}
		})
	}
}

// Without a declared entry nothing moves: an unreachable entry is reported
// as it was, and the request never goes anywhere it was not sent.
func TestARequestWithoutADeclaredEntryStaysWhereItWasSent(t *testing.T) {
	a, stderr := fallbackApp(t)
	_, _, err := a.requestVia(fixtureProfile(closedEntry(t)), http.MethodPost, "api/cli/channel", map[string]any{}, 5*time.Second, false)
	if err == nil || !strings.Contains(err.Error(), "connection refused") || strings.Contains(stderr.String(), "Warning") {
		t.Fatalf("err %v, stderr %q: want the refusal, no fallback", err, stderr)
	}
	// The session's entry answering is meta.client_entry "session".
	own := newAlarmdEntry(t)
	m, _, err := a.requestVia(fixtureProfile(own.URL), http.MethodPost, "api/cli/channel", map[string]any{}, 5*time.Second, false)
	if err != nil || stringField(objectField(objectField(m, "meta"), "client_entry"), "entry") != "session" || <-own.entries != "" {
		t.Fatalf("a direct answer: %v %v", m, err)
	}
}

// The fallback list is the server's: what a login answer declares, each a
// valid entry other than the session's own, at most four. A malformed one
// refuses the answer; none declared is none.
func TestTheFallbackEntriesAreOnlyWhatTheServerDeclared(t *testing.T) {
	p := fixtureProfile("https://admin.example")
	answer := func(fallback any) map[string]any {
		m := sessionResponse(p)
		if fallback != nil {
			m["fallback_base_urls"] = fallback
		}
		return m
	}
	got, err := validateExchange(answer([]any{"https://Alarmd.example:443/ops-alarmd"}), p)
	if err != nil || len(got.FallbackBaseURLs) != 1 || got.FallbackBaseURLs[0] != "https://alarmd.example/ops-alarmd/" {
		t.Fatalf("declared: %+v %v", got.FallbackBaseURLs, err)
	}
	if got, err := validateExchange(answer(nil), p); err != nil || got.FallbackBaseURLs != nil {
		t.Fatalf("none declared: %+v %v", got.FallbackBaseURLs, err)
	}
	for name, fallback := range map[string]any{
		"not a list":        "https://alarmd.example/",
		"not a string":      []any{42},
		"userinfo":          []any{"https://user:pw@alarmd.example/"},
		"query":             []any{"https://alarmd.example/?x=1"},
		"the session's own": []any{p.PublicBaseURL},
		"twice":             []any{"https://alarmd.example/", "https://alarmd.example"},
		"too many":          []any{"https://a.example/", "https://b.example/", "https://c.example/", "https://d.example/", "https://e.example/"},
	} {
		if _, err := validateExchange(answer(fallback), p); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// --entry alarmd sends a call through the entry the server declared, marked
// as such, for an operator re-running an executing call deliberately. It
// needs a declared entry and names no other.
func TestAnOperatorCanSendACallThroughTheDeclaredEntryDeliberately(t *testing.T) {
	own := newAlarmdEntry(t)
	var adminCalls atomic.Int32
	p := fixtureProfile(answering(t, 502, `{"error":{"code":"alarmd_unreachable","message":"x"}}`, &adminCalls))
	p.FallbackBaseURLs = []string{own.URL + "/ops-alarmd/"}
	via, err := onEntry(p, entryAlarmd)
	if err != nil {
		t.Fatal(err)
	}
	a, _ := fallbackApp(t)
	m, _, err := a.requestVia(via, http.MethodPost, "api/cli/channel", map[string]any{}, 5*time.Second, true)
	client := objectField(objectField(m, "meta"), "client_entry")
	if err != nil || adminCalls.Load() != 0 || own.calls.Load() != 1 || <-own.entries != entryAlarmd ||
		stringField(client, "entry") != entryAlarmd || stringField(client, "reason") != "requested" {
		t.Fatalf("deliberate call: admin %d own %d meta %v err %v", adminCalls.Load(), own.calls.Load(), client, err)
	}
	if _, err := onEntry(fixtureProfile(own.URL), entryAlarmd); err == nil {
		t.Fatal("--entry alarmd accepted on a session that declares no entry")
	}
	if _, err := parse([]string{"invoke", "x", "--entry", "https://elsewhere.example/"}); err == nil {
		t.Fatal("--entry accepted a URL")
	}
}
