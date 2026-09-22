package obchannel

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/cliauth"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/obevidence"
)

type testAuth struct {
	calls    atomic.Int32
	err      error
	admitErr error
}

func (a *testAuth) Authenticate(context.Context, string) (cliauth.Session, error) {
	return cliauth.Session{ID: "test-session", EnvironmentID: "test", Scope: cliauth.ScopeReadonly, ExpiresAt: time.Now().Add(time.Hour)}, a.err
}
func (a *testAuth) Admit(_ context.Context, s cliauth.Session, renew bool) (cliauth.Session, error) {
	a.calls.Add(1)
	s.Renewed = renew
	return s, a.admitErr
}
func testChannel(t *testing.T, a *testAuth, ops ...Operation) *Channel {
	t.Helper()
	c, err := New(Options{Auth: a, EnvironmentID: "test", Replica: "replica-1", Build: "test", Operations: ops})
	if err != nil {
		t.Fatal(err)
	}
	return c
}
func call(t *testing.T, c *Channel, body any) (int, Response) {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest("POST", "/api/cli/channel", bytes.NewReader(raw))
	r.Header.Set("Authorization", "Bearer test-only")
	w := httptest.NewRecorder()
	c.ServeHTTP(w, r)
	if w.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("response may be cached")
	}
	var out Response
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatalf("invalid response: %v", err)
	}
	return w.Code, out
}
func envelope(c *Channel, mode, op string, params Params) map[string]any {
	return map[string]any{"channel_version": Version, "mode": mode, "operation": op, "params": params, "expected_catalog_revision": c.revision, "renew_if_due": true}
}

func TestAdmissionOnlyRenewsValidInvocation(t *testing.T) {
	a := &testAuth{}
	var runs atomic.Int32
	op := Operation{ID: "read", Summary: "Read", Fields: map[string]Field{"id": {Type: "string", MinLength: 1}}, Required: []string{"id"}, Run: func(context.Context, Params) Outcome {
		runs.Add(1)
		return Outcome{Complete: true, Value: map[string]string{"value": "observed"}}
	}}
	c := testChannel(t, a, op)
	for _, tc := range []struct {
		mode     string
		params   Params
		revision string
		status   int
	}{{"discover", nil, "", 200}, {"describe", nil, "", 200}, {"invoke", Params{"unknown": "x"}, c.revision, 400}, {"invoke", Params{"id": ""}, c.revision, 400}, {"invoke", Params{"id": "x"}, "old", 409}} {
		input := envelope(c, tc.mode, "read", tc.params)
		input["expected_catalog_revision"] = tc.revision
		status, out := call(t, c, input)
		if status != tc.status {
			t.Fatalf("%s: %d %+v", tc.mode, status, out)
		}
	}
	if a.calls.Load() != 0 || runs.Load() != 0 {
		t.Fatal("non-invocations counted as activity")
	}
	status, out := call(t, c, envelope(c, "invoke", "read", Params{"id": "x"}))
	if status != 200 || out.Status != "ok" || !out.Meta.Session.Renewed || a.calls.Load() != 1 || runs.Load() != 1 {
		t.Fatalf("invoke: %+v", out)
	}
	a.admitErr = &cliauth.Error{Code: "auth_expired_or_revoked", HTTPStatus: 401}
	status, _ = call(t, c, envelope(c, "invoke", "read", Params{"id": "x"}))
	if status != 401 || runs.Load() != 1 {
		t.Fatal("revoked session executed")
	}
}

func TestBudgetsAndStoreOutage(t *testing.T) {
	a := &testAuth{}
	entered := make(chan struct{})
	release := make(chan struct{})
	op := Operation{ID: "wait", Summary: "Bounded read", Run: func(context.Context, Params) Outcome { close(entered); <-release; return Outcome{Complete: true} }}
	c := testChannel(t, a, op)
	done := make(chan struct{})
	go func() { defer close(done); call(t, c, envelope(c, "invoke", "wait", nil)) }()
	<-entered
	status, _ := call(t, c, envelope(c, "invoke", "wait", nil))
	if status != 429 || a.calls.Load() != 1 {
		t.Fatal("busy invocation admitted")
	}
	close(release)
	<-done
	a.err = &cliauth.Error{Code: "auth_store_unavailable", HTTPStatus: 503}
	status, out := call(t, c, envelope(c, "discover", "", nil))
	if status != 503 || out.Error.Code != "auth_store_unavailable" {
		t.Fatalf("outage became login failure: %+v", out)
	}
}

func TestMalformedAndOversizedEnvelopeNeverAdmitted(t *testing.T) {
	a := &testAuth{}
	c := testChannel(t, a)
	for _, raw := range []string{`{"channel_version":"alarmd-ob/v1","mode":"discover","extra":true}`, `{"channel_version":"alarmd-ob/v1","mode":"discover"} {}`, strings.Repeat(" ", MaxRequestBytes) + `{}`} {
		r := httptest.NewRequest("POST", "/api/cli/channel", strings.NewReader(raw))
		r.Header.Set("Authorization", "Bearer test-only")
		w := httptest.NewRecorder()
		c.ServeHTTP(w, r)
		if w.Code != 400 {
			t.Fatalf("got %d", w.Code)
		}
	}
	if a.calls.Load() != 0 {
		t.Fatal("invalid envelope counted as activity")
	}
}

func TestStableCatalogAndConditionalSchema(t *testing.T) {
	ops := StoreOperations(obevidence.New(obevidence.Options{}))
	c := testChannel(t, &testAuth{}, ops...)
	for i := 0; i < 20; i++ {
		other := testChannel(t, &testAuth{}, StoreOperations(nil)...)
		if c.revision != other.revision {
			t.Fatal("catalog depends on map iteration")
		}
	}
	for _, tc := range []struct {
		operation string
		params    Params
		valid     bool
	}{
		{"strategy.config", Params{"view": "source", "strategy_id": "1"}, true},
		{"strategy.config", Params{"view": "source", "strategy_id": "1", "object_digest": strings.Repeat("a", 64)}, false},
		{"strategy.config", Params{"view": "published", "strategy_id": "1", "query_group": "q", "object_digest": strings.Repeat("a", 64)}, true},
		{"strategy.config", Params{"view": "published", "strategy_id": "1"}, false},
		{"store.inspect", Params{"family": "dynamic_config", "fields": []any{"is_access_bk_data"}}, true},
		{"store.inspect", Params{"family": "dynamic_config", "fields": []any{"is_access_bk_data", "is_access_bk_data"}}, false},
		{"store.inspect", Params{"family": "source_strategy", "strategy_id": "18446744073709551616"}, false},
		{"store.inspect", Params{"family": "query_progress", "query_group": "q"}, true},
		{"store.inspect", Params{"family": "target_group", "group_id": "a b"}, false},
		{"store.inspect", Params{"family": "target_group", "group_id": "a", "strategy_id": "1"}, false},
	} {
		if err := validate(c.ops[tc.operation], tc.params); (err == nil) != tc.valid {
			t.Fatalf("%s %+v: %v", tc.operation, tc.params, err)
		}
	}
	_, desc := call(t, c, envelope(c, "describe", "store.inspect", nil))
	encoded, _ := json.Marshal(desc.Result)
	for _, word := range []string{"allOf", "additionalProperties", "query_progress", "uniqueItems", "max_commands"} {
		if !bytes.Contains(encoded, []byte(word)) {
			t.Fatalf("schema missing %s", word)
		}
	}
}

func TestNativeKeepsDomainFactsAndNoCredentialForwarding(t *testing.T) {
	var gotPath, gotQuery string
	native := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotQuery = r.URL.Path, r.URL.RawQuery
		if len(r.Header) != 0 {
			t.Error("channel credentials forwarded to native reader")
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"query_group": "q", "complete": false, "gaps": []string{"worker_missing"}, "records_status": "unavailable", "future_field": 17, "state": "blocked"})
	})
	c := testChannel(t, &testAuth{}, NativeOperations(native)...)
	status, out := call(t, c, envelope(c, "invoke", "object.get", Params{"query_group": "q", "check": "target_ready", "group": "g"}))
	// Use the server's actual check vocabulary rather than inventing an enum.
	if status == 400 {
		check := c.ops["object.get"].Fields["check"].Enum[0]
		status, out = call(t, c, envelope(c, "invoke", "object.get", Params{"query_group": "q", "check": check, "group": "g"}))
	}
	if status != 200 || out.Status != "partial" || out.Evidence.Complete || gotPath != "/api/objects/q" || !strings.Contains(gotQuery, "group=g") {
		t.Fatalf("lost native context: %+v %s %s", out, gotPath, gotQuery)
	}
	result := out.Result.(map[string]any)
	if result["future_field"] != float64(17) || result["state"] != "blocked" {
		t.Fatal("native facts changed")
	}
}

func TestResponseCapAndMissingStoreAreNotSuccess(t *testing.T) {
	c := testChannel(t, &testAuth{}, Operation{ID: "large", Summary: "Large", Run: func(context.Context, Params) Outcome {
		return Outcome{Complete: true, Value: strings.Repeat("a", MaxResponseBytes)}
	}})
	status, out := call(t, c, envelope(c, "invoke", "large", nil))
	if status != 502 || out.Evidence.Complete || out.Result != nil {
		t.Fatalf("oversized evidence: %+v", out)
	}
	c = testChannel(t, &testAuth{}, StoreOperations(nil)...)
	_, out = call(t, c, envelope(c, "invoke", "store.inspect", Params{"family": "source_strategy", "strategy_id": "1"}))
	if out.Status != "partial" {
		t.Fatal("unconfigured source became a healthy result")
	}
	if status := authStatus(errors.New("private error")); status != 503 {
		t.Fatal(status)
	}
}

func TestNativeExplicitCompletenessAndFactTruncation(t *testing.T) {
	for _, body := range []string{
		`{"query_group":"q","view_complete":false,"health":"UNKNOWN","facts_total":0,"facts":[]}`,
		`{"query_group":"q","view_complete":true,"facts_total":2,"facts":[{}]}`,
	} {
		native := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(body)) })
		out := invokeNative(context.Background(), native, "/api/objects/q", nil)
		if out.Complete || len(out.Limitations) == 0 {
			t.Fatalf("native incomplete evidence was promoted: %+v", out)
		}
	}
}

type closingBody struct {
	entered chan<- struct{}
	release <-chan struct{}
}

func (b closingBody) Read([]byte) (int, error) { return 0, io.EOF }
func (b closingBody) Close() error             { b.entered <- struct{}{}; <-b.release; return nil }

func TestRouteSlotsIncludeBodyCleanupAndMethodRejection(t *testing.T) {
	c := testChannel(t, &testAuth{})
	entered := make(chan struct{}, 4)
	release := make(chan struct{})
	done := make(chan struct{}, 4)
	for i := 0; i < 4; i++ {
		go func() {
			r := httptest.NewRequest("GET", "/api/cli/channel", nil)
			r.Body = closingBody{entered: entered, release: release}
			w := httptest.NewRecorder()
			c.ServeHTTP(w, r)
			if w.Code != 405 {
				t.Errorf("method rejected with %d", w.Code)
			}
			done <- struct{}{}
		}()
	}
	for i := 0; i < 4; i++ {
		<-entered
	}
	status, _ := call(t, c, envelope(c, "discover", "", nil))
	if status != 429 {
		t.Errorf("cleanup released a slot prematurely: %d", status)
	}
	close(release)
	for i := 0; i < 4; i++ {
		<-done
	}
	status, _ = call(t, c, envelope(c, "discover", "", nil))
	if status != 200 {
		t.Fatalf("cleanup leaked a slot: %d", status)
	}
}
