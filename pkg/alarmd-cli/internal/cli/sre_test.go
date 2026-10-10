// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License.

package cli

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func jsonHTTP(value any, status int) *http.Response {
	data, _ := json.Marshal(value)
	return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(string(data)))}
}

func TestScriptFileUsesRegisteredStdinAndDescribeDeadline(t *testing.T) {
	for _, source := range []string{"--stdin-file", "--script-file"} {
		t.Run(source, func(t *testing.T) {
			p := fixtureProfile("http://fixture.test")
			p.Scope = execScope
			store := Store{t.TempDir()}
			if err := store.save(p, false); err != nil {
				t.Fatal(err)
			}
			dir := t.TempDir()
			sentinel := filepath.Join(dir, "must-not-exist")
			script := "touch " + sentinel + "\nprintf '%s' 'literal $(false)'\n"
			scriptFile := filepath.Join(dir, "script.sh")
			_ = os.WriteFile(scriptFile, []byte(script), 0600)
			paramsFile := filepath.Join(dir, "params.json")
			_ = os.WriteFile(paramsFile, []byte(`{"target":{"pod":"fixture","containers":["main"]},"argv":["/bin/sh"]}`), 0600)
			var modes []string
			client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				var body map[string]any
				_ = json.NewDecoder(r.Body).Decode(&body)
				mode := stringField(body, "mode")
				modes = append(modes, mode)
				if mode == "describe" {
					return jsonHTTP(envelope(p, "ok", map[string]any{"effect": "exec", "required_scope": execScope, "execution_timeout_ms": 35000, "request_max_bytes": 65536, "input_schema": map[string]any{"properties": map[string]any{"stdin": map[string]any{"type": "string", "parameter_source": "stdin", "maxLength": 32768}}}}), 200), nil
				}
				deadline, ok := r.Context().Deadline()
				remaining := time.Until(deadline)
				if !ok || remaining < 39*time.Second || remaining > 40*time.Second {
					t.Errorf("wrong invoke deadline: %v %s", ok, remaining)
				}
				params := objectField(body, "params")
				if params["stdin"] != script || objectField(params, "target")["pod"] != "fixture" {
					t.Errorf("file input lost: %+v", params)
				}
				return jsonHTTP(envelope(p, "ok", map[string]any{"remote_state": "exited", "stdout": "literal output", "exit_code": 0, "target": map[string]any{"pod_uid": "uid"}, "password": "must redact"}), 200), nil
			})}
			code, out, _, _ := run(t, store, client, "", "invoke", "pod.exec", "--env", p.EnvironmentID, "--params-file", paramsFile, source, scriptFile)
			if code != 0 || strings.Join(modes, ",") != "describe,invoke" {
				t.Fatalf("invoke failed: %d %+v calls %v", code, out, modes)
			}
			if _, err := os.Stat(sentinel); !os.IsNotExist(err) {
				t.Fatal("script executed locally")
			}
			data, err := os.ReadFile(stringField(objectField(out, "meta"), "result_file"))
			if err != nil || !strings.Contains(string(data), "pod_uid") || strings.Contains(string(data), "must redact") {
				t.Fatalf("receipt lost or exposed sensitive field: %s %v", data, err)
			}
		})
	}
}

func TestParamsFromStdinLegacyDescribeAndNoExecRetry(t *testing.T) {
	for _, effect := range []string{"", "exec"} {
		t.Run("effect="+effect, func(t *testing.T) {
			p := fixtureProfile("http://fixture.test")
			if effect == "exec" {
				p.Scope = execScope
				p.RefreshToken = "refresh-credential"
			}
			store := Store{t.TempDir()}
			_ = store.save(p, false)
			invokes := 0
			client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				if !strings.HasSuffix(r.URL.Path, "/channel") {
					t.Error("exec performed automatic renewal/retry")
					return jsonHTTP(map[string]any{}, 500), nil
				}
				var body map[string]any
				_ = json.NewDecoder(r.Body).Decode(&body)
				if body["mode"] == "describe" {
					return jsonHTTP(envelope(p, "ok", map[string]any{"effect": effect}), 200), nil
				}
				invokes++
				if objectField(body, "params")["nested"] == nil {
					t.Error("params stdin lost")
				}
				deadline, ok := r.Context().Deadline()
				if !ok || time.Until(deadline) > 30*time.Second || time.Until(deadline) < 29*time.Second {
					t.Error("legacy deadline changed")
				}
				if effect == "exec" {
					return jsonHTTP(envelope(p, "error", nil), 401), nil
				}
				return jsonHTTP(envelope(p, "ok", map[string]any{}), 200), nil
			})}
			code, out, _, _ := run(t, store, client, `{"nested":{"values":[1,2]}}`, "invoke", "future.operation", "--env", p.EnvironmentID, "--params-file", "-")
			if invokes != 1 || (effect == "" && code != 0) || (effect == "exec" && (code != 1 || stringField(objectField(out, "error"), "code") != credentialsLapsedCode)) {
				t.Fatalf("legacy/new mismatch: %d %+v invokes%d", code, out, invokes)
			}
		})
	}
}

func TestLocalInputBoundariesRejectBeforeInvocation(t *testing.T) {
	p := fixtureProfile("http://fixture.test")
	store := Store{t.TempDir()}
	_ = store.save(p, false)
	scriptFile := filepath.Join(t.TempDir(), "script.txt")
	_ = os.WriteFile(scriptFile, []byte("12345"), 0600)
	for _, tc := range []struct {
		name     string
		contract map[string]any
		args     []string
	}{
		{"legacy lacks stdin", map[string]any{}, []string{"--stdin-file", scriptFile}},
		{"already supplied", map[string]any{"input_schema": map[string]any{"properties": map[string]any{"stdin": map[string]any{"type": "string", "parameter_source": "stdin"}}}}, []string{"--input", `{"stdin":"original"}`, "--stdin-file", scriptFile}},
		{"too long", map[string]any{"input_schema": map[string]any{"properties": map[string]any{"stdin": map[string]any{"type": "string", "parameter_source": "stdin", "maxLength": 4}}}}, []string{"--stdin-file", scriptFile}},
		{"request too large", map[string]any{"request_max_bytes": 2}, []string{"--input", `{"body":{"nested":true}}`}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				calls++
				return jsonHTTP(envelope(p, "ok", tc.contract), 200), nil
			})}
			args := append([]string{"invoke", "future.operation", "--env", p.EnvironmentID}, tc.args...)
			if code, _, _, _ := run(t, store, client, "", args...); code != 2 || calls != 1 {
				t.Fatalf("invalid input sent invoke: exit%d calls%d", code, calls)
			}
		})
	}
	for _, args := range [][]string{{"--params-file", "-", "--script-file", "-"}, {"--input", "{}", "--params-file", "x"}, {"--stdin-file", "x", "--script-file", "y"}} {
		if _, err := parse(args); err == nil {
			t.Fatal("conflicting file flags accepted")
		}
	}
	if _, err := New(strings.NewReader("{}{}"), io.Discard, io.Discard, "test").inputObject(options{paramsFile: "-"}); err == nil {
		t.Fatal("multiple JSON inputs accepted")
	}
}

func TestExplicitExecLoginAndRefreshScopeEquality(t *testing.T) {
	p := fixtureProfile("http://fixture.test")
	p.Scope = execScope
	b := loginBundle{Version: "alarmd-login/v1", EnvironmentID: p.EnvironmentID, EnvironmentName: p.EnvironmentName, PublicBaseURL: p.PublicBaseURL, GrantSecret: testGrant, GrantExpiresAt: time.Now().Add(time.Minute).Format(time.RFC3339), Scope: execScope}
	data, _ := json.Marshal(b)
	codeText := "alarmd-login-v1." + base64.RawURLEncoding.EncodeToString(data)
	calls := 0
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) { calls++; return jsonHTTP(sessionResponse(p), 200), nil })}
	store := Store{t.TempDir()}
	if code, out, _, _ := run(t, store, client, codeText, "auth", "login"); code != 1 || stringField(objectField(out, "error"), "code") != "scope_mismatch" || calls != 0 {
		t.Fatal("default login implicitly acquired exec")
	}
	if code, out, _, _ := run(t, store, client, codeText, "auth", "login", "--scope", execScope); code != 0 || calls != 1 || stringField(objectField(out, "session"), "scope") != execScope {
		t.Fatalf("explicit exec login failed: %+v", out)
	}
	for _, scope := range []string{sessionScope, execScope} {
		original := p
		original.Scope = scope
		response := sessionResponse(original)
		if _, err := validateExchange(response, original); err != nil {
			t.Fatal(err)
		}
		response["scope"] = execScope
		if scope == execScope {
			response["scope"] = sessionScope
		}
		if _, err := validateExchange(response, original); err == nil {
			t.Fatal("renewal changed initial scope")
		}
	}
}

func TestInvokeTimeoutDoesNotRetry(t *testing.T) {
	p := fixtureProfile("http://fixture.test")
	p.Scope = execScope
	store := Store{t.TempDir()}
	_ = store.save(p, false)
	calls := 0
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		calls++
		if calls == 1 {
			return jsonHTTP(envelope(p, "ok", map[string]any{"effect": "exec", "required_scope": execScope, "execution_timeout_ms": 1000}), 200), nil
		}
		return nil, context.DeadlineExceeded
	})}
	if code, _, _, _ := run(t, store, client, "", "invoke", "pod.exec", "--env", p.EnvironmentID); code != 1 || calls != 2 {
		t.Fatalf("timeout retried: exit%d calls%d", code, calls)
	}
}

func TestLegacyInstanceCannotClearExecPairing(t *testing.T) {
	p := fixtureProfile("http://fixture.test")
	p.Scope = execScope
	p.RefreshToken = "exec-v1.credential"
	p.ExpiresAt = time.Now().Add(-time.Hour).Format(time.RFC3339)
	store := Store{t.TempDir()}
	_ = store.save(p, false)
	calls := 0
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls++
		return jsonHTTP(map[string]any{"status": "error", "error": map[string]any{"code": "renewal_expired_or_revoked"}}, 401), nil
	})}
	code, out, _, _ := run(t, store, client, "", "discover", "--env", p.EnvironmentID)
	if code != 1 || calls != 1 || stringField(objectField(out, "error"), "code") != "capability_unavailable" {
		t.Fatalf("old instance rejection unrecognized: %+v calls%d", out, calls)
	}
	saved, err := store.get(p.EnvironmentID)
	if err != nil || saved.RefreshToken != p.RefreshToken || saved.Scope != execScope || saved.AccessToken != p.AccessToken {
		t.Fatalf("legacy response lost pairing: %+v %v", saved, err)
	}
}
