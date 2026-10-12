// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License.

package cliauth

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
)

// A grant is the one deployment operations session, whether the page or CLI
// asks for nothing or, as an earlier build did, for the execution session:
// the code names no scope, the renewal credential has no prefix, and the
// session renews as the one scope.
func TestEveryGrantIsTheOneOperationsSession(t *testing.T) {
	m := newTestManager(t, startRedis(t))
	for _, body := range []string{`{"confirm":true}`, `{"confirm":true,"scope":"deployment_ops_exec"}`} {
		t.Run(body, func(t *testing.T) {
			issued := authRequest(m, http.MethodPost, grantsPath, body, true)
			var grant struct {
				AuthorizationCode string   `json:"authorization_code"`
				Scope             string   `json:"scope"`
				SupportedScopes   []string `json:"supported_scopes"`
			}
			if issued.Code != 200 || json.Unmarshal(issued.Body.Bytes(), &grant) != nil || grant.Scope != ScopeReadonly ||
				len(grant.SupportedScopes) != 1 || grant.SupportedScopes[0] != ScopeReadonly {
				t.Fatalf("grant: %d %s", issued.Code, issued.Body)
			}
			raw, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(grant.AuthorizationCode, "alarmd-login-v1."))
			if err != nil {
				t.Fatal(err)
			}
			var code authorizationPackage
			if json.Unmarshal(raw, &code) != nil || strings.Contains(string(raw), `"scope"`) {
				t.Fatalf("the code names a scope: %s", raw)
			}
			login := decodeLogin(t, exchange(m, code))
			if !validSecret(login.RefreshToken) || strings.HasPrefix(login.RefreshToken, execRenewalPrefix) {
				t.Fatalf("renewal credential %q, want one without a prefix", login.RefreshToken)
			}
			next := decodeLogin(t, refresh(m, login.RefreshToken))
			renewed, err := m.Authenticate(context.Background(), next.AccessToken)
			if err != nil || renewed.Scope != ScopeReadonly || !AllowsScope(renewed.Scope, ScopeReadonly) {
				t.Fatalf("renewed: %+v %v", renewed, err)
			}
		})
	}
}

// An execution pairing an earlier build issued - its credential carrying the
// prefix, its record stored under the prefixed key - still renews here, and
// its successor is issued without the prefix and renews again; the session it
// buys is allowed every operation. No device has to pair again.
func TestAnEarlierExecutionCredentialStillRenews(t *testing.T) {
	m := newTestManager(t, startRedis(t))
	ctx := context.Background()
	refreshSecret, _ := randomSecret()
	old := execRenewalPrefix + refreshSecret
	now := time.Now().UnixMilli()
	pairing, _ := json.Marshal(map[string]any{"pairing_id": "earlier-device", "environment_id": m.environmentID, "scope": legacyScopeExec, "epoch": 0,
		"admin_binding": m.adminBinding(), "created_at_ms": now, "last_used_at_ms": now})
	if err := m.client.Set(ctx, pairingKey(m.prefix, old), pairing, time.Hour).Err(); err != nil {
		t.Fatal(err)
	}
	if err := m.client.ZAdd(ctx, m.pairingsKey(), &redis.Z{Score: float64(now), Member: "earlier-device"}).Err(); err != nil {
		t.Fatal(err)
	}
	renewal, err := m.Refresh(ctx, old)
	if err != nil {
		t.Fatalf("an earlier execution credential did not renew: %v", err)
	}
	if strings.HasPrefix(renewal.RefreshToken, execRenewalPrefix) || !validSecret(renewal.RefreshToken) {
		t.Fatalf("successor %q still carries the prefix", renewal.RefreshToken)
	}
	if !AllowsScope(renewal.Session.Scope, ScopeReadonly) {
		t.Fatalf("the renewed session %q is not allowed the operations", renewal.Session.Scope)
	}
	again, err := m.Refresh(ctx, renewal.RefreshToken)
	if err != nil || !AllowsScope(again.Session.Scope, ScopeReadonly) {
		t.Fatalf("the successor did not renew: %+v %v", again.Session, err)
	}
}

// A session an earlier build granted as an execution session authenticates
// and is allowed every operation, as the one scope is.
func TestAnEarlierExecutionSessionIsTheOneSession(t *testing.T) {
	m := newTestManager(t, startRedis(t))
	token, _ := randomSecret()
	record := storedRecord{SessionID: "earlier", EnvironmentID: m.environmentID, Scope: legacyScopeExec, ExpiresAtMS: time.Now().Add(time.Hour).UnixMilli()}
	raw, _ := json.Marshal(record)
	if err := m.client.Set(context.Background(), m.prefix+"session:"+digest(token), raw, time.Hour).Err(); err != nil {
		t.Fatal(err)
	}
	session, err := m.Authenticate(context.Background(), token)
	if err != nil || !AllowsScope(session.Scope, ScopeReadonly) {
		t.Fatalf("earlier execution session: %+v %v", session, err)
	}
	if AllowsScope("", ScopeReadonly) || AllowsScope("other", ScopeReadonly) || ValidScope(legacyScopeExec) {
		t.Fatal("a scope that is neither the one nor the earlier execution scope is allowed, or an operation may require the earlier one")
	}
}
