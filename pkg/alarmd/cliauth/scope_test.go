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
)

func TestExplicitExecGrantAndRefreshPreserveInitialScope(t *testing.T) {
	m := newTestManager(t, startRedis(t))
	for _, scope := range []string{ScopeReadonly, ScopeExec} {
		t.Run(scope, func(t *testing.T) {
			body := `{"confirm":true}`
			if scope == ScopeExec {
				body = `{"confirm":true,"scope":"deployment_ops_exec"}`
			}
			issued := authRequest(m, http.MethodPost, grantsPath, body, true)
			var grant struct {
				AuthorizationCode string `json:"authorization_code"`
				Scope             string `json:"scope"`
			}
			if issued.Code != 200 || json.Unmarshal(issued.Body.Bytes(), &grant) != nil || grant.Scope != scope {
				t.Fatalf("grant: %d %s", issued.Code, issued.Body)
			}
			raw, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(grant.AuthorizationCode, "alarmd-login-v1."))
			if err != nil {
				t.Fatal(err)
			}
			var code authorizationPackage
			if json.Unmarshal(raw, &code) != nil {
				t.Fatal("invalid code")
			}
			if scope == ScopeReadonly && strings.Contains(string(raw), `"scope"`) {
				t.Fatal("readonly code schema broke old CLI")
			}
			if scope == ScopeExec && code.Scope != ScopeExec {
				t.Fatal("exec code did not declare explicit scope")
			}
			login := decodeLogin(t, exchange(m, code))
			if scope == ScopeExec && (!strings.HasPrefix(login.RefreshToken, execRenewalPrefix) || validSecret(login.RefreshToken)) {
				t.Fatal("old instance can admit exec renewal secret")
			}
			if scope == ScopeReadonly && !validSecret(login.RefreshToken) {
				t.Fatal("readonly renewal format changed")
			}
			session, err := m.Authenticate(context.Background(), login.AccessToken)
			if err != nil || session.Scope != scope {
				t.Fatalf("session: %+v %v", session, err)
			}
			upgraded := authRequest(m, http.MethodPost, refreshPath, `{"environment_id":"test-env","refresh_token":"`+login.RefreshToken+`","scope":"deployment_ops_exec"}`, false)
			if upgraded.Code != 400 || errorCode(t, upgraded) != "invalid_request" {
				t.Fatal("refresh accepted requested scope")
			}
			next := decodeLogin(t, refresh(m, login.RefreshToken))
			renewed, err := m.Authenticate(context.Background(), next.AccessToken)
			if err != nil || renewed.Scope != scope {
				t.Fatalf("renewed scope: %+v %v", renewed, err)
			}
			if AllowsScope(renewed.Scope, ScopeExec) != (scope == ScopeExec) {
				t.Fatal("refresh raised readonly permission")
			}
			replay := decodeLogin(t, refresh(m, login.RefreshToken))
			replayed, err := m.Authenticate(context.Background(), replay.AccessToken)
			if err != nil || replayed.Scope != scope || replay.AccessToken != next.AccessToken {
				t.Fatal("replayed renewal changed authorization")
			}
		})
	}
}

func TestLegacyReadonlySessionPairingCannotAcquireExec(t *testing.T) {
	m := newTestManager(t, startRedis(t))
	token, _ := randomSecret()
	record := storedRecord{SessionID: "legacy", EnvironmentID: m.environmentID, Scope: ScopeReadonly, ExpiresAtMS: time.Now().Add(time.Hour).UnixMilli()}
	raw, _ := json.Marshal(record)
	if err := m.client.Set(context.Background(), m.prefix+"session:"+digest(token), raw, time.Hour).Err(); err != nil {
		t.Fatal(err)
	}
	refreshToken, _, err := m.Upgrade(context.Background(), token)
	if err != nil {
		t.Fatal(err)
	}
	next, err := m.Refresh(context.Background(), refreshToken)
	if err != nil || next.Session.Scope != ScopeReadonly || AllowsScope(next.Session.Scope, ScopeExec) {
		t.Fatalf("legacy pairing elevated: %+v %v", next.Session, err)
	}
	admitted, err := m.Admit(context.Background(), next.Session, true)
	if err != nil || admitted.Scope != ScopeReadonly {
		t.Fatalf("admission changed legacy scope: %+v %v", admitted, err)
	}
}
