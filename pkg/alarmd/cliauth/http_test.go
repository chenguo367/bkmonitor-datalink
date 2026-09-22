// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License.

package cliauth

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
)

func noStoreManager(t *testing.T) *Manager {
	t.Helper()
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", MaxRetries: -1})
	t.Cleanup(func() { _ = client.Close() })
	return newTestManager(t, client)
}

func trustedRequest(method, body string) *http.Request {
	r := httptest.NewRequest(method, grantsPath, strings.NewReader(body))
	r.Header.Set(IssuerKeyHeader, testIssuerKey)
	r.Header.Set(PrincipalHeader, "tenant-a/operator")
	r.Header.Set("Origin", "https://example.test")
	r.Header.Set("Content-Type", "application/json")
	return r
}

func TestGrantTrustAndRequestValidation(t *testing.T) {
	m := noStoreManager(t)
	tests := []struct {
		name   string
		modify func(*http.Request)
		body   string
		status int
		code   string
	}{
		{"no issuer key", func(r *http.Request) { r.Header.Del(IssuerKeyHeader) }, `{"confirm":true}`, 403, "issuer_unauthorized"},
		{"wrong issuer key", func(r *http.Request) { r.Header.Set(IssuerKeyHeader, "incorrect") }, `{"confirm":true}`, 403, "issuer_unauthorized"},
		{"two issuer keys", func(r *http.Request) { r.Header.Add(IssuerKeyHeader, testIssuerKey) }, `{"confirm":true}`, 403, "issuer_unauthorized"},
		{"empty principal", func(r *http.Request) { r.Header.Del(PrincipalHeader) }, `{"confirm":true}`, 403, "issuer_unauthorized"},
		{"long principal", func(r *http.Request) { r.Header.Set(PrincipalHeader, strings.Repeat("x", 257)) }, `{"confirm":true}`, 403, "issuer_unauthorized"},
		{"control in principal", func(r *http.Request) { r.Header.Set(PrincipalHeader, "user\nadmin") }, `{"confirm":true}`, 403, "issuer_unauthorized"},
		{"two principals", func(r *http.Request) { r.Header.Add(PrincipalHeader, "another-user") }, `{"confirm":true}`, 403, "issuer_unauthorized"},
		{"bearer cannot mint", func(r *http.Request) { r.Header.Set("Authorization", "Bearer credential") }, `{"confirm":true}`, 403, "issuer_unauthorized"},
		{"no origin", func(r *http.Request) { r.Header.Del("Origin") }, `{"confirm":true}`, 403, "origin_denied"},
		{"null origin", func(r *http.Request) { r.Header.Set("Origin", "null") }, `{"confirm":true}`, 403, "origin_denied"},
		{"wrong scheme", func(r *http.Request) { r.Header.Set("Origin", "http://example.test") }, `{"confirm":true}`, 403, "origin_denied"},
		{"suffix origin", func(r *http.Request) { r.Header.Set("Origin", "https://example.test.evil.test") }, `{"confirm":true}`, 403, "origin_denied"},
		{"two origins", func(r *http.Request) { r.Header.Add("Origin", "https://example.test") }, `{"confirm":true}`, 403, "origin_denied"},
		{"joined origins", func(r *http.Request) { r.Header.Set("Origin", "https://example.test, https://evil.test") }, `{"confirm":true}`, 403, "origin_denied"},
		{"no MIME", func(r *http.Request) { r.Header.Del("Content-Type") }, `{"confirm":true}`, 415, "invalid_content_type"},
		{"form MIME", func(r *http.Request) { r.Header.Set("Content-Type", "application/x-www-form-urlencoded") }, `confirm=true`, 415, "invalid_content_type"},
		{"two MIME", func(r *http.Request) { r.Header.Add("Content-Type", "application/json") }, `{"confirm":true}`, 415, "invalid_content_type"},
		{"no confirmation", nil, `{}`, 400, "invalid_request"},
		{"false confirmation", nil, `{"confirm":false}`, 400, "invalid_request"},
		{"unknown field", nil, `{"confirm":true,"scope":"admin"}`, 400, "invalid_request"},
		{"wrong type", nil, `{"confirm":"true"}`, 400, "invalid_request"},
		{"trailing JSON", nil, `{"confirm":true}{}`, 400, "invalid_request"},
		{"oversize trailing whitespace", nil, `{"confirm":true}` + strings.Repeat(" ", maxBodyBytes), 413, "request_too_large"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := trustedRequest(http.MethodPost, tt.body)
			if tt.modify != nil {
				tt.modify(r)
			}
			w := httptest.NewRecorder()
			m.Handler().ServeHTTP(w, r)
			if w.Code != tt.status {
				t.Fatalf("status=%d, want=%d body=%s", w.Code, tt.status, w.Body)
			}
			var payload struct {
				Status string `json:"status"`
				Error  Error  `json:"error"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &payload); err != nil {
				t.Fatal(err)
			}
			if payload.Status != "error" || payload.Error.Code != tt.code {
				t.Fatalf("payload=%+v", payload)
			}
			if strings.Contains(w.Body.String(), testIssuerKey) || strings.Contains(w.Body.String(), "credential") {
				t.Fatal("error echoed a request credential")
			}
			if w.Header().Get("Cache-Control") != "no-store" || w.Header().Get("Access-Control-Allow-Origin") != "" {
				t.Fatal("cache or CORS response policy violated")
			}
		})
	}
}

func TestPreviewDoesNotCreateGrantOrUseRedis(t *testing.T) {
	m := noStoreManager(t)
	for i := 0; i < 10; i++ {
		response := authRequest(m, http.MethodGet, grantsPath, "", true)
		if response.Code != 200 || strings.Contains(response.Body.String(), "authorization_code") || strings.Contains(response.Body.String(), testIssuerKey) {
			t.Fatal("invalid grant preview")
		}
		var preview grantPreview
		if err := json.Unmarshal(response.Body.Bytes(), &preview); err != nil {
			t.Fatal(err)
		}
		if preview.Principal != "tenant-a/operator" || preview.Scope != ScopeReadonly || preview.SessionTTLSeconds != 3600 || preview.GrantTTLSeconds != 300 {
			t.Fatalf("preview=%+v", preview)
		}
	}
	if m.grantWindow.count != 0 {
		t.Fatal("preview consumed issuance budget")
	}
	m.issuerConfigured = false
	response := authRequest(m, http.MethodGet, grantsPath, "", true)
	if response.Code != 503 || !strings.Contains(response.Body.String(), "issuer_not_configured") {
		t.Fatal("unconfigured issuer accepted")
	}
}

func TestHTTPBudgetsAndMethodBoundaries(t *testing.T) {
	client := startRedis(t)
	m := newTestManager(t, client)
	for i := 0; i < 6; i++ {
		issue(t, m)
	}
	if seventh := authRequest(m, http.MethodPost, grantsPath, `{"confirm":true}`, true); seventh.Code != 429 {
		t.Fatalf("issuance budget status=%d", seventh.Code)
	}
	if count := len(client.Keys(context.Background(), m.prefix+"grant:*").Val()); count != 6 {
		t.Fatalf("issued %d grants", count)
	}
	m.now = func() time.Time { return time.Unix(61, 0) }
	issue(t, m)
	for i := 0; i < 60; i++ {
		response := authRequest(m, http.MethodPost, exchangePath, `{}`, false)
		if response.Code != 401 {
			t.Fatalf("exchange attempt %d status=%d", i, response.Code)
		}
	}
	if response := authRequest(m, http.MethodPost, exchangePath, `{}`, false); response.Code != 429 {
		t.Fatal("exchange budget not enforced")
	}
	for i := 0; i < cap(m.httpSlots); i++ {
		m.httpSlots <- struct{}{}
	}
	if response := authRequest(m, http.MethodGet, grantsPath, "", true); response.Code != 429 {
		t.Fatal("concurrency budget not enforced")
	}
	for i := 0; i < cap(m.httpSlots); i++ {
		<-m.httpSlots
	}
	for _, path := range []string{grantsPath, exchangePath, sessionPath} {
		if response := authRequest(m, http.MethodOptions, path, "", true); response.Code != 405 || response.Header().Get("Access-Control-Allow-Origin") != "" {
			t.Fatal("CORS preflight accepted")
		}
	}
	if response := authRequest(m, http.MethodGet, sessionPath, "", true); response.Code != 401 {
		t.Fatal("issuer key acted as bearer")
	}
	if response := authRequest(m, http.MethodGet, "/api/cli/channel", "", true); response.Code != 404 {
		t.Fatal("auth handler served a channel request")
	}
}

func TestNewValidatesCoordinatesWithoutExposingSecrets(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", MaxRetries: -1})
	defer client.Close()
	base := Options{Redis: client, Prefix: "state", EnvironmentID: "environment", EnvironmentName: "Environment",
		PublicBaseURL: "https://example.test/prefix", IssuerKey: testIssuerKey}
	m, err := New(base)
	if err != nil || m.publicBaseURL != "https://example.test/prefix/" {
		t.Fatalf("New=%v err=%v", m, err)
	}
	for _, invalidURL := range []string{"", "//example.test", "http://example.test/", "https://user:secret@example.test/", "https://example.test/?token=secret", "https://example.test/#fragment", "https://example.test/../bad", "https://example.test/%2e%2e/bad", "https://example.test/a%2f..%2fb"} {
		t.Run(invalidURL, func(t *testing.T) {
			opts := base
			opts.PublicBaseURL = invalidURL
			if _, err := New(opts); err == nil || strings.Contains(err.Error(), "secret") {
				t.Fatalf("invalid URL result=%v", err)
			}
		})
	}
	for _, mutate := range []func(*Options){
		func(o *Options) { o.Redis = nil },
		func(o *Options) { o.Prefix = "a{wrong-slot}" },
		func(o *Options) { o.EnvironmentID = "" },
		func(o *Options) { o.EnvironmentName = "" },
		func(o *Options) { o.IssuerKey = "weak" },
	} {
		opts := base
		mutate(&opts)
		if _, err := New(opts); err == nil {
			t.Fatal("invalid options accepted")
		}
	}
	base.IssuerKey = ""
	if _, err := New(base); err != nil {
		t.Fatalf("disabled issuance rejected: %v", err)
	}
	base.EnvironmentID = "env{unexpected-slot}"
	m, err = New(base)
	if err != nil || strings.Count(m.prefix, "{") != 1 || strings.Contains(m.prefix, base.EnvironmentID) {
		t.Fatal("environment changed Redis hash tag")
	}
}

type deadlineRecorder struct {
	*httptest.ResponseRecorder
	deadlines []time.Time
}

func (w *deadlineRecorder) SetReadDeadline(deadline time.Time) error {
	w.deadlines = append(w.deadlines, deadline)
	return nil
}

func TestBodyReadDeadlineIsLimitedToTheRoute(t *testing.T) {
	m := noStoreManager(t)
	w := &deadlineRecorder{ResponseRecorder: httptest.NewRecorder()}
	before := time.Now()
	m.Handler().ServeHTTP(w, trustedRequest(http.MethodPost, `{"confirm":false}`))
	if len(w.deadlines) != 2 || w.deadlines[0].Before(before.Add(2*time.Second)) || w.deadlines[0].After(before.Add(4*time.Second)) || !w.deadlines[1].IsZero() {
		t.Fatalf("deadlines=%v, want a bounded read followed by reset", w.deadlines)
	}
}

func TestSlowBodyReleasesHTTPAdmissionSlot(t *testing.T) {
	m := noStoreManager(t)
	server := httptest.NewServer(m.Handler())
	defer server.Close()
	conn, err := net.Dial("tcp", strings.TrimPrefix(server.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	_, err = fmt.Fprintf(conn, "POST %s HTTP/1.1\r\nHost: example.test\r\nOrigin: https://example.test\r\n%s: %s\r\n%s: tenant-a/operator\r\nContent-Type: application/json\r\nContent-Length: 16\r\n\r\n{", grantsPath, IssuerKeyHeader, testIssuerKey, PrincipalHeader)
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: http.MethodPost})
	if err != nil {
		t.Fatalf("slow body did not receive a bounded response: %v", err)
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, response.Body)
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("slow body status=%d", response.StatusCode)
	}
	if len(m.httpSlots) != 0 {
		t.Fatal("slow body retained an authorization slot")
	}
}

func TestSaturatedHandlerDoesNotWaitForRejectedBody(t *testing.T) {
	m := noStoreManager(t)
	for i := 0; i < cap(m.httpSlots); i++ {
		m.httpSlots <- struct{}{}
	}
	server := httptest.NewServer(m.Handler())
	defer server.Close()
	conn, err := net.Dial("tcp", strings.TrimPrefix(server.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	_, err = fmt.Fprintf(conn, "POST %s HTTP/1.1\r\nHost: example.test\r\nContent-Length: 16\r\n\r\n{", grantsPath)
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: http.MethodPost})
	if err != nil {
		t.Fatalf("busy handler waited for body: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("busy status=%d", response.StatusCode)
	}
}
