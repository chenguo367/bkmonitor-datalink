// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License.

package cliauth

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"strings"
	"time"
)

const (
	IssuerKeyHeader = "X-Alarmd-Issuer-Key"
	PrincipalHeader = "X-Alarmd-Principal"
	grantsPath      = "/api/cli/auth/grants"
	exchangePath    = "/api/cli/auth/exchange"
	sessionPath     = "/api/cli/session"
	maxBodyBytes    = 64 << 10
)

// Handler serves only the four auth operations. The surrounding server remains
// responsible for bounded request read time and trusted proxy routing. It must
// not expose issuer credentials through access logs or configuration evidence.
func (m *Manager) Handler() http.Handler { return http.HandlerFunc(m.serveHTTP) }

func (m *Manager) serveHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	select {
	case m.httpSlots <- struct{}{}:
		defer func() { <-m.httpSlots }()
	default:
		// Do not let net/http wait for an unaccepted request body while
		// writing the busy response. No Redis or handler slot is needed.
		defer boundBodyRead(w, r, 0)()
		writeError(w, failure("auth_busy", "Too many concurrent authorization requests.", 429))
		return
	}
	// The same listener also serves long-lived h2c/gRPC streams. Limit only
	// this auth request, including body draining after an early rejection.
	defer boundBodyRead(w, r, 3*time.Second)()
	var err error
	switch r.URL.Path {
	case grantsPath:
		err = m.handleGrants(w, r)
	case exchangePath:
		err = m.handleExchange(w, r)
	case sessionPath:
		err = m.handleSession(w, r)
	default:
		err = failure("not_found", "Unknown authorization route.", 404)
	}
	if err != nil {
		writeError(w, err)
	}
}

func boundBodyRead(w http.ResponseWriter, r *http.Request, timeout time.Duration) func() {
	controller := http.NewResponseController(w)
	deadlineSet := controller.SetReadDeadline(time.Now().Add(timeout)) == nil
	return func() {
		if r.Body != nil {
			_ = r.Body.Close()
		}
		if deadlineSet {
			_ = controller.SetReadDeadline(time.Time{})
		}
	}
}

func (m *Manager) issuerPrincipal(r *http.Request) (string, error) {
	if !m.issuerConfigured {
		return "", failure("issuer_not_configured", "Trusted host authorization is not configured.", 503)
	}
	key, singleKey := singleHeader(r, IssuerKeyHeader)
	principal, singlePrincipal := singleHeader(r, PrincipalHeader)
	hash := sha256.Sum256([]byte(key))
	keyMatches := subtle.ConstantTimeCompare(hash[:], m.issuerHash[:]) == 1
	if !singleKey || !singlePrincipal || !keyMatches || !validText(principal, 256) || len(r.Header.Values("Authorization")) != 0 {
		return "", failure("issuer_unauthorized", "A trusted host must authorize this request and provide its principal.", 403)
	}
	return principal, nil
}

func singleHeader(r *http.Request, name string) (string, bool) {
	values := r.Header.Values(name)
	if len(values) != 1 || values[0] == "" {
		return "", false
	}
	return values[0], true
}

type grantPreview struct {
	EnvironmentID     string `json:"environment_id"`
	EnvironmentName   string `json:"environment_name"`
	PublicBaseURL     string `json:"public_base_url"`
	Principal         string `json:"principal"`
	Scope             string `json:"scope"`
	GrantTTLSeconds   int    `json:"grant_ttl_seconds"`
	SessionTTLSeconds int    `json:"session_ttl_seconds"`
}

type authorizationPackage struct {
	Version         string    `json:"version"`
	EnvironmentID   string    `json:"environment_id"`
	EnvironmentName string    `json:"environment_name"`
	PublicBaseURL   string    `json:"public_base_url"`
	GrantSecret     string    `json:"grant_secret"`
	GrantExpiresAt  time.Time `json:"grant_expires_at"`
}

func (m *Manager) handleGrants(w http.ResponseWriter, r *http.Request) error {
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		return methodNotAllowed(w, "GET, POST")
	}
	principal, err := m.issuerPrincipal(r)
	if err != nil {
		return err
	}
	preview := grantPreview{EnvironmentID: m.environmentID, EnvironmentName: m.environmentName,
		PublicBaseURL: m.publicBaseURL, Principal: principal, Scope: ScopeReadonly,
		GrantTTLSeconds: int(GrantLifetime.Seconds()), SessionTTLSeconds: int(SessionLifetime.Seconds())}
	if r.Method == http.MethodGet {
		return writeJSON(w, preview)
	}
	origin, single := singleHeader(r, "Origin")
	if !single || origin != m.origin {
		return failure("origin_denied", "A grant must be requested from the configured host origin.", 403)
	}
	var input struct {
		Confirm bool `json:"confirm"`
	}
	if err := decodeJSON(w, r, &input); err != nil {
		return err
	}
	if !input.Confirm {
		return failure("invalid_request", "Explicit authorization confirmation is required.", 400)
	}
	if !m.allow(&m.grantWindow, 6) {
		return failure("auth_rate_limited", "The grant issuance limit was reached; try again in the next minute.", 429)
	}
	secret, err := randomSecret()
	if err != nil {
		return err
	}
	record, _ := json.Marshal(storedRecord{Principal: principal, EnvironmentID: m.environmentID, Scope: ScopeReadonly})
	result, err := m.run(r.Context(), issueScript, []string{m.prefix + "grant:" + digest(secret)}, string(record), GrantLifetime.Milliseconds())
	if err != nil {
		return err
	}
	issued, _, err := resultRecord(result)
	if err != nil {
		return err
	}
	expiresAt := time.UnixMilli(issued.ExpiresAtMS).UTC()
	code, _ := json.Marshal(authorizationPackage{Version: "alarmd-login/v1", EnvironmentID: m.environmentID,
		EnvironmentName: m.environmentName, PublicBaseURL: m.publicBaseURL,
		GrantSecret: secret, GrantExpiresAt: expiresAt})
	return writeJSON(w, struct {
		grantPreview
		AuthorizationCode string    `json:"authorization_code"`
		GrantExpiresAt    time.Time `json:"grant_expires_at"`
	}{preview, "alarmd-login-v1." + base64.RawURLEncoding.EncodeToString(code), expiresAt})
}

func (m *Manager) handleExchange(w http.ResponseWriter, r *http.Request) error {
	if r.Method != http.MethodPost {
		return methodNotAllowed(w, "POST")
	}
	if !m.allow(&m.exchangeWindow, 60) {
		return failure("auth_rate_limited", "The grant exchange limit was reached; try again in the next minute.", 429)
	}
	var input struct {
		EnvironmentID string `json:"environment_id"`
		GrantSecret   string `json:"grant_secret"`
	}
	if err := decodeJSON(w, r, &input); err != nil {
		return err
	}
	if input.EnvironmentID != m.environmentID || !validSecret(input.GrantSecret) {
		return failure("grant_invalid_or_expired", "The grant is invalid, expired, or already used; obtain a new code.", 401)
	}
	token, err := randomSecret()
	if err != nil {
		return err
	}
	id, err := randomSecret()
	if err != nil {
		return err
	}
	result, err := m.run(r.Context(), exchangeScript,
		[]string{m.prefix + "grant:" + digest(input.GrantSecret), m.prefix + "session:" + digest(token)},
		m.environmentID, id, ScopeReadonly, SessionLifetime.Milliseconds())
	if err != nil {
		return err
	}
	record, _, err := resultRecord(result)
	if ErrorCode(err) == "auth_expired_or_revoked" {
		return failure("grant_invalid_or_expired", "The grant is invalid, expired, or already used; obtain a new code.", 401)
	}
	if err != nil {
		return err
	}
	return writeJSON(w, struct {
		EnvironmentID   string    `json:"environment_id"`
		EnvironmentName string    `json:"environment_name"`
		PublicBaseURL   string    `json:"public_base_url"`
		AccessToken     string    `json:"access_token"`
		SessionID       string    `json:"session_id"`
		ExpiresAt       time.Time `json:"expires_at"`
		Scope           string    `json:"scope"`
	}{m.environmentID, m.environmentName, m.publicBaseURL, token, id, time.UnixMilli(record.ExpiresAtMS).UTC(), ScopeReadonly})
}

func (m *Manager) handleSession(w http.ResponseWriter, r *http.Request) error {
	if r.Method != http.MethodGet && r.Method != http.MethodDelete {
		return methodNotAllowed(w, "GET, DELETE")
	}
	bearer, err := requestBearer(r)
	if err != nil {
		return err
	}
	action := "read"
	if r.Method == http.MethodDelete {
		action = "delete"
	}
	session, err := m.sessionOperation(r.Context(), digest(bearer), "", action)
	if err != nil {
		return err
	}
	if r.Method == http.MethodDelete {
		return writeJSON(w, struct {
			SessionID string `json:"session_id"`
			Revoked   bool   `json:"revoked"`
		}{session.ID, true})
	}
	return writeJSON(w, session)
}

func requestBearer(r *http.Request) (string, error) {
	header, single := singleHeader(r, "Authorization")
	parts := strings.Split(header, " ")
	if !single || len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || !validSecret(parts[1]) {
		return "", expired()
	}
	return parts[1], nil
}

func decodeJSON(w http.ResponseWriter, r *http.Request, dest interface{}) error {
	contentType, single := singleHeader(r, "Content-Type")
	mediaType, _, err := mime.ParseMediaType(contentType)
	if !single || err != nil || mediaType != "application/json" {
		return failure("invalid_content_type", "Content-Type must be application/json.", 415)
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	err = decoder.Decode(dest)
	if err == nil {
		var trailing interface{}
		if trailingErr := decoder.Decode(&trailing); trailingErr != io.EOF {
			err = trailingErr
			if err == nil {
				err = errors.New("trailing JSON")
			}
		}
	}
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			return failure("request_too_large", "Authorization request body exceeds 64 KiB.", 413)
		}
		return failure("invalid_request", "The authorization request must match the JSON input schema.", 400)
	}
	return nil
}

func methodNotAllowed(w http.ResponseWriter, methods string) error {
	w.Header().Set("Allow", methods)
	return failure("method_not_allowed", "This method is not supported for the authorization route.", 405)
}

func writeJSON(w http.ResponseWriter, value interface{}) error {
	// All response shapes contain only serializable fields. A disconnected
	// client does not trigger a second write or repeat a credential mutation.
	_ = json.NewEncoder(w).Encode(value)
	return nil
}

func writeError(w http.ResponseWriter, err error) {
	var public *Error
	if !errors.As(err, &public) {
		public = failure("internal_error", "Authorization request failed.", 500)
	}
	w.WriteHeader(public.HTTPStatus)
	_ = json.NewEncoder(w).Encode(struct {
		Status string `json:"status"`
		Error  *Error `json:"error"`
	}{"error", public})
}
