// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License.

package redact

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestCredentialEvidence(t *testing.T) {
	s := New("fixture-private-value")
	raw := json.RawMessage(`{"data":{"password":"other-secret","client_secret":"client-credential","api-key":"api-credential","private_key":"private-credential","message":"password='fixture-private-value' Authorization: Bearer example-token api_key=api-text-credential","count":4},"tenant":"tenant-fixture","space":"space-fixture"}`)
	out := s.JSON(raw)
	for _, secret := range []string{"other-secret", "fixture-private-value", "example-token", "client-credential", "api-credential", "private-credential", "api-text-credential"} {
		if strings.Contains(string(out), secret) {
			t.Errorf("credential retained: %s", secret)
		}
	}
	if !json.Valid(out) || !strings.Contains(string(out), `"count":4`) || !strings.Contains(string(out), "space-fixture") {
		t.Fatalf("lost native context: %s", out)
	}
}

func TestFlattenedSpanCredentialFields(t *testing.T) {
	raw := json.RawMessage(`{"attributes.api_key":"flat-api-credential","resource.client_secret":"flat-resource-credential","attributes.http.request.header.Authorization":"flat-auth-credential","resource.service.name":"fixture-service","attributes.token_count":7,"events":[{"attributes":{"exception.api-key":"event-api-credential","message":"fixture-message"}}],"links":[{"attributes.private_key":"link-private-credential"}]}`)
	out := New().JSON(raw)
	for _, credential := range []string{"flat-api-credential", "flat-resource-credential", "flat-auth-credential", "event-api-credential", "link-private-credential"} {
		if strings.Contains(string(out), credential) {
			t.Errorf("flattened Span credential retained: %s", credential)
		}
	}
	for _, retained := range []string{`"resource.service.name":"fixture-service"`, `"attributes.token_count":7`, `"message":"fixture-message"`} {
		if !strings.Contains(string(out), retained) {
			t.Errorf("non-credential Span field changed: %s", out)
		}
	}
}
