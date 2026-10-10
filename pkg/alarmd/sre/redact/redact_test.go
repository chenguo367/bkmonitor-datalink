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
