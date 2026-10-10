// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License.

package blackbox

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestExecPairingSurvivesActualLegacyAuthorizationHandler(t *testing.T) {
	legacyBinary := os.Getenv("BLACKBOX_LEGACY_AUTH_BIN")
	if legacyBinary == "" {
		t.Skip("set BLACKBOX_LEGACY_AUTH_BIN to the legacy-built fixtures/legacy-auth server")
	}
	h := newHarness(t, "http")
	defer h.report()
	request, _ := http.NewRequest(http.MethodPost, h.server.URL+"/alarmd/api/cli/auth/grants", strings.NewReader(`{"confirm":true,"scope":"deployment_ops_exec"}`))
	request.Header.Set("Authorization", "Bearer "+h.secrets[0])
	request.Header.Set("Origin", h.server.URL)
	request.Header.Set("Content-Type", "application/json")
	issued, err := h.server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	var grant map[string]any
	_ = json.NewDecoder(issued.Body).Decode(&grant)
	_ = issued.Body.Close()
	if issued.StatusCode != 200 || grant["scope"] != "deployment_ops_exec" {
		t.Fatal("new handler did not grant explicit exec scope")
	}
	h.run("explicit-exec-login", 0, grant["authorization_code"].(string), "auth", "login", "--scope", "deployment_ops_exec")
	profileBytes, err := os.ReadFile(h.profileDir + "/profiles.json")
	if err != nil {
		t.Fatal(err)
	}
	var profiles struct {
		Profiles map[string]struct {
			RefreshToken string `json:"refresh_token"`
		}
	}
	if json.Unmarshal(profileBytes, &profiles) != nil {
		t.Fatal("profile not readable")
	}
	refresh := profiles.Profiles[environment].RefreshToken
	h.secrets = append(h.secrets, refresh)
	if !strings.HasPrefix(refresh, "exec-v1.") {
		t.Fatal("exec renewal has no legacy-safe prefix")
	}
	command := exec.Command(legacyBinary, h.redis.Options().Addr, "fixture-auth", environment, h.publicBaseURL, h.secrets[0])
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	command.Stderr = io.Discard
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = command.Process.Kill(); _ = command.Wait() })
	scanner := bufio.NewScanner(stdout)
	if !scanner.Scan() {
		t.Fatal("legacy fixture did not listen")
	}
	legacyURL := scanner.Text()
	t.Logf("legacy auth baseline=%s binary_sha256=%s", os.Getenv("BLACKBOX_LEGACY_AUTH_HEAD"), fileDigest(legacyBinary))
	sha := func(value string) string { sum := sha256.Sum256([]byte(value)); return hex.EncodeToString(sum[:]) }
	key := "fixture-auth.cli:{" + sha(environment) + "}:pairing:" + sha(refresh)
	before, err := h.redis.Get(context.Background(), key).Bytes()
	if err != nil {
		t.Fatal("exec pairing not in Redis")
	}
	body, _ := json.Marshal(map[string]string{"environment_id": environment, "refresh_token": refresh})
	oldRequest, _ := http.NewRequest(http.MethodPost, legacyURL+"/api/cli/auth/refresh", bytes.NewReader(body))
	oldRequest.Header.Set("Content-Type", "application/json")
	oldResponse, err := h.server.Client().Do(oldRequest)
	if err != nil {
		t.Fatal(err)
	}
	_ = oldResponse.Body.Close()
	if oldResponse.StatusCode != 401 {
		t.Fatalf("legacy handler status %d", oldResponse.StatusCode)
	}
	after, err := h.redis.Get(context.Background(), key).Bytes()
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("legacy handler deleted or changed exec pairing")
	}
	newRequest, _ := http.NewRequest(http.MethodPost, h.server.URL+"/alarmd/api/cli/auth/refresh", bytes.NewReader(body))
	newRequest.Header.Set("Content-Type", "application/json")
	renewed, err := h.server.Client().Do(newRequest)
	if err != nil {
		t.Fatal(err)
	}
	var response map[string]any
	_ = json.NewDecoder(renewed.Body).Decode(&response)
	_ = renewed.Body.Close()
	if renewed.StatusCode != 200 || response["scope"] != "deployment_ops_exec" {
		t.Fatal("new handler lost original exec authorization")
	}
	if !strings.HasPrefix(response["refresh_token"].(string), "exec-v1.") {
		t.Fatal("renewal lost compatibility prefix")
	}
	session, err := h.manager.Authenticate(context.Background(), response["access_token"].(string))
	if err != nil || session.Scope != "deployment_ops_exec" {
		t.Fatal("renewed exec session unusable")
	}
}
