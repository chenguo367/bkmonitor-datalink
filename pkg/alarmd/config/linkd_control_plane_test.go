// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package config

import (
	"encoding/json"
	"strings"
	"testing"
)

// The link control plane's JWT secret comes from a file and only from one:
// read and cleared, its defaults applied, and a secret written inline is
// refused. A control plane without the file loads, unconfigured: the leader
// counts the missing secret and stays on the existing close paths.
func TestTheLinkControlPlaneSecretComesOnlyFromAFile(t *testing.T) {
	clearCredentialEnvironment(t)
	cfg, err := LoadWithRoles(writeConfig(t, credentialConfig(map[string][]string{"phase_two.linkd": {
		"control_plane:", "  url: http://linkd.example.test:8080",
		"  jwt_secret_file: " + writeCredential(t, "jwt", "example-secret\n")}})), nil)
	if err != nil {
		t.Fatal(err)
	}
	plane := cfg.PhaseTwo.Linkd.ControlPlane
	if plane.JWTSecret != "example-secret" || plane.JWTSecretFile != "" || plane.Username() != "alarmd" || plane.Source() != "built_in_bk" || !plane.Configured() {
		t.Fatalf("control plane = %+v", plane)
	}
	cfg, err = LoadWithRoles(writeConfig(t, credentialConfig(map[string][]string{"phase_two.linkd": {
		"control_plane:", "  url: http://linkd.example.test:8080"}})), nil)
	if err != nil || cfg.PhaseTwo.Linkd.ControlPlane.Configured() {
		t.Fatalf("a control plane without the secret: %v, configured %v", err, cfg.PhaseTwo.Linkd.ControlPlane.Configured())
	}
	for name, block := range map[string][]string{
		"inline":      {"control_plane:", "  url: http://linkd.example.test:8080", "  jwt_secret: example-secret"},
		"URL user":    {"control_plane:", "  url: http://a:b@linkd.example.test", "  jwt_secret_file: " + writeCredential(t, "jwt", "x\n")},
		"source path": {"control_plane:", "  url: http://linkd.example.test", "  event_source_id: a/b", "  jwt_secret_file: " + writeCredential(t, "jwt", "x\n")},
	} {
		if _, err := LoadWithRoles(writeConfig(t, credentialConfig(map[string][]string{"phase_two.linkd": block})), nil); err == nil {
			t.Fatalf("%s: accepted", name)
		}
	}
}

// The resolved secret is never written out: not in YAML, not in JSON (a
// configuration printed for a reader or a fact page).
func TestTheLinkControlPlaneSecretIsNeverMarshalled(t *testing.T) {
	plane := LinkdControlPlaneConfig{URL: "http://192.0.2.10:8090", JWTSecretFile: "/etc/alarmd/secrets/linkd-jwt-secret",
		JWTSecret: "example-secret"}
	encoded, err := json.Marshal(plane)
	if err != nil || strings.Contains(string(encoded), "example-secret") {
		t.Fatalf("json %s (%v) carries the secret", encoded, err)
	}
}
