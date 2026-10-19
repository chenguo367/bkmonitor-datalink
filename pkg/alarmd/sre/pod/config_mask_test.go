// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License.
package pod

import (
	"encoding/json"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// A write's diff shows a ConfigMap the way configmap.get does, and an
// annotation holding something shaped like a key is masked under any name:
// neither path shows what the read masks.
func TestAWriteDiffShowsAConfigMapAsTheReadDoes(t *testing.T) {
	client, _ := fixture()
	provider := mustProvider(t, Options{Client: client, DefaultNamespace: "own", FingerprintKey: testFingerprintKey})
	rootKey := "Q2xhc3NpY0tleU1hdGVyaWFsMTIzNDU2Nzg5MEFCQ0RFRg"
	pem := "-----BEGIN CERTIFICATE-----\nMIIBszCCAVmgAwIBAgIUZ3oX\n-----END CERTIFICATE-----"
	object := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "ConfigMap",
		"metadata": map[string]any{"name": "c", "namespace": "own", "annotations": map[string]any{"example.com/note": pem, "example.com/owner": "team-a"}},
		"data":     map[string]any{"crypt_root_key": rootKey, "host": "databus.data.svc", "app.yaml": "crypt:\n  root_key: " + rootKey + "\nport: 8080\n"}}}
	shown := provider.shown(object, provider.newFingerprints())
	encoded, _ := json.Marshal(shown)
	if strings.Contains(string(encoded), rootKey) || strings.Contains(string(encoded), "MIIBszCC") {
		t.Fatalf("diff shows key material: %s", encoded)
	}
	for _, kept := range []string{`"host":"databus.data.svc"`, "port: 8080", `"example.com/owner":"team-a"`} {
		if !strings.Contains(string(encoded), kept) {
			t.Fatalf("diff lost %s: %s", kept, encoded)
		}
	}
}
