// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License.

package pod

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

type secretFixture struct {
	secrets map[string]*corev1.Secret
	asked   []string
}

func (f *secretFixture) GetSecret(_ context.Context, namespace, name string) (*corev1.Secret, error) {
	f.asked = append(f.asked, namespace+"/"+name)
	if secret, found := f.secrets[namespace+"/"+name]; found {
		return secret.DeepCopy(), nil
	}
	return nil, apierrors.NewForbidden(schema.GroupResource{Resource: "secrets"}, name, nil)
}

func secretProvider(t *testing.T) (*Provider, *secretFixture) {
	t.Helper()
	client, _ := fixture()
	secrets := &secretFixture{secrets: map[string]*corev1.Secret{
		"own/app-redis": {ObjectMeta: metav1.ObjectMeta{Namespace: "own", Name: "app-redis", UID: "secret-uid"}, Type: corev1.SecretTypeOpaque,
			Data: map[string][]byte{"password": []byte("plain-value"), "url": []byte("redis://user:inline-password@192.0.2.10:6379"), "known": []byte("deployment-literal")}},
	}}
	return mustProvider(t, Options{Client: client, Secrets: secrets, DefaultNamespace: "own", LiteralSecrets: []string{"deployment-literal"}, FingerprintKey: testFingerprintKey, FingerprintScope: "deployment"}), secrets
}

// testFingerprintKey stands for the deployment-local key.
var testFingerprintKey = []byte("deployment-local-fingerprint-key-for-tests")

// fingerprintOf is the fingerprint as specified: HMAC-SHA256 of the value
// under the deployment's key, the first 16 hex digits.
func fingerprintOf(key []byte, value string) string {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(value))
	return hex.EncodeToString(mac.Sum(nil))[:16]
}

// A Secret read returns each key's size and fingerprint and never a value;
// the namespace is the request's, this process's own when it names none.
func TestASecretReadNeverReturnsAValue(t *testing.T) {
	provider, secrets := secretProvider(t)
	receipt, err := provider.ReadSecret(context.Background(), SecretRequest{Name: "app-redis"})
	if err != nil || receipt.UID != "secret-uid" || len(receipt.Keys) != 3 || secrets.asked[0] != "own/app-redis" || receipt.FingerprintScope != "deployment" {
		t.Fatalf("read: %+v err=%v asked=%v", receipt, err, secrets.asked)
	}
	for _, key := range receipt.Keys {
		if key.Key == "password" && (key.Bytes != len("plain-value") || key.Fingerprint != fingerprintOf(testFingerprintKey, "plain-value")) {
			t.Fatalf("password key %+v", key)
		}
	}
	encoded, _ := json.Marshal(receipt)
	for _, value := range []string{"plain-value", "inline-password", "deployment-literal", "redis://", "sha256", hex.EncodeToString(digestOf("plain-value"))} {
		if strings.Contains(string(encoded), value) {
			t.Fatalf("receipt carries %q: %s", value, encoded)
		}
	}
}

func digestOf(value string) []byte {
	digest := sha256.Sum256([]byte(value))
	return digest[:]
}

// The fingerprint answers "the same value?" within a deployment and nothing
// across them: two Secrets holding one value fingerprint alike, another value
// differently, and the same value under another deployment's key differently,
// so a table of fingerprints of common values made elsewhere does not apply.
func TestAFingerprintComparesWithinADeploymentOnly(t *testing.T) {
	client, _ := fixture()
	secrets := &secretFixture{secrets: map[string]*corev1.Secret{
		"ns1/one":   {ObjectMeta: metav1.ObjectMeta{Namespace: "ns1", Name: "one"}, Data: map[string][]byte{"password": []byte("shared-sentinel")}},
		"ns2/two":   {ObjectMeta: metav1.ObjectMeta{Namespace: "ns2", Name: "two"}, Data: map[string][]byte{"db-password": []byte("shared-sentinel")}},
		"ns2/other": {ObjectMeta: metav1.ObjectMeta{Namespace: "ns2", Name: "other"}, Data: map[string][]byte{"password": []byte("other-sentinel")}},
	}}
	read := func(provider *Provider, namespace, name string) string {
		receipt, err := provider.ReadSecret(context.Background(), SecretRequest{Namespace: namespace, Name: name})
		if err != nil || len(receipt.Keys) != 1 {
			t.Fatalf("%s/%s: %+v err=%v", namespace, name, receipt, err)
		}
		return receipt.Keys[0].Fingerprint
	}
	here := mustProvider(t, Options{Client: client, Secrets: secrets, FingerprintKey: testFingerprintKey})
	elsewhere := mustProvider(t, Options{Client: client, Secrets: secrets, FingerprintKey: []byte("another-deployment-key")})
	one, two, other := read(here, "ns1", "one"), read(here, "ns2", "two"), read(here, "ns2", "other")
	if one != two || one == other || one == read(elsewhere, "ns1", "one") || one == hex.EncodeToString(digestOf("shared-sentinel"))[:16] {
		t.Fatalf("fingerprints one %s two %s other %s elsewhere %s", one, two, other, read(elsewhere, "ns1", "one"))
	}
	// Without a deployment key a process makes its own, and says so.
	local := mustProvider(t, Options{Client: client, Secrets: secrets})
	if receipt, _ := local.ReadSecret(context.Background(), SecretRequest{Namespace: "ns1", Name: "one"}); receipt.FingerprintScope != "process" || receipt.Keys[0].Fingerprint == one {
		t.Fatalf("a process key: %+v", receipt)
	}
}

// A Secret outside the whitelist is the apiserver's refusal, named as such;
// a name that is not a Secret name is refused before any read.
func TestASecretOutsideTheWhitelistIsForbidden(t *testing.T) {
	provider, secrets := secretProvider(t)
	_, err := provider.ReadSecret(context.Background(), SecretRequest{Namespace: "kube-system", Name: "other"})
	requireCode(t, err, CodeForbidden)
	_, err = provider.ReadSecret(context.Background(), SecretRequest{Name: "Not_A_Name"})
	requireCode(t, err, CodeInvalidRequest)
	if len(secrets.asked) != 1 {
		t.Fatalf("asked %v, want only the whitelisted-shape read", secrets.asked)
	}
}

type configMapFixture map[string]*corev1.ConfigMap

func (f configMapFixture) GetConfigMap(_ context.Context, namespace, name string) (*corev1.ConfigMap, error) {
	if configMap, found := f[namespace+"/"+name]; found {
		return configMap.DeepCopy(), nil
	}
	return nil, apierrors.NewNotFound(schema.GroupResource{Resource: "configmaps"}, name)
}

// A ConfigMap's values come back through the output redaction, always: the
// plain-text credentials modules render into their environment ConfigMaps -
// a password assignment, a connection string's userinfo - are masked, and
// everything else is kept.
func TestAConfigMapIsAlwaysRedacted(t *testing.T) {
	client, _ := fixture()
	configMaps := configMapFixture{"blueking/bk-monitor-unify-query-env": {ObjectMeta: metav1.ObjectMeta{Namespace: "blueking", Name: "bk-monitor-unify-query-env", UID: "cm-uid"},
		Data: map[string]string{"REDIS_PASSWORD": "plain-redis-credential", "MYSQL_URL": "mysql://root:mysql-credential@db:3306/x", "LOG_LEVEL": "info"}}}
	provider := mustProvider(t, Options{Client: client, ConfigMaps: configMaps, DefaultNamespace: "own", FingerprintKey: testFingerprintKey})
	receipt, err := provider.ReadConfigMap(context.Background(), ConfigMapRequest{Namespace: "blueking", Name: "bk-monitor-unify-query-env"})
	if err != nil || receipt.UID != "cm-uid" || len(receipt.Keys) != 3 {
		t.Fatalf("read: %+v err=%v", receipt, err)
	}
	encoded, _ := json.Marshal(receipt)
	for _, credential := range []string{"plain-redis-credential", "mysql-credential"} {
		if strings.Contains(string(encoded), credential) {
			t.Fatalf("ConfigMap credential returned: %s", encoded)
		}
	}
	if !strings.Contains(string(encoded), `"value":"info"`) {
		t.Fatalf("a plain value was changed: %s", encoded)
	}
	for _, key := range receipt.Keys {
		if key.Key == "REDIS_PASSWORD" && (key.Value != "<redacted>" || key.Fingerprint != fingerprintOf(testFingerprintKey, "plain-redis-credential")) {
			t.Fatalf("a credential-named key = %+v, want masked with the fingerprint a Secret holding it would show", key)
		}
		if key.Key == "LOG_LEVEL" && key.Fingerprint != "" {
			t.Fatalf("a plain value carries a fingerprint: %+v", key)
		}
	}
	_, err = provider.ReadConfigMap(context.Background(), ConfigMapRequest{Name: "missing"})
	requireCode(t, err, CodeNotFound)
}

// A Secret read also says what owns it and who made it, so a reader can tell
// whether removing a release would delete it: labels, annotations with the
// last-applied copy dropped and a credential-named one masked, owner
// references and creation time, beside the keys and their sizes.
func TestASecretReadCarriesItsMetadata(t *testing.T) {
	client, _ := fixture()
	created := metav1.NewTime(time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC))
	secrets := &secretFixture{secrets: map[string]*corev1.Secret{
		"ns1/dep-secret": {ObjectMeta: metav1.ObjectMeta{Namespace: "ns1", Name: "dep-secret", UID: "s-uid", CreationTimestamp: created,
			Labels: map[string]string{"app.kubernetes.io/managed-by": "Helm"},
			Annotations: map[string]string{"meta.helm.sh/release-name": "release-a", "meta.helm.sh/release-namespace": "ns1",
				lastAppliedAnnotation: `{"data":{"password":"sentinel-applied"}}`, "example.com/api-token": "sentinel-annotation"},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "Deployment", Name: "d1", UID: "d-uid"}}},
			Type: corev1.SecretTypeOpaque, Data: map[string][]byte{"password": []byte("sentinel-value")}},
	}}
	provider := mustProvider(t, Options{Client: client, Secrets: secrets, DefaultNamespace: "own"})
	receipt, err := provider.ReadSecret(context.Background(), SecretRequest{Namespace: "ns1", Name: "dep-secret"})
	if err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(receipt)
	for _, value := range []string{"sentinel", "last-applied-configuration"} {
		if strings.Contains(string(encoded), value) {
			t.Fatalf("receipt carries %q: %s", value, encoded)
		}
	}
	if receipt.Labels["app.kubernetes.io/managed-by"] != "Helm" || receipt.Annotations["meta.helm.sh/release-name"] != "release-a" ||
		receipt.Annotations["example.com/api-token"] != "<redacted>" || !receipt.CreatedAt.Equal(created.Time) ||
		len(receipt.OwnerReferences) != 1 || receipt.OwnerReferences[0].Name != "d1" || receipt.OwnerReferences[0].Kind != "Deployment" {
		t.Fatalf("metadata = labels %v annotations %v owners %+v created %v", receipt.Labels, receipt.Annotations, receipt.OwnerReferences, receipt.CreatedAt)
	}
}

// The deployment's key is asked for at the first fingerprint, once, and not
// when the provider is built; a source that has none leaves a process key.
func TestTheDeploymentKeyIsAskedForOnceAtFirstUse(t *testing.T) {
	client, _ := fixture()
	secrets := &secretFixture{secrets: map[string]*corev1.Secret{
		"ns1/one": {ObjectMeta: metav1.ObjectMeta{Namespace: "ns1", Name: "one"}, Data: map[string][]byte{"password": []byte("value")}}}}
	asked := 0
	provider := mustProvider(t, Options{Client: client, Secrets: secrets, FingerprintKeySource: func() []byte { asked++; return testFingerprintKey }})
	if asked != 0 {
		t.Fatalf("the key was asked for while building: %d", asked)
	}
	for range 2 {
		receipt, err := provider.ReadSecret(context.Background(), SecretRequest{Namespace: "ns1", Name: "one"})
		if err != nil || receipt.FingerprintScope != "deployment" || receipt.Keys[0].Fingerprint != fingerprintOf(testFingerprintKey, "value") {
			t.Fatalf("receipt %+v err=%v", receipt, err)
		}
	}
	if asked != 1 {
		t.Fatalf("the key was asked for %d times, want once", asked)
	}
	none := mustProvider(t, Options{Client: client, Secrets: secrets, FingerprintKeySource: func() []byte { return nil }})
	if receipt, _ := none.ReadSecret(context.Background(), SecretRequest{Namespace: "ns1", Name: "one"}); receipt.FingerprintScope != "process" {
		t.Fatalf("no deployment key: %+v", receipt)
	}
}
