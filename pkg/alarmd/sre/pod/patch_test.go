// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package pod

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	jsonpatch "gopkg.in/evanphx/json-patch.v4"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
)

// patchingWriter applies a JSON patch to the live object as the apiserver
// does: all of it or, when any op fails, none of it, answering 422.
type patchingWriter struct {
	fakeWriter
	patches []string
}

func (w *patchingWriter) JSONPatch(_ context.Context, resource schema.GroupVersionResource, namespace, name string, patch []byte) (*unstructured.Unstructured, error) {
	w.patches = append(w.patches, string(patch))
	live := w.runner.live[namespace+"/"+name]
	original, _ := json.Marshal(live.Object)
	decoded, err := jsonpatch.DecodePatch(patch)
	if err != nil {
		return nil, apierrors.NewBadRequest(err.Error())
	}
	modified, err := decoded.Apply(original)
	if err != nil {
		return nil, &apierrors.StatusError{ErrStatus: metav1.Status{Status: metav1.StatusFailure, Code: http.StatusUnprocessableEntity,
			Reason: metav1.StatusReasonInvalid, Message: "the server rejected our request due to an error in our request: " + err.Error()}}
	}
	out := &unstructured.Unstructured{}
	if err := out.UnmarshalJSON(modified); err != nil {
		return nil, err
	}
	w.runner.live[namespace+"/"+name] = out
	return out.DeepCopy(), nil
}

func patchedDeployment() *unstructured.Unstructured {
	object := deploymentWithEnv()
	object.SetUID(types.UID("uid-web"))
	return object
}

func deploymentWithEnv() *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": "web", "namespace": "ns1", "annotations": map[string]any{"example.test/hotfix-id": "h1"}},
		"spec": map[string]any{"template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"name": "web", "image": "web:1",
			"env": []any{map[string]any{"name": "KEEP", "value": "k"}, map[string]any{"name": "DROP", "value": "d"}}}}}}}}}
}

func patchProvider(t *testing.T) (*Provider, *fakeDryRunner, *patchingWriter) {
	t.Helper()
	runner := &fakeDryRunner{live: map[string]*unstructured.Unstructured{"ns1/web": patchedDeployment()}, mappings: map[string]schema.GroupVersionResource{
		"apps/v1/Deployment":                {Group: "apps", Version: "v1", Resource: "deployments"},
		"rbac.authorization.k8s.io/v1/Role": {Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"},
	}}
	writer := &patchingWriter{}
	provider := writeProvider(t, runner, &writer.fakeWriter)
	writer.runner = runner
	provider.options.Writer = writer
	return provider, runner, writer
}

func patchRequest(uid string, ops ...PatchOp) ObjectPatchRequest {
	return ObjectPatchRequest{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "ns1", Name: "web", UID: uid, Patch: ops}
}

func raw(value string) json.RawMessage { return json.RawMessage(value) }

// A patch goes to the object the uid names, with a test of that uid first:
// the apiserver applies all of it or none. A removal guarded by a test of the
// entry's whole value changes exactly that entry, and the receipt lists it.
func TestAPatchIsGuardedByTheUIDAndChangesWhatItSays(t *testing.T) {
	provider, runner, writer := patchProvider(t)
	receipt, err := provider.PatchObject(context.Background(), patchRequest("uid-web",
		PatchOp{Op: "test", Path: "/spec/template/spec/containers/0/env/1", Value: raw(`{"name":"DROP","value":"d"}`)},
		PatchOp{Op: "remove", Path: "/spec/template/spec/containers/0/env/1"},
		PatchOp{Op: "test", Path: "/metadata/annotations/example.test~1hotfix-id", Value: raw(`"h1"`)},
		PatchOp{Op: "remove", Path: "/metadata/annotations/example.test~1hotfix-id"}))
	if err != nil || len(receipt.Objects) != 1 || receipt.Objects[0].Outcome != WriteUpdated {
		t.Fatalf("receipt %+v err %v", receipt, err)
	}
	if len(writer.patches) != 1 || !strings.HasPrefix(writer.patches[0], `[{"op":"test","path":"/metadata/uid","value":"uid-web"}`) {
		t.Fatalf("patch sent %v, want the uid test first", writer.patches)
	}
	env := runner.live["ns1/web"].Object["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)["containers"].([]any)[0].(map[string]any)["env"].([]any)
	if len(env) != 1 || env[0].(map[string]any)["name"] != "KEEP" || len(runner.live["ns1/web"].GetAnnotations()) != 0 {
		t.Fatalf("object after %+v", runner.live["ns1/web"].Object)
	}
	if len(receipt.Objects[0].Changes) == 0 || !receipt.Objects[0].WillRoll {
		t.Fatalf("changes or will_roll missing: %+v", receipt.Objects[0])
	}
	// A change outside the Pod template does not roll.
	receipt, err = provider.PatchObject(context.Background(), patchRequest("uid-web",
		PatchOp{Op: "add", Path: "/metadata/labels", Value: raw(`{"team":"sre"}`)}))
	if err != nil || receipt.Objects[0].Outcome != WriteUpdated || receipt.Objects[0].WillRoll {
		t.Fatalf("metadata-only patch %+v err %v", receipt.Objects[0], err)
	}
}

// A uid other than the live object's sends nothing; a failed test changes
// nothing and says so.
func TestAPatchOnAnotherObjectOrAFailedTestChangesNothing(t *testing.T) {
	provider, runner, writer := patchProvider(t)
	receipt, err := provider.PatchObject(context.Background(), patchRequest("uid-old", PatchOp{Op: "remove", Path: "/metadata/annotations"}))
	if err != nil || receipt.Objects[0].Outcome != WritePrecondition || len(writer.patches) != 0 {
		t.Fatalf("receipt %+v err %v patches %v", receipt, err, writer.patches)
	}
	before, _ := json.Marshal(runner.live["ns1/web"].Object)
	receipt, err = provider.PatchObject(context.Background(), patchRequest("uid-web",
		PatchOp{Op: "test", Path: "/spec/template/spec/containers/0/env/1", Value: raw(`{"name":"DROP","value":"changed"}`)},
		PatchOp{Op: "remove", Path: "/spec/template/spec/containers/0/env/1"}))
	after, _ := json.Marshal(runner.live["ns1/web"].Object)
	if err != nil || receipt.Objects[0].Outcome != WriteTestFailed || string(before) != string(after) {
		t.Fatalf("receipt %+v err %v changed %v", receipt, err, string(before) != string(after))
	}
}

// Only test, remove, replace and add; a value where the op needs one; a
// path; and kinds the channel does not write are refused, before any call.
func TestAPatchOutsideTheContractIsRefusedBeforeAnyCall(t *testing.T) {
	provider, _, writer := patchProvider(t)
	for _, ops := range [][]PatchOp{
		nil,
		{{Op: "move", Path: "/a", Value: raw(`"/b"`)}},
		{{Op: "copy", Path: "/a", Value: raw(`"/b"`)}},
		{{Op: "test", Path: "/metadata/name"}},
		{{Op: "remove", Path: "metadata/name"}},
	} {
		if _, err := provider.PatchObject(context.Background(), patchRequest("uid-web", ops...)); err == nil {
			t.Fatalf("ops %+v accepted", ops)
		}
	}
	if _, err := provider.PatchObject(context.Background(), ObjectPatchRequest{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "ns1", Name: "web",
		Patch: []PatchOp{{Op: "remove", Path: "/metadata/annotations"}}}); err == nil {
		t.Fatal("a patch without a uid accepted")
	}
	rbac := patchRequest("uid-1", PatchOp{Op: "remove", Path: "/rules"})
	rbac.APIVersion, rbac.Kind = "rbac.authorization.k8s.io/v1", "Role"
	receipt, err := provider.PatchObject(context.Background(), rbac)
	if err != nil || len(receipt.Objects) != 1 || receipt.Objects[0].Outcome == WriteUpdated {
		t.Fatalf("RBAC patch receipt %+v err %v", receipt, err)
	}
	if len(writer.patches) != 0 {
		t.Fatalf("patches sent %v", writer.patches)
	}
}

// Through the production client a patch is a JSON patch under the
// channel's own field manager, never a dry run.
func TestThePatchIsAJSONPatchAsTheChannelsManager(t *testing.T) {
	var seen *http.Request
	var body string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, _ := io.ReadAll(r.Body)
		seen, body = r.Clone(r.Context()), string(data)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"web","namespace":"ns1"}}`))
	}))
	defer server.Close()
	client, err := NewKubernetesClient(&rest.Config{Host: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	patch := `[{"op":"remove","path":"/metadata/annotations"}]`
	if _, err := client.JSONPatch(context.Background(), schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}, "ns1", "web", []byte(patch)); err != nil {
		t.Fatal(err)
	}
	query := seen.URL.Query()
	if seen.Method != http.MethodPatch || seen.Header.Get("Content-Type") != "application/json-patch+json" || seen.URL.Path != "/apis/apps/v1/namespaces/ns1/deployments/web" ||
		query.Get("fieldManager") != "alarmd-sre" || query.Has("dryRun") || body != patch {
		t.Fatalf("%s %s %s %v body %s", seen.Method, seen.URL.Path, seen.Header.Get("Content-Type"), query, body)
	}
}
