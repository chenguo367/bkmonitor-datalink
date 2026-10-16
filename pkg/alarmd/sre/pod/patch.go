// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package pod

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"strings"

	jsonpatch "gopkg.in/evanphx/json-patch.v4"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// OpPatch is a guarded JSON patch of one object.
const OpPatch = "k8s.patch"

// WriteTestFailed is a patch whose test did not hold: the apiserver applied
// none of it.
const WriteTestFailed = "test_failed"

// PatchOp is one RFC 6902 operation. Value is the op's value as JSON; test,
// add and replace need one, remove takes none.
type PatchOp struct {
	Op    string          `json:"op"`
	Path  string          `json:"path"`
	Value json.RawMessage `json:"value,omitempty"`
}

// ObjectPatchRequest names one object by kind and the uid a read returned,
// and the operations to apply to it.
type ObjectPatchRequest struct {
	APIVersion string    `json:"api_version"`
	Kind       string    `json:"kind"`
	Namespace  string    `json:"namespace,omitempty"`
	Name       string    `json:"name"`
	UID        string    `json:"uid"`
	Patch      []PatchOp `json:"patch"`
}

var patchOps = map[string]bool{"test": true, "remove": true, "replace": true, "add": true}

func validatePatch(request ObjectPatchRequest) error {
	if request.UID == "" {
		return invalid("uid is required: patch the object a read returned, by its uid")
	}
	if len(request.Patch) == 0 {
		return invalid("patch holds no operation")
	}
	for _, op := range request.Patch {
		if !patchOps[op.Op] {
			return invalid("op must be test, remove, replace or add, not " + bound(op.Op))
		}
		if !strings.HasPrefix(op.Path, "/") {
			return invalid("path must be a JSON pointer starting with /")
		}
		if op.Op != "remove" && len(op.Value) == 0 {
			return invalid(op.Op + " needs a value")
		}
	}
	return nil
}

// PatchObject applies a JSON patch to one object, if it is still the one the
// uid names: a test of the uid goes first, and the apiserver applies the
// whole patch or, when any test fails, none of it. The receipt lists the
// fields changed, masked as k8s.list masks them, and whether the Pod
// template changed (will_roll).
func (p *Provider) PatchObject(ctx context.Context, request ObjectPatchRequest) (WriteReceipt, error) {
	receipt := WriteReceipt{Objects: []WriteObject{}, Counts: map[string]int{}}
	if p.options.DryRun == nil || p.options.Writer == nil {
		return receipt, &Error{Code: CodeAPIError, Message: "Kubernetes write adapter is unavailable"}
	}
	if err := validatePatch(request); err != nil {
		return receipt, err
	}
	object := &unstructured.Unstructured{Object: map[string]any{"apiVersion": request.APIVersion, "kind": request.Kind,
		"metadata": map[string]any{"name": request.Name}}}
	if request.Namespace != "" {
		object.SetNamespace(request.Namespace)
	}
	answer := WriteObject{APIVersion: request.APIVersion, Kind: request.Kind, Namespace: request.Namespace, Name: request.Name, UID: request.UID}
	record := func(answer WriteObject) (WriteReceipt, error) {
		receipt.Objects = append(receipt.Objects, answer)
		receipt.Counts[answer.Outcome]++
		return receipt, nil
	}
	resource, outcome, message := p.gate(object, "", WriteRBACNotWritable, WriteNotGrantedKind)
	if outcome != "" {
		answer.Outcome, answer.Message = outcome, message
		return record(answer)
	}
	release, err := p.acquire(ctx)
	if err != nil {
		return receipt, err
	}
	defer release()
	ctx, cancel := p.readContext(ctx)
	defer cancel()
	live, err := p.options.DryRun.GetResource(ctx, resource, request.Namespace, request.Name)
	switch {
	case apierrors.IsNotFound(err):
		answer.Outcome = WriteGone
		return record(answer)
	case err != nil:
		return record(p.writeRefused(answer, err, object))
	case string(live.GetUID()) != request.UID:
		answer.Outcome, answer.Message = WritePrecondition, "the object is not the one the uid names: it was replaced"
		return record(answer)
	}
	answer.HelmRelease = helmRelease(live)
	uid, _ := json.Marshal(request.UID)
	patch, err := json.Marshal(append([]PatchOp{{Op: "test", Path: "/metadata/uid", Value: uid}}, request.Patch...))
	if err != nil {
		answer.Outcome, answer.Message = WriteNotChecked, "the patch does not encode"
		return record(answer)
	}
	patched, err := p.options.Writer.JSONPatch(ctx, resource, request.Namespace, request.Name, patch)
	if err != nil {
		refused := p.writeRefused(answer, err, object)
		if path, failed := failedTest(err, live, request.Patch); failed {
			refused.Outcome = WriteTestFailed
			refused.Message = "test at " + path + " does not hold, evaluated against the object as read at resourceVersion " +
				live.GetResourceVersion() + "; the apiserver applied nothing (" + refused.Message + ")"
		}
		return record(refused)
	}
	fingerprints := p.newFingerprints()
	before, after := p.shown(live, fingerprints), p.shown(patched, fingerprints)
	answer.Changes, answer.ChangesOmitted = changes(before, after)
	answer.WillRoll = !reflect.DeepEqual(templateOf(before), templateOf(after))
	answer.Outcome = WriteUpdated
	if len(answer.Changes) == 0 {
		answer.Outcome = WriteUnchanged
	}
	return record(answer)
}

// failedTest says the apiserver refused a patch because one of its tests did
// not hold, and which. The apiserver answers a failed test with a bare 422
// (no causes, no words of the patch library), the same as a validation or
// admission refusal; so each test op is replayed against the raw object as
// read (never the masked copy shown in receipts). One that does not hold
// makes it a failed test; when every test holds the 422 is a denial. The
// replay sees the object as read, not what the apiserver saw a moment later.
func failedTest(err error, live *unstructured.Unstructured, ops []PatchOp) (string, bool) {
	var status apierrors.APIStatus
	if !errors.As(err, &status) || status.Status().Code != http.StatusUnprocessableEntity {
		return "", false
	}
	original, encodeErr := json.Marshal(live.Object)
	if encodeErr != nil {
		return "", false
	}
	for _, op := range ops {
		if op.Op != "test" {
			continue
		}
		single, _ := json.Marshal([]PatchOp{op})
		decoded, decodeErr := jsonpatch.DecodePatch(single)
		if decodeErr != nil {
			continue
		}
		if _, applyErr := decoded.Apply(original); applyErr != nil {
			return op.Path, true
		}
	}
	return "", false
}

// templateOf is an object's Pod template, nil for an object without one.
func templateOf(object map[string]any) any {
	spec, _ := object["spec"].(map[string]any)
	return spec["template"]
}
