// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package pod

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
)

// The cluster writes: k8s.apply applies objects server-side under the
// channel's own field manager, k8s.delete deletes one object by the uid a
// read returned. Both go through the same kind list as k8s.dry_run, refuse
// RBAC objects, and answer with what changed -- a Secret's values as
// fingerprints only.
const (
	OpApply  = "k8s.apply"
	OpDelete = "k8s.delete"
)

// The bounds of one apply.
const (
	MaxApplyObjects = 50
	// maxApplyChanges bounds one object's diff as returned.
	maxApplyChanges = 50
	// maxChangeValueBytes bounds one value in a diff.
	maxChangeValueBytes = 256
	// applyFieldManager is the field manager every apply writes under.
	applyFieldManager = "alarmd-sre"
)

// The outcomes, closed.
const (
	WriteCreated         = "created"
	WriteUpdated         = "updated"
	WriteUnchanged       = "unchanged"
	WriteConflict        = "conflict"
	WriteDenied          = "denied"
	WriteNotChecked      = "not_checked"
	WriteNotGrantedKind  = "not_granted_kind"
	WriteRBACNotWritable = "rbac_not_writable"
	WriteDeleted         = "deleted"
	WriteGone            = "gone"
	WritePrecondition    = "precondition_failed"
	WriteProtected       = "protected_namespace"
)

// WriteOutcomes is every outcome an apply or a delete can answer.
var WriteOutcomes = []string{WriteCreated, WriteUpdated, WriteUnchanged, WriteConflict, WriteDenied, WriteNotChecked,
	WriteNotGrantedKind, WriteRBACNotWritable, WriteDeleted, WriteGone, WritePrecondition, WriteProtected}

// protectedNamespaces cannot be deleted through the channel: losing one is
// unrecoverable for the cluster. The process's own namespace is added at the
// call (DefaultNamespace).
var protectedNamespaces = map[string]bool{"kube-system": true, "kube-public": true, "kube-node-lease": true, "default": true}

// Writer is the Kubernetes access the writes have; its options are fixed in
// the adapter.
type Writer interface {
	Apply(ctx context.Context, resource schema.GroupVersionResource, namespace, name string, data []byte, force bool) (*unstructured.Unstructured, error)
	Delete(ctx context.Context, resource schema.GroupVersionResource, namespace, name string, uid types.UID, resourceVersion string) error
}

// ApplyRequest is a manifest stream to apply, the namespace a namespaced
// object without one goes to, and whether to take ownership of fields
// another manager holds.
type ApplyRequest struct {
	Manifests string `json:"manifests"`
	Namespace string `json:"namespace,omitempty"`
	Force     bool   `json:"force,omitempty"`
}

// FieldChange is one changed field: its path and its value before and
// after, masked as k8s.list masks them; a Secret's values are
// fingerprints.
type FieldChange struct {
	Path   string `json:"path"`
	Before string `json:"before,omitempty"`
	After  string `json:"after,omitempty"`
}

// WriteObject is one object's answer.
type WriteObject struct {
	APIVersion string        `json:"api_version"`
	Kind       string        `json:"kind"`
	Namespace  string        `json:"namespace,omitempty"`
	Name       string        `json:"name"`
	UID        string        `json:"uid,omitempty"`
	Outcome    string        `json:"outcome"`
	Reason     string        `json:"reason,omitempty"`
	Code       int32         `json:"code,omitempty"`
	Message    string        `json:"message,omitempty"`
	Causes     []DryRunCause `json:"causes,omitempty"`
	Changes    []FieldChange `json:"changes,omitempty"`
	// ChangesOmitted is how many changed fields past maxApplyChanges are not
	// listed.
	ChangesOmitted int `json:"changes_omitted,omitempty"`
	// HelmRelease names the helm release that owns the object
	// (namespace/name), when it has one: the next helm upgrade reverts or
	// conflicts with this write.
	HelmRelease string `json:"helm_release,omitempty"`
}

// WriteReceipt is an apply's or a delete's whole answer.
type WriteReceipt struct {
	Objects []WriteObject  `json:"objects"`
	Counts  map[string]int `json:"counts"`
	Force   bool           `json:"force,omitempty"`
}

// gate is the check every write and dry run makes before any call: the
// object names itself, its kind is not RBAC and is granted, and it has a
// namespace when it needs one. It answers the resource, or why not.
func (p *Provider) gate(object *unstructured.Unstructured, defaultNamespace string, rbacOutcome, grantOutcome string) (schema.GroupVersionResource, string, string) {
	apiVersion, kind := object.GetAPIVersion(), object.GetKind()
	if apiVersion == "" || kind == "" {
		return schema.GroupVersionResource{}, WriteNotChecked, "the object names no apiVersion or kind"
	}
	if object.GetName() == "" {
		return schema.GroupVersionResource{}, WriteNotChecked, "the object has no metadata.name (generateName is not supported here)"
	}
	version, err := schema.ParseGroupVersion(apiVersion)
	if err != nil {
		return schema.GroupVersionResource{}, WriteNotChecked, "apiVersion does not parse: " + err.Error()
	}
	if version.Group == rbacGroup {
		return schema.GroupVersionResource{}, rbacOutcome, "RBAC objects are not sent: the apiserver's escalation check would need bind and escalate, which this channel does not hold"
	}
	if !dryRunGranted[schema.GroupKind{Group: version.Group, Kind: kind}] {
		return schema.GroupVersionResource{}, grantOutcome, "the channel is granted writes only on the kinds the chart grants; extend the chart's grant and this list together"
	}
	resource, namespaced, err := p.options.DryRun.Mapping(apiVersion, kind)
	if err != nil {
		return schema.GroupVersionResource{}, WriteNotChecked, "this cluster serves no " + apiVersion + " " + kind + ": " + err.Error()
	}
	if namespaced && object.GetNamespace() == "" {
		if defaultNamespace == "" {
			return schema.GroupVersionResource{}, WriteNotChecked, "a namespaced object without a namespace, and the request names none"
		}
		object.SetNamespace(defaultNamespace)
	}
	return resource, "", ""
}

// Apply applies the manifests server-side, object by object.
func (p *Provider) Apply(ctx context.Context, request ApplyRequest) (WriteReceipt, error) {
	receipt := WriteReceipt{Objects: []WriteObject{}, Counts: map[string]int{}, Force: request.Force}
	if p.options.DryRun == nil || p.options.Writer == nil {
		return receipt, &Error{Code: CodeAPIError, Message: "Kubernetes write adapter is unavailable"}
	}
	if len(request.Manifests) > MaxDryRunBytes {
		return receipt, &Error{Code: "apply_too_large", Message: fmt.Sprintf("manifests exceed %d bytes", MaxDryRunBytes)}
	}
	if request.Namespace != "" && len(validation.IsDNS1123Label(request.Namespace)) != 0 {
		return receipt, invalid("namespace must be one Kubernetes namespace name")
	}
	objects, err := decodeManifests(request.Manifests)
	if err != nil {
		return receipt, invalid("manifests do not decode: " + err.Error())
	}
	if len(objects) == 0 {
		return receipt, invalid("manifests hold no object")
	}
	if len(objects) > MaxApplyObjects {
		return receipt, &Error{Code: "apply_too_many_objects", Message: fmt.Sprintf("%d objects, at most %d in one apply", len(objects), MaxApplyObjects)}
	}
	release, err := p.acquire(ctx)
	if err != nil {
		return receipt, err
	}
	defer release()
	for _, object := range objects {
		answer := p.applyOne(ctx, object, request)
		receipt.Objects = append(receipt.Objects, answer)
		receipt.Counts[answer.Outcome]++
	}
	return receipt, nil
}

func (p *Provider) applyOne(ctx context.Context, object *unstructured.Unstructured, request ApplyRequest) WriteObject {
	resource, outcome, message := p.gate(object, request.Namespace, WriteRBACNotWritable, WriteNotGrantedKind)
	answer := WriteObject{APIVersion: object.GetAPIVersion(), Kind: object.GetKind(), Namespace: object.GetNamespace(), Name: object.GetName()}
	if outcome != "" {
		answer.Outcome, answer.Message = outcome, message
		return answer
	}
	data, err := json.Marshal(object.Object)
	if err != nil {
		answer.Outcome, answer.Message = WriteNotChecked, "the object does not encode"
		return answer
	}
	ctx, cancel := p.readContext(ctx)
	defer cancel()
	live, err := p.options.DryRun.GetResource(ctx, resource, answer.Namespace, answer.Name)
	existed := err == nil
	if err != nil && !apierrors.IsNotFound(err) {
		return p.writeRefused(answer, err, object)
	}
	if existed {
		answer.HelmRelease = helmRelease(live)
	}
	applied, err := p.options.Writer.Apply(ctx, resource, answer.Namespace, answer.Name, data, request.Force)
	if err != nil {
		return p.writeRefused(answer, err, object)
	}
	answer.UID = string(applied.GetUID())
	if answer.HelmRelease == "" {
		answer.HelmRelease = helmRelease(applied)
	}
	fingerprints := p.newFingerprints()
	var before map[string]any
	if existed {
		before = p.shown(live, fingerprints)
	}
	answer.Changes, answer.ChangesOmitted = changes(before, p.shown(applied, fingerprints))
	// Unchanged is judged on the object as shown, the same comparison the
	// changes listed come from: what a reader sees changed is what changed.
	switch {
	case !existed:
		answer.Outcome = WriteCreated
	case len(answer.Changes) == 0:
		answer.Outcome = WriteUnchanged
	default:
		answer.Outcome = WriteUpdated
	}
	return answer
}

// ObjectDeleteRequest names one object by kind and the uid a read returned; a
// resource version, given, must still be the object's.
type ObjectDeleteRequest struct {
	APIVersion      string `json:"api_version"`
	Kind            string `json:"kind"`
	Namespace       string `json:"namespace,omitempty"`
	Name            string `json:"name"`
	UID             string `json:"uid"`
	ResourceVersion string `json:"resource_version,omitempty"`
}

// DeleteObject deletes one object, if it is still the one the uid names; its
// dependents go in the background.
func (p *Provider) DeleteObject(ctx context.Context, request ObjectDeleteRequest) (WriteReceipt, error) {
	receipt := WriteReceipt{Objects: []WriteObject{}, Counts: map[string]int{}}
	if p.options.DryRun == nil || p.options.Writer == nil {
		return receipt, &Error{Code: CodeAPIError, Message: "Kubernetes write adapter is unavailable"}
	}
	if request.UID == "" {
		return receipt, invalid("uid is required: delete the object a read returned, by its uid")
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
	if request.Kind == "Namespace" && request.APIVersion == "v1" &&
		(protectedNamespaces[request.Name] || request.Name == p.options.DefaultNamespace) {
		answer.Outcome, answer.Message = WriteProtected, "namespace "+request.Name+" is not deleted through the channel: the cluster or this process depends on it"
		return record(answer)
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
	if live, err := p.options.DryRun.GetResource(ctx, resource, request.Namespace, request.Name); err == nil {
		answer.HelmRelease = helmRelease(live)
	}
	err = p.options.Writer.Delete(ctx, resource, request.Namespace, request.Name, types.UID(request.UID), request.ResourceVersion)
	switch {
	case err == nil:
		answer.Outcome = WriteDeleted
	case apierrors.IsNotFound(err):
		answer.Outcome = WriteGone
	case apierrors.IsConflict(err):
		answer.Outcome, answer.Message = WritePrecondition, bound(err.Error())
	default:
		answer = p.writeRefused(answer, err, object)
	}
	return record(answer)
}

// writeRefused files an apiserver error on a write: a conflict as its own
// outcome (an apply with force takes ownership), the status verbatim but for
// the object's Secret values, an error without a status as not checked.
func (p *Provider) writeRefused(answer WriteObject, err error, object *unstructured.Unstructured) WriteObject {
	refused := p.refused(DryRunObject{}, err, object)
	answer.Reason, answer.Code, answer.Message, answer.Causes = refused.Reason, refused.Code, refused.Message, refused.Causes
	switch refused.Verdict {
	case DryRunManagerConflict:
		answer.Outcome = WriteConflict
		answer.Message += " (force=true takes ownership of these fields)"
	case DryRunDenied:
		answer.Outcome = WriteDenied
	default:
		answer.Outcome = WriteNotChecked
	}
	return answer
}

// helmRelease is the release that owns an object, as helm annotates it.
func helmRelease(object *unstructured.Unstructured) string {
	annotations := object.GetAnnotations()
	name := annotations["meta.helm.sh/release-name"]
	if name == "" {
		return ""
	}
	if namespace := annotations["meta.helm.sh/release-namespace"]; namespace != "" {
		return namespace + "/" + name
	}
	return name
}

// shown is an object as a write's diff compares it: as k8s.list shows it
// (a Secret as its keys, sizes and fingerprints; credential-named values
// masked), without the fields every write moves.
func (p *Provider) shown(object *unstructured.Unstructured, fingerprints *fingerprints) map[string]any {
	raw := object.DeepCopy().Object
	if object.GetKind() == "Secret" {
		raw = p.keysOnlyRow(raw, true, fingerprints)
	}
	cleaned := p.cleanObject(raw, fingerprints)
	for _, field := range []string{"managedFields", "resourceVersion", "generation", "uid", "creationTimestamp"} {
		unstructured.RemoveNestedField(cleaned, "metadata", field)
	}
	delete(cleaned, "status")
	return cleaned
}

// changes lists the leaves that differ between before and after, by path,
// sorted, at most maxApplyChanges, and how many more.
func changes(before, after map[string]any) ([]FieldChange, int) {
	left, right := map[string]string{}, map[string]string{}
	flatten("", before, left)
	flatten("", after, right)
	paths := map[string]bool{}
	for path := range left {
		paths[path] = true
	}
	for path := range right {
		paths[path] = true
	}
	var listed []FieldChange
	for path := range paths {
		if left[path] != right[path] {
			listed = append(listed, FieldChange{Path: path, Before: left[path], After: right[path]})
		}
	}
	sort.Slice(listed, func(i, j int) bool { return listed[i].Path < listed[j].Path })
	if len(listed) > maxApplyChanges {
		return listed[:maxApplyChanges], len(listed) - maxApplyChanges
	}
	return listed, 0
}

func flatten(prefix string, value any, into map[string]string) {
	switch v := value.(type) {
	case map[string]any:
		for key, item := range v {
			flatten(join(prefix, key), item, into)
		}
	case []any:
		for index, item := range v {
			flatten(fmt.Sprintf("%s[%d]", prefix, index), item, into)
		}
	case nil:
		if prefix != "" {
			into[prefix] = "null"
		}
	default:
		text := fmt.Sprint(v)
		if encoded, err := json.Marshal(v); err == nil && !reflect.DeepEqual(v, text) {
			text = string(encoded)
		}
		if len(text) > maxChangeValueBytes {
			text = strings.ToValidUTF8(text[:maxChangeValueBytes], "") + "..."
		}
		into[prefix] = text
	}
}

func join(prefix, key string) string {
	if prefix == "" {
		return key
	}
	return prefix + "." + key
}

// writePatchOptions is every apply's options: the channel's own field
// manager, ownership forced only when asked, and no dry run.
func writePatchOptions(force bool) metav1.PatchOptions {
	return metav1.PatchOptions{FieldManager: applyFieldManager, Force: &force}
}

// deleteOptions is every delete's options: only the object the uid (and the
// resource version, when given) names, its dependents in the background.
func deleteOptions(uid types.UID, resourceVersion string) metav1.DeleteOptions {
	background := metav1.DeletePropagationBackground
	preconditions := &metav1.Preconditions{UID: &uid}
	if resourceVersion != "" {
		preconditions.ResourceVersion = &resourceVersion
	}
	return metav1.DeleteOptions{Preconditions: preconditions, PropagationPolicy: &background}
}
