// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package pod

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/validation"
)

type Provider struct {
	options  Options
	permits  chan struct{}
	redactor *redactor
}

// Scopes exposes a copy of safe deployment declarations, never credentials.
func (p *Provider) Scopes() []Scope { scopes, _ := normalizeScopes(p.options.Scopes); return scopes }
func (p *Provider) Limits() Limits {
	return Limits{Concurrency: p.options.Concurrency, DefaultTimeoutMS: p.options.DefaultTimeout.Milliseconds(), MaxTimeoutMS: p.options.MaxTimeout.Milliseconds(), MaxOutputBytes: p.options.MaxOutputBytes, MaxStdinBytes: p.options.MaxStdinBytes, MaxPods: p.options.MaxPods}
}

func New(options Options) (*Provider, error) {
	if options.Client == nil || len(options.Scopes) == 0 {
		return nil, invalid("a client and explicit scopes are required")
	}
	if options.Concurrency == 0 {
		options.Concurrency = 1
	}
	if options.DefaultTimeout == 0 {
		options.DefaultTimeout = 30 * time.Second
	}
	if options.MaxTimeout == 0 {
		options.MaxTimeout = 30 * time.Second
	}
	if options.MaxOutputBytes == 0 {
		options.MaxOutputBytes = 512 << 10
	}
	if options.MaxStdinBytes == 0 {
		options.MaxStdinBytes = 32 << 10
	}
	if options.MaxPods == 0 {
		options.MaxPods = 32
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.Concurrency < 1 || options.Concurrency > 64 || options.DefaultTimeout <= 0 || options.DefaultTimeout > options.MaxTimeout || options.MaxTimeout > 30*time.Second || options.MaxOutputBytes < 1 || options.MaxOutputBytes > 512<<10 || options.MaxStdinBytes < 1 || options.MaxStdinBytes > 32<<10 || options.MaxPods < 1 || options.MaxPods > 128 {
		return nil, invalid("provider budgets exceed supported bounds")
	}
	// Freeze caller-owned configuration, including maps, before concurrent reads.
	scopes, err := normalizeScopes(options.Scopes)
	if err != nil {
		return nil, err
	}
	options.Scopes = scopes
	options.LiteralSecrets = append([]string(nil), options.LiteralSecrets...)
	return &Provider{options: options, permits: make(chan struct{}, options.Concurrency), redactor: newRedactor(options.LiteralSecrets)}, nil
}

// ValidateScopes checks configuration without a Kubernetes client or any I/O.
// An empty slice disables this optional provider at the composition root.
func ValidateScopes(scopes []Scope) error { _, err := normalizeScopes(scopes); return err }

func normalizeScopes(input []Scope) ([]Scope, error) {
	scopes := make([]Scope, len(input))
	for i, scope := range input {
		if len(validation.IsDNS1123Label(scope.Namespace)) != 0 || len(scope.MatchLabels) == 0 {
			return nil, invalid("each scope needs a valid namespace and nonempty labels")
		}
		copyLabels := map[string]string{}
		for key, value := range scope.MatchLabels {
			copyLabels[key] = value
		}
		if len(validation.IsValidLabelValue(scope.Release)) != 0 || len(validation.IsValidLabelValue(scope.Module)) != 0 {
			return nil, invalid("invalid release/module index")
		}
		for key, value := range copyLabels {
			if len(validation.IsQualifiedName(key)) != 0 || len(validation.IsValidLabelValue(value)) != 0 {
				return nil, invalid("invalid scope label")
			}
		}
		if scope.Timeout.Contract == "" && scope.Timeout.PythonPath != "" || scope.Timeout.Contract != "" && (scope.Timeout.Contract != TimeoutContract || !filepath.IsAbs(scope.Timeout.PythonPath) || strings.ContainsRune(scope.Timeout.PythonPath, '\x00')) {
			return nil, invalid("unsupported remote timeout contract")
		}
		for _, workload := range scope.Workloads {
			if !rootKind(workload.Kind) || len(validation.IsDNS1123Subdomain(workload.Name)) != 0 {
				return nil, invalid("invalid workload allowlist")
			}
		}
		scope.MatchLabels = copyLabels
		moduleKey := "app.kubernetes.io/component"
		if _, exists := copyLabels["kingeye.app/module"]; exists {
			moduleKey = "kingeye.app/module"
		}
		for _, index := range []struct{ value, key string }{{scope.Release, "app.kubernetes.io/instance"}, {scope.Module, moduleKey}} {
			if actual, exists := copyLabels[index.key]; exists && index.value != "" && actual != index.value {
				return nil, invalid("release/module index conflicts with its declared label")
			}
		}
		scope.Release = scopeRelease(scope)
		scope.Module = scopeModule(scope)
		scope.Workloads = append([]Workload(nil), scope.Workloads...)
		scopes[i] = scope
	}
	return scopes, nil
}

func invalid(message string) error { return &Error{Code: CodeInvalidRequest, Message: message} }
func denied(message string) error  { return &Error{Code: CodeNotInScope, Message: message} }
func apiError(err error) error {
	if err == nil {
		return nil
	}
	var known *Error
	if errors.As(err, &known) {
		return known
	}
	code := CodeAPIError
	switch {
	case apierrors.IsForbidden(err), apierrors.IsUnauthorized(err):
		code = CodeForbidden
	case apierrors.IsNotFound(err):
		code = CodeNotFound
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		code = CodeCanceled
	}
	return &Error{Code: code, Message: "Kubernetes evidence request did not succeed"}
}

func (p *Provider) acquire(ctx context.Context) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, apiError(err)
	}
	select {
	case p.permits <- struct{}{}:
		return func() { <-p.permits }, nil
	default:
		return nil, &Error{Code: CodeBusy, Message: "diagnostic concurrency budget is occupied"}
	}
}
func (p *Provider) readContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, p.options.MaxTimeout)
}
func (p *Provider) namespace(value string) (string, error) {
	if value == "" {
		for _, scope := range p.options.Scopes {
			if value == "" {
				value = scope.Namespace
			} else if scope.Namespace != value {
				return "", invalid("namespace is required for multiple namespace scopes")
			}
		}
	}
	for _, scope := range p.options.Scopes {
		if scope.Namespace == value {
			return value, nil
		}
	}
	return "", denied("namespace is outside the deployment scope")
}

func (p *Provider) Discover(ctx context.Context, request DiscoverRequest) (DiscoverReceipt, error) {
	result := DiscoverReceipt{Targets: []Observation{}, ObservedAt: p.options.Now().UTC()}
	release, err := p.acquire(ctx)
	if err != nil {
		return result, err
	}
	defer release()
	ctx, cancel := p.readContext(ctx)
	defer cancel()
	namespace, err := p.namespace(request.Namespace)
	if err != nil {
		return result, err
	}
	limit := request.Limit
	if limit == 0 {
		limit = p.options.MaxPods
	}
	if limit < 1 || limit > p.options.MaxPods || len(request.Continue) > 4096 {
		return result, invalid("invalid discovery limit or continuation")
	}
	// One scope per page avoids combining Kubernetes continuation tokens. When
	// more than one selector is declared for a namespace, choose it explicitly
	// by the configured release/module. Identical selectors are equivalent.
	var selected *Scope
	for i := range p.options.Scopes {
		scope := &p.options.Scopes[i]
		if scope.Namespace != namespace || request.Release != "" && scopeRelease(*scope) != request.Release || request.Module != "" && scopeModule(*scope) != request.Module {
			continue
		}
		if selected != nil && labels.Set(selected.MatchLabels).String() != labels.Set(scope.MatchLabels).String() {
			return result, invalid("release/module is required to select one discovery scope")
		}
		selected = scope
	}
	if selected == nil {
		return result, denied("release/module is outside the deployment scope")
	}
	pods, err := p.options.Client.ListPods(ctx, namespace, metav1.ListOptions{LabelSelector: labels.Set(selected.MatchLabels).String(), Limit: int64(limit), Continue: request.Continue})
	if err != nil {
		return result, apiError(err)
	}
	result.Continue = pods.Continue
	if len(pods.Items) > limit {
		return result, &Error{Code: CodeAPIError, Message: "API ignored the discovery limit"}
	}
	for i := range pods.Items {
		obj := &pods.Items[i]
		observations, _, err := p.observe(ctx, obj, "")
		if err != nil {
			var failure *Error
			if errors.As(err, &failure) && failure.Code == CodeNotInScope {
				result.Rejected++
				continue
			}
			return result, err
		}
		for _, observation := range observations {
			if request.Workload == "" || request.Workload == observation.Workload.Name {
				result.Targets = append(result.Targets, observation)
			}
		}
	}
	sort.Slice(result.Targets, func(i, j int) bool {
		a, b := result.Targets[i].Target, result.Targets[j].Target
		return a.Pod < b.Pod || a.Pod == b.Pod && a.Container < b.Container
	})
	return result, nil
}

func (p *Provider) Inspect(ctx context.Context, request TargetRequest) (Observation, error) {
	release, err := p.acquire(ctx)
	if err != nil {
		return Observation{}, err
	}
	defer release()
	ctx, cancel := p.readContext(ctx)
	defer cancel()
	observed, _, err := p.resolve(ctx, request.Target, false)
	return observed, err
}

func validateTarget(target Target, strict bool) error {
	if len(validation.IsDNS1123Label(target.Namespace)) != 0 || len(validation.IsDNS1123Subdomain(target.Pod)) != 0 || len(validation.IsDNS1123Label(target.Container)) != 0 {
		return invalid("namespace, pod and container are required Kubernetes names")
	}
	if strict && (target.UID == "" || target.ContainerID == "" || target.ImageID == "") {
		return invalid("pod UID, container ID and image ID from discovery are required")
	}
	return nil
}
func (p *Provider) resolve(ctx context.Context, target Target, strict bool) (Observation, Scope, error) {
	if err := validateTarget(target, strict); err != nil {
		return Observation{}, Scope{}, err
	}
	if _, err := p.namespace(target.Namespace); err != nil {
		return Observation{}, Scope{}, err
	}
	obj, err := p.options.Client.GetPod(ctx, target.Namespace, target.Pod)
	if err != nil {
		return Observation{}, Scope{}, apiError(err)
	}
	observations, scope, err := p.observe(ctx, obj, target.Container)
	if err != nil {
		return Observation{}, Scope{}, err
	}
	observed := observations[0]
	if target.UID != "" && target.UID != observed.Target.UID || target.ContainerID != "" && target.ContainerID != observed.Target.ContainerID || target.ImageID != "" && target.ImageID != observed.Target.ImageID {
		return observed, scope, &Error{Code: CodeTargetChanged, Message: "pod or container identity differs from discovery"}
	}
	return observed, scope, nil
}
func (p *Provider) observe(ctx context.Context, obj *corev1.Pod, container string) ([]Observation, Scope, error) {
	var matching []Scope
	for _, scope := range p.options.Scopes {
		if scope.Namespace == obj.Namespace && labels.SelectorFromSet(scope.MatchLabels).Matches(labels.Set(obj.Labels)) {
			matching = append(matching, scope)
		}
	}
	if len(matching) == 0 {
		return nil, Scope{}, denied("pod labels are outside the deployment scope")
	}
	chain, ownerLabels, err := p.ownerChain(ctx, obj)
	if err != nil {
		return nil, Scope{}, err
	}
	root := chain[len(chain)-1]
	var scope Scope
	found := false
	for _, candidate := range matching {
		owned := true
		for _, ownerLabel := range ownerLabels {
			if !labels.SelectorFromSet(candidate.MatchLabels).Matches(labels.Set(ownerLabel)) {
				owned = false
				break
			}
		}
		if !owned {
			continue
		}
		allowed := len(candidate.Workloads) == 0
		for _, workload := range candidate.Workloads {
			if workload.Kind == root.Kind && workload.Name == root.Name && (workload.UID == "" || workload.UID == root.UID) {
				allowed = true
			}
		}
		if allowed {
			scope = candidate
			found = true
			break
		}
	}
	if !found {
		return nil, Scope{}, denied("owner workload is outside the deployment scope")
	}
	var observations []Observation
	for _, spec := range obj.Spec.Containers {
		if container != "" && container != spec.Name {
			continue
		}
		observation := Observation{Target: Target{Namespace: obj.Namespace, Pod: obj.Name, UID: string(obj.UID), Container: spec.Name}, Release: scopeRelease(scope), Module: scopeModule(scope), Workload: root, OwnerChain: chain, Image: spec.Image, Phase: string(obj.Status.Phase), Conditions: []Condition{}, NodeName: obj.Spec.NodeName, ObservedAt: p.options.Now().UTC()}
		if obj.DeletionTimestamp != nil {
			deletedAt := obj.DeletionTimestamp.Time.UTC()
			observation.DeletionTimestamp = &deletedAt
		}
		for i, condition := range obj.Status.Conditions {
			if i >= 16 {
				break
			}
			observation.Conditions = append(observation.Conditions, Condition{Type: boundedStatus(string(condition.Type)), Status: string(condition.Status), Reason: boundedStatus(condition.Reason)})
		}
		for _, status := range obj.Status.ContainerStatuses {
			if status.Name == spec.Name {
				observation.Target.ContainerID = status.ContainerID
				observation.Target.ImageID = status.ImageID
				observation.Ready = status.Ready
				observation.Running = status.State.Running != nil
				observation.RestartCount = status.RestartCount
				if status.State.Waiting != nil {
					observation.WaitingReason = boundedStatus(status.State.Waiting.Reason)
				}
				observation.CurrentTermination = termination(status.State.Terminated)
				observation.LastTermination = termination(status.LastTerminationState.Terminated)
				break
			}
		}
		observations = append(observations, observation)
	}
	if len(observations) == 0 {
		return nil, Scope{}, denied("container is not a regular container of this pod")
	}
	return observations, scope, nil
}

func scopeRelease(scope Scope) string {
	if scope.Release != "" {
		return scope.Release
	}
	return scope.MatchLabels["app.kubernetes.io/instance"]
}
func scopeModule(scope Scope) string {
	if scope.Module != "" {
		return scope.Module
	}
	if module := scope.MatchLabels["kingeye.app/module"]; module != "" {
		return module
	}
	return scope.MatchLabels["app.kubernetes.io/component"]
}
func boundedStatus(value string) string {
	if len(value) > 128 {
		return value[:128]
	}
	return value
}
func termination(status *corev1.ContainerStateTerminated) *Termination {
	if status == nil {
		return nil
	}
	return &Termination{Reason: boundedStatus(status.Reason), ExitCode: status.ExitCode, Signal: status.Signal, StartedAt: status.StartedAt.Time.UTC(), FinishedAt: status.FinishedAt.Time.UTC()}
}
func controller(owners []metav1.OwnerReference) (metav1.OwnerReference, error) {
	var selected metav1.OwnerReference
	count := 0
	for _, owner := range owners {
		if owner.Controller != nil && *owner.Controller {
			selected = owner
			count++
		}
	}
	if count != 1 || selected.UID == "" {
		return selected, denied("pod owner chain has no unique controller identity")
	}
	return selected, nil
}
func rootKind(kind string) bool {
	return kind == "Deployment" || kind == "StatefulSet" || kind == "DaemonSet" || kind == "CronJob" || kind == "Job"
}
func (p *Provider) ownerChain(ctx context.Context, obj *corev1.Pod) ([]Workload, []map[string]string, error) {
	owners := obj.OwnerReferences
	var chain []Workload
	var ownerLabels []map[string]string
	childKind := "Pod"
	childLabels := obj.Labels
	for depth := 0; depth < 3; depth++ {
		ref, err := controller(owners)
		if err != nil {
			return nil, nil, err
		}
		valid := childKind == "Pod" && (ref.Kind == "ReplicaSet" || ref.Kind == "StatefulSet" || ref.Kind == "DaemonSet" || ref.Kind == "Job") || childKind == "ReplicaSet" && ref.Kind == "Deployment" || childKind == "Job" && ref.Kind == "CronJob"
		if !valid {
			return nil, nil, denied("unsupported workload owner chain")
		}
		owner, err := p.options.Client.GetOwner(ctx, obj.Namespace, ref)
		if err != nil {
			return nil, nil, apiError(err)
		}
		if owner.Namespace != obj.Namespace || owner.Name != ref.Name || owner.Kind != ref.Kind || owner.UID != string(ref.UID) {
			return nil, nil, denied("owner reference identity does not match the live resource")
		}
		if ref.Kind != "CronJob" {
			if owner.Selector == nil {
				return nil, nil, denied("workload has no selector")
			}
			selector, err := metav1.LabelSelectorAsSelector(owner.Selector)
			if err != nil || selector.Empty() || !selector.Matches(labels.Set(childLabels)) || !selector.Matches(labels.Set(obj.Labels)) {
				return nil, nil, denied("workload selector does not match its pod/owner chain")
			}
		}
		chain = append(chain, Workload{Kind: owner.Kind, Name: owner.Name, UID: owner.UID})
		ownerLabels = append(ownerLabels, owner.Labels)
		if rootKind(ref.Kind) && len(owner.Owners) == 0 {
			return chain, ownerLabels, nil
		}
		owners = owner.Owners
		childKind = owner.Kind
		childLabels = owner.Labels
	}
	return nil, nil, denied("owner chain exceeded the supported depth")
}

func (p *Provider) outputLimit(value int64) (int64, error) {
	if value == 0 {
		value = p.options.MaxOutputBytes
	}
	if value < 1 || value > p.options.MaxOutputBytes {
		return 0, invalid("output budget exceeds provider limit")
	}
	return value, nil
}
func (p *Provider) postObserve(ctx context.Context, target Target) (*Observation, bool, string) {
	// Cancellation can prevent final verification; do not silently turn it into
	// an unbounded detached API call.
	if ctx.Err() != nil {
		return nil, false, "post_observation_unavailable"
	}
	ctx, cancel := p.readContext(ctx)
	defer cancel()
	observed, _, err := p.resolve(ctx, target, true)
	if err != nil {
		var failure *Error
		if errors.As(err, &failure) && (failure.Code == CodeTargetChanged || failure.Code == CodeNotFound || failure.Code == CodeNotInScope) {
			if observed.Target.Pod != "" {
				return &observed, true, "target_changed"
			}
			return nil, true, "target_changed"
		}
		return nil, false, "post_observation_unavailable"
	}
	return &observed, false, "identity_verified_before_and_after"
}

func (p *Provider) Logs(ctx context.Context, request LogsRequest) (LogsReceipt, error) {
	result := LogsReceipt{Target: request.Target, Previous: request.Previous, ObservedAt: p.options.Now().UTC(), EvidenceScope: "identity_verified_before", RedactionContract: RedactionContract}
	release, err := p.acquire(ctx)
	if err != nil {
		return result, err
	}
	defer release()
	ctx, cancel := p.readContext(ctx)
	defer cancel()
	if request.TailLines == 0 {
		request.TailLines = 200
	}
	if request.TailLines < 1 || request.TailLines > 1000 || request.SinceSeconds < 0 || request.SinceSeconds > 86400 {
		return result, invalid("log window exceeds supported bounds")
	}
	limit, err := p.outputLimit(request.OutputBytes)
	if err != nil {
		return result, err
	}
	_, _, err = p.resolve(ctx, request.Target, true)
	if err != nil {
		return result, err
	}
	// Ask for one sentinel byte beyond the returned budget so an API-side cap
	// cannot turn a truncated log into an apparently complete response.
	apiLimit := limit + 1
	options := corev1.PodLogOptions{Container: request.Target.Container, TailLines: &request.TailLines, Previous: request.Previous, LimitBytes: &apiLimit}
	if request.SinceSeconds > 0 {
		options.SinceSeconds = &request.SinceSeconds
	}
	stream, err := p.options.Client.OpenLogs(ctx, request.Target.Namespace, request.Target.Pod, options)
	if err != nil {
		return result, apiError(err)
	}
	defer stream.Close()
	data, readErr := io.ReadAll(io.LimitReader(stream, limit+1))
	result.Truncated = int64(len(data)) > limit
	if result.Truncated {
		data = data[:limit]
	}
	var redactedCut bool
	result.Output, redactedCut = p.redactor.bounded(string(data), limit)
	result.Truncated = result.Truncated || redactedCut
	result.ObservedAfter, result.TargetChanged, result.EvidenceScope = p.postObserve(ctx, request.Target)
	if readErr != nil {
		return result, apiError(readErr)
	}
	if result.TargetChanged {
		return result, &Error{Code: CodeTargetChanged, Message: "target identity changed while logs were read"}
	}
	return result, nil
}

func (p *Provider) String() string {
	return fmt.Sprintf("pod provider (%d deployment scopes)", len(p.options.Scopes))
}
