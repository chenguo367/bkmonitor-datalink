// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package k8sread

import (
	"bytes"
	"context"
	"errors"
	"net/url"
	"sort"
	"strconv"
	"sync"
	"time"
)

type deploymentObject struct {
	Metadata objectMeta `json:"metadata"`
	Spec     struct {
		Replicas *int32 `json:"replicas"`
		Selector struct {
			MatchLabels      map[string]string `json:"matchLabels"`
			MatchExpressions []any             `json:"matchExpressions"`
		} `json:"selector"`
	} `json:"spec"`
	Status struct {
		Replicas            int32 `json:"replicas"`
		ReadyReplicas       int32 `json:"readyReplicas"`
		UpdatedReplicas     int32 `json:"updatedReplicas"`
		AvailableReplicas   int32 `json:"availableReplicas"`
		UnavailableReplicas int32 `json:"unavailableReplicas"`
		Conditions          []struct {
			Type               string    `json:"type"`
			Status             string    `json:"status"`
			Reason             string    `json:"reason"`
			Message            string    `json:"message"`
			LastTransitionTime time.Time `json:"lastTransitionTime"`
		} `json:"conditions"`
	} `json:"status"`
}

type podObject struct {
	Metadata objectMeta `json:"metadata"`
	Spec     struct {
		NodeName   string `json:"nodeName"`
		Containers []struct {
			Name  string `json:"name"`
			Image string `json:"image"`
		} `json:"containers"`
	} `json:"spec"`
	Status struct {
		Phase      string     `json:"phase"`
		Reason     string     `json:"reason"`
		StartTime  *time.Time `json:"startTime"`
		Conditions []struct {
			Type   string `json:"type"`
			Status string `json:"status"`
		} `json:"conditions"`
		ContainerStatuses []containerStatus `json:"containerStatuses"`
	} `json:"status"`
}

type containerState struct {
	Running *struct {
		StartedAt time.Time `json:"startedAt"`
	} `json:"running"`
	Waiting *struct {
		Reason  string `json:"reason"`
		Message string `json:"message"`
	} `json:"waiting"`
	Terminated *struct {
		Reason     string    `json:"reason"`
		Message    string    `json:"message"`
		ExitCode   int32     `json:"exitCode"`
		Signal     int32     `json:"signal"`
		StartedAt  time.Time `json:"startedAt"`
		FinishedAt time.Time `json:"finishedAt"`
	} `json:"terminated"`
}

type containerStatus struct {
	Name         string         `json:"name"`
	Ready        bool           `json:"ready"`
	RestartCount int32          `json:"restartCount"`
	Image        string         `json:"image"`
	State        containerState `json:"state"`
	LastState    containerState `json:"lastState"`
}

// DeploymentStatus is the Deployment's own account of its rollout.
type DeploymentStatus struct {
	Desired     int32       `json:"desired"`
	Replicas    int32       `json:"replicas"`
	Ready       int32       `json:"ready"`
	Updated     int32       `json:"updated"`
	Available   int32       `json:"available"`
	Unavailable int32       `json:"unavailable"`
	Conditions  []Condition `json:"conditions,omitempty"`
}

// Condition is one Deployment condition.
type Condition struct {
	Type    string    `json:"type"`
	Status  string    `json:"status"`
	Reason  string    `json:"reason,omitempty"`
	Message string    `json:"message,omitempty"`
	Since   time.Time `json:"since"`
}

// Pod is one of alarmd's Pods.
type Pod struct {
	Name       string      `json:"name"`
	ReplicaSet string      `json:"replica_set,omitempty"`
	Node       string      `json:"node,omitempty"`
	Phase      string      `json:"phase"`
	Reason     string      `json:"reason,omitempty"`
	Ready      bool        `json:"ready"`
	Deleting   bool        `json:"deleting"`
	CreatedAt  time.Time   `json:"created_at"`
	StartedAt  *time.Time  `json:"started_at,omitempty"`
	Containers []Container `json:"containers"`
}

// Container is one container's state and restart record. LastTermination is
// how its previous run ended, which is what a restart count needs beside it.
type Container struct {
	Name            string          `json:"name"`
	Image           string          `json:"image,omitempty"`
	Ready           bool            `json:"ready"`
	Restarts        int32           `json:"restarts"`
	State           ContainerState  `json:"state"`
	LastTermination *ContainerState `json:"last_termination,omitempty"`
}

// ContainerState is one of running, waiting, terminated or unknown, with the
// fields that state has.
type ContainerState struct {
	State      string     `json:"state"`
	Reason     string     `json:"reason,omitempty"`
	Message    string     `json:"message,omitempty"`
	ExitCode   *int32     `json:"exit_code,omitempty"`
	Signal     int32      `json:"signal,omitempty"`
	StartedAt  *time.Time `json:"started_at,omitempty"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
}

func stateOf(s containerState) *ContainerState {
	switch {
	case s.Running != nil:
		at := s.Running.StartedAt
		return &ContainerState{State: "running", StartedAt: &at}
	case s.Waiting != nil:
		return &ContainerState{State: "waiting", Reason: s.Waiting.Reason, Message: bounded(s.Waiting.Message)}
	case s.Terminated != nil:
		t := s.Terminated
		code, started, finished := t.ExitCode, t.StartedAt, t.FinishedAt
		return &ContainerState{State: "terminated", Reason: t.Reason, Message: bounded(t.Message), ExitCode: &code,
			Signal: t.Signal, StartedAt: &started, FinishedAt: &finished}
	}
	return nil
}

// PodsResult is the workload: the Deployment's status and every Pod its
// selector matches, up to MaxPods.
type PodsResult struct {
	Scope      Scope            `json:"scope"`
	Deployment DeploymentStatus `json:"deployment"`
	Pods       []Pod            `json:"pods"`
	// Truncated says there were more Pods than MaxPods.
	Truncated bool `json:"truncated"`
}

func (r *Reader) paths(scope Scope) (string, string) {
	return "/api/v1/namespaces/" + url.PathEscape(scope.Namespace), "/apis/apps/v1/namespaces/" + url.PathEscape(scope.Namespace)
}

func (r *Reader) listPods(ctx context.Context, scope Scope) ([]podObject, bool, error) {
	base, _ := r.paths(scope)
	var list struct {
		Items    []podObject `json:"items"`
		Metadata struct {
			Continue string `json:"continue"`
		} `json:"metadata"`
	}
	query := url.Values{"labelSelector": {scope.Selector}, "limit": {strconv.Itoa(MaxPods)}}
	if err := r.getJSON(ctx, "pods", base+"/pods", query, &list); err != nil {
		return nil, false, err
	}
	sort.Slice(list.Items, func(i, j int) bool { return list.Items[i].Metadata.Name < list.Items[j].Metadata.Name })
	return list.Items, list.Metadata.Continue != "", nil
}

// Pods reads the Deployment's status and its Pods.
func (r *Reader) Pods(ctx context.Context) (PodsResult, error) {
	scope, err := r.Resolve(ctx)
	if err != nil {
		return PodsResult{}, err
	}
	_, apps := r.paths(scope)
	var dep deploymentObject
	if err := r.getJSON(ctx, "deployments/"+scope.Deployment, apps+"/deployments/"+url.PathEscape(scope.Deployment), nil, &dep); err != nil {
		return PodsResult{}, err
	}
	pods, truncated, err := r.listPods(ctx, scope)
	if err != nil {
		return PodsResult{}, err
	}
	result := PodsResult{Scope: scope, Truncated: truncated, Pods: make([]Pod, 0, len(pods)),
		Deployment: DeploymentStatus{Replicas: dep.Status.Replicas, Ready: dep.Status.ReadyReplicas, Updated: dep.Status.UpdatedReplicas,
			Available: dep.Status.AvailableReplicas, Unavailable: dep.Status.UnavailableReplicas}}
	if dep.Spec.Replicas != nil {
		result.Deployment.Desired = *dep.Spec.Replicas
	}
	for _, c := range dep.Status.Conditions {
		result.Deployment.Conditions = append(result.Deployment.Conditions, Condition{Type: c.Type, Status: c.Status, Reason: c.Reason,
			Message: bounded(c.Message), Since: c.LastTransitionTime})
	}
	for _, p := range pods {
		pod := Pod{Name: p.Metadata.Name, ReplicaSet: p.Metadata.controller("ReplicaSet"), Node: p.Spec.NodeName, Phase: p.Status.Phase,
			Reason: p.Status.Reason, Deleting: p.Metadata.DeletionTimestamp != nil, CreatedAt: p.Metadata.CreationTimestamp,
			StartedAt: p.Status.StartTime, Containers: []Container{}}
		for _, c := range p.Status.Conditions {
			if c.Type == "Ready" {
				pod.Ready = c.Status == "True"
			}
		}
		statuses := map[string]containerStatus{}
		for _, s := range p.Status.ContainerStatuses {
			statuses[s.Name] = s
		}
		// Every container in the spec, reported or not: one without a status
		// yet reads as unknown, never as missing.
		for _, spec := range p.Spec.Containers {
			container := Container{Name: spec.Name, Image: spec.Image, State: ContainerState{State: "unknown"}}
			if s, ok := statuses[spec.Name]; ok {
				container.Ready, container.Restarts = s.Ready, s.RestartCount
				if state := stateOf(s.State); state != nil {
					container.State = *state
				}
				container.LastTermination = stateOf(s.LastState)
			}
			pod.Containers = append(pod.Containers, container)
		}
		result.Pods = append(result.Pods, pod)
	}
	return result, nil
}

// Event is one Kubernetes event on alarmd's Deployment, ReplicaSets or Pods.
type Event struct {
	Type      string    `json:"type"`
	Reason    string    `json:"reason"`
	Message   string    `json:"message"`
	Kind      string    `json:"kind"`
	Name      string    `json:"name"`
	Count     int32     `json:"count"`
	FirstAt   time.Time `json:"first_at"`
	LastAt    time.Time `json:"last_at"`
	Component string    `json:"component,omitempty"`
}

// ObjectRef names one object the events were read for.
type ObjectRef struct {
	Kind string `json:"kind"`
	Name string `json:"name"`
}

// ObjectFailure is one object whose events could not be read.
type ObjectFailure struct {
	ObjectRef
	Code    string `json:"code"`
	Message string `json:"message"`
}

// EventsResult is every event read, newest first, up to MaxEvents, and the
// objects they were read for. Failed lists the objects whose read failed:
// no event for an object listed there is not "none happened".
type EventsResult struct {
	Scope     Scope           `json:"scope"`
	Objects   []ObjectRef     `json:"objects"`
	Events    []Event         `json:"events"`
	Failed    []ObjectFailure `json:"failed,omitempty"`
	Truncated bool            `json:"truncated"`
}

type eventObject struct {
	Type           string `json:"type"`
	Reason         string `json:"reason"`
	Message        string `json:"message"`
	Count          int32  `json:"count"`
	InvolvedObject struct {
		Kind string `json:"kind"`
		Name string `json:"name"`
	} `json:"involvedObject"`
	FirstTimestamp *time.Time `json:"firstTimestamp"`
	LastTimestamp  *time.Time `json:"lastTimestamp"`
	EventTime      *time.Time `json:"eventTime"`
	Series         *struct {
		Count            int32      `json:"count"`
		LastObservedTime *time.Time `json:"lastObservedTime"`
	} `json:"series"`
	Source struct {
		Component string `json:"component"`
	} `json:"source"`
	ReportingComponent string `json:"reportingComponent"`
}

func (e eventObject) event() Event {
	out := Event{Type: e.Type, Reason: e.Reason, Message: bounded(e.Message), Kind: e.InvolvedObject.Kind, Name: e.InvolvedObject.Name,
		Count: e.Count, Component: e.Source.Component}
	if out.Component == "" {
		out.Component = e.ReportingComponent
	}
	pick := func(times ...*time.Time) time.Time {
		for _, t := range times {
			if t != nil && !t.IsZero() {
				return *t
			}
		}
		return time.Time{}
	}
	var observed *time.Time
	if e.Series != nil {
		observed = e.Series.LastObservedTime
		if out.Count == 0 {
			out.Count = e.Series.Count
		}
	}
	out.FirstAt = pick(e.FirstTimestamp, e.EventTime, e.LastTimestamp)
	out.LastAt = pick(observed, e.LastTimestamp, e.EventTime, e.FirstTimestamp)
	if out.Count == 0 {
		out.Count = 1
	}
	return out
}

// Events reads the events on the Deployment, its ReplicaSets (newest
// MaxReplicaSets) and its Pods; with pod set, on that Pod only.
func (r *Reader) Events(ctx context.Context, pod string) (EventsResult, error) {
	scope, err := r.Resolve(ctx)
	if err != nil {
		return EventsResult{}, err
	}
	base, apps := r.paths(scope)
	var objects []ObjectRef
	if pod != "" {
		if _, err := r.podInScope(ctx, scope, pod); err != nil {
			return EventsResult{}, err
		}
		objects = append(objects, ObjectRef{Kind: "Pod", Name: pod})
	} else {
		pods, _, err := r.listPods(ctx, scope)
		if err != nil {
			return EventsResult{}, err
		}
		objects = append(objects, ObjectRef{Kind: "Deployment", Name: scope.Deployment})
		var sets struct {
			Items []struct {
				Metadata objectMeta `json:"metadata"`
			} `json:"items"`
		}
		if err := r.getJSON(ctx, "replicasets", apps+"/replicasets", url.Values{"labelSelector": {scope.Selector}}, &sets); err != nil {
			return EventsResult{}, err
		}
		sort.Slice(sets.Items, func(i, j int) bool {
			return sets.Items[i].Metadata.CreationTimestamp.After(sets.Items[j].Metadata.CreationTimestamp)
		})
		for i, set := range sets.Items {
			if i == MaxReplicaSets {
				break
			}
			if set.Metadata.controller("Deployment") == scope.Deployment {
				objects = append(objects, ObjectRef{Kind: "ReplicaSet", Name: set.Metadata.Name})
			}
		}
		for _, p := range pods {
			objects = append(objects, ObjectRef{Kind: "Pod", Name: p.Metadata.Name})
		}
	}

	result := EventsResult{Scope: scope, Objects: objects, Events: []Event{}}
	var mu sync.Mutex
	var wg sync.WaitGroup
	slots := make(chan struct{}, 4)
	for _, object := range objects {
		wg.Add(1)
		go func(object ObjectRef) {
			defer wg.Done()
			slots <- struct{}{}
			defer func() { <-slots }()
			var list struct {
				Items []eventObject `json:"items"`
			}
			query := url.Values{"fieldSelector": {"involvedObject.kind=" + object.Kind + ",involvedObject.name=" + object.Name}}
			err := r.getJSON(ctx, "events", base+"/events", query, &list)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				failure := ObjectFailure{ObjectRef: object, Code: CodeAPIError, Message: err.Error()}
				var named *Error
				if errors.As(err, &named) {
					failure.Code = named.Code
				}
				result.Failed = append(result.Failed, failure)
				return
			}
			for _, item := range list.Items {
				result.Events = append(result.Events, item.event())
			}
		}(object)
	}
	wg.Wait()
	// Every object failed the same way: the read did not happen, and that is
	// the answer, by its own name.
	if len(result.Failed) == len(objects) && len(objects) > 0 {
		first := result.Failed[0]
		return EventsResult{}, &Error{Code: first.Code, Resource: "events", Message: first.Message}
	}
	sort.Slice(result.Failed, func(i, j int) bool { return result.Failed[i].Name < result.Failed[j].Name })
	sort.SliceStable(result.Events, func(i, j int) bool { return result.Events[i].LastAt.After(result.Events[j].LastAt) })
	if len(result.Events) > MaxEvents {
		result.Events, result.Truncated = result.Events[:MaxEvents], true
	}
	return result, nil
}

// LogRequest asks for the tail of one container's log.
type LogRequest struct {
	Pod       string
	Container string
	// Previous reads the container's previous run: the one that crashed.
	Previous bool
	Lines    int
}

// LogResult is the tail, bounded by lines and by MaxLogBytes. Truncated
// says the byte bound cut it: fewer lines than asked, the oldest dropped and
// the newest kept.
type LogResult struct {
	Scope     Scope  `json:"scope"`
	Pod       string `json:"pod"`
	Container string `json:"container"`
	Previous  bool   `json:"previous"`
	Lines     int    `json:"lines_requested"`
	Text      string `json:"text"`
	Bytes     int    `json:"bytes"`
	Truncated bool   `json:"truncated"`
}

// podInScope reads one Pod and checks it is the Deployment's: its labels
// match the selector and its controlling ReplicaSet is controlled by the
// Deployment. Labels alone are what any workload can copy.
func (r *Reader) podInScope(ctx context.Context, scope Scope, name string) (podObject, error) {
	base, apps := r.paths(scope)
	var pod podObject
	if err := r.getJSON(ctx, "pods/"+name, base+"/pods/"+url.PathEscape(name), nil, &pod); err != nil {
		return podObject{}, err
	}
	outside := &Error{Code: CodeOutOfScope, Resource: "pods/" + name, Message: "not a Pod of " + scope.Deployment}
	replicaSet := pod.Metadata.controller("ReplicaSet")
	if !scope.matches(pod.Metadata.Labels) || replicaSet == "" {
		return podObject{}, outside
	}
	var rs struct {
		Metadata objectMeta `json:"metadata"`
	}
	if err := r.getJSON(ctx, "replicasets/"+replicaSet, apps+"/replicasets/"+url.PathEscape(replicaSet), nil, &rs); err != nil {
		return podObject{}, err
	}
	if rs.Metadata.controller("Deployment") != scope.Deployment {
		return podObject{}, outside
	}
	return pod, nil
}

// Logs reads the tail of a container's log. The Pod must be one of
// alarmd's; the container defaults to the Pod's only one, or to "alarmd".
func (r *Reader) Logs(ctx context.Context, request LogRequest) (LogResult, error) {
	scope, err := r.Resolve(ctx)
	if err != nil {
		return LogResult{}, err
	}
	base, _ := r.paths(scope)
	pod, err := r.podInScope(ctx, scope, request.Pod)
	if err != nil {
		return LogResult{}, err
	}
	container := request.Container
	if container == "" {
		switch {
		case len(pod.Spec.Containers) == 1:
			container = pod.Spec.Containers[0].Name
		default:
			container = "alarmd"
		}
	}
	known := false
	for _, c := range pod.Spec.Containers {
		known = known || c.Name == container
	}
	if !known {
		return LogResult{}, &Error{Code: CodeOutOfScope, Resource: "pods/" + request.Pod, Message: "the Pod has no container " + container}
	}
	lines := request.Lines
	if lines <= 0 {
		lines = DefaultLogLines
	}
	if lines > MaxLogLines {
		lines = MaxLogLines
	}
	// No limitBytes: the server counts it forward from the start of the last
	// N lines, so a cut would drop the newest lines - the ones before a
	// crash. The whole tail is read, bounded by maxLogReadBytes, and only its
	// last MaxLogBytes are kept.
	query := url.Values{"container": {container}, "tailLines": {strconv.Itoa(lines)}, "timestamps": {"true"}}
	if request.Previous {
		query.Set("previous", "true")
	}
	body, err := r.get(ctx, "pods/"+request.Pod+"/log", base+"/pods/"+url.PathEscape(request.Pod)+"/log", query, maxLogReadBytes)
	if err != nil {
		// The server answers a previous log that was never written with 400
		// and says so; it is a missing object, not a server fault.
		var named *Error
		if request.Previous && errors.As(err, &named) && named.Status == 400 {
			named.Code = CodeNotFound
		}
		return LogResult{}, err
	}
	if len(body) > maxLogReadBytes {
		// The newest bytes are past the read bound: a tail that would lose
		// them is not returned as one.
		return LogResult{}, &Error{Code: CodeAPIError, Resource: "pods/" + request.Pod + "/log", Message: "the requested lines exceed the read bound; ask for fewer"}
	}
	truncated := false
	if len(body) > MaxLogBytes {
		// Keep the newest bytes, from the first whole line in them.
		body, truncated = body[len(body)-MaxLogBytes:], true
		if cut := bytes.IndexByte(body, '\n'); cut >= 0 && cut+1 < len(body) {
			body = body[cut+1:]
		}
	}
	return LogResult{Scope: scope, Pod: request.Pod, Container: container, Previous: request.Previous, Lines: lines,
		Text: string(body), Bytes: len(body), Truncated: truncated}, nil
}
