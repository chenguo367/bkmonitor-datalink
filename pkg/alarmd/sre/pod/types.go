// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

// Package pod provides scoped Pod evidence and bounded, non-TTY execution.
// It does not authenticate callers: the channel must require a distinct exec
// scope before invoking Exec. Existing Worker k8sread operations are separate.
package pod

import (
	"context"
	"io"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	TimeoutContract        = "python3_process_group_v1"
	VerificationTimeout    = 3 * time.Second
	TerminationGrace       = 4 * time.Second
	RemoteNotStarted       = "remote_not_started"
	RemoteCompleted        = "remote_completed"
	RemoteTimedOut         = "remote_timeout_confirmed"
	RemoteUnknown          = "remote_state_unknown"
	CodeInvalidRequest     = "invalid_request"
	CodeNotInScope         = "not_in_scope"
	CodeTargetChanged      = "target_changed"
	CodeTargetNotRunning   = "target_not_running"
	CodeBusy               = "provider_busy"
	CodeTimeoutUnsupported = "remote_timeout_unsupported"
	CodeCommandStartFailed = "command_start_failed"
	CodeAPIError           = "apiserver_error"
	CodeForbidden          = "rbac_forbidden"
	CodeNotFound           = "not_found"
	CodeCanceled           = "request_canceled"
)

// Error deliberately omits raw API/transport errors, which can contain URLs,
// authorization material or supplied scripts. Code is the public contract.
type Error struct {
	Code    string
	Message string
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

// Scope is server configuration, never supplied by an execution request.
// MatchLabels is the mandatory, authoritative label constraint. Release/Module
// are optional discovery index values, so custom module conventions are usable.
type Scope struct {
	Namespace   string            `yaml:"namespace" json:"namespace"`
	MatchLabels map[string]string `yaml:"match_labels" json:"match_labels"`
	Release     string            `yaml:"release,omitempty" json:"release,omitempty"`
	Module      string            `yaml:"module,omitempty" json:"module,omitempty"`
	Workloads   []Workload        `yaml:"workloads,omitempty" json:"workloads,omitempty"`
	Timeout     RuntimeContract   `yaml:"timeout,omitempty" json:"timeout,omitempty"`
}

// RuntimeContract declares a tested capability of the selected target image.
// PythonPath must be absolute. The wrapper uses Python 3 standard-library POSIX
// process groups; descendants that deliberately detach are outside this contract.
type RuntimeContract struct {
	Contract   string `yaml:"contract" json:"contract"`
	PythonPath string `yaml:"python_path" json:"python_path"`
}

type Workload struct {
	Kind string `json:"kind" yaml:"kind"`
	Name string `json:"name" yaml:"name"`
	UID  string `json:"uid,omitempty" yaml:"uid,omitempty"`
}

// Client exposes only the Kubernetes reads needed for evidence and owner checks.
type Client interface {
	ListPods(context.Context, string, metav1.ListOptions) (*corev1.PodList, error)
	GetPod(context.Context, string, string) (*corev1.Pod, error)
	GetOwner(context.Context, string, metav1.OwnerReference) (OwnerResource, error)
	OpenLogs(context.Context, string, string, corev1.PodLogOptions) (io.ReadCloser, error)
}

type OwnerResource struct {
	Kind, Namespace, Name, UID string
	Labels                     map[string]string
	Owners                     []metav1.OwnerReference
	Selector                   *metav1.LabelSelector
}

// Executor transports one exec. It must honor context cancellation, never
// retry, and never allocate a TTY. A known process exit is distinct from a
// transport error; neither cancellation nor transport failure proves termination.
type Executor interface {
	Stream(context.Context, StreamRequest) (StreamResult, error)
}
type StreamRequest struct {
	Target         Target
	Argv           []string
	Stdin          io.Reader
	Stdout, Stderr io.Writer
}
type StreamResult struct{ ExitCode *int }

type Options struct {
	Scopes                        []Scope
	Client                        Client
	Executor                      Executor
	Concurrency                   int
	DefaultTimeout, MaxTimeout    time.Duration
	MaxOutputBytes, MaxStdinBytes int64
	MaxPods                       int
	// LiteralSecrets supplements the documented credential_fields_v1 redactor.
	// Secrets stay in memory and are never included in an observation or receipt.
	LiteralSecrets []string
	Now            func() time.Time
}

// Target is the identity a caller received from Discover or Inspect. All six
// fields are required for Logs and Exec; Inspect may initially omit identities.
type Target struct {
	Namespace   string `json:"namespace"`
	Pod         string `json:"pod"`
	UID         string `json:"uid"`
	Container   string `json:"container"`
	ContainerID string `json:"container_id"`
	ImageID     string `json:"image_id"`
}
type Observation struct {
	Target             Target       `json:"target"`
	Release            string       `json:"release,omitempty"`
	Module             string       `json:"module,omitempty"`
	Workload           Workload     `json:"workload"`
	OwnerChain         []Workload   `json:"owner_chain"`
	Image              string       `json:"image"`
	Phase              string       `json:"phase"`
	Ready              bool         `json:"ready"`
	Running            bool         `json:"running"`
	RestartCount       int32        `json:"restart_count"`
	WaitingReason      string       `json:"waiting_reason,omitempty"`
	CurrentTermination *Termination `json:"current_termination,omitempty"`
	LastTermination    *Termination `json:"last_termination,omitempty"`
	Conditions         []Condition  `json:"conditions"`
	DeletionTimestamp  *time.Time   `json:"deletion_timestamp,omitempty"`
	NodeName           string       `json:"node_name,omitempty"`
	ObservedAt         time.Time    `json:"observed_at"`
}
type Termination struct {
	Reason     string    `json:"reason"`
	ExitCode   int32     `json:"exit_code"`
	Signal     int32     `json:"signal"`
	StartedAt  time.Time `json:"started_at"`
	FinishedAt time.Time `json:"finished_at"`
}
type Condition struct {
	Type   string `json:"type"`
	Status string `json:"status"`
	Reason string `json:"reason,omitempty"`
}
type Limits struct {
	Concurrency      int   `json:"concurrency"`
	DefaultTimeoutMS int64 `json:"default_timeout_ms"`
	MaxTimeoutMS     int64 `json:"max_timeout_ms"`
	MaxOutputBytes   int64 `json:"max_output_bytes"`
	MaxStdinBytes    int64 `json:"max_stdin_bytes"`
	MaxPods          int   `json:"max_pods"`
}
type DiscoverRequest struct {
	Namespace string `json:"namespace"`
	Release   string `json:"release,omitempty"`
	Module    string `json:"module,omitempty"`
	Workload  string `json:"workload,omitempty"`
	Continue  string `json:"continue,omitempty"`
	Limit     int    `json:"limit,omitempty"`
}
type DiscoverReceipt struct {
	Targets    []Observation `json:"targets"`
	Continue   string        `json:"continue,omitempty"`
	Rejected   int           `json:"rejected"`
	ObservedAt time.Time     `json:"observed_at"`
}
type TargetRequest struct {
	Target Target `json:"target"`
}
type LogsRequest struct {
	Target       Target `json:"target"`
	TailLines    int64  `json:"tail_lines,omitempty"`
	SinceSeconds int64  `json:"since_seconds,omitempty"`
	Previous     bool   `json:"previous,omitempty"`
	OutputBytes  int64  `json:"output_bytes,omitempty"`
}
type LogsReceipt struct {
	Target            Target       `json:"target"`
	Previous          bool         `json:"previous"`
	Output            string       `json:"output"`
	Truncated         bool         `json:"truncated"`
	ObservedAfter     *Observation `json:"observed_after,omitempty"`
	TargetChanged     bool         `json:"target_changed"`
	EvidenceScope     string       `json:"evidence_scope"`
	RedactionContract string       `json:"redaction_contract"`
	ObservedAt        time.Time    `json:"observed_at"`
}
type ExecRequest struct {
	Target      Target   `json:"target"`
	Argv        []string `json:"argv"`
	Stdin       string   `json:"stdin,omitempty"`
	TimeoutMS   int64    `json:"timeout_ms,omitempty"`
	OutputBytes int64    `json:"output_bytes,omitempty"`
}
type ExecReceipt struct {
	RequestID         string       `json:"request_id"`
	Target            Target       `json:"target"`
	ObservedAfter     *Observation `json:"observed_after,omitempty"`
	ScriptDigest      string       `json:"script_digest"`
	StartedAt         time.Time    `json:"started_at"`
	FinishedAt        time.Time    `json:"finished_at"`
	Stdout            string       `json:"stdout"`
	Stderr            string       `json:"stderr"`
	ExitCode          *int         `json:"exit_code,omitempty"`
	Truncated         bool         `json:"truncated"`
	RemoteState       string       `json:"remote_state"`
	TargetChanged     bool         `json:"target_changed"`
	EvidenceScope     string       `json:"evidence_scope"`
	RedactionContract string       `json:"redaction_contract"`
}
