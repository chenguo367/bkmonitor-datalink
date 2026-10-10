// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License.

package obchannel

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/cliauth"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/sre/pod"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/sre/redact"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/sre/uq"
)

// SREOperations runs on the channel. These operations do not accept a Worker
// target and are never reachable through the internal Worker evidence RPC.
func SREOperations(pods *pod.Provider, queries *uq.Provider, scrubber *redact.Scrubber, podReason string) []Operation {
	if scrubber == nil {
		scrubber = redact.New()
	}
	text := Field{Type: "string", MinLength: 1, MaxLength: 256}
	nonnegative, positive := int64(0), int64(1)
	maxTimeout, maxOutput, maxTail, maxSince, maxLimit := int64(30000), int64(512<<10), int64(1000), int64(86400), int64(32)
	target := Field{Type: "object", Properties: map[string]Field{"namespace": text, "pod": text, "uid": text, "container": text, "container_id": {Type: "string", MinLength: 1, MaxLength: 512}, "image_id": {Type: "string", MinLength: 1, MaxLength: 1024}}, Required: []string{"namespace", "pod", "uid", "container", "container_id", "image_id"}}
	inspectTarget := target
	inspectTarget.Required = []string{"namespace", "pod"}
	podAvailable := func() Availability {
		if pods == nil {
			reason := podReason
			if reason == "" {
				reason = "pod_provider_unconfigured: declare sre.pods target scopes"
			}
			return Availability{Reason: reason}
		}
		return Availability{Available: true}
	}
	podLimits := map[string]any{"max_timeout_ms": maxTimeout, "max_output_bytes": maxOutput, "max_stdin_bytes": 32 << 10, "max_targets_per_page": 32, "scope": "deployment_declared_namespace_labels_and_owner_chain", "retry": "explicit_only; canceled or unknown executions must be inspected before retry", "non_tty": true}
	podLimits["verification_timeout_ms"] = pod.VerificationTimeout.Milliseconds()
	podLimits["termination_grace_ms"] = pod.TerminationGrace.Milliseconds()
	if pods != nil {
		podLimits["target_scopes"] = pods.Scopes()
		podLimits["provider_limits"] = pods.Limits()
	}
	execAvailable := func() Availability {
		if available := podAvailable(); !available.Available {
			return available
		}
		for _, scope := range pods.Scopes() {
			if scope.Timeout.Contract == pod.TimeoutContract {
				return Availability{Available: true}
			}
		}
		return Availability{Reason: "remote_timeout_unsupported: Pod scopes have no registered execution runtime contract"}
	}
	wrapPod := func(value any, err error, complete bool) Outcome {
		out := Outcome{Value: value, Complete: complete, Limitations: []string{"Target labels, owner chain and container identity are checked by the channel ServiceAccount; arbitrary application content requires its domain masking."}}
		if err != nil {
			out.Complete = false
			var named *pod.Error
			if errors.As(err, &named) {
				out.Error = &Failure{Code: "pod_" + named.Code, Message: named.Message}
			} else {
				out.Error = &Failure{Code: "pod_provider_failed", Message: "Pod provider did not complete."}
			}
		}
		return out
	}
	ops := []Operation{
		{ID: "pod.targets", Summary: "发现部署声明范围内的 Pod 与容器，返回 UID、containerID、imageID 和 owner 链。", EvidenceScope: "sre_pod_targets", ExecutionPool: "sre", ExecutionTimeout: 30 * time.Second, Availability: podAvailable, Limits: podLimits,
			Fields: map[string]Field{"namespace": text, "release": text, "module": text, "workload": text, "continue": {Type: "string", MaxLength: 4096}, "limit": {Type: "integer", Minimum: &positive, Maximum: &maxLimit}}, OutputSchema: SchemaOf(pod.DiscoverReceipt{}),
			Run: func(ctx context.Context, p Params) Outcome {
				v, err := pods.Discover(ctx, pod.DiscoverRequest{Namespace: p.String("namespace"), Release: p.String("release"), Module: p.String("module"), Workload: p.String("workload"), Continue: p.String("continue"), Limit: p.Int("limit", 0)})
				out := wrapPod(v, err, v.Continue == "")
				if v.Continue != "" {
					next := Params{"continue": v.Continue}
					for _, key := range []string{"namespace", "release", "module", "workload", "limit"} {
						if value, present := p[key]; present {
							next[key] = value
						}
					}
					out.Next = []Call{{Operation: "pod.targets", Params: next, Reason: "读取同一目标范围的下一页，直至 continuation 为空。"}}
				}
				return out
			}},
		{ID: "pod.get", Summary: "读取范围内一个 Pod 的容器身份、运行状态与 owner 链；首次可省略 UID，后续使用返回身份。", EvidenceScope: "sre_pod_target", ExecutionPool: "sre", ExecutionTimeout: 30 * time.Second, Availability: podAvailable, Limits: podLimits, Fields: map[string]Field{"target": inspectTarget}, Required: []string{"target"}, OutputSchema: SchemaOf(pod.Observation{}),
			Run: func(ctx context.Context, p Params) Outcome {
				v, err := pods.Inspect(ctx, pod.TargetRequest{Target: podTarget(p)})
				return wrapPod(v, err, err == nil)
			}},
		{ID: "pod.logs", Summary: "读取指定 Pod/container 身份的有界日志，执行前后复核容器变化。", EvidenceScope: "sre_pod_logs", ExecutionPool: "sre", ExecutionTimeout: 30 * time.Second, Availability: podAvailable, Limits: podLimits, Fields: map[string]Field{"target": target, "tail_lines": {Type: "integer", Minimum: &positive, Maximum: &maxTail}, "since_seconds": {Type: "integer", Minimum: &positive, Maximum: &maxSince}, "previous": {Type: "boolean"}, "output_bytes": {Type: "integer", Minimum: &positive, Maximum: &maxOutput}}, Required: []string{"target"}, OutputSchema: SchemaOf(pod.LogsReceipt{}),
			Run: func(ctx context.Context, p Params) Outcome {
				v, err := pods.Logs(ctx, pod.LogsRequest{Target: podTarget(p), TailLines: int64(p.Int("tail_lines", 0)), SinceSeconds: int64(p.Int("since_seconds", 0)), Previous: p.Bool("previous"), OutputBytes: int64(p.Int("output_bytes", 0))})
				out := wrapPod(v, err, err == nil && !v.Truncated && !v.TargetChanged && v.EvidenceScope == "identity_verified_before_and_after")
				if v.ObservedAfter == nil {
					out.Limitations = append(out.Limitations, "Post-read target identity verification did not complete.")
				}
				return out
			}},
		{ID: "pod.exec", Summary: "以显式执行授权在目标容器运行非 TTY argv/stdin，返回远端执行回执；请求不会自动重试。", Effect: "exec", RequiredScope: cliauth.ScopeExec, EvidenceScope: "sre_pod_exec", ExecutionPool: "exec", ExecutionTimeout: 45 * time.Second, Availability: execAvailable, Limits: podLimits,
			Fields: map[string]Field{"target": target, "argv": {Type: "array", MinItems: 1, MaxItems: 128, Items: &Field{Type: "string", MaxLength: 32768}}, "stdin": {Type: "string", Source: "stdin", MaxLength: 32768}, "timeout_ms": {Type: "integer", Minimum: &positive, Maximum: &maxTimeout}, "output_bytes": {Type: "integer", Minimum: &positive, Maximum: &maxOutput}}, Required: []string{"target", "argv"}, OutputSchema: SchemaOf(pod.ExecReceipt{}),
			Run: func(ctx context.Context, p Params) Outcome {
				argv := []string{}
				if values, ok := p["argv"].([]any); ok {
					for _, value := range values {
						argv = append(argv, value.(string))
					}
				}
				v, err := pods.Exec(ctx, pod.ExecRequest{Target: podTarget(p), Argv: argv, Stdin: p.String("stdin"), TimeoutMS: int64(p.Int("timeout_ms", 0)), OutputBytes: int64(p.Int("output_bytes", 0))})
				out := wrapPod(v, err, err == nil && !v.Truncated && !v.TargetChanged && v.RemoteState == pod.RemoteCompleted && v.EvidenceScope == "identity_verified_before_and_after")
				if v.ObservedAfter == nil && v.RemoteState != pod.RemoteNotStarted {
					out.Limitations = append(out.Limitations, "Remote execution receipt is retained; post-execution target identity verification did not complete.")
				}
				if v.RemoteState == pod.RemoteUnknown {
					out.Limitations = append(out.Limitations, "Remote process termination is unconfirmed. Do not automatically repeat this execution.")
				}
				return out
			}},
	}
	queryAvailable := func() Availability {
		if queries == nil || len(queries.Egresses()) == 0 {
			return Availability{Reason: "uq_provider_unconfigured: declare sre.uq.egresses"}
		}
		return Availability{Available: true}
	}
	uqLimits := map[string]any{"wire_contract": "structured_query_ts_v1", "max_timeout_ms": maxTimeout, "max_window_seconds": int64(uq.MaxWindow / time.Second), "max_native_request_bytes": uq.MaxBodyBytes, "max_native_response_bytes": uq.MaxResponseBytes, "pagination": "explicit_native_cursor_or_offset_in_body; one_page_per_invocation", "authority": "deployment_operator_readonly; page_user_authorization_is_separate", "redaction_contract": redact.Contract, "log_content": "status/count/digest only before SaaS topic masking; content and cursors omitted"}
	ops = append(ops, Operation{ID: "uq.egresses", Summary: "读取部署登记的 UQ 出口、端点、tenant/space 范围与时间单位合同。", EvidenceScope: "sre_uq_egresses", Availability: queryAvailable, Fields: map[string]Field{}, Limits: uqLimits, OutputSchema: SchemaOf([]uq.Egress{}), Run: func(context.Context, Params) Outcome { return Outcome{Value: queries.Egresses(), Complete: true} }}, Operation{
		ID: "uq.query", Summary: "按登记出口和端点转发原生 UQ JSON 查询，保留实际出口、scope、下游状态与响应。", EvidenceScope: "sre_uq_native_query", ExecutionPool: "sre", ExecutionTimeout: 35 * time.Second, Availability: queryAvailable, Limits: uqLimits,
		Fields: map[string]Field{"egress": text, "endpoint": {Type: "string", Source: "uq.egresses endpoints[].path", MinLength: 1, MaxLength: 256}, "tenant": text, "space": text, "body": {Type: "object", AdditionalProperties: true}, "timeout_ms": {Type: "integer", Minimum: &nonnegative, Maximum: &maxTimeout}}, Required: []string{"egress", "endpoint", "tenant", "space", "body"}, OutputSchema: SchemaOf(uq.Receipt{}),
		Run: func(ctx context.Context, p Params) Outcome {
			body, _ := p["body"].(map[string]any)
			v, err := queries.Query(ctx, uq.Request{Egress: p.String("egress"), Endpoint: p.String("endpoint"), Tenant: p.String("tenant"), Space: p.String("space"), Body: body, TimeoutMS: int64(p.Int("timeout_ms", 0))})
			if v.Native != nil {
				v.Native = scrubber.JSON(v.Native)
			}
			v.TraceID = scrubber.Text(v.TraceID)
			out := Outcome{Value: v, Complete: v.Complete, Limitations: v.Limitations}
			if err != nil {
				var named *uq.Error
				if errors.As(err, &named) {
					out.Error = &Failure{Code: named.Code, Message: named.Message}
				} else {
					out.Error = &Failure{Code: "uq_provider_failed", Message: "Native query did not complete."}
				}
			}
			return out
		},
	})
	return ops
}

func podTarget(p Params) pod.Target {
	value, _ := json.Marshal(p["target"])
	var target pod.Target
	_ = json.Unmarshal(value, &target)
	return target
}
