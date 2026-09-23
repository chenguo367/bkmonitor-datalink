// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package obchannel

import (
	"context"
	"errors"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/k8sread"
)

// k8sBoundary is said on every answer: this path reads through a running
// replica, so it has nothing to say when none is running.
const k8sBoundary = "Read through the answering replica's own ServiceAccount; with every replica down this path cannot answer and kubectl is the only reader."

// K8sOperations reads alarmd's own workload from the Kubernetes API: its
// Pods, the events on it, and a bounded log tail. Any replica answers, the
// entry one by default, so a crashing replica can be read from a healthy one.
func K8sOperations(reader *k8sread.Reader) []Operation {
	pod := Field{Type: "string", Description: "alarmd 的 Pod 名。", Source: "k8s.pods pods[].name", Pattern: "^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$", MinLength: 1, MaxLength: 253}
	container := Field{Type: "string", Description: "容器名；省略时取 Pod 唯一的容器，多个容器时取 alarmd。", Source: "k8s.pods pods[].containers[].name", Pattern: "^[a-z0-9]([-a-z0-9]*[a-z0-9])?$", MinLength: 1, MaxLength: 63}
	minLines, maxLines := int64(1), int64(k8sread.MaxLogLines)
	limits := map[string]any{"max_pods": k8sread.MaxPods, "max_events": k8sread.MaxEvents, "max_replica_sets": k8sread.MaxReplicaSets,
		"max_log_lines": k8sread.MaxLogLines, "max_log_bytes": k8sread.MaxLogBytes, "verbs": "GET only", "scope": "the Deployment this replica belongs to"}
	ops := []Operation{
		{ID: "k8s.pods", Summary: "读取 alarmd 自己的 Deployment 状态与各 Pod 的阶段、就绪、重启次数和上次退出原因。", Fields: map[string]Field{}, OutputSchema: SchemaOf(k8sread.PodsResult{}),
			Run: func(ctx context.Context, _ Params) Outcome {
				result, err := reader.Pods(ctx)
				out := k8sOutcome(result, err)
				if err == nil && result.Truncated {
					out.Complete = false
					out.Limitations = append(out.Limitations, "More Pods match than the bound; the list is cut.")
				}
				return out
			}},
		{ID: "k8s.events", Summary: "读取 alarmd 的 Deployment、ReplicaSet 与 Pod 上的 Kubernetes 事件，新的在前；可只看一个 Pod。", Fields: map[string]Field{"pod": pod}, OutputSchema: SchemaOf(k8sread.EventsResult{}),
			Run: func(ctx context.Context, p Params) Outcome {
				result, err := reader.Events(ctx, p.String("pod"))
				out := k8sOutcome(result, err)
				if err != nil {
					return out
				}
				if len(result.Failed) > 0 {
					out.Complete = false
					out.Limitations = append(out.Limitations, "Events of the objects in result.failed could not be read; no event for them is not none happened.")
				}
				if result.Truncated {
					out.Complete = false
					out.Limitations = append(out.Limitations, "More events than the bound; the oldest are cut.")
				}
				return out
			}},
		{ID: "k8s.logs", Summary: "读取 alarmd 某个 Pod 容器日志的末尾若干行；previous=true 读上一次运行（崩溃前）的日志。", Fields: map[string]Field{
			"pod": pod, "container": container,
			"previous": {Type: "boolean", Description: "读上一次运行的日志，即崩溃或被杀之前的那一次。"},
			"lines":    {Type: "integer", Description: "末尾行数，默认 200。", Minimum: &minLines, Maximum: &maxLines},
		}, Required: []string{"pod"}, OutputSchema: SchemaOf(k8sread.LogResult{}), Examples: []Params{{"pod": "bk-monitor-alarmd-trigger-5bdb679ddf-abcde", "previous": true}},
			Run: func(ctx context.Context, p Params) Outcome {
				result, err := reader.Logs(ctx, k8sread.LogRequest{Pod: p.String("pod"), Container: p.String("container"), Previous: p.Bool("previous"), Lines: p.Int("lines", k8sread.DefaultLogLines)})
				out := k8sOutcome(result, err)
				if err == nil && result.Truncated {
					out.Complete = false
					out.Limitations = append(out.Limitations, "The log reached the byte bound; fewer lines than asked, the oldest dropped.")
				}
				return out
			}},
	}
	for i := range ops {
		ops[i].EvidenceScope = "deployment_workload"
		ops[i].Limits = limits
	}
	return ops
}

// k8sOutcome carries a named failure as the channel's failure, code for
// code: an empty list is only ever the answer of a read that happened.
func k8sOutcome(value any, err error) Outcome {
	if err != nil {
		var named *k8sread.Error
		if errors.As(err, &named) {
			return Outcome{Error: &Failure{Code: "k8s_" + named.Code, Message: named.Error()}, Limitations: []string{k8sBoundary}}
		}
		return Outcome{Error: &Failure{Code: "k8s_" + k8sread.CodeAPIError, Message: err.Error()}, Limitations: []string{k8sBoundary}}
	}
	return Outcome{Value: value, Complete: true, Limitations: []string{k8sBoundary}}
}
