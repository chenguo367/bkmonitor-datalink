// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License.

package metric

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// sreActionMetrics counts the SRE channel's execution and management calls.
// The label lists are the channel's, bound once at wiring (PresetSREActions),
// so every cell exists before the first call and a zero is a reading.
type sreActionMetrics struct {
	mu      sync.Mutex
	ops     []string
	results []string
	actions *prometheus.CounterVec
}

func newSREActionMetrics() *sreActionMetrics {
	return &sreActionMetrics{actions: prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricNamespace, Subsystem: metricSubsystem, Name: "sre_actions_total",
		Help: "Execution, management and Secret-read calls the SRE channel made, by operation (pod.exec, pod.delete, " +
			"workload.restart, workload.scale, secret.get, configmap.get) and result: done; target_changed (the Pod or workload was " +
			"replaced since it was read, and nothing was changed); forbidden (the apiserver refused the " +
			"ServiceAccount); refused (an invalid or unsupported request, a busy provider, or a command that " +
			"could not start - nothing ran); failed (an apiserver or transport failure); unknown (an execution " +
			"whose remote end is unconfirmed). Each call also writes one sre_action line naming the session, " +
			"the request, the namespace and object and the result. Every cell exists once the channel is " +
			"wired: six operations by six results, 36 series. A secret.get line names the Secret and whether its " +
			"values were revealed, never a value.",
	}, []string{"op", "result"})}
}

// PresetSREActions binds the closed label lists and creates every cell. A nil
// recorder is a no-op.
func (r *Recorder) PresetSREActions(ops, results []string) {
	if r == nil || r.phaseTwo.sreActions == nil {
		return
	}
	m := r.phaseTwo.sreActions
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ops, m.results = append([]string(nil), ops...), append([]string(nil), results...)
	for _, op := range ops {
		for _, result := range results {
			m.actions.WithLabelValues(op, result)
		}
	}
}

// RecordSREAction counts one call; a label outside the bound lists is not
// counted rather than creating a series the bound says cannot exist.
func (r *Recorder) RecordSREAction(op, result string) {
	if r == nil || r.phaseTwo.sreActions == nil {
		return
	}
	m := r.phaseTwo.sreActions
	m.mu.Lock()
	ops, results := m.ops, m.results
	m.mu.Unlock()
	if !knownLabel(ops, op) || !knownLabel(results, result) {
		return
	}
	m.actions.WithLabelValues(op, result).Inc()
}
