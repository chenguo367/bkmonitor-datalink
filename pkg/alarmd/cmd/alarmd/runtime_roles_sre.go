// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License.

package main

import (
	"log/slog"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/config"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/metric"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/obchannel"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/sre/pod"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/sre/redact"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/sre/uq"
)

// channelSREOperations builds the SRE operations of a channel. The Pod
// provider needs nothing the deployment declares: in a cluster it reads any
// namespace a request names, this Pod's own by default, through the Pod's
// ServiceAccount; out of one its operations say they are unavailable and why.
// audit hears every execution and management call.
func channelSREOperations(cfg config.Config, audit func(obchannel.SREAction)) ([]obchannel.Operation, func(), error) {
	secrets := sreDeploymentSecrets(cfg)
	podReason := ""
	pods, err := pod.NewInCluster(pod.Options{ModuleLabel: cfg.SRE.ModuleLabel, LiteralSecrets: secrets})
	if err != nil {
		podReason = "pod_provider_unavailable: " + err.Error()
		pods = nil
	}
	querySource := cfg.PhaseTwo.Access.QuerySource
	if querySource == "" {
		querySource = config.DefaultQuerySource
	}
	queries, err := uq.NewWithQuerySource(cfg.SRE.WithUQEndpoint(cfg.PhaseTwo.Access.UQEndpoint).UQ.Egresses, nil, querySource)
	if err != nil {
		return nil, nil, err
	}
	return obchannel.SREOperations(obchannel.SREOptions{Pods: pods, Queries: queries, Scrubber: redact.New(secrets...), PodReason: podReason, Audit: audit}), queries.Close, nil
}

// sreAuditSink writes each execution or management call's sre_action line
// and counts it: who (session and request), what, where, and the result.
func sreAuditSink(logger *observability.Logger, recorder *metric.Recorder) func(obchannel.SREAction) {
	recorder.PresetSREActions(obchannel.SREActionOps, obchannel.SREActionResults)
	return func(action obchannel.SREAction) {
		recorder.RecordSREAction(action.Op, action.Result)
		attrs := []slog.Attr{slog.String("op", action.Op), slog.String("namespace", action.Namespace), slog.String("kind", action.Kind),
			slog.String("name", action.Name), slog.String("uid", action.UID), slog.String("detail", action.Detail), slog.String("code", action.Code),
			slog.String("session_id", action.SessionID), slog.String("request_id", action.RequestID)}
		duration := time.Duration(action.DurationMS) * time.Millisecond
		if action.Result == obchannel.SREResultDone {
			logger.Info("sre_action", action.Result, 0, duration, attrs...)
			return
		}
		logger.Warn("sre_action", action.Result, 0, duration, attrs...)
	}
}

func sreDeploymentSecrets(cfg config.Config) []string {
	values := []string{cfg.CLI.AdminKey, cfg.PhaseTwo.Linkd.Password, cfg.Redis.Password, cfg.Redis.SentinelPassword}
	for _, connection := range []config.RedisConnectionConfig{cfg.RuntimeStoreRedis(), cfg.StrategySourceRedis(), cfg.CMDBCacheRedis(), cfg.Kafka.LegacyAdapter.ServiceRedis} {
		values = append(values, connection.Password, connection.SentinelPassword)
	}
	if connection, ok := cfg.TargetGroupRedis(); ok {
		values = append(values, connection.Password, connection.SentinelPassword)
	}
	if connection, ok := cfg.DynamicConfigRedis(); ok {
		values = append(values, connection.Password, connection.SentinelPassword)
	}
	return values
}
