// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License.

package main

import (
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/config"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/obchannel"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/sre/pod"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/sre/redact"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/sre/uq"
)

func channelSREOperations(cfg config.Config) ([]obchannel.Operation, func(), error) {
	secrets := sreDeploymentSecrets(cfg)
	var pods *pod.Provider
	podReason := ""
	if len(cfg.SRE.Pods) > 0 {
		var err error
		pods, err = pod.NewInCluster(pod.Options{Scopes: cfg.SRE.Pods, LiteralSecrets: secrets})
		if err != nil {
			podReason = "pod_provider_unavailable: ServiceAccount configuration could not be opened"
		}
	}
	queries, err := uq.New(cfg.SRE.UQ.Egresses, nil)
	if err != nil {
		return nil, nil, err
	}
	return obchannel.SREOperations(pods, queries, redact.New(secrets...), podReason), queries.Close, nil
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
