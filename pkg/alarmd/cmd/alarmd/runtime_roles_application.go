// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package main

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"google.golang.org/grpc"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/config"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/controlplane"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/metric"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/ownership"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/redisfailure"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/roles"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/viewstream/pb"
)

// A process has one identity, regardless of how many roles it hosts. Its
// registration and evidence endpoint can run while business dependencies wait.
type phaseTwoRoleRuntime struct {
	StreamIdentity viewStreamIdentity
	Incarnation    string
	Channel        *roleChannelRuntime
	EvidenceServer *grpc.Server
	config         config.Config
	store          *ownership.RedisStore
	client         redis.UniversalClient
	cancel         context.CancelFunc
	done           chan struct{}
	closeOnce      sync.Once
	closeErr       error
	failed         func(error)
}

type roleEvidenceServer struct {
	pb.UnimplementedControlServiceServer
	handler func(context.Context, *pb.EvidenceRequest) (*pb.EvidenceResult, error)
}

func (server *roleEvidenceServer) ReadEvidence(ctx context.Context, request *pb.EvidenceRequest) (*pb.EvidenceResult, error) {
	return server.handler(ctx, request)
}

func openPhaseTwoRoleRuntime(cfg config.Config, recorder *metric.Recorder, facts *observability.RuntimeConfigFacts) (_ *phaseTwoRoleRuntime, resultErr error) {
	identity, err := newViewStreamIdentity(cfg.HTTP.Listen, viewStreamRoutes(cfg.RuntimeStoreRedis())...)
	if err != nil {
		return nil, err
	}
	incarnation, err := newViewStreamIncarnation()
	if err != nil {
		return nil, err
	}
	runtime := &phaseTwoRoleRuntime{StreamIdentity: identity, Incarnation: incarnation, config: cfg,
		failed: func(err error) { recorder.ObserveDiagnosticRedisFailure("instance", redisfailure.Reason(err)) }}
	defer func() {
		if resultErr != nil {
			_ = runtime.Close()
		}
	}()
	runtime.Channel, err = openProductionRoleChannel(cfg, ChannelBinding{
		Roles: cfg.EffectiveRoles(), InstanceID: cfg.PhaseTwo.Worker.ID, EnvironmentID: cfg.CLI.EnvironmentID,
		Facts: func() *observability.RuntimeConfigFacts { return facts },
		Control: cliControlBinding{Incarnation: incarnation, StreamToken: identity.Token, Metrics: recorder.Gatherer(),
			RedisDialRetries: recorder.ObserveDiagnosticRedisDialRetry},
	})
	if err != nil {
		return nil, err
	}
	runtime.EvidenceServer = grpc.NewServer()
	pb.RegisterControlServiceServer(runtime.EvidenceServer, &roleEvidenceServer{handler: runtime.Channel.EvidenceHandler})
	// This pool is lazy and independent of startup Pings. A source outage
	// cannot hold the channel's listener or its instance registration hostage.
	runtime.client = redis.NewUniversalClient(cliRedisOptions(cfg.RuntimeStoreRedis(), "instance", recorder.ObserveDiagnosticRedisDialRetry))
	runtime.store, err = ownership.NewRedisStoreWithClient(runtime.client, productionPhaseTwoPrefix(cfg.Redis.StatePrefix, "ownership"))
	if err != nil {
		return nil, err
	}
	return runtime, nil
}

func (runtime *phaseTwoRoleRuntime) Start(parent context.Context, health *phaseTwoApplicationHealth) {
	ctx, cancel := context.WithCancel(parent)
	runtime.cancel, runtime.done = cancel, make(chan struct{})
	go func() {
		defer close(runtime.done)
		interval := runtime.config.PhaseTwo.Worker.RegistrationRenewInterval.Duration()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			channelState := observability.HealthState("disabled")
			if runtime.config.HasRole(roles.Channel) && runtime.Channel.HTTPConfigured {
				probe, cancelProbe := context.WithTimeout(ctx, time.Second)
				readyErr := runtime.Channel.Ready(probe)
				cancelProbe()
				channelState = observability.HealthReady
				if readyErr != nil {
					channelState = observability.HealthNotReady
				}
			}
			if ctx.Err() != nil {
				return
			}
			health.channelState.Store(&channelState)
			if !runtime.config.HasRole(roles.Control) && !runtime.config.HasRole(roles.Worker) {
				ready := channelState == observability.HealthReady
				state := observability.HealthNotReady
				var reasons []observability.ReasonCode
				if ready {
					state = observability.HealthReady
				} else {
					reasons = []observability.ReasonCode{"auth_store_unavailable"}
				}
				if channelState == "disabled" {
					reasons = []observability.ReasonCode{"channel_disabled"}
				}
				health.Update(phaseTwoReadiness{State: state, Reasons: reasons, SnapshotReady: true,
					AssignmentReady: true, RuntimeStateReady: true, OutputSinkReady: true})
			}
			statuses := make(map[roles.Role]string)
			snapshot := health.HealthSnapshot()
			for _, role := range runtime.config.EffectiveRoles() {
				statuses[role] = string(snapshot.State)
				if role == roles.Control {
					statuses[role] = string(health.controlReadiness(snapshot.State))
				}
				if role == roles.Channel {
					statuses[role] = string(channelState)
				}
			}
			registration := ownership.InstanceRegistration{InstanceID: runtime.config.PhaseTwo.Worker.ID,
				Roles: runtime.config.EffectiveRoles(), Endpoint: runtime.StreamIdentity.Endpoint,
				Incarnation: runtime.Incarnation, InternalToken: runtime.StreamIdentity.Token,
				ExpiresAt: time.Now().Add(runtime.config.PhaseTwo.Worker.RegistrationTTL.Duration()), RoleStatus: statuses}
			round, stop := context.WithTimeout(ctx, time.Second)
			err := runtime.store.RegisterInstance(round, registration)
			stop()
			if err != nil && ctx.Err() == nil {
				runtime.failed(err)
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

func (runtime *phaseTwoRoleRuntime) Run(ctx context.Context, health *phaseTwoApplicationHealth) error {
	// Only channel-only reaches this path. Start reports its authentication
	// dependency on the role's cadence, independently of business readiness.
	<-ctx.Done()
	state := observability.HealthDraining
	health.channelState.Store(&state)
	health.Update(phaseTwoReadiness{State: observability.HealthDraining})
	return nil
}

func (runtime *phaseTwoRoleRuntime) Close() error {
	if runtime == nil {
		return nil
	}
	runtime.closeOnce.Do(func() {
		if runtime.cancel != nil {
			runtime.cancel()
			<-runtime.done
		}
		var errs []error
		if runtime.Channel != nil {
			errs = append(errs, runtime.Channel.Close())
		}
		if runtime.client != nil {
			errs = append(errs, runtime.client.Close())
		}
		runtime.closeErr = errors.Join(errs...)
	})
	return runtime.closeErr
}

func openProductionRoleBundle(ctx context.Context, cfg config.Config, recorder *metric.Recorder,
	logger *observability.Logger, health *phaseTwoApplicationHealth, runtime *phaseTwoRoleRuntime) (*phaseTwoWorkerBundle, error) {
	if !cfg.HasRole(roles.Control) && !cfg.HasRole(roles.Worker) {
		return nil, nil
	}
	external := defaultPhaseTwoProductionExternalDependencies()
	external.RoleRuntime = runtime
	return openProductionPhaseTwoBundleWithDependencies(ctx, cfg, recorder, logger, health,
		func(client redis.Cmdable, prefix string) (controlplane.StrategySource, error) {
			return controlplane.NewLegacyRedisStrategySource(client, prefix)
		}, external)
}
