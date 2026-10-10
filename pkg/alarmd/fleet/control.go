// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package fleet

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/ownership"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/roles"
)

// ControlFacts carries a control instance's observations independently of
// Worker snapshots. The lease term and process incarnation bind these facts
// to the current leader, including when control runs without a Worker.
type ControlFacts struct {
	Replica          string                 `json:"replica"`
	Incarnation      string                 `json:"incarnation"`
	ControlEpoch     uint64                 `json:"control_epoch"`
	TakenAt          time.Time              `json:"taken_at"`
	Source           *SourceFacts           `json:"source,omitempty"`
	Activation       *ActivationFacts       `json:"activation,omitempty"`
	ActivationHeader *ActivationHeaderFacts `json:"activation_header,omitempty"`
	ControlSource    *ControlSourceFacts    `json:"control_source,omitempty"`
	Rebalance        *RebalanceFacts        `json:"rebalance,omitempty"`
	AssignmentScope  *AssignmentScopeFacts  `json:"assignment_scope,omitempty"`
	AssignmentSweep  *AssignmentSweepFacts  `json:"assignment_sweep,omitempty"`
	LeaderRound      *LeaderRoundFacts      `json:"leader_round,omitempty"`
	ViewStream       *ViewStreamFacts       `json:"view_stream,omitempty"`
}

func (facts ControlFacts) Validate() error {
	if facts.Replica == "" || facts.Incarnation == "" || facts.ControlEpoch == 0 || facts.TakenAt.IsZero() {
		return errors.New("alarmd fleet: control facts require replica, incarnation, epoch and capture time")
	}
	if facts.ViewStream != nil && facts.ViewStream.Leading && facts.ViewStream.ControlEpoch != facts.ControlEpoch {
		return errors.New("alarmd fleet: control stream epoch differs from control facts epoch")
	}
	return nil
}

func (store *RedisStore) controlKey(replica string) string {
	return store.replicaKey("fleet-control", replica)
}

// PublishControl uses the snapshot retention and byte budget, on a key that
// no Worker coverage or capacity reader consults. An oversized record is
// refused as a whole so control facts never silently lose part of a round.
func (store *RedisStore) PublishControl(ctx context.Context, facts ControlFacts) error {
	if err := facts.Validate(); err != nil {
		return err
	}
	payload, err := json.Marshal(facts)
	if err != nil {
		return fmt.Errorf("alarmd fleet: encode control facts: %w", err)
	}
	if len(payload) > store.maxAnomalyBytes {
		return errors.New("alarmd fleet: control facts exceed byte budget")
	}
	if err := store.client.Set(ctx, store.controlKey(facts.Replica), payload, store.ttl).Err(); err != nil {
		return fmt.Errorf("alarmd fleet: publish control facts: %w", err)
	}
	return nil
}

func (store *RedisStore) LoadControl(ctx context.Context, replica string) (ControlFacts, bool, error) {
	if replica == "" {
		return ControlFacts{}, false, errors.New("alarmd fleet: control read requires a replica identity")
	}
	var facts ControlFacts
	found := false
	_, err := store.read(ctx, []string{replica}, store.controlKey, nil, func(_ int, text string) error {
		if len(text) > store.maxAnomalyBytes {
			return errors.New("alarmd fleet: stored control facts exceed byte budget")
		}
		if err := json.Unmarshal([]byte(text), &facts); err != nil {
			return fmt.Errorf("alarmd fleet: decode control facts: %w", err)
		}
		if err := facts.Validate(); err != nil {
			return err
		}
		if facts.Replica != replica {
			return errors.New("alarmd fleet: stored control replica differs from requested identity")
		}
		found = true
		return nil
	})
	if err != nil {
		return ControlFacts{}, false, err
	}
	return facts, found, nil
}

// ControlRegistry reads a live control lease and the process it names.
type ControlRegistry interface {
	ReadActiveControlLeader(context.Context) (ownership.QueryGroupOwner, bool, error)
	ReadInstance(context.Context, string) (ownership.InstanceRegistration, bool, error)
}

type ControlFactsReader interface {
	LoadControl(context.Context, string) (ControlFacts, bool, error)
}

// ControlFactsSource returns the current leader's facts. false with no error
// means a leader registered by an older binary, whose Worker snapshot is the
// compatibility source. Any new-record or lease failure refuses that fallback.
type ControlFactsSource interface {
	ControlFacts(context.Context, time.Time) (ControlFacts, bool, error)
}

type registeredControlFacts struct {
	registry ControlRegistry
	reader   ControlFactsReader
}

func NewControlFactsSource(registry ControlRegistry, reader ControlFactsReader) (ControlFactsSource, error) {
	if registry == nil || reader == nil {
		return nil, errors.New("alarmd fleet: control source requires registry and facts reader")
	}
	return registeredControlFacts{registry: registry, reader: reader}, nil
}

func (source registeredControlFacts) ControlFacts(ctx context.Context, at time.Time) (ControlFacts, bool, error) {
	leader, found, err := source.registry.ReadActiveControlLeader(ctx)
	if err != nil {
		return ControlFacts{}, false, err
	}
	if !found {
		return ControlFacts{}, false, errors.New("alarmd fleet: active control leader is absent")
	}
	instance, found, err := source.registry.ReadInstance(ctx, leader.OwnerID)
	if err != nil {
		return ControlFacts{}, false, err
	}
	if !found {
		return ControlFacts{}, false, nil
	}
	if err := instance.Validate(); err != nil {
		return ControlFacts{}, false, err
	}
	if instance.InstanceID != leader.OwnerID || !instance.ExpiresAt.After(at) || !instance.Roles.Has(roles.Control) {
		return ControlFacts{}, false, errors.New("alarmd fleet: control leader instance is expired or not a control instance")
	}
	facts, found, err := source.reader.LoadControl(ctx, leader.OwnerID)
	if err != nil {
		return ControlFacts{}, false, err
	}
	if !found {
		return ControlFacts{}, false, errors.New("alarmd fleet: control leader facts are absent")
	}
	if err := facts.Validate(); err != nil {
		return ControlFacts{}, false, err
	}
	if facts.Replica != leader.OwnerID || facts.ControlEpoch != leader.OwnerEpoch || facts.Incarnation != instance.Incarnation {
		return ControlFacts{}, false, errors.New("alarmd fleet: control facts do not match leader epoch or instance incarnation")
	}
	// The read may straddle a takeover. Judge its answer against the lease
	// again so an old owner's record is never attached to a new term.
	current, found, err := source.registry.ReadActiveControlLeader(ctx)
	if err != nil {
		return ControlFacts{}, false, err
	}
	if !found || current.OwnerID != leader.OwnerID || current.OwnerEpoch != leader.OwnerEpoch {
		return ControlFacts{}, false, errors.New("alarmd fleet: control leader changed while reading facts")
	}
	return facts, true, nil
}

// GapControlFactsUnavailable says the new control source could not supply
// facts for the active leader. It affects the verdict without changing any
// Worker count, execution coverage or capacity figure.
const GapControlFactsUnavailable GapKind = "CONTROL_FACTS_UNAVAILABLE"

type controlRead struct {
	facts         *ControlFacts
	err           error
	authoritative bool
}

// SetControlFactsSource is wired once before the service is served. Omitting
// it preserves the embedded control observations of older deployments.
func (service *Service) SetControlFactsSource(source ControlFactsSource) {
	service.control = source
}

func (service *Service) readControl(ctx context.Context, at time.Time) controlRead {
	if service.control == nil {
		return controlRead{}
	}
	facts, found, err := service.control.ControlFacts(ctx, at)
	read := controlRead{authoritative: found || err != nil, err: err}
	if err != nil || !found {
		return read
	}
	if err := facts.Validate(); err != nil {
		read.err = err
	} else if at.Sub(facts.TakenAt) > service.freshness {
		read.err = errors.New("alarmd fleet: control facts are stale")
	} else {
		read.facts = &facts
	}
	return read
}

func controlFactsOf(snapshot Snapshot) ControlFacts {
	return ControlFacts{Replica: snapshot.Replica, TakenAt: snapshot.TakenAt,
		Source: snapshot.Source, Activation: snapshot.Activation, ActivationHeader: snapshot.ActivationHeader, ControlSource: snapshot.ControlSource,
		Rebalance: snapshot.Rebalance, AssignmentScope: snapshot.AssignmentScope, AssignmentSweep: snapshot.AssignmentSweep,
		LeaderRound: snapshot.LeaderRound, ViewStream: snapshot.ViewStream}
}

// mergeControlFacts is shared by embedded legacy facts and independent control
// records, before source standing and health are decided by the aggregate.
func mergeControlFacts(view *View, snapshot ControlFacts) {
	replica := snapshot.Replica
	if snapshot.ControlSource != nil {
		if snapshot.ControlSource.StaleBeyondBound {
			view.Degradations = append(view.Degradations, Degradation{Kind: DegradationControlSourceStale, Replica: replica,
				Stage: snapshot.ControlSource.LastFailureExit, Text: snapshot.ControlSource.LastFailure})
		}
		if snapshot.ControlSource.LeaderAbsentBeyondBound {
			view.Degradations = append(view.Degradations, Degradation{Kind: DegradationControlLeaderAbsent, Replica: replica})
		}
	}
	if snapshot.Activation != nil {
		view.Activation, view.ActivationReplica = snapshot.Activation, replica
		if snapshot.Activation.BehindBeyondBound {
			view.Degradations = append(view.Degradations, Degradation{Kind: DegradationActivationBehind, Replica: replica})
		}
		if snapshot.Activation.BlockedQueryGroups > 0 {
			view.Degradations = append(view.Degradations, Degradation{Kind: DegradationActivationBlocked, Replica: replica,
				Text: fmt.Sprintf("%d held back (%s): %s; timeline keys to delete: %s", snapshot.Activation.BlockedQueryGroups,
					snapshot.Activation.BlockedReasons, snapshot.Activation.BlockedSamples, snapshot.Activation.BlockedKeys)})
		}
	}
	if header := snapshot.ActivationHeader; header != nil {
		age := header.MissingSeconds
		view.Degradations = append(view.Degradations, Degradation{Kind: DegradationActivationHeaderMissing, Replica: replica,
			Text: "last rebuild: " + header.LastRebuild, AgeSeconds: &age})
	}
	if snapshot.Rebalance != nil && (view.Rebalance == nil || snapshot.Rebalance.PlannedAt.After(view.Rebalance.PlannedAt)) {
		facts := *snapshot.Rebalance
		view.Rebalance, view.RebalanceReplica = &facts, replica
	}
	if snapshot.AssignmentScope != nil && (view.AssignmentScope == nil || snapshot.AssignmentScope.At.After(view.AssignmentScope.At)) {
		facts := *snapshot.AssignmentScope
		view.AssignmentScope, view.AssignmentScopeReplica = &facts, replica
	}
	if snapshot.AssignmentSweep != nil && (view.AssignmentSweep == nil || snapshot.AssignmentSweep.At.After(view.AssignmentSweep.At)) {
		facts := *snapshot.AssignmentSweep
		view.AssignmentSweep, view.AssignmentSweepReplica = &facts, replica
	}
	if snapshot.LeaderRound != nil && (view.LeaderRound == nil || snapshot.LeaderRound.At.After(view.LeaderRound.At)) {
		facts := *snapshot.LeaderRound
		view.LeaderRound, view.LeaderRoundReplica = &facts, replica
	}
	if snapshot.ViewStream != nil && snapshot.ViewStream.Leading {
		if snapshot.ViewStream.PublishFailingBeyondBound {
			view.Degradations = append(view.Degradations, Degradation{Kind: DegradationViewPublishFailing, Replica: replica,
				Text: snapshot.ViewStream.PublishFailureReason})
		}
		if snapshot.ViewStream.NoSessionsBeyondBound {
			view.Degradations = append(view.Degradations, Degradation{Kind: DegradationViewStreamNoSessions, Replica: replica})
		}
	}
	if snapshot.ViewStream != nil && viewStreamPreferred(view.ViewStream, snapshot.ViewStream) {
		facts := *snapshot.ViewStream
		facts.Lagging = append(make([]ViewStreamLagging, 0, len(snapshot.ViewStream.Lagging)), snapshot.ViewStream.Lagging...)
		view.ViewStream, view.ViewStreamReplica = &facts, replica
	}
	if snapshot.Source != nil && (view.Source == nil || snapshot.Source.At.After(view.Source.At)) {
		facts := *snapshot.Source
		view.Source, view.SourceReplica = &facts, replica
	}
}
