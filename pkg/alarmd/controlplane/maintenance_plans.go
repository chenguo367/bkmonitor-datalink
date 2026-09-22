package controlplane

import (
	"context"
	"errors"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/strategy"
)

// MaintenancePlan is the current activated plan, independent of query cadence
// and input availability. Compiled content uses the ordinary bounded compiler
// cache. Callers must hold and validate the QG's existing ownership session.
type MaintenancePlan struct {
	Identity execution.PlanIdentity
	Compiled *strategy.CompiledPlan
	// Partial detector compilation cannot prove every level inactive, but the
	// executable levels still need membership tracking for ordinary recovery.
	CloseUnavailable bool
}

func (runtime *RedisCatalogRuntime) CurrentPlans(ctx context.Context, qg execution.QueryGroupIdentity, at execution.EvaluationTime) ([]MaintenancePlan, error) {
	segment, err := runtime.readPersistedSegment(ctx, qg, at)
	if err != nil {
		return nil, err
	}
	s := segment.Schedule.Segment
	publication := SnapshotPublicationRef{SnapshotRevision: s.Publication.SnapshotRevision, PublicationEpoch: uint64(s.Publication.PublicationEpoch)}
	group, err := runtime.repository.LoadSegmentQueryGroup(ctx, s, at, func(ctx context.Context) (QueryGroup, error) {
		return runtime.repository.loadPublishedQueryGroup(ctx, publication, qg)
	})
	if err != nil {
		return nil, err
	}
	active := make(map[execution.PlanIdentity]PlanActivationRecord, len(segment.Plans))
	for _, record := range segment.Plans {
		active[record.Fact.Plan] = record
	}
	plans := make([]MaintenancePlan, 0, len(group.Plans))
	for _, plan := range group.Plans {
		record, ok := active[plan.Identity]
		if !ok || record.Fact.Selected.ScheduleRevision != plan.ScheduleRevision {
			continue
		}
		result, err := runtime.compiler.Compile(ctx, strategy.CompileRequest{Plan: plan.Plan,
			DatasetContract: group.QueryPlan.Normalization.DatasetContract, StateSemantics: runtime.stateSemantics})
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		compiled, ok := result.Plan()
		if err != nil || !ok || result.PlanTerminal() != nil {
			if err == nil {
				err = errors.New("activated maintenance plan cannot be compiled")
			}
			runtime.repository.observe(ctx, observability.Observation{Component: observability.ComponentRuntime,
				Stage: observability.StageEffectiveTimeMaintenance, Result: observability.ResultDegraded,
				ReasonCode: observability.ReasonInternalUnknown, Err: err,
				Trace: observability.TraceFields{QueryGroupKey: string(qg)}})
			continue
		}
		plans = append(plans, MaintenancePlan{Identity: plan.Identity, Compiled: compiled, CloseUnavailable: len(result.LevelTerminals()) != 0})
	}
	return plans, nil
}
