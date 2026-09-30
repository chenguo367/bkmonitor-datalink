package controlplane

import (
	"context"
	"errors"
	"sort"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

func carryReadHoldLinks(records []PlanActivationRecord, group execution.QueryGroupIdentity, previous map[execution.PlanKey]ReadHoldPredecessorRef, carried map[execution.PlanKey]PlanActivationRecord) {
	for i := range records {
		key := records[i].Fact.Key()
		if ref, ok := previous[key]; ok && ref.QueryGroup != group {
			copy := ref
			records[i].PreviousReadHold = &copy
		} else if old := carried[key].PreviousReadHold; old != nil {
			copy := *old
			records[i].PreviousReadHold = &copy
		}
	}
}

// ReadHoldPredecessors reads only this Segment's retained Plan links. The
// metadata does not participate in the frozen execution contract or QG ID.
type ReadHoldPredecessor struct {
	ReadHoldPredecessorRef
	Plans []execution.PlanKey
}

func (repository *RedisCatalogRepository) ReadHoldPredecessors(ctx context.Context, schedule execution.FrozenQueryGroupSchedule) ([]ReadHoldPredecessor, error) {
	timeline, err := repository.loadScheduleTimelineHinted(ctx, schedule.Segment.QueryGroup)
	if err != nil {
		return nil, err
	}
	for _, segment := range timeline.Segments {
		if segment.Schedule.Segment.Start != schedule.Segment.Start {
			continue
		}
		seen := map[ReadHoldPredecessorRef]int{}
		var refs []ReadHoldPredecessor
		for _, plan := range segment.Plans {
			if plan.PreviousReadHold == nil {
				continue
			}
			ref := *plan.PreviousReadHold
			if ref.QueryGroup == "" || ref.QueryGroup == schedule.Segment.QueryGroup || ref.ClosedAt <= 0 || ref.ClosedAt > schedule.Segment.Start {
				return nil, errors.New("alarmd controlplane: invalid read hold predecessor")
			}
			index, ok := seen[ref]
			if !ok {
				index = len(refs)
				seen[ref] = index
				refs = append(refs, ReadHoldPredecessor{ReadHoldPredecessorRef: ref})
			}
			refs[index].Plans = append(refs[index].Plans, plan.Fact.Key())
		}
		sort.Slice(refs, func(i, j int) bool {
			if refs[i].QueryGroup != refs[j].QueryGroup {
				return refs[i].QueryGroup < refs[j].QueryGroup
			}
			return refs[i].ClosedAt < refs[j].ClosedAt
		})
		return refs, nil
	}
	return nil, ErrScheduleUnavailable
}
