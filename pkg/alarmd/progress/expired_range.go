package progress

import (
	"context"
	"errors"
	"math"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/ownership"
)

func (store *Store) BeginRange(ctx context.Context, request execution.ExpiredRangeRequest) (execution.ExpiredRangeResult, error) {
	return store.changeRange(ctx, request, false)
}

func (store *Store) CommitRange(ctx context.Context, request execution.ExpiredRangeRequest) (execution.ExpiredRangeResult, error) {
	return store.changeRange(ctx, request, true)
}

func (store *Store) changeRange(ctx context.Context, request execution.ExpiredRangeRequest, commit bool) (execution.ExpiredRangeResult, error) {
	if err := ctx.Err(); err != nil {
		return execution.ExpiredRangeResult{}, err
	}
	if err := request.Validate(); err != nil {
		return execution.ExpiredRangeResult{}, err
	}
	identity := execution.ProgressIdentity{QueryGroup: request.Projection.First.Contract.Slot.QueryGroup}
	name, err := store.namespace(identity)
	if err != nil {
		return execution.ExpiredRangeResult{}, err
	}
	result, err := fencedWrite(ctx, store, ProgressReadRange, identity.QueryGroup, name, request.OwnerFence, rangeIO(),
		func(result execution.ExpiredRangeResult) bool { return result.Status == execution.ProgressConflict },
		func(raw []byte, missing bool) (execution.ExpiredRangeResult, error) {
			return store.changeRangeOn(ctx, request, commit, identity, name, raw, missing)
		})
	// Counted once, from the pass the write ended on, as a commit is.
	if err == nil && result.Status == execution.ProgressConflict && store.options.Conflicts != nil {
		store.options.Conflicts(ProgressConflictRange, execution.ConflictWord(result.Conflict))
	}
	return result, err
}

// rangeConflict is an expired range refused by the fact refusal names.
func rangeConflict(refusal execution.ProgressSkipRefusal) execution.ExpiredRangeResult {
	return execution.ExpiredRangeResult{Status: execution.ProgressConflict, Conflict: refusal}
}

// changeRangeOn is changeRange on the record raw (missing when there is
// none).
func (store *Store) changeRangeOn(ctx context.Context, request execution.ExpiredRangeRequest, commit bool,
	identity execution.ProgressIdentity, name string, raw []byte, missing bool) (execution.ExpiredRangeResult, error) {
	p := request.Projection
	kind, reason := p.CompletionKind(), p.CompletionReason()
	// A range only starts from an actually persisted NextSlot; cold starts
	// retain the existing single Slot path.
	if missing {
		return rangeConflict(execution.SkipRefusalProgressMissing), nil
	}
	current, err := decode(raw)
	if err != nil {
		return execution.ExpiredRangeResult{}, &DeterministicInvalidError{Err: err}
	}
	if current.Identity != identity {
		return rangeConflict(execution.ConflictIdentityMismatch), nil
	}
	if current.UnfinishedSlot != nil {
		return rangeConflict(execution.ConflictSlotInFlight), nil
	}
	if current.UnfinishedRange == nil && current.NextSlot >= p.Next {
		return execution.ExpiredRangeResult{Status: execution.ProgressCommitted, AlreadyCommitted: true}, nil
	}
	switch {
	case current.NextSlot != p.First.Contract.Slot.EvaluationTime:
		return rangeConflict(execution.SkipRefusalCursorMoved), nil
	case current.UnfinishedRange != nil && !current.UnfinishedRange.Equal(p):
		return rangeConflict(execution.SkipRefusalRangeInFlight), nil
	case commit && current.UnfinishedRange == nil:
		return rangeConflict(execution.ConflictRangeMissing), nil
	}
	// Recheck the actual successor on both Begin and Commit. A later cutover
	// cannot silently turn the stored proof into a different range.
	next, err := store.options.Slots.NextSlotAfter(ctx, identity.QueryGroup, p.Last.Contract.Slot.EvaluationTime)
	if err != nil {
		return execution.ExpiredRangeResult{}, err
	}
	if next != p.Next {
		return rangeConflict(execution.ConflictGridMoved), nil
	}
	if prior := current.CurrentOrRecentGap; prior != nil && current.LastCompletionKind == kind &&
		prior.Kind == kind && prior.ReasonCode == reason && prior.Count > math.MaxUint32-p.Count {
		continuous, readErr := store.options.Slots.NextSlotAfter(ctx, identity.QueryGroup, prior.LastSlot)
		if readErr != nil {
			return execution.ExpiredRangeResult{}, readErr
		}
		if continuous == p.First.Contract.Slot.EvaluationTime {
			return execution.ExpiredRangeResult{}, errors.New("progress: expired range recent Gap count overflow before Begin")
		}
	}
	if commit {
		gap := &execution.ProgressGapSummary{Kind: kind,
			ReasonCode: reason,
			FirstSlot:  p.First.Contract.Slot.EvaluationTime, LastSlot: p.Last.Contract.Slot.EvaluationTime, Count: p.Count}
		prior := current.CurrentOrRecentGap
		if current.LastCompletionKind == gap.Kind && prior != nil && prior.Kind == gap.Kind && prior.ReasonCode == gap.ReasonCode {
			continuous, err := store.options.Slots.NextSlotAfter(ctx, identity.QueryGroup, prior.LastSlot)
			if err != nil {
				return execution.ExpiredRangeResult{}, err
			}
			if continuous == gap.FirstSlot {
				if prior.Count > math.MaxUint32-p.Count {
					return execution.ExpiredRangeResult{}, errors.New("progress: expired range recent Gap count overflow")
				}
				gap.FirstSlot, gap.Count = prior.FirstSlot, prior.Count+p.Count
				gap.NextProbeAt = prior.NextProbeAt
			}
		}
		current.NextSlot, current.LastCompletionKind = p.Next, gap.Kind
		current.CurrentOrRecentGap, current.UnfinishedRange = gap, nil
	} else {
		copy := p.Clone()
		current.UnfinishedRange = &copy
	}
	value, err := encode(current)
	if err != nil {
		return execution.ExpiredRangeResult{}, err
	}
	// The fence's refusals come back with a status and the ownership error
	// beside it, so the status is read first: an error alone is an IO failure,
	// and judged before the status a stale fence or a moved scope would be
	// named REDIS_UNAVAILABLE and retried on an IO backoff.
	status, _ := store.options.Control.FencedCompareAndSet(ctx, ownership.FencedCASRequest{
		Fence: request.OwnerFence, Namespace: name, ContentScope: request.ContentScope,
		Expected: raw, Value: value, TTL: 0,
	})
	store.settleWrite(identity.QueryGroup, request.OwnerFence, status, false, raw, value)
	switch status {
	case ownership.FencedCASApplied:
		return execution.ExpiredRangeResult{Status: execution.ProgressCommitted}, nil
	case ownership.FencedCASConflict:
		return rangeConflict(execution.SkipRefusalCASConflict), nil
	case ownership.FencedCASStaleOwner:
		return execution.ExpiredRangeResult{Status: execution.ProgressStaleOwner}, nil
	case ownership.FencedCASContentMoved:
		return execution.ExpiredRangeResult{Status: execution.ProgressContentMoved}, nil
	default:
		return rangeIO(), nil
	}
}

func rangeIO() execution.ExpiredRangeResult {
	return execution.ExpiredRangeResult{Status: execution.ProgressRetryableIO, ReasonCode: execution.ReasonCode(contract.ReasonRedisUnavailable)}
}

var _ execution.ExpiredRangeStore = (*Store)(nil)
