// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package main

import (
	"context"
	"errors"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/access"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/fleet"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/ownership"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/redisfailure"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/trigger"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/worker"
)

// slotStoreCall is the store call that did not answer behind a failed Slot,
// for its completion row, and whether that call is what failed the Slot.
//
// The call is the one its error carries, or, for a Slot whose error is its
// own deadline and nothing more, the call of this attempt that ran out the
// time it was issued with (redisfailure.Witness): a store that turns a failed
// read into a status, and a worker that then stops on the Slot's deadline,
// leave the error with nothing to say the time ran out inside the store. nil
// when there is neither.
//
// A call that failed with time left -- refused, cut, no Sentinel, no pooled
// connection, a read past the client's own timeout -- failed the Slot. A call
// that ran out the time it was issued with did only when that wait was at
// least half of the attempt: one issued with the last moments of a Slot
// whose time went elsewhere, or one that ran out a short time of its own
// before the Slot went on and spent the rest, is only the last thing that
// waited. Its facts still go on the row.
func slotStoreCall(err error, witness *redisfailure.Witness, attempt time.Duration) (*redisfailure.CallError, bool) {
	call := redisfailure.CallErrorOf(err)
	if call == nil && errors.Is(err, context.DeadlineExceeded) {
		call = witness.Call()
	}
	if call == nil {
		return nil, false
	}
	return call, !call.RanOutItsTime() || 2*call.Waited >= attempt
}

// storeCallFacts is a Slot's store call that did not answer (slotStoreCall),
// for its completion line; nil for none.
func storeCallFacts(call *redisfailure.CallError) *observability.StoreCallFacts {
	if call == nil {
		return nil
	}
	return &observability.StoreCallFacts{Client: call.Client, Caller: call.Caller, Reason: call.Reason,
		Waited: call.Waited, Left: call.Left, LeftKnown: call.LeftKnown}
}

// slotFailureReason names a failed Slot's reason and, where the refusal has
// one, the apply site that produced it.
//
// internal_unknown is the answer for a failure this site looked at and could
// not name. It is meant to be rare, and every word added here was added
// because it was not: a failure that names itself one level down and is not
// asked about here arrives as an unclassified defect, and a deployment cannot
// tell one such failure from another or count how often either happens.
//
// A function of its own so the classification can be exercised without
// standing up a runtime. The block it came from decides a Slot's whole
// completion line, and a rule nobody can call is a rule nobody tests.
//
// storeCall is the Slot's store call that did not answer (slotStoreCall),
// nil for none.
func slotFailureReason(err error, storeCall *redisfailure.CallError) (observability.ReasonCode, string) {
	reason := observability.ReasonInternalUnknown
	gapApplySite := ""
	if errors.Is(err, access.ErrFrozenQueryPlanUnavailable) {
		reason = observability.ReasonContractDeterministic
	}
	// A gap guard conflict is a classifiable refusal, and it repeats on every
	// round for the same Query Group. internal_unknown is where a site that
	// looked at a failure and could not name it puts things; this one has a
	// name and carries the two values it compared.
	// A Slot the ownership store refused part-way -- the Query Group moved
	// while the Slot ran, at its fence check or its side-effect admission --
	// names the refusal, the same word the handover itself reports.
	if refusal, refused := ownership.RefusalReason(err); refused {
		reason = observability.ReasonCode(refusal)
	}
	// A Slot whose query never reached its backend because the permit wait
	// ran out at the query's deadline: the query's own word, which the query
	// failure facts have carried all along.
	// A host-placed series whose alert identity no CMDB index could read
	// fails its query as a query that could not be answered
	// (series_identity, QUERY_UNAVAILABLE).
	// A query that ran out of its time at the provider - its request
	// cancelled or expired at its own deadline, or the provider stopping
	// partway - names the provider's word too, as its query_completed line
	// does, rather than reading as an internal error.
	var admission interface{ QueryFailure() (string, string) }
	if errors.As(err, &admission) {
		if category, code := admission.QueryFailure(); category == observability.QueryFailureCategoryAdmission ||
			category == observability.QueryFailureCategoryProviderTransport || category == observability.QueryFailureCategoryDelivery ||
			category == observability.QueryFailureCategorySeriesIdentity {
			reason = observability.ReasonCode(code)
		}
	}
	// The Slot's own retained bytes over its Query Group's share: the same
	// word the capacity rejection line already gives it. It arrived here as
	// internal_unknown on every retry of a Slot no retry could fit.
	if _, refused := execution.ShareRefusalOf(err); refused {
		reason = observability.ReasonCode(contract.ReasonQGBudgetShareExceeded)
	}
	// Within its share and refused for what others hold of the pool: the
	// capacity rejection line's word, not internal_unknown.
	if worker.PoolHeldByOthers(err) {
		reason = observability.ReasonCode(contract.ReasonResourcePoolHeldByOthers)
	}
	// A Slot that has to be read in slices and cannot be, by why.
	if word, sliced := worker.InputSliceRefusal(err); sliced {
		reason = observability.ReasonCode(word)
	}
	if conflict, named := worker.GapGuardConflictReason(err); named {
		reason = observability.ReasonCode(conflict)
	}
	if conflict, named := worker.StateConflictReason(err); named {
		reason = observability.ReasonCode(conflict)
	}
	// Two series batches disagreeing about one Plan's gap marker. The
	// Slot-wide merge resolves every other difference between batches and
	// refuses only this one, so without a word here the one shape it cannot
	// resolve is also the one nobody can count.
	if disagree, named := worker.GapGuardDisagreeReason(err); named {
		reason = observability.ReasonCode(disagree)
	}
	// Loaded Runtime State whose Level contract is not the compiled Plan's.
	// The error has carried this code in the query failure facts since it was
	// written; the completion line was not asking, so a deployment where every
	// Query Group reported it read as thousands of unclassified defects.
	var contractMismatch *execution.StateContractMismatchError
	if errors.As(err, &contractMismatch) {
		reason = observability.ReasonCode(contract.ReasonStateLevelContractMismatch)
	}
	// The trigger evaluator refusing its own state before deciding. Which
	// invariant failed stays in the error's text and on the line's operation
	// field; the word is the class, because one word per invariant is a
	// vocabulary nobody can hold.
	var triggerInvariant *trigger.InternalErrorV2
	if errors.As(err, &triggerInvariant) {
		reason = observability.ReasonCode(contract.ReasonTriggerInvariant)
	}
	// The gap marker store's own refusals, which are not the same as the
	// conflict above: that one is this Slot refusing before it writes, these
	// are the store refusing the write because the marker moved under it. Both
	// were internal_unknown, so a Query Group conflicting on every other Slot
	// for half an hour arrived as an unclassified defect that the same-Slot
	// retry then cleared.
	// A no-data memory read in parts that moved under the read, or a part
	// the store could not answer: retried, and named rather than
	// internal_unknown (worker.NoDataRosterMovedError).
	if refusal, named := worker.NoDataRosterReadReason(err); named {
		reason = observability.ReasonCode(refusal)
	}
	// A Slot with a segment skip word whose stable segments did not read
	// fails its first attempt by that name (014 B3 §9.7).
	if unread, named := worker.SegmentReadFailedReason(err); named {
		reason = observability.ReasonCode(unread)
	}
	if refusal, named := worker.GapApplyReason(err); named {
		reason = observability.ReasonCode(refusal)
		// The site goes on the line as its own field, not only inside the
		// error text. Which of the Slot's two applies refused is the question
		// this refusal exists to answer, and an answer that has to be parsed
		// out of a sentence is one nobody can group or count by.
		gapApplySite = worker.GapApplySiteOf(err)
	}
	// A Slot whose own store did not answer: a call of a job whose data this
	// deployment keeps (fleet.StoreOwnCaller) that had time to reach the
	// store and failed there, marked by the call hook, in the error or in the
	// attempt's witness. A gap preflight whose read ran out its time on a hung
	// master arrived here as nothing more than "context deadline exceeded" and
	// read as an unclassified defect. It is the store's word only when nothing
	// above named the Slot, and not for another job's store on the same
	// client.
	if reason == observability.ReasonInternalUnknown && storeCall != nil && fleet.StoreOwnCaller(storeCall.Caller) {
		reason = observability.ReasonCode(contract.ReasonRedisUnavailable)
	}
	return reason, gapApplySite
}
