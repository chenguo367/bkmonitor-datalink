package worker

import "github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"

// queryAvailabilityEvidence folds the existing validated binding traversal.
// Duplicate consumers of a physical query do not change these set predicates.
// Aggregate PRIMARY facts and completion causes deliberately play no role.
type queryAvailabilityEvidence struct {
	primarySeen           bool
	primaryNotUnavailable bool
	available             bool
	primaryDelivered      bool
	nonBackendFailure     bool
	// reason is the first unavailable primary's own reason.
	reason execution.ReasonCode
}

func (e *queryAvailabilityEvidence) observe(binding execution.NamedInputBinding, physical execution.PhysicalQueryCompletion, streamed, sourceBackend bool) {
	if binding.Role != execution.InputRolePrimary {
		return
	}
	e.primarySeen = true
	e.nonBackendFailure = e.nonBackendFailure || (physical.Completeness == execution.CompletenessUnavailable && !sourceBackend)
	e.primaryNotUnavailable = e.primaryNotUnavailable || physical.Completeness != execution.CompletenessUnavailable
	e.primaryDelivered = e.primaryDelivered || streamed
	if physical.Completeness == execution.CompletenessUnavailable && e.reason == "" {
		e.reason = execution.AttributedReason(binding.ReasonCode, binding.UnavailableAttribution)
	}
	// FULL_EMPTY establishes availability too, even when a local consumer or
	// algorithm fails. PARTIAL needs actual usable data, not merely an empty
	// partial response. An UNAVAILABLE completion invalidates provisional data.
	e.available = e.available || physical.Completeness == execution.CompletenessFull ||
		(physical.Completeness == execution.CompletenessPartial && streamed && binding.Disposition == execution.AccessAvailable)
}

func (e queryAvailabilityEvidence) availability() execution.QueryAvailability {
	if e.available {
		return execution.QueryAvailabilityAvailable
	}
	if e.primarySeen && !e.primaryNotUnavailable && !e.primaryDelivered && !e.nonBackendFailure {
		return execution.QueryAvailabilityUnavailable
	}
	return execution.QueryAvailabilityUnknown
}

// unavailableReason is why the primary query was unavailable, when it was.
func (e queryAvailabilityEvidence) unavailableReason() execution.ReasonCode {
	if e.availability() != execution.QueryAvailabilityUnavailable {
		return ""
	}
	return e.reason
}
