package observability

// CapacityRejectionFacts is a log-only snapshot of one failed shared
// reservation. Values use CapacityBudget units; nil OwnUsed means unknown,
// not zero. No query, key, URL, or raw error is accepted here.
type CapacityRejectionFacts struct {
	Phase                        string
	OwnUsed                      *uint64
	SharedUsed, Requested, Limit uint64
	// Usage is what this execution had taken of every budget at the moment one
	// of them refused, each against the limit it was measured with.
	//
	// Without it a rejection names only the budget that was reached, and
	// "the count refused while the byte budget was ninety percent free" cannot
	// be said - which is the one sentence that distinguishes a process at its
	// memory limit from a process stopped by a number standing in for memory.
	// Reading it from the ceilings in /metrics does not work either: those give
	// the limits, never what this Slot had of them.
	Usage []CapacityBudgetUsage
}

// CapacityBudgetUsage is one budget's own usage at the moment of a rejection,
// beside the limit it was compared against.
type CapacityBudgetUsage struct {
	Budget  CapacityBudget
	OwnUsed uint64
	Limit   uint64
}

func normalizeCapacityRejection(observation Observation) *CapacityRejectionFacts {
	if observation.Component != ComponentResource || observation.Stage != StageResourceHard ||
		observation.Err == nil || observation.CapacityBudget == "" || observation.CapacityRejection == nil {
		return nil
	}
	f := *observation.CapacityRejection
	switch f.Phase {
	case "normal_input", "normal_gap", "normal_output", "slot_output", "query_free", "snapshot_prepare":
	default:
		f.Phase = "other"
	}
	if f.OwnUsed != nil {
		own := *f.OwnUsed
		f.OwnUsed = &own
	}
	// Copied, and only for budgets this build names: the facts arrive from the
	// caller's own struct, and a row is a log line rather than a place to
	// forward whatever a future budget calls itself.
	if len(f.Usage) != 0 {
		usage := make([]CapacityBudgetUsage, 0, len(f.Usage))
		for _, entry := range f.Usage {
			if !knownCapacityBudget(entry.Budget) {
				continue
			}
			usage = append(usage, entry)
		}
		f.Usage = usage
	}
	return &f
}

func knownCapacityBudget(budget CapacityBudget) bool {
	switch budget {
	case CapacityBudgetSeries, CapacityBudgetRetainedBytes, CapacityBudgetStateMutations,
		CapacityBudgetEvents, CapacityBudgetGapMutations:
		return true
	}
	return false
}

// SlotBudgetUsageFacts is what one Slot took of each budget, reported on its
// completion row whatever the outcome.
//
// It is on the completion row rather than only on rejections because a number
// that exists only when something failed has no distribution behind it: there
// is nothing to take a median of, so "this budget is the one under pressure"
// cannot be told from "this budget is nowhere near its limit". Both halves are
// carried - what was used and what it was measured against - because a usage
// without its limit is not a reading, and the limits in /metrics are the
// process ceilings rather than what this Slot was admitted against.
type SlotBudgetUsageFacts struct {
	StateMutations uint64 `json:"state_mutations"`
	GapMutations   uint64 `json:"gap_mutations"`
	Events         uint64 `json:"events"`
	RetainedBytes  uint64 `json:"retained_bytes"`
	Series         uint64 `json:"series"`

	StateMutationsLimit uint64 `json:"state_mutations_limit"`
	GapMutationsLimit   uint64 `json:"gap_mutations_limit"`
	EventsLimit         uint64 `json:"events_limit"`
	RetainedBytesLimit  uint64 `json:"retained_bytes_limit"`
	SeriesLimit         uint64 `json:"series_limit"`
}
