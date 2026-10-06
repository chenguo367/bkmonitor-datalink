package fleet

import (
	"encoding/json"
	"sort"
)

// ReadHoldFacts is a compact projection of the owner's fenced record. An
// absent projection is unknown; it is never interpreted as a zero hold.
type ReadHoldFacts struct {
	Millis              int64  `json:"read_hold_ms"`
	ArrivalAgeMillis    int64  `json:"arrival_age_ms"`
	LimitMillis         int64  `json:"limit_ms"`
	AtLimit             bool   `json:"at_limit"`
	RaisedAfterLowering uint64 `json:"raised_after_lowering"`
	// NoWholeWindowArrival is the samples classed window read early in
	// which no series arrived whole after the first read.
	NoWholeWindowArrival uint64  `json:"no_whole_window_arrival"`
	Rung                 string  `json:"rung,omitempty"`
	Buckets              []int64 `json:"buckets,omitempty"`
	Annotation           string  `json:"annotation"`
}

func withinReadHoldBudget(facts map[string]ReadHoldFacts, budget int) map[string]ReadHoldFacts {
	if budget <= 0 || len(facts) == 0 {
		return facts
	}
	keys := make([]string, 0, len(facts))
	for key := range facts {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	kept, spent := map[string]ReadHoldFacts{}, 2
	for _, key := range keys {
		entry, err := json.Marshal(map[string]ReadHoldFacts{key: facts[key]})
		size := len(entry) - 2
		if len(kept) > 0 {
			size++
		}
		if err != nil || spent+size > budget {
			break
		}
		spent += size
		kept[key] = facts[key]
	}
	return kept
}
