package main

import (
	"encoding/json"
	"net/http"
	"slices"
	"sort"
	"strconv"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/fleet"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/lookback"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/readhold"
)

type lookbackGroupReading struct {
	QueryGroup     execution.QueryGroupIdentity `json:"query_group"`
	HoldKnown      bool                         `json:"hold_known"`
	ReadHoldMillis int64                        `json:"read_hold_ms"`
	ReadHold       *readhold.Record             `json:"read_hold,omitempty"`
	Annotation     string                       `json:"annotation,omitempty"`
	Supplement     *lookback.SupplementReading  `json:"supplement,omitempty"`
	Counters       *lookback.GroupReading       `json:"counters,omitempty"`
}

type lookbackGroupPage struct {
	Total  int                    `json:"total_owned"`
	Next   string                 `json:"next,omitempty"`
	Groups []lookbackGroupReading `json:"groups"`
}

func (holds *productionReadHolds) groupPage(engine *lookback.Engine, after string, limit int) lookbackGroupPage {
	holds.mu.Lock()
	groups := make([]execution.QueryGroupIdentity, 0, len(holds.groups))
	for qg := range holds.groups {
		groups = append(groups, qg)
	}
	holds.mu.Unlock()
	groups = slices.DeleteFunc(groups, func(qg execution.QueryGroupIdentity) bool {
		_, err := holds.owner(qg)
		return err != nil
	})
	sort.Slice(groups, func(i, j int) bool { return groups[i] < groups[j] })
	page := lookbackGroupPage{Total: len(groups), Groups: []lookbackGroupReading{}}
	start := sort.Search(len(groups), func(i int) bool { return string(groups[i]) > after })
	end := min(start+limit, len(groups))
	for _, qg := range groups[start:end] {
		inspection := holds.controller.Inspect(qg)
		// A record that did not decode says nothing until the group's next
		// write replaces it.
		record, known := inspection.Record, inspection.Loaded && !inspection.Corrupt && (!inspection.Missing || inspection.Seeded)
		row := lookbackGroupReading{QueryGroup: qg, HoldKnown: known}
		if known {
			row.ReadHold = &record
			row.ReadHoldMillis = record.HoldMillis
			if record.PendingHoldMillis != nil {
				row.ReadHoldMillis = *record.PendingHoldMillis
			}
			row.Annotation = "alarmd 当前自动推后 " + strconv.FormatFloat(float64(row.ReadHoldMillis)/1000, 'f', -1, 64) + " 秒"
		}
		if engine != nil {
			if reading, found := engine.GroupReading(qg); found {
				row.Counters = &reading
			}
			if reading, found := engine.SupplementReading(qg); found {
				row.Supplement = &reading
			}
		}
		page.Groups = append(page.Groups, row)
	}
	if end < len(groups) && end > start {
		page.Next = string(groups[end-1])
	}
	return page
}

type lookbackAPIReading struct {
	cliLookbackReading
	WorkerID string `json:"worker_id"`
	lookbackGroupPage
}

// The answering replica reads its own bounded memory. Native APIs retain the
// same surface policy as the other fleet reads; no diagnostic Redis/CLI pool.
func withLookbackAPI(next http.Handler, engine *lookback.Engine, standing lookbackStanding, holds *productionReadHolds, now func() time.Time) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/lookback" {
			next.ServeHTTP(w, r)
			return
		}
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "GET required", http.StatusMethodNotAllowed)
			return
		}
		limit := 200
		after := r.URL.Query().Get("after")
		if len(after) > 256 {
			http.Error(w, "after must be at most 256 bytes", http.StatusBadRequest)
			return
		}
		if value := r.URL.Query().Get("limit"); value != "" {
			parsed, err := strconv.Atoi(value)
			if err != nil || parsed < 1 || parsed > 200 {
				http.Error(w, "limit must be 1..200", http.StatusBadRequest)
				return
			}
			limit = parsed
		}
		reading := lookbackAPIReading{cliLookbackReading: cliLookbackReading{Scope: "answering_replica", ReadAt: now().UTC(), lookbackStanding: standing}}
		if engine != nil {
			stats := productionLookbackStats(engine, holds)
			reading.Stats = &stats
		}
		if holds != nil {
			reading.WorkerID = holds.cfg.PhaseTwo.Worker.ID
			reading.lookbackGroupPage = holds.groupPage(engine, after, limit)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(reading)
	})
}

func productionLookbackStats(engine *lookback.Engine, holds *productionReadHolds) lookback.Stats {
	stats := engine.Stats()
	if holds != nil {
		stats.ReadHoldTransitions, stats.ReadHoldTransitionOvertaken = holds.transitions.Load(), holds.overtaken.Load()
		// Every reason and source at zero: a series that is absent reads as
		// a build without it.
		controller := holds.controller.Stats()
		stats.ReadHoldPredecessors = make(map[string]uint64, len(readhold.PredecessorReasons)+len(readhold.LinkSkipReasons))
		for _, reason := range readhold.PredecessorReasons {
			stats.ReadHoldPredecessors[reason] = controller.Predecessors[reason]
		}
		holds.linksMu.Lock()
		for _, reason := range readhold.LinkSkipReasons {
			stats.ReadHoldPredecessors[reason] = holds.links[reason]
		}
		holds.linksMu.Unlock()
		stats.ReadHoldClamped = map[string]uint64{readhold.ClampKnown: controller.Clamped[readhold.ClampKnown],
			readhold.ClampFallback: controller.Clamped[readhold.ClampFallback]}
		stats.ReadHoldOwnCorrupt, stats.ReadHoldRetireCloseFailed = controller.OwnCorrupt, holds.retireCloseFailed.Load()
		stats.ReadHoldCloseSkipped = holds.closeSkipped.Load()
	}
	return stats
}

func (holds *productionReadHolds) fleetFacts() map[string]fleet.ReadHoldFacts {
	holds.mu.Lock()
	groups := make([]execution.QueryGroupIdentity, 0, len(holds.groups))
	for qg := range holds.groups {
		groups = append(groups, qg)
	}
	holds.mu.Unlock()
	facts := make(map[string]fleet.ReadHoldFacts)
	for _, qg := range groups {
		if _, err := holds.owner(qg); err != nil {
			continue
		}
		inspection := holds.controller.Inspect(qg)
		if !inspection.Loaded || inspection.Missing || inspection.Corrupt {
			continue
		}
		record := inspection.Record
		millis := record.HoldMillis
		if record.PendingHoldMillis != nil {
			millis = *record.PendingHoldMillis
		}
		facts[string(qg)] = fleet.ReadHoldFacts{Millis: millis, ArrivalAgeMillis: record.ArrivalAgeMillis, LimitMillis: record.LimitMillis,
			AtLimit: record.AtLimit, RaisedAfterLowering: record.RaisedAfterLowering, NoWholeWindowArrival: record.Noise, Rung: record.Rung,
			Buckets:    append([]int64(nil), record.Buckets[:min(len(record.Buckets), fleet.MaxReadEarlyBuckets)]...),
			Annotation: "alarmd 当前自动推后 " + strconv.FormatFloat(float64(millis)/1000, 'f', -1, 64) + " 秒"}
	}
	return facts
}
