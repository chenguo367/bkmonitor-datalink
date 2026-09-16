// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package linkdoutput

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
)

func decision(mutate func(*contract.TriggerEventV1)) *contract.TriggerEventV1 {
	event := &contract.TriggerEventV1{
		EventID: strings.Repeat("a", 64), EventKind: contract.TriggerEventAbnormal,
		EventSemanticDigest: strings.Repeat("c", 64),
		PrimaryLevelID:      2, TenantID: "tenant-a", BusinessID: "2",
		SignalType: contract.SignalTypeMetric,
		StrategyRef: &contract.StrategySnapshotRef{
			TenantID: "tenant-a", BusinessID: 2, StrategyID: 123, Revision: 7,
		},
		DedupeMD5:      strings.Repeat("b", 32),
		EvaluationTime: 1756684860,
		RecordRef: contract.TriggerRecordRefV1{SourceTime: 1756684800, Dimensions: map[string]json.RawMessage{
			"bk_target_ip":       json.RawMessage(`"127.0.0.1"`),
			"bk_target_cloud_id": json.RawMessage(`"0"`),
			"device":             json.RawMessage(`"sda"`),
		}},
		Observed: contract.TriggerObservedV1{Values: map[string]json.RawMessage{"value": json.RawMessage(`92.5`)}, Unit: "%"},
		LevelResults: []contract.LevelResultV1{{
			LevelID: 2, Priority: 1, Result: contract.LevelResultAbnormal,
			DecisionWindow: contract.DecisionWindowV1{Trigger: contract.TriggerWindowEvidenceV1{
				WindowSize: 5, RequiredAnomalies: 2, ObservedAnomalies: 3, AnomalyBeginTime: 1756684680,
			}},
		}},
		Subject: &contract.MonitorSubjectContext{
			Subject:    contract.MonitorSubject{Type: contract.MonitorSubjectHost, ID: "127.0.0.1|0"},
			Dimensions: map[string]json.RawMessage{"device": json.RawMessage(`"sda"`)},
		},
	}
	if mutate != nil {
		mutate(event)
	}
	return event
}

// noDataDecision is the same strategy's absence round: the tag in the record's
// dimensions, the period count carried on the point, and no measurement.
func noDataDecision() *contract.TriggerEventV1 {
	return decision(func(event *contract.TriggerEventV1) {
		event.RecordRef.Dimensions[contract.NoDataDimensionTag] = json.RawMessage("true")
		event.Observed = contract.TriggerObservedV1{Values: map[string]json.RawMessage{
			contract.NoDataPeriodFactField: json.RawMessage("5"),
		}}
		event.DedupeMD5 = strings.Repeat("d", 32)
	})
}

func convert(t *testing.T, event *contract.TriggerEventV1) map[string]json.RawMessage {
	t.Helper()
	converter, err := NewConverter(func() time.Time { return time.Unix(1756684870, 0).UTC() }, nil)
	if err != nil {
		t.Fatalf("NewConverter() error = %v", err)
	}
	written, err := converter.Convert(event)
	if err != nil {
		t.Fatalf("Convert() error = %v", err)
	}
	var message map[string]json.RawMessage
	if err := json.Unmarshal(written.Payload, &message); err != nil {
		t.Fatalf("decode written message: %v", err)
	}
	return message
}

func assertFields(t *testing.T, message map[string]json.RawMessage, want map[string]string) {
	t.Helper()
	for field, expected := range want {
		if got := string(message[field]); got != expected {
			t.Errorf("%s = %s, want %s", field, got, expected)
		}
	}
}

// The whole message, field by field, because every one of them is read by
// something downstream and a silently wrong one is not a failure there.
//
// The field names are the consumer's, taken from its RawEvent contract, not
// names chosen here: a message whose fields this process finds reasonable and
// the consumer does not read is a message that arrives and does nothing.
func TestAnAbnormalDecisionIsWrittenAsARawEvent(t *testing.T) {
	message := convert(t, decision(nil))
	assertFields(t, message, map[string]string{
		"bk_tenant_id":  `"tenant-a"`,
		"event_id":      `"` + strings.Repeat("a", 64) + `"`,
		"alert_id":      `"` + strings.Repeat("b", 32) + `"`,
		"data_time":     `"2025-09-01T00:00:00Z"`,
		"occurred_time": `"2025-09-01T00:01:00Z"`,
		"title":         `"Strategy 123 level 2 triggered"`,
		"content":       `"value=92.5% at 2025-09-01T00:00:00Z; 3 of 5 points in the window were anomalous"`,
		"action":        `"triggered"`,
		"severity":      `"warning"`,
		// Whole, with the target identity still in it.
		"dimensions":  `{"bk_target_cloud_id":"0","bk_target_ip":"127.0.0.1","device":"sda"}`,
		"observation": `{"signal_type":"metric","evaluation_family":"metric_algorithm","value":92.5,"unit":"%","observed_at":"2025-09-01T00:00:00Z"}`,
		"subject":     `{"type":"HOST","native_id":"127.0.0.1|0"}`,
		"strategy":    `{"id":"123","version":"7","bk_biz_id":"2"}`,
		"extra": `{"anomaly_begin_time":"2025-08-31T23:58:00Z","window":{"size":5,"anomalies":3,"required":2},` +
			`"event_semantic_digest":"` + strings.Repeat("c", 64) + `","emitted_time":"2025-09-01T00:01:10Z"}`,
	})
	// The fields the consumer fills are absent rather than empty. A value here
	// is either overwritten, which makes it noise, or believed, which makes it
	// a fact this process did not establish.
	for _, field := range []string{"record_id", "received_time", "alert_start_time", "status", "action_reason", "labels", "produced_at", "alarm_source_id"} {
		if _, written := message[field]; written {
			t.Errorf("%s = %s, want it left to the consumer", field, message[field])
		}
	}
}

// The recovery of the same series: the same alert_id, the consumer's own word
// for it, and the round that decided it.
func TestARecoveryDecisionIsWrittenAsARawEvent(t *testing.T) {
	message := convert(t, decision(func(event *contract.TriggerEventV1) {
		event.EventKind = contract.TriggerEventRecovery
		event.LevelResults[0].Result = contract.LevelResultRecovery
		event.EvaluationTime = 1756685160
	}))
	assertFields(t, message, map[string]string{
		"alert_id":      `"` + strings.Repeat("b", 32) + `"`,
		"action":        `"recovered"`,
		"title":         `"Strategy 123 level 2 recovered"`,
		"occurred_time": `"2025-09-01T00:06:00Z"`,
		"data_time":     `"2025-09-01T00:00:00Z"`,
	})
}

// An absence round: its own family, no value, the tag in the dimensions, and
// the period count as the evaluation carried it.
func TestANoDataDecisionIsWrittenAsARawEvent(t *testing.T) {
	message := convert(t, noDataDecision())
	assertFields(t, message, map[string]string{
		"action":   `"triggered"`,
		"alert_id": `"` + strings.Repeat("d", 32) + `"`,
		"content":  `"no data for 5 periods, as of 2025-09-01T00:00:00Z"`,
		// evaluation_family is what tells this apart from a threshold alert on
		// the same series for a consumer that routes on it; the tag in the
		// dimensions is what tells the two apart for one that does not.
		"observation": `{"signal_type":"metric","evaluation_family":"no_data"}`,
		"dimensions":  `{"__NO_DATA_DIMENSION__":true,"bk_target_cloud_id":"0","bk_target_ip":"127.0.0.1","device":"sda"}`,
	})
	var extra map[string]json.RawMessage
	if err := json.Unmarshal(message["extra"], &extra); err != nil {
		t.Fatal(err)
	}
	if string(extra["no_data_periods"]) != "5" {
		t.Fatalf("extra.no_data_periods = %s, want the count the evaluation carried", extra["no_data_periods"])
	}
}

// The action has two values and no others.
//
// updated and closed are the consumer's, and both are lifetime decisions: one
// says an open alert changed, the other that it timed out. This process sees
// neither, and writing one would take a decision away from the only component
// that can make it.
// The words are written out rather than taken from this package's constants.
// Checking a produced action against ActionRecovered would pass for any string
// that constant happened to hold, including the one the previous protocol
// used; these two are the consumer's vocabulary, and they are the thing under
// test.
func TestTheActionIsOnlyEverTriggeredOrRecovered(t *testing.T) {
	want := map[string]string{
		contract.TriggerEventAbnormal: "triggered",
		contract.TriggerEventRecovery: "recovered",
	}
	seen := map[string]bool{}
	for kind, expected := range want {
		action, err := actionFor(kind)
		if err != nil {
			t.Fatalf("kind %s: %v", kind, err)
		}
		if action != expected {
			t.Fatalf("kind %s produced action %q, want %q", kind, action, expected)
		}
		seen[action] = true
	}
	if len(seen) != 2 {
		t.Fatalf("the two decision kinds produced %d actions, want one each", len(seen))
	}
	// And nothing else is a decision kind.
	for _, kind := range []string{"", "UPDATED", "CLOSED", contract.LevelResultNormal} {
		if _, err := actionFor(kind); err == nil {
			t.Fatalf("kind %q produced an action", kind)
		}
	}
}

// The target's identity fields stay in the dimensions.
//
// The consumer computes its fingerprint from the strategy id and these
// dimensions. With the identity taken out -- which is what the previous
// protocol did, moving it into the subject -- every object of one strategy
// projects onto the same dimensions, so they all collapse into one alert. The
// subject carries the identity too, and that is not a duplicate: the subject is
// for resolving the instance, the dimensions are for telling instances apart.
func TestTheTargetIdentityStaysInTheDimensions(t *testing.T) {
	message := convert(t, decision(nil))
	var dimensions map[string]json.RawMessage
	if err := json.Unmarshal(message["dimensions"], &dimensions); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"bk_target_ip", "bk_target_cloud_id", "device"} {
		if _, kept := dimensions[field]; !kept {
			t.Fatalf("dimensions = %v, want %s: the consumer's fingerprint reads these, and without the "+
				"identity every object of this strategy is one alert", dimensions, field)
		}
	}
}

// Two rounds of the same series carry the same alert key, and a different
// series carries a different one.
//
// The consumer uses it to tie a recovery to the trigger it closes, so a key
// that moved between rounds would leave every alert open; one shared across
// series would close somebody else's.
func TestTheAlertKeyIsTheSeriesDedupeKey(t *testing.T) {
	event := decision(nil)
	first := convert(t, event)
	second := convert(t, decision(func(e *contract.TriggerEventV1) {
		// A later round of the same series: a new point, a new decision time.
		e.RecordRef.SourceTime = 1756684860
		e.EvaluationTime = 1756684920
	}))
	if string(first["alert_id"]) != string(second["alert_id"]) {
		t.Fatalf("alert_id = %s then %s; two rounds of one series must carry one key",
			first["alert_id"], second["alert_id"])
	}
	if string(first["alert_id"]) != `"`+event.DedupeMD5+`"` {
		t.Fatalf("alert_id = %s, want the series dedupe key %q", first["alert_id"], event.DedupeMD5)
	}
	other := convert(t, noDataDecision())
	if string(other["alert_id"]) == string(first["alert_id"]) {
		t.Fatalf("the absence alert and the threshold alert on one series share key %s", first["alert_id"])
	}
}

// data_time is the data point, occurred_time is the round. Reading the clock
// for either would make every replayed Slot claim the anomaly happened now.
func TestTheTimesAreTheDataPointAndTheRoundAndNotTheClock(t *testing.T) {
	message := convert(t, decision(nil))
	var extra map[string]json.RawMessage
	if err := json.Unmarshal(message["extra"], &extra); err != nil {
		t.Fatal(err)
	}
	if string(message["data_time"]) == string(extra["emitted_time"]) ||
		string(message["occurred_time"]) == string(extra["emitted_time"]) {
		t.Fatalf("message = %v: the data point and the round must come from the decision, not the clock", message)
	}
}

// The platform's three levels are named; a level beyond them is passed through
// rather than rejected or guessed at, because levels are stated in the strategy
// snapshot and the platform may grow more.
func TestSeverityNamesTheBuiltInLevelsAndPassesOthersThrough(t *testing.T) {
	for name, test := range map[string]struct {
		level contract.LevelResultV1
		want  string
	}{
		"fatal":                     {contract.LevelResultV1{LevelID: 1}, "critical"},
		"warning":                   {contract.LevelResultV1{LevelID: 2}, "warning"},
		"remind":                    {contract.LevelResultV1{LevelID: 3}, "info"},
		"a level with its own name": {contract.LevelResultV1{LevelID: 9, LevelCode: "notice"}, "notice"},
		"a level with no name":      {contract.LevelResultV1{LevelID: 9}, "level_9"},
	} {
		if got := severityFor(test.level); got != test.want {
			t.Errorf("%s: severity = %q, want %q", name, got, test.want)
		}
	}
	// A derived name is the one case the consumer cannot map, so it has to be
	// distinguishable from the rest: it means this build has no mapping for a
	// level the platform grew, and the alert will land on a default severity.
	if SeverityIsBuiltIn(contract.LevelResultV1{LevelID: 9}) {
		t.Fatal("a level with neither a built-in name nor its own must be reported as unmapped")
	}
	if !SeverityIsBuiltIn(contract.LevelResultV1{LevelID: 9, LevelCode: "notice"}) {
		t.Fatal("a level that names itself is mapped")
	}
}

// A record with no object is a real answer. Writing a subject for it would
// attach the alert to something.
func TestARecordWithNoObjectCarriesNoSubject(t *testing.T) {
	message := convert(t, decision(func(e *contract.TriggerEventV1) {
		e.Subject = &contract.MonitorSubjectContext{Dimensions: map[string]json.RawMessage{}}
	}))
	if _, written := message["subject"]; written {
		t.Fatalf("subject = %s, want none", message["subject"])
	}
	// And the dimensions are still whole: no subject is not a reason to drop
	// what the record said.
	var dimensions map[string]json.RawMessage
	if err := json.Unmarshal(message["dimensions"], &dimensions); err != nil {
		t.Fatal(err)
	}
	if len(dimensions) != 3 {
		t.Fatalf("dimensions = %v, want the record's own", dimensions)
	}
}

// A Plan this build could not label leaves the field out rather than guessing.
// A consumer routing on signal_type would send a log alert down the metric path
// on a wrong value, and has nothing to notice it by; an absent field it can see.
func TestAnUnnamedSignalTypeIsOmittedRatherThanGuessed(t *testing.T) {
	message := convert(t, decision(func(e *contract.TriggerEventV1) { e.SignalType = "" }))
	var observation map[string]json.RawMessage
	if err := json.Unmarshal(message["observation"], &observation); err != nil {
		t.Fatal(err)
	}
	if _, written := observation["signal_type"]; written {
		t.Fatalf("observation = %v, want no signal type", observation)
	}
	if string(observation["evaluation_family"]) != `"metric_algorithm"` {
		t.Fatalf("observation = %v: the family is decided here and is always written", observation)
	}
}

// A decision with no frozen revision has no alert identity here, so it is
// refused rather than written with holes in it.
func TestADecisionWithNoAlertIdentityIsRefused(t *testing.T) {
	for name, mutate := range map[string]func(*contract.TriggerEventV1){
		"no strategy revision": func(e *contract.TriggerEventV1) { e.StrategyRef = nil },
		"no series identity":   func(e *contract.TriggerEventV1) { e.DedupeMD5 = "" },
	} {
		converter, err := NewConverter(time.Now, nil)
		if err != nil {
			t.Fatalf("NewConverter() error = %v", err)
		}
		if _, err := converter.Convert(decision(mutate)); err == nil {
			t.Fatalf("%s: Convert() accepted a decision with no alert identity", name)
		}
	}
}
