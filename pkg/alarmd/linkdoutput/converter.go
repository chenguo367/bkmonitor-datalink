// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

// Package linkdoutput writes a decision as the RawEvent the alert link daemon
// consumes.
//
// It carries facts and nothing else: whether this object is now triggered or
// recovered, at which level, from which data point, and what the round saw.
// Everything an alert needs on top of that -- when the alert started, whether
// this supersedes an earlier level, when it closes, why it recovered, which
// model and instance the subject resolves to -- belongs to the consumer, which
// is why none of it is computed here and no state is kept for it.
//
// The field names and the action words are the consumer's, copied from its
// RawEvent contract rather than chosen here. Where this writes something the
// contract does not name -- subject.type, observation.value, extra -- it is
// named in the projection contract and the consumer ignores what it does not
// read; where the contract names a field this process has no fact for --
// record_id, received_time, alert_start_time, status, inst_id, model_id -- it
// is left out, because the consumer fills those and a value here would either
// be overwritten or believed.
package linkdoutput

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
)

// The two actions this process produces.
//
// updated and closed are the consumer's other two and are deliberately never
// written: a repeated triggered is how a continuing anomaly is stated, and
// closing is a lifetime decision made on a timeout this process does not
// observe.
const (
	ActionTriggered = "triggered"
	ActionRecovered = "recovered"
)

// The platform's alert levels, which are a closed set of three
// (constants/alert.py: FATAL / WARNING / REMIND). They are spelled out here
// because the consumer reads the severity as a name rather than a number.
var builtInSeverities = map[uint32]string{1: "critical", 2: "warning", 3: "info"}

// The evaluation families this process produces. A keyword strategy on a log
// or event source is still an algorithm over a counted series here, so it is
// metric_algorithm; only absence detection is its own family.
const (
	evaluationFamilyMetricAlgorithm = "metric_algorithm"
	evaluationFamilyNoData          = "no_data"
)

// Event is one message on the wire.
type Event struct {
	EventID   string
	AlertID   string
	Payload   []byte
	Severity  string
	Action    string
	SubjectKd string
}

// wireSubject is the alert instance.
//
// inst_id and model_id are the consumer's to fill: it resolves them from its
// own model, and this process does not know their vocabulary. type is what
// this process does know -- its own target kind -- and is what the resolution
// is configured against.
type wireSubject struct {
	Type     string `json:"type"`
	NativeID string `json:"native_id"`
}

// wireObservation is what the event was observed from and how it was judged.
type wireObservation struct {
	SignalType       string          `json:"signal_type,omitempty"`
	EvaluationFamily string          `json:"evaluation_family"`
	Value            json.RawMessage `json:"value,omitempty"`
	Unit             string          `json:"unit,omitempty"`
	ObservedAt       string          `json:"observed_at,omitempty"`
}

type wireStrategy struct {
	ID         string `json:"id"`
	Version    string `json:"version"`
	BusinessID string `json:"bk_biz_id"`
}

type wireWindow struct {
	Size      uint32 `json:"size"`
	Anomalies uint32 `json:"anomalies"`
	Required  uint32 `json:"required"`
}

type wireExtra struct {
	AnomalyBeginTime     string                     `json:"anomaly_begin_time,omitempty"`
	Window               *wireWindow                `json:"window,omitempty"`
	AdditionalDimensions map[string]json.RawMessage `json:"additional_dimensions,omitempty"`
	EventSemanticDigest  string                     `json:"event_semantic_digest,omitempty"`
	NoDataPeriods        json.RawMessage            `json:"no_data_periods,omitempty"`
	EmittedTime          string                     `json:"emitted_time,omitempty"`
}

type wireEvent struct {
	TenantID     string                     `json:"bk_tenant_id"`
	EventID      string                     `json:"event_id"`
	AlertID      string                     `json:"alert_id"`
	DataTime     string                     `json:"data_time"`
	OccurredTime string                     `json:"occurred_time"`
	Title        string                     `json:"title"`
	Content      string                     `json:"content"`
	Action       string                     `json:"action"`
	Dimensions   map[string]json.RawMessage `json:"dimensions"`
	Severity     string                     `json:"severity"`
	Observation  wireObservation            `json:"observation"`
	Subject      *wireSubject               `json:"subject,omitempty"`
	Strategy     wireStrategy               `json:"strategy"`
	Extra        wireExtra                  `json:"extra"`
}

// Converter turns decisions into wire messages. Now is injected because the
// emitted time is the only value that is the moment of writing.
type Converter struct {
	now func() time.Time
	// onUnmappedSeverity is called for each event whose level had no name in
	// this build. Such an event still ships - refusing it would silence an
	// alert over a naming gap - but it arrives at the consumer under a default
	// severity, and that is the only place the gap is visible.
	onUnmappedSeverity func(level uint32)
}

func NewConverter(now func() time.Time, onUnmappedSeverity func(level uint32)) (*Converter, error) {
	if now == nil {
		return nil, errors.New("alarmd linkdoutput: a clock is required")
	}
	return &Converter{now: now, onUnmappedSeverity: onUnmappedSeverity}, nil
}

// Convert writes one decision.
//
// A decision without a frozen strategy revision cannot be written: the alert it
// would open is identified by a key this process derives from that revision,
// and the strategy it names only exists there. The caller decides what to do
// about such a decision - this returns an error rather than a message with
// holes in it.
func (converter *Converter) Convert(event *contract.TriggerEventV1) (Event, error) {
	if converter == nil || event == nil {
		return Event{}, errors.New("alarmd linkdoutput: a decision is required")
	}
	if event.StrategyRef == nil {
		return Event{}, errors.New("alarmd linkdoutput: a decision without a frozen strategy revision has no alert identity")
	}
	if event.DedupeMD5 == "" {
		return Event{}, errors.New("alarmd linkdoutput: a decision without a series identity has no alert identity")
	}
	action, err := actionFor(event.EventKind)
	if err != nil {
		return Event{}, err
	}
	primary, err := primaryLevel(event)
	if err != nil {
		return Event{}, err
	}
	severity := severityFor(primary)
	if !SeverityIsBuiltIn(primary) && converter.onUnmappedSeverity != nil {
		converter.onUnmappedSeverity(primary.LevelID)
	}
	// The record's own dimensions, whole. The object's identity fields stay in
	// here rather than being taken out into the subject: the consumer's
	// fingerprint and its dimension enrichment both read this map, and a
	// fingerprint computed over dimensions with the identity removed would put
	// every object of one strategy under one alert.
	dimensions := event.RecordRef.Dimensions
	if dimensions == nil {
		dimensions = map[string]json.RawMessage{}
	}
	var subject *wireSubject
	var additional map[string]json.RawMessage
	if event.Subject != nil {
		subject = wireSubjectFor(event.Subject.Subject)
		additional = event.Subject.Subject.Additional
	}
	noData := isNoDataEvent(dimensions)
	message := wireEvent{
		TenantID: event.TenantID, EventID: event.EventID, AlertID: event.DedupeMD5,
		DataTime: wireTime(event.RecordRef.SourceTime), OccurredTime: wireTime(event.EvaluationTime),
		Title: title(event, primary, action), Content: content(event, primary, noData),
		Action: action, Dimensions: dimensions, Severity: severity,
		Observation: observationFor(event, noData),
		Subject:     subject,
		Strategy: wireStrategy{
			ID: strconv.FormatInt(event.StrategyRef.StrategyID, 10),
			// The frozen revision. The consumer stores it beside the alert so
			// two alerts of one strategy can be told apart by the configuration
			// that produced them.
			Version:    strconv.FormatInt(event.StrategyRef.Revision, 10),
			BusinessID: event.BusinessID,
		},
		Extra: wireExtra{
			Window: &wireWindow{
				Size:      primary.DecisionWindow.Trigger.WindowSize,
				Anomalies: primary.DecisionWindow.Trigger.ObservedAnomalies,
				Required:  primary.DecisionWindow.Trigger.RequiredAnomalies,
			},
			AdditionalDimensions: additional,
			EventSemanticDigest:  event.EventSemanticDigest,
			EmittedTime:          wireTime(converter.now().Unix()),
		},
	}
	if begin := primary.DecisionWindow.Trigger.AnomalyBeginTime; begin > 0 {
		// In extra and not in alert_start_time. It is the earliest anomalous
		// point still inside this window, so it moves as the window slides;
		// written as the alert's start it would make a long-running alert look
		// like it started again every round. The consumer takes the start from
		// the trigger it opened the alert on.
		message.Extra.AnomalyBeginTime = wireTime(begin)
	}
	if noData {
		message.Extra.NoDataPeriods = event.Observed.Values[contract.NoDataPeriodFactField]
	}
	payload, err := json.Marshal(message)
	if err != nil {
		return Event{}, fmt.Errorf("alarmd linkdoutput: encode decision: %w", err)
	}
	kind := ""
	if subject != nil {
		kind = subject.Type
	}
	return Event{
		EventID: event.EventID, AlertID: event.DedupeMD5, Payload: payload,
		Severity: severity, Action: action, SubjectKd: kind,
	}, nil
}

func actionFor(kind string) (string, error) {
	switch kind {
	case contract.TriggerEventAbnormal:
		return ActionTriggered, nil
	case contract.TriggerEventRecovery:
		return ActionRecovered, nil
	default:
		return "", fmt.Errorf("alarmd linkdoutput: decision kind %q has no action", kind)
	}
}

// isNoDataEvent reads the tag the absence evaluation puts in the group's
// dimensions.
//
// From the dimensions rather than from anything about the Plan: a strategy that
// detects no-data also detects thresholds, so the Plan cannot say which of the
// two this event is. The tag is on the series, which is what the event is
// about, and it is the same thing the existing alert pipeline reads.
func isNoDataEvent(dimensions map[string]json.RawMessage) bool {
	_, tagged := dimensions[contract.NoDataDimensionTag]
	return tagged
}

func observationFor(event *contract.TriggerEventV1, noData bool) wireObservation {
	observation := wireObservation{
		SignalType:       event.SignalType,
		EvaluationFamily: evaluationFamilyMetricAlgorithm,
	}
	if noData {
		// No value, on purpose. The synthetic point a no-data round produces
		// carries a marker rather than a measurement, and publishing it as the
		// observed value would put a number on a page whose whole subject is
		// that there was no number.
		observation.EvaluationFamily = evaluationFamilyNoData
		return observation
	}
	observation.Unit = event.Observed.Unit
	observation.ObservedAt = wireTime(event.RecordRef.SourceTime)
	observation.Value = soleObservedValue(event.Observed)
	return observation
}

// soleObservedValue is the observation's value when there is exactly one, and
// nothing when there is not.
//
// A decision over several values has no single observed value, and picking one
// would state a measurement the strategy did not make. The values are all in
// the content either way, which is where a reader sees them.
func soleObservedValue(observed contract.TriggerObservedV1) json.RawMessage {
	if len(observed.Values) != 1 {
		return nil
	}
	for _, value := range observed.Values {
		return value
	}
	return nil
}

// primaryLevel returns the level the event was aggregated to. The aggregation
// already happened - lowest priority among the levels that agree with the
// event's kind, then lowest level id - so this only has to find it again.
func primaryLevel(event *contract.TriggerEventV1) (contract.LevelResultV1, error) {
	for _, level := range event.LevelResults {
		if level.LevelID == event.PrimaryLevelID {
			return level, nil
		}
	}
	return contract.LevelResultV1{}, fmt.Errorf(
		"alarmd linkdoutput: primary level %d is absent from the decision", event.PrimaryLevelID,
	)
}

// severityFor names the level for the consumer.
//
// The three built-in levels are the platform's whole set today and their names
// do not change. A level outside them is not rejected: levels are stated in the
// strategy snapshot and the platform may extend them, so the snapshot's own
// identifier is used when it has one, and otherwise an identifier derived from
// the level. Neither case invents a business meaning - a consumer that does not
// recognise the name maps it or falls back on its own terms.
func severityFor(level contract.LevelResultV1) string {
	if name, builtIn := builtInSeverities[level.LevelID]; builtIn {
		return name
	}
	if level.LevelCode != "" {
		return level.LevelCode
	}
	return "level_" + strconv.FormatUint(uint64(level.LevelID), 10)
}

// SeverityIsBuiltIn reports whether a level had a name of its own rather than
// one derived from its number. A derived name is the signal that the platform
// grew a level this build has no mapping for, which is worth counting: the
// consumer will fall back to its default severity and the alert arrives at the
// wrong level, quietly.
func SeverityIsBuiltIn(level contract.LevelResultV1) bool {
	_, builtIn := builtInSeverities[level.LevelID]
	return builtIn || level.LevelCode != ""
}

// wireSubjectFor writes this process's own target kind and target id.
//
// The kind is not translated into the consumer's model name: that mapping is
// the consumer's configuration, it differs per deployment, and a guess here
// would attach an alert to the wrong model in a way nothing downstream could
// tell from a right one.
func wireSubjectFor(subject contract.MonitorSubject) *wireSubject {
	if subject.Type == "" || subject.ID == "" {
		// A record with no object is a real answer - custom reporting with no
		// target dimensions has none - and no subject says so. Inventing one
		// would attach the alert to something.
		return nil
	}
	return &wireSubject{Type: subject.Type, NativeID: subject.ID}
}

func wireTime(epochSeconds int64) string {
	return time.Unix(epochSeconds, 0).UTC().Format(time.RFC3339)
}

// title and content are a plain statement of the decision. They are a starting
// point the consumer enriches with the strategy and the resource, so they name
// only what this process established, and never guess a metric's meaning.
func title(event *contract.TriggerEventV1, primary contract.LevelResultV1, action string) string {
	return fmt.Sprintf("Strategy %d level %d %s", event.StrategyRef.StrategyID, primary.LevelID, action)
}

func content(event *contract.TriggerEventV1, primary contract.LevelResultV1, noData bool) string {
	window := primary.DecisionWindow.Trigger
	if noData {
		return fmt.Sprintf(
			"no data for %s periods, as of %s",
			noDataPeriods(event.Observed), wireTime(event.RecordRef.SourceTime),
		)
	}
	values := observedValues(event.Observed)
	if values == "" {
		values = "no value"
	}
	return fmt.Sprintf(
		"%s at %s; %d of %d points in the window were anomalous",
		values, wireTime(event.RecordRef.SourceTime), window.ObservedAnomalies, window.WindowSize,
	)
}

// noDataPeriods is how many periods the group has been silent, as the absence
// evaluation counted it and carried it on the point. It is not recomputed here
// from anything: this process would have to know the period and the clock to do
// that, and the number would then disagree with the one that was detected on.
func noDataPeriods(observed contract.TriggerObservedV1) string {
	if raw, carried := observed.Values[contract.NoDataPeriodFactField]; carried {
		return string(raw)
	}
	return "an unknown number of"
}

func observedValues(observed contract.TriggerObservedV1) string {
	names := make([]string, 0, len(observed.Values))
	for name := range observed.Values {
		names = append(names, name)
	}
	sort.Strings(names)
	rendered := ""
	for _, name := range names {
		if rendered != "" {
			rendered += ", "
		}
		rendered += name + "=" + string(observed.Values[name])
		if observed.Unit != "" {
			rendered += observed.Unit
		}
	}
	return rendered
}
