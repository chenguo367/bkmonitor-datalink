// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package uq

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// The states an output of a named_outputs/v1 answer is in, as the provider
// writes them.
const (
	answerSuccess      = "SUCCESS"
	answerSuccessEmpty = "SUCCESS_EMPTY"
	answerPartial      = "PARTIAL"
	answerError        = "ERROR"
)

// namedOutputName is the label the provider names a PromQL series' metric
// by. Two outputs of one expression differ in it whenever one is a bare
// selector, so it is left out of the key the outputs are aligned on - and
// only there: the series' own identity keeps every label it has.
const namedOutputName = "__name__"

// namedOutputAnswer is one entry of a named_outputs/v1 answer's outputs.
type namedOutputAnswer struct {
	ReferenceName string            `json:"reference_name"`
	State         string            `json:"state"`
	Series        []json.RawMessage `json:"series"`
	Status        *responseStatus   `json:"status"`
	IsPartial     *bool             `json:"is_partial"`
}

// alignKey is where a point sits: its source time, as detection's records
// keep it, and its labels without __name__.
type alignKey struct {
	time   int64
	labels string
}

// legacyPoint is one record the legacy output delivered, as the alignment
// needs it.
type legacyPoint struct {
	recordID string
	key      alignKey
	value    json.RawMessage
}

// answeredOutput is a non-legacy output as it answered: its state and its
// points by where they sit.
type answeredOutput struct {
	state  execution.NamedOutputState
	points map[alignKey]json.RawMessage
}

// namedOutputDecoding is a named-outputs query's answer as it is read: the
// legacy output goes the way every query's series go and is noted point by
// point as it is delivered; every other output is indexed. Nothing is
// aligned until the whole answer is in, because the outputs come in the
// order the request listed them and an output's state is not known before
// its entry ends.
type namedOutputDecoding struct {
	outputs *execution.NamedOutputs
	// contract is the answer's contract_version, empty for a provider that
	// answered as if nothing was asked (the older provider).
	contract string
	legacy   *namedOutputAnswer
	answered map[string]*answeredOutput
	points   []legacyPoint
	// malformed is an answer that broke the contract: an output named twice
	// or by no name. duplicate is an output with two points at one key.
	malformed bool
	duplicate bool
}

func newNamedOutputDecoding(spec execution.PhysicalQuerySpec) *namedOutputDecoding {
	if spec.PlanFacts.NamedOutputs == nil {
		return nil
	}
	return &namedOutputDecoding{outputs: spec.PlanFacts.NamedOutputs, answered: map[string]*answeredOutput{}}
}

// alignLabels is a series' labels as the alignment keys them: every group
// key but __name__, in order, each with its value as the provider wrote it.
func alignLabels(series responseSeries) string {
	pairs := make([]string, 0, len(series.GroupKeys))
	for index, key := range series.GroupKeys {
		if key == namedOutputName {
			continue
		}
		value := nullDimension
		if index < len(series.GroupValues) {
			value = series.GroupValues[index]
		}
		pairs = append(pairs, key+"\x00"+string(value))
	}
	sort.Strings(pairs)
	return strings.Join(pairs, "\x01")
}

// acceptOutputs reads the outputs array of a named_outputs/v1 answer: the
// legacy output's series are accepted as any query's are, every other
// output is indexed.
func (session *seriesDecoder) acceptOutputs(ctx context.Context, decoder *json.Decoder) error {
	named := session.named
	start, err := decoder.Token()
	if err != nil || start != json.Delim('[') {
		return fmt.Errorf("alarmd access uq: outputs must be an array")
	}
	seen := make(map[string]bool, len(named.outputs.Outputs))
	for decoder.More() {
		if err := ctx.Err(); err != nil {
			return err
		}
		var answer namedOutputAnswer
		if err := decoder.Decode(&answer); err != nil {
			return fmt.Errorf("alarmd access uq: decode output: %w", err)
		}
		if answer.ReferenceName == "" || seen[answer.ReferenceName] {
			named.malformed = true
			continue
		}
		seen[answer.ReferenceName] = true
		if answer.ReferenceName == named.outputs.LegacyRef {
			for _, raw := range answer.Series {
				if err := session.acceptRaw(ctx, raw); err != nil {
					return err
				}
			}
			answer.Series = nil
			named.legacy = &answer
			continue
		}
		if !named.asked(answer.ReferenceName) {
			// An output nobody asked for is no output of this query.
			continue
		}
		if err := session.indexOutput(ctx, answer); err != nil {
			return err
		}
	}
	_, err = decoder.Token()
	return err
}

func (named *namedOutputDecoding) asked(ref string) bool {
	for _, output := range named.outputs.Outputs {
		if output.Ref == ref {
			return true
		}
	}
	return false
}

// acceptRaw is one series of the answer as it arrived, accepted.
func (session *seriesDecoder) acceptRaw(ctx context.Context, raw json.RawMessage) error {
	if int64(len(raw)) > session.client.limits.MaxSeriesBytes {
		return ErrSeriesBytesExceeded
	}
	var series responseSeries
	if err := json.Unmarshal(raw, &series); err != nil {
		return fmt.Errorf("alarmd access uq: decode series: %w", err)
	}
	return session.accept(ctx, series, uint64(len(raw)))
}

// indexOutput keeps every point of one non-legacy output by where it sits.
// Its series count against the same response limits the legacy output's
// do: they are read into memory as theirs are.
func (session *seriesDecoder) indexOutput(ctx context.Context, answer namedOutputAnswer) error {
	spec := session.attempt.Spec
	output := &answeredOutput{state: answeredState(answer), points: map[alignKey]json.RawMessage{}}
	session.named.answered[answer.ReferenceName] = output
	for _, raw := range answer.Series {
		if err := ctx.Err(); err != nil {
			return err
		}
		if int64(len(raw)) > session.client.limits.MaxSeriesBytes {
			return ErrSeriesBytesExceeded
		}
		var series responseSeries
		if err := json.Unmarshal(raw, &series); err != nil {
			return fmt.Errorf("alarmd access uq: decode series: %w", err)
		}
		session.totalSeries++
		if session.totalSeries > session.client.limits.MaxSeries {
			return ErrTotalSeriesExceeded
		}
		if uint64(len(series.Values)) > session.client.limits.MaxRecords-session.totalRecords {
			return ErrTotalRecordsExceeded
		}
		session.totalRecords += uint64(len(series.Values))
		if len(series.Columns) == 0 || len(series.Columns) != len(series.Types) {
			return fmt.Errorf("alarmd access uq: invalid series schema")
		}
		labels := alignLabels(series)
		for _, row := range series.Values {
			if len(row) != len(series.Columns) {
				return fmt.Errorf("alarmd access uq: row width differs from columns")
			}
			timestamp, value, err := rowFacts(series.Columns, row, spec.PlanFacts.QueryList)
			if err != nil {
				return err
			}
			sourceTime, err := spec.PlanFacts.Normalization.NormalizeSourceTime(timestamp)
			if err != nil {
				return err
			}
			key := alignKey{time: sourceTime, labels: labels}
			if _, exists := output.points[key]; exists {
				session.named.duplicate = true
				continue
			}
			output.points[key] = value
		}
	}
	return nil
}

// answeredState is the state an output answered in, as an aligned value
// reads it: a state the provider left out is read from is_partial, as the
// platform reads it; an empty success is a success with no points; and a
// state this build does not know is read as an error, whose value nobody
// shows.
func answeredState(answer namedOutputAnswer) execution.NamedOutputState {
	switch answer.State {
	case answerSuccess, answerSuccessEmpty:
		return execution.NamedOutputSuccess
	case answerPartial:
		return execution.NamedOutputPartial
	case answerError:
		return execution.NamedOutputError
	case "":
		if answer.IsPartial != nil && *answer.IsPartial {
			return execution.NamedOutputPartial
		}
		return execution.NamedOutputSuccess
	default:
		return execution.NamedOutputError
	}
}

// noteDelivered keeps the points of a legacy series that was delivered.
func (named *namedOutputDecoding) noteDelivered(series responseSeries, dataset *execution.Dataset, valueField string) {
	labels := alignLabels(series)
	for index := 0; index < dataset.Len(); index++ {
		record, _ := dataset.Record(index)
		value, _ := record.Value(valueField)
		named.points = append(named.points, legacyPoint{recordID: record.RecordID(),
			key: alignKey{time: record.SourceTime(), labels: labels}, value: value})
	}
}

// legacyAnswer is the status and is_partial the legacy output answered with,
// read where the older answer's top level carries them. The legacy output's
// own are all that decide its completion: the answer's top level gathers
// every output's, and an optional output that failed does not fail the
// query.
//
// An ERROR output carries no series and its own status; one that came
// without a status is given one, so that it fails the query as every status
// code does rather than reading as a query that found nothing - a timed-out
// legacy output is not an empty one.
func (named *namedOutputDecoding) legacyAnswer() (*responseStatus, *bool) {
	status, isPartial := named.legacy.Status, named.legacy.IsPartial
	switch named.legacy.State {
	case answerError:
		if status == nil || status.Code == "" {
			status = &responseStatus{Code: answerError}
		}
	case answerPartial:
		if isPartial != nil && !*isPartial {
			partial := true
			isPartial = &partial
		}
	}
	return status, isPartial
}

// finishNamed completes a named-outputs query's answer: its legacy output as
// any query's answer, then every output aligned to the records it
// delivered. An answer that broke the contract, or an output with two points
// at one key, is a query that is not used.
func (session *seriesDecoder) finishNamed(ctx context.Context, status *responseStatus, isPartial *bool, resultTableIDs []string) (execution.ProviderCompletion, error) {
	named := session.named
	refused := ""
	switch {
	case named.contract == "":
		// The older provider answered as if nothing was asked: the top level
		// is the legacy output's.
	case named.contract != execution.NamedOutputsContract || named.malformed || named.legacy == nil:
		refused = execution.ResponseFailureNamedOutputContract
	default:
		status, isPartial = named.legacyAnswer()
	}
	completion, err := session.finish(ctx, status, isPartial, resultTableIDs)
	if err != nil || completion.Completeness == execution.CompletenessUnavailable && refused == "" {
		return completion, err
	}
	reason := execution.ReasonCode(contract.ReasonQueryUnavailable)
	if refused == "" {
		if records := named.align(completion.Completeness); records != nil && !named.duplicate {
			completion.NamedOutputs = records
			return completion, nil
		}
		refused, reason = execution.ResponseFailureNamedOutputDuplicateKey, execution.ReasonCode(contract.ReasonQueryNamedOutputDuplicateKey)
	}
	// The legacy series were handed on already; the completion keeps their
	// state and delivery only so that delivery is conserved, and every
	// binding of an unavailable completion is UNKNOWN.
	return session.client.responseContractUnavailable(session.attempt, reason, execution.ResponseRouteDetail(refused),
		completion.DataState, completion.Delivery, completion.RouteFacts.ResultTableIDs, completion.Stats), nil
}

// align is every output at every record the legacy output delivered, as
// the platform aligns them (process_named_outputs_data): the legacy output
// at its own value, in the state the query completed in; an output that
// answered at the record's key at the value it answered, in its own state;
// one that did not, in its own state when that is ERROR or UNSUPPORTED and
// MISSING otherwise. An output of an answer under no contract is
// UNSUPPORTED, and so is one the answer left out. Nil when two records of
// the legacy output sit at one key.
func (named *namedOutputDecoding) align(completeness execution.Completeness) *execution.NamedOutputRecords {
	legacyState := execution.NamedOutputSuccess
	if completeness == execution.CompletenessPartial {
		legacyState = execution.NamedOutputPartial
	}
	refs := make([]string, 0, len(named.outputs.Outputs))
	answers := make([]*answeredOutput, 0, len(named.outputs.Outputs))
	for _, output := range named.outputs.Outputs {
		refs = append(refs, output.Ref)
		answers = append(answers, named.answered[output.Ref])
	}
	records := &execution.NamedOutputRecords{Refs: refs, Records: make(map[string][]execution.NamedOutputValue, len(named.points))}
	keys := make(map[alignKey]bool, len(named.points))
	for _, point := range named.points {
		if keys[point.key] {
			return nil
		}
		keys[point.key] = true
		values := make([]execution.NamedOutputValue, len(refs))
		for index, ref := range refs {
			answer := answers[index]
			switch {
			case ref == named.outputs.LegacyRef:
				values[index] = execution.NamedOutputValue{Value: point.value, State: legacyState}
			case answer == nil:
				values[index] = execution.NamedOutputValue{State: execution.NamedOutputUnsupported}
			default:
				if value, found := answer.points[point.key]; found {
					values[index] = execution.NamedOutputValue{Value: value, State: answer.state}
				} else if answer.state == execution.NamedOutputError || answer.state == execution.NamedOutputUnsupported {
					values[index] = execution.NamedOutputValue{State: answer.state}
				} else {
					values[index] = execution.NamedOutputValue{State: execution.NamedOutputMissing}
				}
			}
		}
		records.Records[point.recordID] = values
	}
	return records
}
