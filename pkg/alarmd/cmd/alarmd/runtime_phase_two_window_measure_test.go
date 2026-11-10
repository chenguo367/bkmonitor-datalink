// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

//go:build unix

package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/config"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/controlplane"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	enginekafka "github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/kafka"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/metric"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// The window measurements of decision-020 batch 8 (plan 5.2): a Query Group
// of 500,000 series - and 100,000, the two peaks of which may differ by one
// window at most - at 1, 5, 15 and 30 points a series, with no-data off and
// on (max_groups 500,000), through the production bundle on a real Redis
// under the budgets a 4 GiB replica derives. Each prints one line a shape and
// asserts only that the run completed. Run on demand with
// ALARMD_MEASURE_WINDOWS=1; ALARMD_MEASURE_WINDOWS_SHAPE narrows the run to
// the shapes it names, comma separated (500000x30/on), which is how the
// serial run gives each shape its own process: the peak resident set the
// kernel reports is the process's lifetime peak.

// windowMeasureLimitBytes is the replica the measurements size for.
const windowMeasureLimitBytes = 4 << 30

// windowMeasureMaxGroups is the no-data bound of the shapes that detect it.
const windowMeasureMaxGroups = 500_000

type windowMeasureShape struct {
	series, points int
	noData         bool
}

func (shape windowMeasureShape) String() string {
	state := "off"
	if shape.noData {
		state = "on"
	}
	return fmt.Sprintf("%dx%d/%s", shape.series, shape.points, state)
}

// windowMeasureShapes is every shape, or the ones
// ALARMD_MEASURE_WINDOWS_SHAPE names.
func windowMeasureShapes() []windowMeasureShape {
	var shapes []windowMeasureShape
	for _, series := range []int{500_000, 100_000} {
		for _, points := range []int{1, 5, 15, 30} {
			for _, noData := range []bool{false, true} {
				shapes = append(shapes, windowMeasureShape{series: series, points: points, noData: noData})
			}
		}
	}
	named := strings.TrimSpace(os.Getenv("ALARMD_MEASURE_WINDOWS_SHAPE"))
	if named == "" {
		return shapes
	}
	wanted := map[string]bool{}
	for _, name := range strings.Split(named, ",") {
		wanted[strings.TrimSpace(name)] = true
	}
	var chosen []windowMeasureShape
	for _, shape := range shapes {
		if wanted[shape.String()] {
			chosen = append(chosen, shape)
		}
	}
	return chosen
}

// windowMeasureStrategy is the largest Query Group's strategy
// (largeGroupStrategy), with no-data detected per device when the shape asks
// for it: device_name alone, so a group is a series and the round needs no
// host's business to judge it.
func windowMeasureStrategy(t *testing.T, shape windowMeasureShape) []byte {
	t.Helper()
	raw := largeGroupStrategy(t)
	if !shape.noData {
		return raw
	}
	var document map[string]any
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	item := document["items"].([]any)[0].(map[string]any)
	item["no_data_config"] = map[string]any{"is_enabled": true, "continuous": 1, "level": 1,
		"agg_dimension": []any{"device_name"}, "max_groups": windowMeasureMaxGroups}
	encoded, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

// writeWindowMeasureAnswer writes the answer for a query starting at start
// (seconds) to path: series series of points points each, 60 / points
// seconds apart inside the minute the query reads, every tenth series at 90
// - over the strategy's threshold of 80, so the Slot has events to send -
// and the rest at 12.5. The dimensions are largeGroupAnswer's.
func writeWindowMeasureAnswer(path string, series, points int, start int64) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	out := bufio.NewWriterSize(file, 1<<20)
	step := int64(60 / points)
	_, _ = out.WriteString(`{"series":[`)
	var line []byte
	for index := 0; index < series; index++ {
		line = line[:0]
		if index > 0 {
			line = append(line, ',')
		}
		line = fmt.Appendf(line, `{"name":"_result0","columns":["_time","_result"],"types":["int64","float64"],`+
			`"group_keys":["bk_target_cloud_id","bk_target_ip","device_name"],"group_values":["%03d","198.51.100.%03d","device-%025d"],`+
			`"values":[`, index%1000, index%256, index)
		value := "12.5"
		if index%10 == 0 {
			value = "90"
		}
		for point := 0; point < points; point++ {
			if point > 0 {
				line = append(line, ',')
			}
			line = append(line, '[')
			line = strconv.AppendInt(line, (start+int64(point)*step)*1000, 10)
			line = append(line, ',')
			line = append(line, value...)
			line = append(line, ']')
		}
		line = append(line, "]}"...)
		if _, err := out.Write(line); err != nil {
			_ = file.Close()
			return err
		}
	}
	_, _ = out.WriteString(`],"status":null,"trace_id":"window-measure","is_partial":false,"result_table_id":["system.cpu"]}`)
	if err := out.Flush(); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

// windowMeasureAnswers serves each query's answer from a file written for
// its start, so the answer neither sits on the heap measured nor costs the
// process more than a copy while the Slot runs. A start no file was written
// for ahead of its Slot is written when it is asked for, and counted.
type windowMeasureAnswers struct {
	dir            string
	series, points int
	mu             sync.Mutex
	files          map[int64]string
	lateWrites     atomic.Int64
}

func (answers *windowMeasureAnswers) prepare(start int64) (string, error) {
	answers.mu.Lock()
	defer answers.mu.Unlock()
	if path, ok := answers.files[start]; ok {
		return path, nil
	}
	path := filepath.Join(answers.dir, fmt.Sprintf("answer-%d.json", start))
	if err := writeWindowMeasureAnswer(path, answers.series, answers.points, start); err != nil {
		return "", err
	}
	answers.files[start] = path
	return path, nil
}

// drop removes every answer file but the one for keep.
func (answers *windowMeasureAnswers) drop(keep int64) {
	answers.mu.Lock()
	defer answers.mu.Unlock()
	for start, path := range answers.files {
		if start != keep {
			_ = os.Remove(path)
			delete(answers.files, start)
		}
	}
}

func (answers *windowMeasureAnswers) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	var query struct {
		StartTime string `json:"start_time"`
	}
	if err := json.NewDecoder(request.Body).Decode(&query); err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}
	start, err := strconv.ParseInt(strings.TrimSpace(query.StartTime), 10, 64)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}
	answers.mu.Lock()
	_, ready := answers.files[start]
	answers.mu.Unlock()
	if !ready {
		answers.lateWrites.Add(1)
	}
	path, err := answers.prepare(start)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}
	file, err := os.Open(path)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}
	defer file.Close()
	writer.Header().Set("Content-Type", "application/json")
	_, _ = io.Copy(writer, file)
}

// countingWindowEventSink encodes every event as the output does and keeps
// none: half a million kept events would be heap the Slot does not hold.
type countingWindowEventSink struct {
	batches, events atomic.Int64
}

func (sink *countingWindowEventSink) ConfigureStandardOutput(enginekafka.StandardEventConverter) error {
	return nil
}

func (sink *countingWindowEventSink) ConfigureLegacyOutput(enginekafka.LegacyEventConverter, string, int) error {
	return nil
}

func (sink *countingWindowEventSink) ProtocolNegotiation() *enginekafka.ProtocolNegotiation {
	return nil
}

func (sink *countingWindowEventSink) WriteBatch(_ context.Context, events []contract.TriggerEventV1) error {
	for index := range events {
		if _, err := contract.EncodeTriggerEventV1(&events[index]); err != nil {
			return err
		}
	}
	sink.batches.Add(1)
	sink.events.Add(int64(len(events)))
	return nil
}

func (sink *countingWindowEventSink) Shutdown(context.Context) error { return nil }

func (sink *countingWindowEventSink) Close() error { return nil }

// windowMeasureRuntimeSample reads, in one call, the runtime's heap objects,
// the live heap its last cycle marked, and the memory it holds from the
// system less what it has returned - the nearest the runtime comes to its
// resident set.
func windowMeasureRuntimeSample(samples []metrics.Sample) (objects, live, held uint64) {
	metrics.Read(samples)
	objects, live = samples[0].Value.Uint64(), samples[1].Value.Uint64()
	held = samples[2].Value.Uint64() - samples[3].Value.Uint64()
	return objects, live, held
}

func windowMeasureRuntimeSamples() []metrics.Sample {
	return []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}, {Name: "/gc/heap/live:bytes"},
		{Name: "/memory/classes/total:bytes"}, {Name: "/memory/classes/heap/released:bytes"}}
}

// windowMeasureCPU is the process's CPU time so far: user and system from the
// kernel, and the collector's share from the runtime.
func windowMeasureCPU() (process, collector float64) {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err == nil {
		process = float64(usage.Utime.Sec+usage.Stime.Sec) + float64(usage.Utime.Usec+usage.Stime.Usec)/1e6
	}
	sample := []metrics.Sample{{Name: "/cpu/classes/gc/total:cpu-seconds"}}
	metrics.Read(sample)
	if sample[0].Value.Kind() == metrics.KindFloat64 {
		collector = sample[0].Value.Float64()
	}
	return process, collector
}

// windowMeasureMaxRSS is the process's peak resident set over its lifetime,
// in bytes: the kernel reports bytes on darwin and KiB elsewhere.
func windowMeasureMaxRSS() uint64 {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return 0
	}
	if runtime.GOOS == "darwin" {
		return uint64(usage.Maxrss)
	}
	return uint64(usage.Maxrss) * 1024
}

// windowMeasureMetricSum sums the samples of one family whose labels hold
// every pair in labels.
func windowMeasureMetricSum(recorder *metric.Recorder, name string, labels map[string]string) float64 {
	families, err := recorder.Gatherer().Gather()
	if err != nil {
		return math.NaN()
	}
	var sum float64
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, series := range family.GetMetric() {
			matched := 0
			for _, label := range series.GetLabel() {
				if want, ok := labels[label.GetName()]; ok && want == label.GetValue() {
					matched++
				}
			}
			if matched != len(labels) {
				continue
			}
			switch {
			case series.GetCounter() != nil:
				sum += series.GetCounter().GetValue()
			case series.GetGauge() != nil:
				sum += series.GetGauge().GetValue()
			}
		}
	}
	return sum
}

// windowMeasureRedisCommands is the server's calls by command since its
// counters were last reset, and its CPU seconds so far.
func windowMeasureRedisCommands(ctx context.Context, client *redis.Client) (map[string]int64, float64) {
	calls := map[string]int64{}
	if info, err := client.Info(ctx, "commandstats").Result(); err == nil {
		for _, line := range strings.Split(info, "\n") {
			line = strings.TrimSpace(line)
			name, rest, found := strings.Cut(strings.TrimPrefix(line, "cmdstat_"), ":")
			if !found || !strings.HasPrefix(line, "cmdstat_") {
				continue
			}
			for _, field := range strings.Split(rest, ",") {
				if value, ok := strings.CutPrefix(field, "calls="); ok {
					if count, err := strconv.ParseInt(value, 10, 64); err == nil {
						calls[name] = count
					}
				}
			}
		}
	}
	var cpu float64
	if info, err := client.Info(ctx, "cpu").Result(); err == nil {
		for _, line := range strings.Split(info, "\n") {
			line = strings.TrimSpace(line)
			for _, key := range []string{"used_cpu_sys:", "used_cpu_user:"} {
				if value, ok := strings.CutPrefix(line, key); ok {
					if seconds, err := strconv.ParseFloat(value, 64); err == nil {
						cpu += seconds
					}
				}
			}
		}
	}
	return calls, cpu
}

func windowMeasureCommandLine(calls map[string]int64) string {
	names := make([]string, 0, len(calls))
	for name, count := range calls {
		if count > 0 && name != "info" && name != "config|resetstat" && name != "config" {
			names = append(names, name)
		}
	}
	sort.Slice(names, func(left, right int) bool { return calls[names[left]] > calls[names[right]] })
	parts := make([]string, 0, len(names))
	for _, name := range names {
		parts = append(parts, fmt.Sprintf("%s:%d", name, calls[name]))
	}
	return strings.Join(parts, ",")
}

// windowMeasureCapacity sizes the configuration for a 4 GiB replica the way
// Load does (config.Config.WithContainerCapacity reading the pod limit), and
// checks it came out as config.DeriveCoordinator says for that container: a
// process that read its capacity before the limit was set measures another
// replica, and is refused rather than measured.
func windowMeasureCapacity(t *testing.T, cfg config.Config) config.Config {
	t.Helper()
	inputs := config.DetectCapacityInputs()
	if inputs.MemoryLimitBytes != windowMeasureLimitBytes {
		t.Fatalf("the process read a memory limit of %d (%s) before the measurement set %d; run it alone",
			inputs.MemoryLimitBytes, inputs.MemorySource, uint64(windowMeasureLimitBytes))
	}
	cfg = cfg.WithContainerCapacity()
	want := config.DeriveCoordinator(inputs, uint64(cfg.Limits.Store.MaxKeysPerBatch),
		uint64(cfg.Limits.Store.MaxKeysPerBatch)*execution.StateApplyMaxChunks)
	if cfg.PhaseTwo.Coordinator != want {
		t.Fatalf("coordinator %+v, want the 4 GiB derivation %+v", cfg.PhaseTwo.Coordinator, want)
	}
	return cfg
}

// windowMeasureResult is what one shape's measured Slot came to.
type windowMeasureResult struct {
	completed                       bool
	result, reason, cause           string
	usage                           observability.SlotBudgetUsageFacts
	timing                          observability.SlotTimingFacts
	took                            time.Duration
	cpu, gcCPU, redisCPU            float64
	objectsPeak, livePeak, heldPeak uint64
	objectsBefore, liveBefore       uint64
	maxRSSBefore, maxRSSAfter       uint64
	reservedPeak                    uint64
	rejected                        float64
	refusals                        map[string]int
	batches, events                 int64
	keptRuns, outside               uint64
	witnessItems, witnessBytes      float64
	commands                        map[string]int64
	lateAnswers                     int64
	runErr                          error
}

// TestMeasureWindowedSlotsOnAFourGiBReplica runs three Slots of each shape a
// minute apart and measures the third, the State warm from the first two:
// whether it succeeded and with what, the share's refusals, the round's wall
// time and the process's CPU time over it, the peaks of the heap objects, of
// the live heap the collector marked and of the memory the runtime holds,
// sampled every two milliseconds, the process's lifetime peak resident set
// before and after, the most the pool held, the windows, the event batches,
// the record-id counters, the witness cache and the Redis commands by kind.
// One line a shape; it asserts that each run completed and nothing more.
func TestMeasureWindowedSlotsOnAFourGiBReplica(t *testing.T) {
	if os.Getenv("ALARMD_MEASURE_WINDOWS") == "" {
		t.Skip("a measurement: set ALARMD_MEASURE_WINDOWS=1")
	}
	t.Setenv(config.MemoryLimitEnvironment, strconv.FormatUint(windowMeasureLimitBytes, 10))
	inputs := config.DetectCapacityInputs()
	// The collector's budget and the cores of a 4c/4Gi replica, as startup
	// installs them (applyPhaseTwoGoRuntime), given back after.
	previousProcs := runtime.GOMAXPROCS(4)
	previousGC := debug.SetGCPercent(100)
	debug.SetGCPercent(previousGC)
	previousLimit := debug.SetMemoryLimit(-1)
	applyPhaseTwoGoRuntime(config.DeriveGoRuntime(inputs), runtimeGoBudgetSetters())
	defer func() {
		runtime.GOMAXPROCS(previousProcs)
		debug.SetGCPercent(previousGC)
		debug.SetMemoryLimit(previousLimit)
	}()
	shapes := windowMeasureShapes()
	if len(shapes) == 0 {
		t.Fatalf("ALARMD_MEASURE_WINDOWS_SHAPE=%q names no shape", os.Getenv("ALARMD_MEASURE_WINDOWS_SHAPE"))
	}
	for _, shape := range shapes {
		t.Run(strings.ReplaceAll(shape.String(), "/", "_"), func(t *testing.T) {
			measured := measureWindowedShape(t, shape)
			t.Logf("shape=%s %s", shape, measured.line(shape))
			if !measured.completed {
				t.Errorf("shape %s: the measured Slot did not complete (%v)", shape, measured.runErr)
			}
		})
		runtime.GC()
		debug.FreeOSMemory()
	}
}

func (measured windowMeasureResult) line(shape windowMeasureShape) string {
	usage := measured.usage
	refusals := make([]string, 0, len(measured.refusals))
	for reason, count := range measured.refusals {
		refusals = append(refusals, fmt.Sprintf("%s:%d", reason, count))
	}
	sort.Strings(refusals)
	runErr := ""
	if measured.runErr != nil {
		runErr = measured.runErr.Error()
	}
	return fmt.Sprintf("series=%d points=%d no_data=%t result=%s reason=%s cause=%s run_err=%q "+
		"share_rejected=%.0f refusals=[%s] took=%s slot_ms=%d input_ms=%d preflight_ms=%d evaluate_ms=%d "+
		"cpu=%.2fs gc_cpu=%.2fs redis_cpu=%.2fs "+
		"heap_objects_peak=%d (before %d) live_marked_peak=%d (before %d) runtime_held_peak=%d "+
		"maxrss_before=%d maxrss_after=%d reserved_peak=%d retained_peak=%d (input %d output %d state %d gap %d) share=%d "+
		"windows=%d ceiling_windows=%d raised_windows=%d usage_series=%d state_mutations=%d events=%d "+
		"write_batches=%d events_written=%d record_ids_kept_runs=%d record_ids_outside_window=%d "+
		"witness_items=%.0f witness_bytes=%.0f late_answers=%d redis=[%s]",
		shape.series, shape.points, shape.noData, measured.result, measured.reason, measured.cause, runErr,
		measured.rejected, strings.Join(refusals, ","), measured.took.Round(time.Millisecond),
		measured.timing.Slot, measured.timing.Input, measured.timing.Preflight, measured.timing.Evaluate,
		measured.cpu, measured.gcCPU, measured.redisCPU,
		measured.objectsPeak, measured.objectsBefore, measured.livePeak, measured.liveBefore, measured.heldPeak,
		measured.maxRSSBefore, measured.maxRSSAfter, measured.reservedPeak, usage.RetainedBytes,
		usage.RetainedInputBytes, usage.RetainedOutputBytes, usage.RetainedStateBytes, usage.RetainedGapBytes,
		usage.RetainedShareBytes, usage.Windows, usage.WindowCeilingWindows, usage.WindowCeilingRaisedWindows,
		usage.Series, usage.StateMutations, usage.Events, measured.batches, measured.events,
		measured.keptRuns, measured.outside, measured.witnessItems, measured.witnessBytes,
		measured.lateAnswers, windowMeasureCommandLine(measured.commands))
}

// measureWindowedShape runs one shape: its own Redis, bundle and answers.
func measureWindowedShape(t *testing.T, shape windowMeasureShape) windowMeasureResult {
	t.Helper()
	address, redisClient := startPhaseTwoRedis(t)
	ctx := context.Background()
	if err := redisClient.Set(ctx, "alarm-config.strategy_ids", `[1001]`, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := redisClient.Set(ctx, "alarm-config.strategy_1001", windowMeasureStrategy(t, shape), 0).Err(); err != nil {
		t.Fatal(err)
	}
	// An hour ahead of the real clock, on a minute boundary: every deadline
	// derived from the injected clock is measured against the real one, and a
	// Slot of half a million series runs for minutes.
	base := time.Now().Unix() + 3600
	base = base - base%60
	var clock atomic.Int64
	clock.Store(base)
	now := func() time.Time { return time.Unix(clock.Load(), 0) }
	answers := &windowMeasureAnswers{dir: t.TempDir(), series: shape.series, points: shape.points, files: map[int64]string{}}
	uqServer := httptest.NewServer(answers)
	defer uqServer.Close()

	cfg := largeGroupConfig(t, address, uqServer.URL)
	cfg.Redis.StatePrefix = "alarmd-window-measure"
	cfg.PhaseTwo.Worker.RegistrationTTL = config.Duration(2 * time.Hour)
	cfg.PhaseTwo.Ownership.ControlLeaderTTL = config.Duration(2 * time.Hour)
	cfg.PhaseTwo.Ownership.LeaseTTL = config.Duration(2 * time.Hour)
	cfg = windowMeasureCapacity(t, cfg)
	if err := cfg.Validate(); err != nil {
		t.Fatalf("configuration rejected: %v", err)
	}

	sink := &countingWindowEventSink{}
	var measuring atomic.Bool
	var mu sync.Mutex
	measured := windowMeasureResult{refusals: map[string]int{}}
	observer := observability.ObserverFunc(func(_ context.Context, observation observability.Observation) {
		if !measuring.Load() {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		if observation.CapacityRejection != nil || observation.ReasonCode == observability.CompletionCauseResourceShareExceeded {
			measured.refusals[string(observation.ReasonCode)]++
		}
		if observation.Stage == observability.StageSlotCompleted {
			measured.completed = true
			measured.result, measured.reason = string(observation.Result), string(observation.ReasonCode)
			measured.cause = observation.SlotCompletionCause
			if observation.SlotBudgetUsage != nil {
				measured.usage = *observation.SlotBudgetUsage
			}
			if observation.SlotTiming != nil {
				measured.timing = *observation.SlotTiming
			}
		}
	})
	recorder := metric.NewRecorder(metric.BuildInfo{})
	bundle, err := openProductionPhaseTwoBundleWithDependencies(
		ctx, cfg, recorder, observability.Discard(observability.ComponentRuntime), newPhaseTwoApplicationHealth(),
		func(client redis.Cmdable, prefix string) (controlplane.StrategySource, error) {
			return controlplane.NewLegacyRedisStrategySource(client, prefix)
		},
		phaseTwoProductionExternalDependencies{
			Now: now, HTTPClient: uqServer.Client(), AdditionalObserver: observer,
			PrepareEvents: preparedEvents(func(enginekafka.DecisionSinkConfig) (productionPhaseTwoEventSink, error) {
				return sink, nil
			}),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := bundle.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = bundle.Shutdown(ctx) }()
	settleExecutableView(ctx, bundle, 10*time.Second)

	// The Slot at base+60k reads the minute before it.
	const slots = 3
	for slot := int64(0); slot < slots-1; slot++ {
		if _, err := answers.prepare(base + slot*60 - 60); err != nil {
			t.Fatal(err)
		}
		clock.Store(base + slot*60 + 1)
		if err := bundle.runScheduledOnce(ctx); err != nil {
			t.Fatalf("Slot %d: %v", slot, err)
		}
		answers.drop(-1)
	}
	measuredStart := base + (slots-1)*60 - 60
	if _, err := answers.prepare(measuredStart); err != nil {
		t.Fatal(err)
	}

	runtime.GC()
	samples := windowMeasureRuntimeSamples()
	measured.objectsBefore, measured.liveBefore, _ = windowMeasureRuntimeSample(samples)
	measured.maxRSSBefore = windowMeasureMaxRSS()
	rejectedBefore := windowMeasureMetricSum(recorder, "alarmd_capacity_transition_total", map[string]string{"result": "rejected"})
	keptBefore, outsideBefore := execution.DatasetRecordIDCounts()
	batchesBefore, eventsBefore := sink.batches.Load(), sink.events.Load()
	if err := redisClient.ConfigResetStat(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	_, redisCPUBefore := windowMeasureRedisCommands(ctx, redisClient)
	answers.lateWrites.Store(0)

	var objectsPeak, livePeak, heldPeak, reservedPeak atomic.Uint64
	done := make(chan struct{})
	sampled := make(chan struct{})
	go func() {
		defer close(sampled)
		local := windowMeasureRuntimeSamples()
		ticker := time.NewTicker(2 * time.Millisecond) // fixed-wait: the sampling interval of a measurement; it waits on no condition
		defer ticker.Stop()
		for tick := 0; ; tick++ {
			objects, live, held := windowMeasureRuntimeSample(local)
			objectsPeak.Store(max(objectsPeak.Load(), objects))
			livePeak.Store(max(livePeak.Load(), live))
			heldPeak.Store(max(heldPeak.Load(), held))
			// The pool every 50 ms: a gather of every family is not free.
			if tick%25 == 0 {
				if reserved, ok := retainedReservedOf(recorder); ok {
					reservedPeak.Store(max(reservedPeak.Load(), reserved))
				}
			}
			select {
			case <-done:
				return
			case <-ticker.C:
			}
		}
	}()
	cpuBefore, gcBefore := windowMeasureCPU()
	measuring.Store(true)
	clock.Store(base + (slots-1)*60 + 1)
	started := time.Now()
	runErr := bundle.runScheduledOnce(ctx)
	took := time.Since(started)
	measuring.Store(false)
	cpuAfter, gcAfter := windowMeasureCPU()
	close(done)
	<-sampled

	mu.Lock()
	defer mu.Unlock()
	measured.runErr, measured.took = runErr, took
	measured.cpu, measured.gcCPU = cpuAfter-cpuBefore, gcAfter-gcBefore
	measured.objectsPeak, measured.livePeak, measured.heldPeak = objectsPeak.Load(), livePeak.Load(), heldPeak.Load()
	measured.reservedPeak = reservedPeak.Load()
	measured.maxRSSAfter = windowMeasureMaxRSS()
	measured.rejected = windowMeasureMetricSum(recorder, "alarmd_capacity_transition_total",
		map[string]string{"result": "rejected"}) - rejectedBefore
	keptAfter, outsideAfter := execution.DatasetRecordIDCounts()
	measured.keptRuns, measured.outside = keptAfter-keptBefore, outsideAfter-outsideBefore
	measured.batches, measured.events = sink.batches.Load()-batchesBefore, sink.events.Load()-eventsBefore
	measured.witnessItems = windowMeasureMetricSum(recorder, "alarmd_state_witness_items", nil)
	measured.witnessBytes = windowMeasureMetricSum(recorder, "alarmd_state_witness_bytes", nil)
	var redisCPUAfter float64
	measured.commands, redisCPUAfter = windowMeasureRedisCommands(ctx, redisClient)
	measured.redisCPU = redisCPUAfter - redisCPUBefore
	measured.lateAnswers = answers.lateWrites.Load()
	answers.drop(-1)
	return measured
}

// windowMeasureField is one dimension of a shape: its name and the value a
// series index gives it.
type windowMeasureField struct {
	name  string
	value func(index int) string
}

// TestMeasureDimensionRepetitionForInterning reads, for the synthetic shape
// the window measurements use (the largest Query Group's three dimensions at
// 500,000 series) and for the largest Query Group itself (95,760 series of
// the same three), and beside them the disk shape and the wide shape the
// input measurements read (access/uq columnar_decode_test.go), how often each
// dimension's values repeat: distinct values over all values per field, and
// the bytes a Query Group interning each field's values would no longer keep
// - every repetition's bytes, the string headers staying as they are. It is
// arithmetic over the generators, with no bundle; the interning decision of
// decision-020 batch 8 (trade-off two) reads it beside the window
// measurements. Runs with ALARMD_MEASURE_WINDOWS=1.
func TestMeasureDimensionRepetitionForInterning(t *testing.T) {
	if os.Getenv("ALARMD_MEASURE_WINDOWS") == "" {
		t.Skip("a measurement: set ALARMD_MEASURE_WINDOWS=1")
	}
	largest := []windowMeasureField{
		{"bk_target_cloud_id", func(index int) string { return fmt.Sprintf("%03d", index%1000) }},
		{"bk_target_ip", func(index int) string { return fmt.Sprintf("198.51.100.%03d", index%256) }},
		{"device_name", func(index int) string { return fmt.Sprintf("device-%025d", index) }},
	}
	disk := []windowMeasureField{
		{"bk_target_ip", func(index int) string { return fmt.Sprintf("192.0.2.%d", index%256) }},
		{"bk_target_cloud_id", func(int) string { return "0" }},
		{"device_name", func(index int) string { return fmt.Sprintf("/dev/vd%c%d", 'a'+index%26, index%7) }},
		{"mountpoint", func(index int) string { return fmt.Sprintf("/data%d", index) }},
	}
	wide := append([]windowMeasureField(nil), disk...)
	for tag := 0; tag < 21; tag++ {
		wide = append(wide, windowMeasureField{fmt.Sprintf("tag%02d", tag),
			func(index int) string { return fmt.Sprintf("%0320d", index*21+tag) }})
	}
	for _, shape := range []struct {
		name   string
		series int
		fields []windowMeasureField
	}{
		{"synthetic", 500_000, largest},
		{"largest_query_group", 95_760, largest},
		{"disk", 500_000, disk},
		{"wide", 50_000, wide},
	} {
		var total, saved, names uint64
		for _, field := range shape.fields {
			seen := make(map[string]struct{}, 1024)
			var fieldBytes, fieldSaved uint64
			for index := 0; index < shape.series; index++ {
				value := field.value(index)
				fieldBytes += uint64(len(value))
				if _, repeated := seen[value]; repeated {
					fieldSaved += uint64(len(value))
					continue
				}
				seen[value] = struct{}{}
			}
			// The name repeats on every series as well; interning it saves
			// all but one copy.
			nameSaved := uint64(len(field.name)) * uint64(shape.series-1)
			total += fieldBytes
			saved += fieldSaved
			names += nameSaved
			t.Logf("shape=%s field=%s distinct=%d total=%d ratio=%.6f value_bytes=%d interned_saves=%d (%d a series) name_saves=%d",
				shape.name, field.name, len(seen), shape.series, float64(len(seen))/float64(shape.series), fieldBytes,
				fieldSaved, fieldSaved/uint64(shape.series), nameSaved)
		}
		t.Logf("shape=%s series=%d value_bytes=%d interned_saves=%d (%d a series) name_saves=%d (%d a series)",
			shape.name, shape.series, total, saved, saved/uint64(shape.series), names, names/uint64(shape.series))
	}
}
