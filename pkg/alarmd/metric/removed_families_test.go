// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package metric

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// removedFamilies are families taken out because nothing read them: their
// detail is on a page that does, or another family says the same. A family
// is added back only with a reader named for it.
var removedFamilies = []string{
	"lookback_samples_total", "lookback_completion_total", "lookback_completion_max_seconds", "lookback_probes_total",
	"lookback_empty_first_reads_total", "lookback_empty_first_read_completion_total", "lookback_groups",
	"lookback_rest_seconds", "lookback_yield_releases_total", "lookback_yield_release_seconds_total",
	"lookback_yield_release_max_seconds", "lookback_earlier_reads_total",
	"active_qg_set_encode_duration_seconds", "run_one_attempted_total", "schedule_cutover_last_duration_seconds",
	"schedule_cutover_payload_size_bytes", "state_write_change_reason_total",
	"platform_setting_enabled", "platform_setting_entries", "platform_setting_source",
	// The health snapshot's own families: the deployment's health is
	// fleet_health and /api/health, and a replica's readiness is /readyz.
	"health_ready", "health_state", "health_reason", "health_assigned_claims", "health_inflight_messages",
	"health_worker_queue_depth", "health_worker_queue_bytes", "health_consumer_lag_records",
	"health_last_progress_timestamp_seconds", "health_last_recovery_timestamp_seconds",
	// What the generic observations said they moved, by stage. Of the stages
	// still emitted only effective-time maintenance reported a volume, its
	// events, which effective_close_total counts by outcome.
	"observed_messages_total", "observed_records_total", "observed_plans_total", "observed_levels_total",
	"observed_events_total", "observed_bytes_total", "observed_keys_total", "observed_state_bytes_total",
	// The open alert set's mode: the index copy has one state, so the gauge
	// was 1 on self_maintained for good and said nothing.
	"open_alert_set_mode",
	// The one-time upgrade of a v1 activation body, which no release wrote;
	// such a body is refused now, not upgraded.
	"legacy_active_qg_migration_total", "legacy_active_qg_migration_scan_keys",
	"legacy_active_qg_migration_duration_seconds",
	// The canonical encoder's rollout: its mode, the shadow comparison and
	// what the comparison covered, concluded with the single-pass form
	// answering and the established one kept only for what it declines.
	"canonical_encoding_mode", "canonical_encoding_shadow_sample_stride", "canonical_encoding_shadow_total",
	"canonical_encoding_covered_call_sites", "canonical_encoding_distinct_findings", "canonical_encoding_identity_part_total",
	// The split dry run and the dimension census it read: a suspended
	// design's reading, run every Leader round and acted on by nothing.
	"split_plan_total", "split_round_objects_total", "split_rounds_total", "shard_query_total",
	"catalog_shardability_plans_total", "dimension_census_total", "dimension_census_values_total",
}

// removedLabelValues are label values taken out of families that stay.
// A reader still filtering on one would read nothing and take it for zero,
// the same way as for a removed family.
//
// everyday marks a value that is also an ordinary word the sources use in
// other senses - a closed list, a closed record - so only its emission is
// guarded, not its spelling.
var removedLabelValues = []struct {
	family, label, value string
	everyday             bool
}{
	// The alert closes send wherever the link's Console is configured:
	// nothing is decided and held back any more, so there is no count of
	// what would have been sent and no side saying whether sending is on.
	{"absent_strategy_close_total", "outcome", "would_send", false},
	{"target_scope_close_total", "outcome", "would_send", false},
	{"absent_strategy_difference", "side", "send_armed", false},
	// Expired-range creation is always on, so the range gate never refuses
	// for it being off.
	{"range_gate_total", "outcome", "range_creation_disabled", false},
	// The close counts said closed where they counted decisions and sends:
	// a deployment read 354 alert_closed while the link still listed nearly every
	// one of those alerts active. They now say close_decided and close_sent,
	// and whether the link closed anything is read per strategy, from what
	// its reconcile still lists active after an earlier send.
	{"absent_strategy_close_total", "outcome", "alert_closed", false},
	{"absent_strategy_close_total", "outcome", "closed", true},
	{"target_scope_close_total", "outcome", "closed", true},
}

// A removed label value is emitted by no family, whatever its source
// reports: every cell of these families comes from a closed list, so a
// source still counting the old word is not enough to bring it back.
func TestARemovedLabelValueIsNotEmitted(t *testing.T) {
	r := NewRecorder(BuildInfo{})
	everything := func() map[string]uint64 {
		counts := map[string]uint64{}
		for _, removed := range removedLabelValues {
			counts[removed.value] = 1
		}
		return counts
	}
	r.SetAbsentCloseSource(everything, everything, func() map[string]int {
		sides := map[string]int{}
		for _, removed := range removedLabelValues {
			sides[removed.value] = 1
		}
		return sides
	})
	r.SetTargetScopeCloseSource(everything)
	families, err := r.registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	emitted := map[string]bool{}
	for _, family := range families {
		for _, series := range family.GetMetric() {
			for _, label := range series.GetLabel() {
				emitted[family.GetName()+"|"+label.GetName()+"|"+label.GetValue()] = true
			}
		}
	}
	for _, removed := range removedLabelValues {
		name := "bkmonitor_alarmd_" + removed.family
		if len(emittedOf(emitted, name)) == 0 {
			t.Fatalf("%s emitted nothing: the guard is not reading it", name)
		}
		if emitted[name+"|"+removed.label+"|"+removed.value] {
			t.Errorf("%s{%s=%q} is emitted again", name, removed.label, removed.value)
		}
	}
}

func emittedOf(emitted map[string]bool, family string) []string {
	var cells []string
	for cell := range emitted {
		if strings.HasPrefix(cell, family+"|") {
			cells = append(cells, cell)
		}
	}
	return cells
}

// No code, page, operation or test of alarmd or its CLI names a removed
// label value as a word.
func TestNoSourceNamesARemovedLabelValue(t *testing.T) {
	var values []string
	for _, removed := range removedLabelValues {
		if !removed.everyday {
			values = append(values, regexp.QuoteMeta(removed.value))
		}
	}
	pattern := regexp.MustCompile(`(?:^|[^A-Za-z0-9_])(` + strings.Join(values, "|") + `)(?:[^A-Za-z0-9_]|$)`)
	self, err := filepath.Abs("removed_families_test.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, root := range []string{"..", filepath.Join("..", "..", "alarmd-cli")} {
		err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
			if err != nil || entry.IsDir() {
				return err
			}
			switch filepath.Ext(path) {
			case ".go", ".html", ".js", ".md", ".yaml", ".json":
			default:
				return nil
			}
			if abs, _ := filepath.Abs(path); abs == self {
				return nil
			}
			body, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			if match := pattern.FindSubmatch(body); match != nil {
				t.Errorf("%s names the removed label value %s", path, match[1])
			}
			return nil
		})
		if err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
	}
}

// A removed family is registered nowhere, bound or not.
func TestARemovedFamilyIsNotRegistered(t *testing.T) {
	r := NewRecorder(BuildInfo{})
	descs := make(chan *prometheus.Desc, 64)
	go func() { r.registry.Describe(descs); close(descs) }()
	registered := map[string]bool{}
	for desc := range descs {
		text := desc.String()
		if start := strings.Index(text, `fqName: "`); start >= 0 {
			name := text[start+len(`fqName: "`):]
			registered[name[:strings.IndexByte(name, '"')]] = true
		}
	}
	for _, name := range removedFamilies {
		if registered["bkmonitor_alarmd_"+name] {
			t.Errorf("%s is registered again", "bkmonitor_alarmd_"+name)
		}
	}
}

// No code, page, operation or test of alarmd or its CLI names a removed
// family: a reader that still asked for one would read nothing and take it
// for zero. A removed family whose bare name is also a label of a family
// still registered (health_state is fleet_health's) is looked for by its
// full name only: the label is not a reader of the family.
func TestNoSourceNamesARemovedFamily(t *testing.T) {
	labels := map[string]bool{}
	for _, family := range alarmdFamilies(t, describedRecorder(t)) {
		for _, label := range family.labels {
			labels[label] = true
		}
	}
	pattern := removedFamilyPattern(removedFamilies, labels)
	self, err := filepath.Abs("removed_families_test.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, root := range []string{"..", filepath.Join("..", "..", "alarmd-cli")} {
		err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
			if err != nil || entry.IsDir() {
				return err
			}
			switch filepath.Ext(path) {
			case ".go", ".html", ".js", ".md", ".yaml", ".json":
			default:
				return nil
			}
			if abs, _ := filepath.Abs(path); abs == self {
				return nil
			}
			body, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			if match := pattern.FindSubmatch(body); match != nil {
				for _, name := range match[1:] {
					if len(name) > 0 {
						t.Errorf("%s names the removed family %s", path, name)
					}
				}
			}
			return nil
		})
		if err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
	}
}

// removedFamilyPattern finds a removed family's name as a word, bare or
// with the namespace, and only with the namespace when the bare name is one
// of labels.
func removedFamilyPattern(removed []string, labels map[string]bool) *regexp.Regexp {
	var bare, prefixed []string
	for _, name := range removed {
		if labels[name] {
			prefixed = append(prefixed, regexp.QuoteMeta(name))
		} else {
			bare = append(bare, regexp.QuoteMeta(name))
		}
	}
	var names []string
	if len(bare) > 0 {
		names = append(names, `(?:bkmonitor_alarmd_)?(`+strings.Join(bare, "|")+`)`)
	}
	if len(prefixed) > 0 {
		names = append(names, `bkmonitor_alarmd_(`+strings.Join(prefixed, "|")+`)`)
	}
	return regexp.MustCompile(`(?:^|[^A-Za-z0-9_])(?:` + strings.Join(names, "|") + `)(?:_bucket|_sum|_count)?(?:[^A-Za-z0-9_]|$)`)
}

func TestARemovedNameIsFoundBareUnlessItIsALabel(t *testing.T) {
	pattern := removedFamilyPattern([]string{"gone_total", "kind"}, map[string]bool{"kind": true})
	for text, found := range map[string]bool{
		"rate(gone_total[5m])":                  true,
		"bkmonitor_alarmd_gone_total_bucket":    true,
		"sum by (kind) (bkmonitor_alarmd_kind)": true,
		`[]string{"kind"}`:                      false,
		"still_gone_total":                      false,
		"gone_totals":                           false,
	} {
		if got := pattern.MatchString(text); got != found {
			t.Errorf("%q: found %v, want %v", text, got, found)
		}
	}
}
