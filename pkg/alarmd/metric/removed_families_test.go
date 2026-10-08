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
// for zero.
func TestNoSourceNamesARemovedFamily(t *testing.T) {
	short := make([]string, len(removedFamilies))
	for i, name := range removedFamilies {
		short[i] = regexp.QuoteMeta(name)
	}
	pattern := regexp.MustCompile(`(?:^|[^A-Za-z0-9_])(?:bkmonitor_alarmd_)?(` + strings.Join(short, "|") + `)(?:_bucket|_sum|_count)?(?:[^A-Za-z0-9_]|$)`)
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
				t.Errorf("%s names the removed family %s", path, match[1])
			}
			return nil
		})
		if err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
	}
}
