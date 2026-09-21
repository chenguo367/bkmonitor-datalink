// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package fleet

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// Every reason the control plane attaches to the capability disposition has
// words here, and every word here is a reason the control plane still
// produces. The literals are read from the source, not from a hand-kept
// list: the line that once said "a deployment parameter" for every reason
// was written against the reasons of its day and never learned the ones
// that came after -- one of which is what a live deployment's five
// strategies were withheld for.
func TestEveryCapabilityReasonHasWordsAndEveryWordIsAReason(t *testing.T) {
	produced := map[string]bool{}
	literal := regexp.MustCompile(`"([A-Z][A-Z0-9_]{5,})"`)
	// Reasons attached inline beside the disposition, and the ones handed
	// to the compilers' unsupported constructors and the target plan's
	// decoder, whose names are the only place they are spelled.
	sources := map[string]*regexp.Regexp{
		"../controlplane": regexp.MustCompile(`DispositionUnsupported,?[^\n]*\n?[^\n]*Reason: "([A-Z_]+)"|queryUnsupported\("([A-Z_]+)"|return "(UNSUPPORTED_[A-Z_]+)"`),
		"../targetplan":   regexp.MustCompile(`Reason[A-Za-z]* += "([A-Z_]+)"`),
		"../contract":     regexp.MustCompile(`Reason(?:SnapshotRetentionInsufficient|CompletionOffsetBelowReserve) += "([A-Z_]+)"`),
	}
	for dir, pattern := range sources {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
				continue
			}
			source, err := os.ReadFile(filepath.Join(dir, entry.Name()))
			if err != nil {
				t.Fatal(err)
			}
			for _, match := range pattern.FindAllStringSubmatch(string(source), -1) {
				for _, group := range match[1:] {
					if group != "" && literal.MatchString(`"`+group+`"`) {
						produced[group] = true
					}
				}
			}
		}
	}
	// Reasons that belong to other dispositions come through the same
	// constructors' neighbours; the ones this line never sees are left out.
	notCapability := map[string]bool{"UNSUPPORTED_TARGET_SCOPE_UNRESOLVABLE": false}
	if len(produced) < 8 {
		t.Fatalf("read %d reasons from the source, too few to be the set: %v", len(produced), produced)
	}
	for reason := range produced {
		if _, known := withheldReasonWords[reason]; !known {
			if _, excused := notCapability[reason]; excused {
				continue
			}
			t.Errorf("the control plane withholds under %q and the page has no words for it: it would read as unknown", reason)
		}
	}
	for _, reason := range KnownWithheldReasons() {
		if !produced[reason] {
			t.Errorf("words for %q, which nothing in the control plane produces", reason)
		}
		if words := withheldReasonWords[reason]; words.What == "" || words.Next == "" || words.Kind == "" || words.Kind == WithheldUnknownReason {
			t.Errorf("%q has incomplete words: %+v", reason, words)
		}
	}
	unknown := WithheldWordsOf("SOMETHING_NEW")
	if unknown.Kind != WithheldUnknownReason || !strings.Contains(unknown.What, "SOMETHING_NEW") || strings.Contains(unknown.Next, "部署参数") && !strings.Contains(unknown.Next, "别") {
		t.Errorf("an unknown reason = %+v, want named as unknown and not sent to a parameter", unknown)
	}
}

// The capability line counts strategies by kind of cause and names no cause
// the reasons under it do not carry: five strategies withheld for a target
// this build cannot resolve read as "this build does not support", not as a
// deployment parameter -- which is what the page used to say of every
// reason on this line, and what an operator was sent to change.
func TestTheCapabilityLineNamesTheKindsUnderItAndNotOneCauseForAll(t *testing.T) {
	facts := NewSourceFacts(now, map[string]int{"UNSUPPORTED_PHASE2_CAPABILITY": 7, "ACCEPTED": 40}, []WithheldObject{
		{StrategyID: "25", Scope: "PLAN", Disposition: "UNSUPPORTED_PHASE2_CAPABILITY", Reason: "TARGET_PLAN_MODEL_REPRESENTATION_UNRESOLVED", FieldPath: "items[0].target_plan.model_match"},
		{StrategyID: "351", Scope: "PLAN", Disposition: "UNSUPPORTED_PHASE2_CAPABILITY", Reason: "TARGET_PLAN_MODEL_REPRESENTATION_UNRESOLVED", FieldPath: "items[0].target_plan.model_match"},
		{StrategyID: "387", Scope: "PLAN", Disposition: "UNSUPPORTED_PHASE2_CAPABILITY", Reason: "TARGET_PLAN_MODEL_REPRESENTATION_UNRESOLVED", FieldPath: "items[0].target_plan.model_match"},
		{StrategyID: "472", Scope: "PLAN", Disposition: "UNSUPPORTED_PHASE2_CAPABILITY", Reason: "TARGET_PLAN_MODEL_REPRESENTATION_UNRESOLVED", FieldPath: "items[0].target_plan.model_match"},
		{StrategyID: "474", Scope: "PLAN", Disposition: "UNSUPPORTED_PHASE2_CAPABILITY", Reason: "TARGET_PLAN_MODEL_REPRESENTATION_UNRESOLVED", FieldPath: "items[0].target_plan.model_match"},
		{StrategyID: "600", Scope: "PLAN", Disposition: "UNSUPPORTED_PHASE2_CAPABILITY", Reason: "SNAPSHOT_RETENTION_INSUFFICIENT"},
		{StrategyID: "601", Scope: "PLAN", Disposition: "UNSUPPORTED_PHASE2_CAPABILITY", Reason: "NEW_WORD_NOBODY_EXPLAINED"},
	})
	view := View{Source: facts, SourceReplica: "pod-a"}
	var line *CheckReport
	for _, report := range ReportChecks(nil, nil, &view, now) {
		if report.Code == CheckCapabilityUnsupported {
			copied := report
			line = &copied
		}
	}
	if line == nil {
		t.Fatal("no capability line")
	}
	if line.Strategies != 7 || len(line.Groups) != 3 {
		t.Fatalf("line = %+v, want 7 strategies in 3 groups", line)
	}
	if want := "7 条策略这个部署跑不了（3 种原因）——部署参数不够 1 条、本构建不支持 5 条、原因待查 1 条；处理办法按原因组看"; line.Line != want {
		t.Errorf("line = %q\nwant %q", line.Line, want)
	}
	if strings.Contains(line.Line, "快照保留期") {
		t.Errorf("the line still states a cause for every reason: %q", line.Line)
	}
	words := map[string]*WithheldReasonWords{}
	for _, group := range line.Groups {
		words[group.Key] = group.Words
	}
	if model := words["TARGET_PLAN_MODEL_REPRESENTATION_UNRESOLVED"]; model == nil || model.Kind != WithheldBuildCapability ||
		!strings.Contains(model.What, "model_inst_id") || !strings.Contains(model.Next, "都不用改") {
		t.Errorf("model representation words = %+v, want this build's gap with nothing for the operator to change", model)
	}
	if retention := words["SNAPSHOT_RETENTION_INSUFFICIENT"]; retention == nil || retention.Kind != WithheldDeploymentParameter || !strings.Contains(retention.Next, "快照保留期") {
		t.Errorf("retention words = %+v, want the deployment parameter", retention)
	}
	if unknown := words["NEW_WORD_NOBODY_EXPLAINED"]; unknown == nil || unknown.Kind != WithheldUnknownReason || !strings.Contains(unknown.What, "NEW_WORD_NOBODY_EXPLAINED") {
		t.Errorf("unknown reason words = %+v, want the reason named as unknown", unknown)
	}
}
