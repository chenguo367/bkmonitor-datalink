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
	"testing"
	"time"
)

// Whether the standard raw event can leave is decided from two facts the
// fleet already had and never put together: the leader's revisioned Plan
// count and the replicas' protocol choice. Each reason is reached by exactly
// one arrangement of the two, and a source that did not report the count is
// unknown rather than no.
func TestTheOutputPathIsDecidedFromRevisionedPlansAndTheProtocolChoice(t *testing.T) {
	source := func(plans, revisioned int, known bool) *SourceFacts {
		facts := NewSourceFacts(time.Unix(1000, 0), map[string]int{"ACCEPTED": plans}, nil)
		facts.Plans, facts.RevisionedPlans, facts.PlansKnown = plans, revisioned, known
		return facts
	}
	groups := func(words ...string) []OutputProtocolGroup {
		var result []OutputProtocolGroup
		for _, word := range words {
			result = append(result, OutputProtocolGroup{Protocol: OutputProtocolFacts{Configured: word, Explicit: word != "" && word != "auto"}, Replicas: []string{"pod-" + word}})
		}
		return result
	}
	for name, test := range map[string]struct {
		view      View
		reachable bool
		reason    string
	}{
		"no source round at all": {
			view: View{OutputProtocols: groups("auto")}, reason: OutputPathSourceUnknown},
		"a source round from a build that did not count": {
			view: View{Source: source(100, 0, false), OutputProtocols: groups("auto")}, reason: OutputPathSourceUnknown},
		"auto with no revisioned Plans": {
			view: View{Source: source(2401, 0, true), OutputProtocols: groups("auto")}, reason: OutputPathNoRevisionedPlans},
		"auto with revisioned Plans": {
			view: View{Source: source(2401, 7, true), OutputProtocols: groups("auto")}, reachable: true, reason: OutputPathReachable},
		"every replica forced legacy, revisions or not": {
			view: View{Source: source(2401, 7, true), OutputProtocols: groups("legacy")}, reason: OutputPathProtocolLegacy},
		"legacy beside auto during a rollout": {
			view: View{Source: source(2401, 7, true), OutputProtocols: groups("legacy", "auto")}, reachable: true, reason: OutputPathReachable},
		"native with revisioned Plans": {
			view: View{Source: source(2401, 7, true), OutputProtocols: groups("native")}, reachable: true, reason: OutputPathReachable},
		"native with none refuses them all": {
			view: View{Source: source(2401, 0, true), OutputProtocols: groups("native")}, reason: OutputPathNoRevisionedPlans},
		"replicas that published no choice are left out of the decision": {
			view: View{Source: source(2401, 7, true), OutputProtocols: groups("", "legacy")}, reason: OutputPathProtocolLegacy},
		"no replica published a choice at all": {
			view: View{Source: source(2401, 7, true), OutputProtocols: groups("")}, reachable: true, reason: OutputPathReachable},
	} {
		name, test := name, test
		t.Run(name, func(t *testing.T) {
			facts := OutputPathOf(&test.view)
			if facts.StandardRawEventReachable != test.reachable || facts.Reason != test.reason {
				t.Fatalf("output path = %+v, want reachable %t, %s", facts, test.reachable, test.reason)
			}
			if test.view.Source != nil && test.view.Source.PlansKnown && (facts.Plans != test.view.Source.Plans || facts.RevisionedPlans != test.view.Source.RevisionedPlans) {
				t.Fatalf("output path carries %d/%d plans, want the source's %d/%d", facts.Plans, facts.RevisionedPlans, test.view.Source.Plans, test.view.Source.RevisionedPlans)
			}
			known := false
			for _, word := range OutputPathReasons {
				known = known || word == facts.Reason
			}
			if !known {
				t.Fatalf("reason %q is not in OutputPathReasons", facts.Reason)
			}
		})
	}
}

// The sentence is on the verdict route beside the protocol groups: on the
// live shape -- every replica auto, a source that lists thousands and
// publishes no revision -- it says the standard path is unreachable and why.
func TestTheVerdictRouteSaysWhetherTheStandardRawEventCanLeave(t *testing.T) {
	snapshots := healthySnapshots()
	for index := range snapshots {
		snapshots[index].OutputProtocol = &OutputProtocolFacts{Configured: "auto"}
	}
	facts := NewSourceFacts(now.Add(-time.Minute), map[string]int{"ACCEPTED": 2477, "CONFIG_REJECTED": 318}, nil)
	facts.Plans, facts.RevisionedPlans, facts.PlansKnown = 2477, 0, true
	snapshots[0].Source = facts
	handler := handlerWith(t, snapshots, Expectation{QueryGroups: 949, Known: true}, replicas())
	_, health := get(t, handler, "/api/health")
	path, _ := health["output_path"].(map[string]any)
	if path == nil || path["standard_raw_event_reachable"] != false || path["reason"] != OutputPathNoRevisionedPlans ||
		path["plans"] != 2477.0 || path["revisioned_plans"] != 0.0 {
		t.Fatalf("output_path = %v, want unreachable for want of revisioned Plans, 2477/0", health["output_path"])
	}
	// The same deployment once the source publishes revisions.
	facts.RevisionedPlans = 2477
	handler = handlerWith(t, snapshots, Expectation{QueryGroups: 949, Known: true}, replicas())
	_, health = get(t, handler, "/api/health")
	if path, _ := health["output_path"].(map[string]any); path["standard_raw_event_reachable"] != true || path["reason"] != OutputPathReachable {
		t.Fatalf("output_path with revisions = %v, want reachable", health["output_path"])
	}
}
