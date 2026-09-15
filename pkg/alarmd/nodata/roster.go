// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package nodata

import "sort"

// Host dimension names, as the backend's host scenario writes them into the
// groups it expects.
const (
	HostIPDimension    = "bk_target_ip"
	HostCloudDimension = "bk_target_cloud_id"
)

// HostIdentity is one host a target resolved to. Both halves are text because
// that is what the group holds: the backend runs format_dicts_value_to_str over
// its instances before anything compares or hashes them.
type HostIdentity struct {
	IP      string
	CloudID string
}

// ResolvedTarget is the strategy's monitoring target, as the caller resolved it
// against the CMDB index.
//
// Hosts empty and Resolvable false are different states that produce the same
// expected set, which is why both are carried rather than collapsed. The
// backend collapses them - `if not target_instances` catches its None and its
// empty list alike - and then reports the whole item as absent when no data
// arrived either. The result is the same and the diagnosis is not: one is a
// target that currently matches no host, the other is a target shape this build
// cannot enumerate, and only the second is a gap in alarmd.
type ResolvedTarget struct {
	Hosts []HostIdentity
	// Resolvable says the caller could turn this target into a host set at all.
	// The first cut resolves a static host list; a topology node or a service
	// instance target is not resolvable here and says so rather than quietly
	// producing an empty one.
	Resolvable bool
}

// RosterRequest is what the expected set is derived from. Everything in it is
// already resolved: this package reads no CMDB index and no store, so that the
// derivation can be tested against the backend's branch by branch.
type RosterRequest struct {
	AggDimension []string
	// Target is the strategy's target. Nil means it names none, which is not
	// the same as one that resolves to no host.
	Target *ResolvedTarget
	Memory map[string]GroupMemory
}

// RosterUnsupportedError says this build cannot derive an expected set for the
// combination of target shape and no-data dimensions the Plan carries.
//
// It is an error rather than an empty roster because the two are different
// facts and only one of them is a judgement. An empty roster is a real answer -
// a static target that currently matches no host has an empty expected set, and
// the backend agrees - while this is the absence of an answer. Returning empty
// for it would expect nothing where the backend expects a set, and would do it
// silently, every round, for as long as the strategy exists.
//
// Nothing about this decision changes between rounds: the target shape and the
// dimensions are both frozen in the Plan, so a Slot learns nothing a compiler
// did not already know. The Plan is therefore refused when it is built, and
// this exists to make the derivation refuse it too rather than trust that the
// compiler caught it.
type RosterUnsupportedError struct{ Reason string }

func (err *RosterUnsupportedError) Error() string {
	return "alarmd nodata: no expected set can be derived: " + err.Reason
}

// BuildRoster derives the expected set for one item.
//
// It follows the backend's branch structure rather than the summary of it. The
// backend decides in two steps, and the second one is easy to lose: it first
// asks whether the no-data dimensions contain bk_target_ip at all - and returns
// nothing when they do not, rather than falling back to history - and only then
// asks whether the dimension set is exactly the target's, which is what decides
// between expecting the target instances and filtering history by them.
//
// The first cut answers three of the five combinations those two steps produce.
// No target at all is history. A target with dimensions that do not name the
// host is the whole item, because the backend never consults the target and
// then has nothing expected. A static host target with exactly the host pair is
// the target's hosts, empty included - a target that matches no host right now
// is an answer, and the backend gives the same one.
//
// The other two are refused rather than answered emptily. A target this build
// cannot enumerate, and a dimension set that names the host without being the
// pair, both have an expected set in the backend that this cut cannot produce;
// returning empty for either would expect nothing where the backend expects a
// set, silently, every round.
func BuildRoster(request RosterRequest) (Roster, error) {
	roster := Roster{Groups: map[string]Group{}}
	if request.Target == nil {
		roster.Source = RosterHistory
		roster.Groups = historyGroups(request.Memory)
		return roster, nil
	}
	if !namesTheHostDimension(request.AggDimension) {
		// The backend's first step: no-data dimensions that do not name
		// bk_target_ip make it return nothing at all rather than fall back to
		// history, and its whole-item rule then reports the item as a whole
		// when no data arrived. So the expected set is empty and the source is
		// the whole item - not a target that happened to resolve to nothing,
		// which is a different reading with the same count.
		roster.Source = RosterWhole
		return roster, nil
	}
	if !request.Target.Resolvable {
		return Roster{}, &RosterUnsupportedError{
			Reason: "the target is not a static host list, and enumerating it is not in this build",
		}
	}
	if !hostPairIsTheWholeDimensionSet(request.AggDimension) {
		return Roster{}, &RosterUnsupportedError{
			Reason: "the no-data dimensions name the host but are not the host pair, so the expected set " +
				"is history filtered by the target, which is not in this build",
		}
	}
	roster.Source = RosterTargetStatic
	for _, host := range request.Target.Hosts {
		group := hostTargetGroup(host)
		roster.Groups[group.Key()] = group
	}
	return roster, nil
}

// namesTheHostDimension is the backend's first step, which decides whether the
// target is consulted at all: `if "bk_target_ip" not in no_data_dimensions`.
func namesTheHostDimension(aggDimension []string) bool {
	for _, dimension := range aggDimension {
		if dimension == HostIPDimension {
			return true
		}
	}
	return false
}

// hostPairIsTheWholeDimensionSet is the backend's
// set(no_data_dimensions) == set(target_instances[0].keys()) with the keys it
// puts there: the host pair and nothing else.
func hostPairIsTheWholeDimensionSet(aggDimension []string) bool {
	if len(aggDimension) != 2 {
		return false
	}
	seen := map[string]bool{}
	for _, dimension := range aggDimension {
		seen[dimension] = true
	}
	return seen[HostIPDimension] && seen[HostCloudDimension]
}

func hostTargetGroup(host HostIdentity) Group {
	dimensions := []Dimension{
		{Name: HostCloudDimension, Value: host.CloudID},
		{Name: HostIPDimension, Value: host.IP},
	}
	sort.Slice(dimensions, func(left, right int) bool { return dimensions[left].Name < dimensions[right].Name })
	return Group{dimensions: dimensions}
}

// historyGroups is the expected set for an item with no target: every group the
// memory has seen.
//
// The whole-item key is excluded. It is in the memory because the absence
// evaluation records when the item as a whole went absent, and it is not a
// series - carrying it into a roster would make the item expect itself, so it
// would be judged as a group whose absence is the very thing that put it there.
func historyGroups(memory map[string]GroupMemory) map[string]Group {
	whole := WholeItemGroup().Key()
	groups := make(map[string]Group, len(memory))
	for key, entry := range memory {
		if key == whole || entry.LastSeen == 0 {
			continue
		}
		group, ok := ParseGroupKey(key)
		if !ok {
			continue
		}
		groups[key] = group
	}
	return groups
}
