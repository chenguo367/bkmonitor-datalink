// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package nodata

import (
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"testing"
)

var hostPair = []string{HostIPDimension, HostCloudDimension}

// With no target, the expected set is every group the memory has seen. This is
// where a history roster comes from: the absence evaluation records groups that
// arrived without being expected, and they become what is expected next time.
func TestRosterWithoutATargetIsWhatTheMemoryHasSeen(t *testing.T) {
	seen, never := hostTargetGroup(HostIdentity{IP: "10.0.0.1", CloudID: "0"}), hostTargetGroup(HostIdentity{IP: "10.0.0.2", CloudID: "0"})
	roster := mustBuildRoster(t, RosterRequest{
		AggDimension: hostPair,
		Memory: map[string]GroupMemory{
			seen.Key():  {LastSeen: 100},
			never.Key(): {FirstAbsent: 100},
		},
	})
	if roster.Source != RosterHistory {
		t.Fatalf("Source = %q, want %q", roster.Source, RosterHistory)
	}
	if _, expected := roster.Groups[seen.Key()]; !expected {
		t.Fatal("a group the memory has seen is not expected")
	}
	if _, expected := roster.Groups[never.Key()]; expected {
		t.Fatal("a group the memory has never seen is expected; LastSeen 0 means never")
	}
}

// The whole-item key lives in the memory because the absence evaluation records
// when the item as a whole went absent. It is not a series, and expecting it
// would make the item expect itself: it would then be judged absent because it
// was absent, which is the state that put it in the memory.
func TestRosterNeverExpectsTheWholeItemGroup(t *testing.T) {
	whole := WholeItemGroup().Key()
	roster := mustBuildRoster(t, RosterRequest{
		AggDimension: hostPair,
		Memory:       map[string]GroupMemory{whole: {LastSeen: 100, FirstAbsent: 50}},
	})
	if _, expected := roster.Groups[whole]; expected {
		t.Fatal("the roster expects the whole-item group")
	}
	if len(roster.Groups) != 0 {
		t.Fatalf("roster = %v, want nothing expected", roster.Groups)
	}
}

// A static host target with the host pair as its dimensions expects exactly the
// hosts it resolved to.
func TestRosterFromAStaticTargetExpectsItsHosts(t *testing.T) {
	roster := mustBuildRoster(t, RosterRequest{
		AggDimension: hostPair,
		Target: &ResolvedTarget{Resolvable: true, Hosts: []HostIdentity{
			{IP: "10.0.0.1", CloudID: "0"}, {IP: "10.0.0.2", CloudID: "0"},
		}},
		// A memory full of other groups does not widen a target's expected set.
		Memory: map[string]GroupMemory{hostTargetGroup(HostIdentity{IP: "10.0.0.9", CloudID: "0"}).Key(): {LastSeen: 100}},
	})
	if roster.Source != RosterTargetStatic {
		t.Fatalf("Source = %q, want %q", roster.Source, RosterTargetStatic)
	}
	if len(roster.Groups) != 2 {
		t.Fatalf("roster = %v, want the two hosts and nothing else", roster.Groups)
	}
	for _, host := range []HostIdentity{{IP: "10.0.0.1", CloudID: "0"}, {IP: "10.0.0.2", CloudID: "0"}} {
		if _, expected := roster.Groups[hostTargetGroup(host).Key()]; !expected {
			t.Fatalf("host %+v is in the target and not expected", host)
		}
	}
}

// A static target that currently matches no host is an answer, not a gap: the
// expected set is empty and the backend's is too. It must not fall back to
// history, which would expect groups the backend does not.
func TestRosterFromAStaticTargetThatMatchesNoHostExpectsNothing(t *testing.T) {
	seen := hostTargetGroup(HostIdentity{IP: "10.0.0.1", CloudID: "0"})
	roster := mustBuildRoster(t, RosterRequest{
		AggDimension: hostPair, Target: &ResolvedTarget{Resolvable: true},
		Memory: map[string]GroupMemory{seen.Key(): {LastSeen: 100}},
	})
	if roster.Source != RosterTargetStatic {
		t.Fatalf("Source = %q, want %q: an empty static target is still a target", roster.Source, RosterTargetStatic)
	}
	if len(roster.Groups) != 0 {
		t.Fatalf("roster = %v, want nothing expected rather than a fall back to history", roster.Groups)
	}
}

// A target whose no-data dimensions never name the host is the backend's first
// step: the target is not consulted at all and nothing is expected. The source
// says the whole item rather than a target, because there is no target reading
// here to report - the same empty count means two different things and this is
// the one that is not about a target.
func TestRosterWithDimensionsThatDoNotNameTheHostIsTheWholeItem(t *testing.T) {
	roster := mustBuildRoster(t, RosterRequest{
		AggDimension: []string{"device"},
		Target:       &ResolvedTarget{Resolvable: true, Hosts: []HostIdentity{{IP: "10.0.0.1", CloudID: "0"}}},
		Memory:       map[string]GroupMemory{hostTargetGroup(HostIdentity{IP: "10.0.0.9", CloudID: "0"}).Key(): {LastSeen: 100}},
	})
	if roster.Source != RosterWhole {
		t.Fatalf("Source = %q, want %q", roster.Source, RosterWhole)
	}
	if len(roster.Groups) != 0 {
		t.Fatalf("roster = %v, want nothing expected", roster.Groups)
	}
}

// The two combinations this cut cannot derive are refused rather than answered
// with an empty set. The backend expects a set for both, so empty would expect
// nothing where it expects something - silently, every round, for as long as
// the strategy exists.
func TestRosterRefusesWhatThisCutCannotDerive(t *testing.T) {
	for name, request := range map[string]RosterRequest{
		"a target this build cannot enumerate": {
			AggDimension: hostPair, Target: &ResolvedTarget{Resolvable: false},
		},
		"dimensions that name the host without being the pair": {
			AggDimension: []string{HostIPDimension, HostCloudDimension, "device"},
			Target:       &ResolvedTarget{Resolvable: true, Hosts: []HostIdentity{{IP: "10.0.0.1", CloudID: "0"}}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			roster, err := BuildRoster(request)
			var unsupported *RosterUnsupportedError
			if !errors.As(err, &unsupported) {
				t.Fatalf("BuildRoster() = %v, %v, want a RosterUnsupportedError", roster, err)
			}
		})
	}
}

func mustBuildRoster(t *testing.T, request RosterRequest) Roster {
	t.Helper()
	roster, err := BuildRoster(request)
	if err != nil {
		t.Fatalf("BuildRoster() error = %v", err)
	}
	return roster
}

// The target's hosts are expected only when the dimensions are exactly the host
// pair. Every other shape either never consults the target or is refused, and
// none of them may quietly expect the bare pair, which is a different set of
// objects than the backend or the next cut produces.
func TestRosterNeverExpectsTheBareHostPairForOtherDimensions(t *testing.T) {
	for name, dimensions := range map[string][]string{
		"host pair plus another": {HostIPDimension, HostCloudDimension, "device"},
		"ip alone":               {HostIPDimension},
		"unrelated":              {"device"},
		"none":                   nil,
	} {
		t.Run(name, func(t *testing.T) {
			roster, err := BuildRoster(RosterRequest{
				AggDimension: dimensions,
				Target:       &ResolvedTarget{Resolvable: true, Hosts: []HostIdentity{{IP: "10.0.0.1", CloudID: "0"}}},
			})
			if err != nil {
				return
			}
			if len(roster.Groups) != 0 {
				t.Fatalf("roster = %v, want nothing expected for dimensions that are not the host pair", roster.Groups)
			}
		})
	}
}

// The memory holds a group under its key, and the history roster has to expect
// the group. That round trip has to be exact for adversarial values, because a
// key that parses into a different group is a group whose alerts never recover.
func TestGroupKeyRoundTripsThroughParse(t *testing.T) {
	adversarial := []string{"", "=", ",", `\`, `\\`, `a=b,c=d`, `a\,b`, `a\=b`, "  spaced  ", "值", `\\=\,`}
	random := rand.New(rand.NewSource(1))
	for round := 0; round < 200; round++ {
		count := 1 + random.Intn(4)
		dimensions := make(map[string]string, count)
		for index := 0; index < count; index++ {
			name := fmt.Sprintf("d%d%s", index, adversarial[random.Intn(len(adversarial))])
			dimensions[name] = adversarial[random.Intn(len(adversarial))]
		}
		names := make([]string, 0, len(dimensions))
		for name := range dimensions {
			names = append(names, name)
		}
		group, ok := Project(dimensions, names)
		if !ok {
			t.Fatalf("Project(%v) reported invalid for its own dimension names", dimensions)
		}
		parsed, ok := ParseGroupKey(group.Key())
		if !ok {
			t.Fatalf("ParseGroupKey(%q) refused a key this package wrote", group.Key())
		}
		if !reflect.DeepEqual(parsed.Dimensions(), group.Dimensions()) {
			t.Fatalf("round trip of %q gave %+v, want %+v", group.Key(), parsed.Dimensions(), group.Dimensions())
		}
		if parsed.Key() != group.Key() {
			t.Fatalf("round trip changed the key: %q became %q", group.Key(), parsed.Key())
		}
	}
}

func TestParseGroupKeyRefusesTextThisPackageDidNotWrite(t *testing.T) {
	for name, key := range map[string]string{
		"empty":                  "",
		"no tag":                 "a=b",
		"tag with a value":       "a=b,__NO_DATA_DIMENSION__=false",
		"pair with no separator": "ab,__NO_DATA_DIMENSION__=true",
		"trailing escape":        `a=b\`,
		"empty name":             "=b,__NO_DATA_DIMENSION__=true",
		"out of name order":      "b=2,a=1,__NO_DATA_DIMENSION__=true",
	} {
		t.Run(name, func(t *testing.T) {
			if group, ok := ParseGroupKey(key); ok {
				t.Fatalf("ParseGroupKey(%q) = %+v, want it refused", key, group.Dimensions())
			}
		})
	}
}

// The whole-item group's key parses to the whole-item group, because the
// history roster reads every key in the memory and has to recognise that one to
// exclude it rather than treat it as unknown text.
func TestParseGroupKeyReadsTheWholeItemGroup(t *testing.T) {
	group, ok := ParseGroupKey(WholeItemGroup().Key())
	if !ok {
		t.Fatal("ParseGroupKey refused the whole-item key")
	}
	if len(group.Dimensions()) != 0 {
		t.Fatalf("whole-item group parsed to %+v", group.Dimensions())
	}
}
