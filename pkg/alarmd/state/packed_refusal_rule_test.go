package state

import (
	"errors"
	"strings"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// Every way a framed write can be refused names its rule, and the name is one
// of the bounded set.
//
// The rule is what the admission line carries. Eight refusals shared one
// reason before it: a line saying STATE_CORRUPT sent a reader to read eight
// producers, and three Query Groups refusing inside one minute could not be
// told apart from each other or from the one case anybody had seen before.
//
// Driven through encodeRuntimePacked, the function the store actually calls,
// so a rule reachable only in a constructed error does not count.
func TestEveryFramedRefusalNamesItsRule(t *testing.T) {
	identity := packedIdentity()
	good := func() execution.StateMutation {
		return packedMutation(t, []execution.StateHistoryPoint{
			packedPoint(t, identity, 1758400000, execution.LevelFactNormal),
			packedPoint(t, identity, 1758400060, execution.LevelFactNormal),
		}, 1)
	}

	for _, testCase := range []struct {
		rule   string
		mutate func(execution.StateMutation) execution.StateMutation
	}{
		{rule: PackedRuleDuplicateLevel, mutate: func(m execution.StateMutation) execution.StateMutation {
			m.Levels = append(m.Levels, m.Levels[0])
			return m
		}},
		{rule: PackedRuleLevelNotInMutation, mutate: func(m execution.StateMutation) execution.StateMutation {
			m.Points[0].Levels[0].LevelID = 9
			return m
		}},
		{rule: PackedRuleNoDetectFingerprint, mutate: func(m execution.StateMutation) execution.StateMutation {
			m.Points[0].Levels[0].DetectFingerprint = ""
			return m
		}},
		{rule: PackedRuleTwoFingerprints, mutate: func(m execution.StateMutation) execution.StateMutation {
			m.Points[1].Levels[0].DetectFingerprint = strings.Repeat("0e", 32)
			return m
		}},
		{rule: PackedRuleSourceTimeNotRising, mutate: func(m execution.StateMutation) execution.StateMutation {
			m.Points[1] = m.Points[0]
			return m
		}},
		{rule: PackedRuleRecordIDNotDerived, mutate: func(m execution.StateMutation) execution.StateMutation {
			m.Points[0].RecordID = strings.Repeat("ff", 32)
			return m
		}},
		{rule: PackedRuleRecordIDUnderivable, mutate: func(m execution.StateMutation) execution.StateMutation {
			// The derivation refuses the series digest itself, before it can
			// compare an id against anything.
			m.Identity.SeriesIdentityDigest = execution.SeriesIdentityDigest("not-a-digest")
			return m
		}},
		{rule: PackedRuleUnencodableFactState, mutate: func(m execution.StateMutation) execution.StateMutation {
			m.Points[0].Levels[0].Result = execution.LevelFactResult("SOMETHING_THIS_BUILD_CANNOT_STORE")
			return m
		}},
	} {
		t.Run(testCase.rule, func(t *testing.T) {
			_, err := encodeRuntimePacked(testCase.mutate(good()), 1)
			if !errors.Is(err, ErrPackedContract) {
				t.Fatalf("encode error = %v, want the framed-record refusal", err)
			}
			if rule := PackedRefusalRule(err); rule != testCase.rule {
				t.Fatalf("rule = %q, want %q: the line carries the rule, so a refusal that names the "+
					"wrong one sends a reader to the wrong producer", rule, testCase.rule)
			}
		})
	}

	// The control: the unmutated record frames. Without it every case above
	// would pass on an encoder that refused everything.
	if _, err := encodeRuntimePacked(good(), 1); err != nil {
		t.Fatalf("the unmutated record was refused (%v), so these cases prove nothing about which rule fired", err)
	}
}

// Every rule the encoder can name is in the published list, and every name in
// the list is one a reader can meet.
//
// The list is what a reader groups by. A rule added to the encoder and not to
// the list reaches the line as a word nothing documents; a name in the list no
// refusal produces is a row on a dashboard that is always empty, which reads
// as "this never happens" rather than "nothing can report it".
func TestThePublishedRuleListIsExactlyWhatCanBeReported(t *testing.T) {
	published := map[string]bool{}
	for _, rule := range PackedRuleNames {
		if published[rule] {
			t.Fatalf("%q is listed twice", rule)
		}
		published[rule] = true
	}
	if len(PackedRuleNames) != 8 {
		t.Fatalf("the list holds %d rules; every refusal in encodeRuntimePacked needs exactly one, so a "+
			"change to either has to change this number on purpose", len(PackedRuleNames))
	}
	// A refusal this build does not name reports nothing rather than a
	// neighbouring rule, so an unnamed rule shows up as a reason with no rule
	// beside it instead of being counted as one that was named.
	if rule := PackedRefusalRule(errors.New(contract.ReasonStateCorrupt)); rule != "" {
		t.Fatalf("a plain error was given the rule %q", rule)
	}
	if rule := PackedRefusalRule(nil); rule != "" {
		t.Fatalf("a nil error was given the rule %q", rule)
	}
}
