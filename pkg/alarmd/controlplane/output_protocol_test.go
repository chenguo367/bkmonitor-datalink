// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package controlplane

import (
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
)

// The choice is resolved once, per strategy, when its Plan is built. The table
// is the whole contract: what a deployment asked for, what the strategy is, and
// what bytes come out.
func TestTheDeploymentChoiceResolvesToOneFormatPerStrategy(t *testing.T) {
	for name, test := range map[string]struct {
		configured string
		revision   int64
		want       string
		honoured   bool
	}{
		"unset keeps what the revision already decided": {
			configured: "", revision: 7, want: contract.WireFormatTriggerEvent, honoured: true,
		},
		"unset and no revision is the compatibility protocol": {
			configured: "", revision: 0, want: contract.WireFormatPythonCompatible, honoured: true,
		},
		"auto is the same rule, named": {
			configured: outputProtocolAuto, revision: 7, want: contract.WireFormatTriggerEvent, honoured: true,
		},
		"legacy forces compatibility even with a revision": {
			configured: outputProtocolLegacy, revision: 7, want: contract.WireFormatPythonCompatible, honoured: true,
		},
		"native publishes the standard raw event": {
			configured: outputProtocolNative, revision: 7, want: contract.WireFormatStandardRawEvent, honoured: true,
		},
		"native cannot be honoured without a revision": {
			configured: outputProtocolNative, revision: 0, honoured: false,
		},
	} {
		name, test := name, test
		t.Run(name, func(t *testing.T) {
			format, honoured := resolveWireFormat(test.configured, test.revision)
			if honoured != test.honoured {
				t.Fatalf("honoured = %t, want %t", honoured, test.honoured)
			}
			if honoured && format != test.want {
				t.Fatalf("format = %q, want %q", format, test.want)
			}
		})
	}
}

// The one case that must never be a silent fallback: a forced native choice
// meeting a strategy with no revision has to refuse the strategy, because
// publishing it the other way is exactly what the choice was made to stop.
func TestAForcedNativeChoiceRefusesRatherThanFallsBack(t *testing.T) {
	format, honoured := resolveWireFormat(outputProtocolNative, 0)
	if honoured {
		t.Fatalf("format = %q, want the strategy refused", format)
	}
	if format == contract.WireFormatPythonCompatible {
		t.Fatal("a forced native choice must not fall back to the compatibility protocol")
	}
}

// The strategy-level read applies the readers' own rule to an object, and
// says which half decided: a frozen word is reported as frozen, and an object
// from before the choice existed gets the revision rule, named as such. The
// table pins that an object with a word never has the rule applied over it --
// which is the whole point of freezing it.
func TestTheEffectiveFormatIsTheFrozenWordOrTheRevisionRuleNamedAsSuch(t *testing.T) {
	for name, test := range map[string]struct {
		frozen    string
		revision  int64
		want      string
		decidedBy string
	}{
		"frozen standard raw event is reported frozen": {
			frozen: contract.WireFormatStandardRawEvent, revision: 7, want: contract.WireFormatStandardRawEvent, decidedBy: WireFormatDecidedFrozen,
		},
		"frozen compatibility with a revision stays compatibility": {
			frozen: contract.WireFormatPythonCompatible, revision: 7, want: contract.WireFormatPythonCompatible, decidedBy: WireFormatDecidedFrozen,
		},
		"no word and a revision is the trigger event by the rule": {
			frozen: "", revision: 7, want: contract.WireFormatTriggerEvent, decidedBy: WireFormatDecidedByRevision,
		},
		"no word and no revision is compatibility by the rule": {
			frozen: "", revision: 0, want: contract.WireFormatPythonCompatible, decidedBy: WireFormatDecidedByRevision,
		},
	} {
		name, test := name, test
		t.Run(name, func(t *testing.T) {
			format, decidedBy := EffectiveWireFormat(test.frozen, test.revision)
			if format != test.want || decidedBy != test.decidedBy {
				t.Fatalf("EffectiveWireFormat(%q, %d) = %q by %q, want %q by %q", test.frozen, test.revision, format, decidedBy, test.want, test.decidedBy)
			}
		})
	}
}

// The exported list is the three words the reconciler accepts, in the same
// spelling, so a reader pinned to the list cannot drift from what the
// configuration validates.
func TestTheExportedChoicesAreTheOnesTheReconcilerAccepts(t *testing.T) {
	reconciler := &SourceReconciler{}
	for _, word := range OutputProtocolChoices {
		if err := reconciler.ConfigureOutputProtocol(word); err != nil {
			t.Fatalf("ConfigureOutputProtocol(%q) = %v, want accepted", word, err)
		}
	}
	if len(OutputProtocolChoices) != 3 {
		t.Fatalf("choices = %v, want the three the configuration validates", OutputProtocolChoices)
	}
	if err := reconciler.ConfigureOutputProtocol("shadow"); err == nil {
		t.Fatal("a word outside the list was accepted")
	}
}
