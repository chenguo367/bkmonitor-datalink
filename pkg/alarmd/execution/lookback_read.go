// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package execution

import "context"

// LookbackRead is which of the lookback's reads a request to the provider
// is: the provider's trace and a test's fake provider tell them from a
// Slot's formal read by it. LookbackReadNone, the zero value, is a read that
// is not the lookback's. The kinds are a closed set, each named once in
// lookbackReadNames; a value outside it is a defect and reads as none.
type LookbackRead uint8

const (
	// LookbackReadNone is a read that is not the lookback's: a Slot's own.
	LookbackReadNone LookbackRead = iota
	// LookbackReadRecheck is a rung of a sample's recheck, from 1.5 steps
	// after its first read on.
	LookbackReadRecheck
	// LookbackReadDeepRecheck is a sample's deep recheck, read once at the
	// deepest rung after its planned ones.
	LookbackReadDeepRecheck
	// LookbackReadProbation is the reread of a round of a Query Group whose
	// read hold is in probation.
	LookbackReadProbation
	// LookbackReadEarlier is a trial read before the readiness without the
	// read hold, whether the hold may be lowered.
	LookbackReadEarlier
	// LookbackReadFloorProbe is a round of the shadow probe of a group's
	// read hold floor, read at the floor.
	LookbackReadFloorProbe
	// LookbackReadDirected is a directed Slot's reread at its rung.
	LookbackReadDirected
	// LookbackReadDirectedEarly is a directed Slot's early read, before its
	// next Slot reads.
	LookbackReadDirectedEarly
	lookbackReadEnd
)

// lookbackReadNames names each kind, as the provider request names it; the
// none kind has no name.
var lookbackReadNames = [lookbackReadEnd]string{
	LookbackReadNone:          "",
	LookbackReadRecheck:       "recheck",
	LookbackReadDeepRecheck:   "deep_recheck",
	LookbackReadProbation:     "probation",
	LookbackReadEarlier:       "earlier",
	LookbackReadFloorProbe:    "floor_probe",
	LookbackReadDirected:      "directed",
	LookbackReadDirectedEarly: "directed_early",
}

// Name is the kind's name, and false for none and for a value outside the
// set.
func (kind LookbackRead) Name() (string, bool) {
	if kind == LookbackReadNone || kind >= lookbackReadEnd {
		return "", false
	}
	return lookbackReadNames[kind], true
}

// LookbackReadKinds is every kind of the lookback's reads, none excepted, in
// the table's order.
func LookbackReadKinds() []LookbackRead {
	kinds := make([]LookbackRead, 0, lookbackReadEnd-1)
	for kind := LookbackReadNone + 1; kind < lookbackReadEnd; kind++ {
		kinds = append(kinds, kind)
	}
	return kinds
}

type lookbackReadKey struct{}

// WithLookbackRead carries the kind of the lookback's read a request is made
// for, from where the lookback starts it to the provider (access/uq's
// Recheck). None carries nothing.
func WithLookbackRead(ctx context.Context, kind LookbackRead) context.Context {
	if kind == LookbackReadNone {
		return ctx
	}
	return context.WithValue(ctx, lookbackReadKey{}, kind)
}

// LookbackReadFrom is the kind ctx carries, none when it carries none.
func LookbackReadFrom(ctx context.Context) LookbackRead {
	kind, _ := ctx.Value(lookbackReadKey{}).(LookbackRead)
	return kind
}
