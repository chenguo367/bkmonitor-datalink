package config

import "testing"

// The share is the operator's: three percent of one and two gibibytes scale
// together, the sum of the parts stays inside the share, an unknown memory
// source enables nothing -- and so does no allocation, which is the default.
func TestObservationBudgetUsesBothResourcesAndHasNoUnknownFallback(t *testing.T) {
	share := PhaseTwoObservationConfig{MemoryPercent: 3}
	a := DeriveObservationCapacity(CapacityInputs{CPUBudget: 1, MemoryLimitBytes: 1 << 30, MemorySource: "pod_limit"}, share)
	b := DeriveObservationCapacity(CapacityInputs{CPUBudget: 2, MemoryLimitBytes: 2 << 30, MemorySource: "pod_limit"}, share)
	if a.DirectoryBytes*2 != b.DirectoryBytes || a.SampleRecordsPerMinute*2 != b.SampleRecordsPerMinute {
		t.Fatalf("budget did not scale: %+v %+v", a, b)
	}
	if a.DirectoryBytes+a.CostBytes+a.SampleBufferBytes > (1<<30)/100*3 {
		t.Fatal("exceeded memory share")
	}
	if got := DeriveObservationCapacity(CapacityInputs{CPUBudget: 2, MemoryLimitBytes: 2 << 30, MemorySource: memorySourceFallback}, share); got != (ObservationCapacity{}) {
		t.Fatalf("unknown budget enabled: %+v", got)
	}
}

// Nothing but the lookback is on until an operator says how much. A
// container that knows its limit and its cores is not an allocation; the
// default configuration runs detection and, of the diagnostics, only the
// late-data lookback, which reads no control plane and writes no store, in
// an eighth of the default share. Past the bound reads as none at all,
// though validation refuses it before it gets here.
func TestObservationBudgetIsOffWithoutAnOperatorAllocation(t *testing.T) {
	known := CapacityInputs{CPUBudget: 2, MemoryLimitBytes: 2 << 30, MemorySource: "pod_limit"}
	lookbackOnly := ObservationCapacity{LookbackBytes: (2 << 30) / 100 * DefaultLookbackSharePercent / 8}
	if got := DeriveObservationCapacity(known, PhaseTwoObservationConfig{}); got != lookbackOnly {
		t.Fatalf("the default allocation = %+v, want the lookback alone %+v", got, lookbackOnly)
	}
	if got := DeriveObservationCapacity(known, defaultPhaseTwoRuntime().Observation); got != lookbackOnly {
		t.Fatalf("the default phase-two configuration = %+v, want the lookback alone", got)
	}
	unknown := CapacityInputs{CPUBudget: 2, MemoryLimitBytes: 2 << 30, MemorySource: memorySourceFallback}
	if got := DeriveObservationCapacity(unknown, PhaseTwoObservationConfig{}); got != (ObservationCapacity{}) {
		t.Fatalf("an unknown container runs the lookback: %+v", got)
	}
	if got := DeriveObservationCapacity(known, PhaseTwoObservationConfig{MemoryPercent: ObservationMemoryPercentMax + 1}); got != (ObservationCapacity{}) {
		t.Fatalf("a share past the bound enabled the diagnostics: %+v", got)
	}
	if got := DeriveObservationCapacity(known, PhaseTwoObservationConfig{MemoryPercent: 1}); got == (ObservationCapacity{}) {
		t.Fatal("one percent of a known container enabled nothing")
	}
	for percent, valid := range map[int]bool{-1: false, 0: true, 1: true, ObservationMemoryPercentMax: true, ObservationMemoryPercentMax + 1: false} {
		if err := (PhaseTwoObservationConfig{MemoryPercent: percent}).validate(); (err == nil) != valid {
			t.Errorf("memory_percent %d validates as %v, want %v", percent, err == nil, valid)
		}
	}
}

// An allocated share gives the lookback an eighth, out of the directory's
// half, so the parts still sum inside the share.
func TestTheLookbackShareComesOutOfTheDirectorys(t *testing.T) {
	known := CapacityInputs{CPUBudget: 2, MemoryLimitBytes: 2 << 30, MemorySource: "pod_limit"}
	got := DeriveObservationCapacity(known, PhaseTwoObservationConfig{MemoryPercent: 4})
	share := (2 << 30) / 100 * 4
	if got.LookbackBytes != share/8 || got.DirectoryBytes != share*3/8 || got.CostBytes != share/4 || got.SampleBufferBytes != share/4 {
		t.Fatalf("share %d split as %+v", share, got)
	}
	if got.DirectoryBytes+got.LookbackBytes+got.CostBytes+got.SampleBufferBytes > share {
		t.Fatalf("the parts exceed the share: %+v", got)
	}
}
