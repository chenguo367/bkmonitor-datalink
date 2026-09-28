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

// Nothing is on until an operator says how much. A container that knows its
// limit and its cores is not an allocation; the default configuration runs
// detection and none of the diagnostics that read the control plane on their
// own account. Past the bound reads as none too, though validation refuses it
// before it gets here.
func TestObservationBudgetIsOffWithoutAnOperatorAllocation(t *testing.T) {
	known := CapacityInputs{CPUBudget: 2, MemoryLimitBytes: 2 << 30, MemorySource: "pod_limit"}
	if got := DeriveObservationCapacity(known, PhaseTwoObservationConfig{}); got != (ObservationCapacity{}) {
		t.Fatalf("the default allocation enabled the diagnostics: %+v", got)
	}
	if got := DeriveObservationCapacity(known, defaultPhaseTwoRuntime().Observation); got != (ObservationCapacity{}) {
		t.Fatalf("the default phase-two configuration allocates a share: %+v", got)
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

// The lookback's share comes out of the directory's, so the parts still sum
// inside the share; off, nothing moves. Enabled with no share is refused.
func TestTheLookbackShareComesOutOfTheDirectorys(t *testing.T) {
	known := CapacityInputs{CPUBudget: 2, MemoryLimitBytes: 2 << 30, MemorySource: "pod_limit"}
	off := DeriveObservationCapacity(known, PhaseTwoObservationConfig{MemoryPercent: 4})
	on := DeriveObservationCapacity(known, PhaseTwoObservationConfig{MemoryPercent: 4, LookbackEnabled: true})
	share := (2 << 30) / 100 * 4
	if off.LookbackBytes != 0 || on.LookbackBytes != share/8 || on.DirectoryBytes != share*3/8 || off.DirectoryBytes != share/2 {
		t.Fatalf("off %+v on %+v", off, on)
	}
	if on.DirectoryBytes+on.LookbackBytes+on.CostBytes+on.SampleBufferBytes > share || on.CostBytes != off.CostBytes {
		t.Fatalf("the lookback's share did not come out of the directory's: %+v", on)
	}
	if err := (PhaseTwoObservationConfig{LookbackEnabled: true}).validate(); err == nil {
		t.Fatal("the lookback enabled with no memory share validated")
	}
	if err := (PhaseTwoObservationConfig{MemoryPercent: 1, LookbackEnabled: true}).validate(); err != nil {
		t.Fatal(err)
	}
}
