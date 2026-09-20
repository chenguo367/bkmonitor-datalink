package config

import "testing"

func TestObservationBudgetUsesBothResourcesAndHasNoUnknownFallback(t *testing.T) {
	a := DeriveObservationCapacity(CapacityInputs{CPUBudget: 1, MemoryLimitBytes: 1 << 30, MemorySource: "pod_limit"})
	b := DeriveObservationCapacity(CapacityInputs{CPUBudget: 2, MemoryLimitBytes: 2 << 30, MemorySource: "pod_limit"})
	if a.DirectoryBytes*2 != b.DirectoryBytes || a.SampleRecordsPerMinute*2 != b.SampleRecordsPerMinute {
		t.Fatalf("budget did not scale: %+v %+v", a, b)
	}
	if a.DirectoryBytes+a.CostBytes+a.SampleBufferBytes > 1<<30/32 {
		t.Fatal("exceeded memory share")
	}
	if got := DeriveObservationCapacity(CapacityInputs{CPUBudget: 2, MemoryLimitBytes: 2 << 30, MemorySource: memorySourceFallback}); got != (ObservationCapacity{}) {
		t.Fatalf("unknown budget enabled: %+v", got)
	}
}
