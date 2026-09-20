package config

// ObservationCapacity partitions an additional 1/32 of known container memory
// for diagnostics. It is an admission allowance, not a claim of measured RSS.
// Entry/buffer counts must be derived from the consuming type's size estimate.
// Missing resource discovery disables the new collectors rather than treating
// the existing execution fallback as a diagnostics allocation.
type ObservationCapacity struct {
	DirectoryBytes         int
	DirectoryReadBytes     int
	DirectoryCommands      int
	CostBytes              int
	SampleBufferBytes      int
	SampleBytesPerMinute   int
	SampleRecordsPerMinute int
}

func DeriveObservationCapacity(in CapacityInputs) ObservationCapacity {
	if in.MemorySource == "" || in.MemorySource == memorySourceFallback || in.MemoryLimitBytes == 0 || in.CPUBudget <= 0 {
		return ObservationCapacity{}
	}
	// Keep integer conversions below the platform limit and return a disabled
	// allowance for a resource input no platform could represent.
	if in.MemoryLimitBytes/32 > uint64(int(^uint(0)>>1)) {
		return ObservationCapacity{}
	}
	mem := int(in.MemoryLimitBytes / 32)
	// A per-core allowance scales the command and sample rate, independently
	// of retained memory. These are conservative admission coefficients, not
	// a fixed deployment capacity or a CPU-per-strategy attribution.
	ops := min(min(in.CPUBudget, mem/4096)*64, mem/4096)
	if ops < 8 || mem < 64<<10 {
		return ObservationCapacity{}
	}
	return ObservationCapacity{DirectoryBytes: mem / 2, DirectoryReadBytes: mem / 16,
		DirectoryCommands: ops, CostBytes: mem / 4, SampleBufferBytes: mem / 4,
		SampleRecordsPerMinute: ops, SampleBytesPerMinute: min(mem/16, ops*4096)}
}
