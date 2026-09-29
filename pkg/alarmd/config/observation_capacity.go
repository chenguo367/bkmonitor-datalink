package config

// ObservationCapacity partitions the operator's share of known container
// memory for diagnostics. It is an admission allowance, not a claim of
// measured RSS. Entry/buffer counts must be derived from the consuming type's
// size estimate. Missing resource discovery disables the new collectors rather
// than treating the existing execution fallback as a diagnostics allocation;
// so does an operator who has not allocated a share.
type ObservationCapacity struct {
	DirectoryBytes         int
	DirectoryReadBytes     int
	DirectoryCommands      int
	CostBytes              int
	SampleBufferBytes      int
	SampleBytesPerMinute   int
	SampleRecordsPerMinute int
	// LookbackBytes is the kept first reads' share: an eighth of the
	// allocated share, or of DefaultLookbackSharePercent without one.
	LookbackBytes int
}

// DefaultLookbackSharePercent is the share the lookback takes its eighth of
// when a deployment allocates none, the share a deployment that allocates
// one usually gives. The lookback runs wherever the container's memory is
// known; the other diagnostics only where an operator allocated a share.
const DefaultLookbackSharePercent = 5

func DeriveObservationCapacity(in CapacityInputs, allocation PhaseTwoObservationConfig) ObservationCapacity {
	// An invalid share is refused by configuration validation; here it
	// reads as none of anything.
	if allocation.MemoryPercent < 0 || allocation.MemoryPercent > ObservationMemoryPercentMax {
		return ObservationCapacity{}
	}
	if in.MemorySource == "" || in.MemorySource == memorySourceFallback || in.MemoryLimitBytes == 0 || in.CPUBudget <= 0 {
		return ObservationCapacity{}
	}
	// Keep integer conversions below the platform limit and return a disabled
	// allowance for a resource input no platform could represent.
	if in.MemoryLimitBytes/100 > uint64(int(^uint(0)>>1))/uint64(max(allocation.MemoryPercent, DefaultLookbackSharePercent)) {
		return ObservationCapacity{}
	}
	if allocation.MemoryPercent == 0 {
		// No share allocated: the default is a deployment that runs
		// detection and, of this, the lookback alone.
		return ObservationCapacity{LookbackBytes: int(in.MemoryLimitBytes/100*DefaultLookbackSharePercent) / 8}
	}
	mem := int(in.MemoryLimitBytes / 100 * uint64(allocation.MemoryPercent))
	// A per-core allowance scales the command and sample rate, independently
	// of retained memory. These are conservative admission coefficients, not
	// a fixed deployment capacity or a CPU-per-strategy attribution.
	ops := min(min(in.CPUBudget, mem/4096)*64, mem/4096)
	if ops < 8 || mem < 64<<10 {
		return ObservationCapacity{}
	}
	// The lookback's eighth comes out of the directory's half, so the parts
	// still sum to the share.
	return ObservationCapacity{DirectoryBytes: mem * 3 / 8, DirectoryReadBytes: mem / 16,
		DirectoryCommands: ops, CostBytes: mem / 4, SampleBufferBytes: mem / 4,
		SampleRecordsPerMinute: ops, SampleBytesPerMinute: min(mem/16, ops*4096), LookbackBytes: mem / 8}
}
