// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License.

package observability

import (
	"context"
	"testing"
	"time"
)

// The reading carries the period the census learned, and none while it has
// learned none: which Query Groups a Worker's high-water over a window may
// not have seen run (014 C1 section 8).
func TestTheCensusReadingCarriesTheLearnedPeriod(t *testing.T) {
	now := time.Unix(36000, 0)
	census := NewRetainedPeakCensus(5*time.Minute, func() time.Time { return now })
	census.Observe(context.Background(), slotCompletionOf("qg-daily", 36000, 0, 700))
	if peaks := census.RetainedPeaks(); len(peaks) != 1 || peaks[0].Interval != 0 {
		t.Fatalf("after one Slot the census reads %+v, want no period yet", peaks)
	}
	now = time.Unix(36000+86400, 0)
	census.Observe(context.Background(), slotCompletionOf("qg-daily", 36000+86400, 0, 400))
	if peaks := census.RetainedPeaks(); len(peaks) != 1 || peaks[0].Interval != 24*time.Hour {
		t.Fatalf("after two daily Slots the census reads %+v, want the day", peaks)
	}
}
