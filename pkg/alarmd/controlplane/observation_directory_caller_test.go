package controlplane_test

import (
	"context"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/controlplane"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/redisfailure"
)

// callerSpy records the job each read's context names.
type callerSpy struct {
	redis.Cmdable
	callers []string
}

func (s *callerSpy) GetRange(ctx context.Context, key string, start, end int64) *redis.StringCmd {
	s.callers = append(s.callers, redisfailure.Caller(ctx))
	return s.Cmdable.GetRange(ctx, key, start, end)
}

// The directory names its Redis reads: every read of a refresh as
// directory_refresh, and a point read for an answer - a Plan's content or
// its output context - as directory_read, so
// the diagnostics client's failures say which of the two lost its read.
func TestTheDirectoryNamesItsRedisReads(t *testing.T) {
	h, _, at := directoryFixture(t, 32)
	spy := &callerSpy{Cmdable: h.client}
	d, err := controlplane.NewObservationDirectory(h.newRepository(t), controlplane.DirectoryLimits{WireBytes: 1 << 20, Commands: 32, Entries: 100,
		Timeout: time.Second, FreshFor: time.Minute}, spy)
	if err != nil {
		t.Fatal(err)
	}
	d.Refresh(h.ctx, at)
	refreshed := append([]string(nil), spy.callers...)
	rows := d.Page(at, "", "", "", 0, 20).Rows
	if len(refreshed) == 0 || len(rows) == 0 {
		t.Fatalf("refresh read %v and listed %d rows, want both", refreshed, len(rows))
	}
	for _, caller := range refreshed {
		if caller != redisfailure.CallerDirectoryRefresh {
			t.Fatalf("a refresh read named itself %q: %v", caller, refreshed)
		}
	}
	// A fresh repository, so the point read goes to Redis rather than to the
	// objects the refresh already holds.
	point := &callerSpy{Cmdable: h.client}
	d, err = controlplane.NewObservationDirectory(h.newRepository(t), controlplane.DirectoryLimits{WireBytes: 1 << 20, Commands: 32, Entries: 100,
		Timeout: time.Second, FreshFor: time.Minute}, point)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.EffectivePlan(h.ctx, rows[0]); err != nil {
		t.Fatal(err)
	}
	planned := len(point.callers)
	output := rows[0]
	output.OutputContext = "output-context-not-cached"
	_ = d.EffectiveOutput(h.ctx, output)
	if planned == 0 || len(point.callers) == planned {
		t.Fatalf("point reads = %v after the plan read's %d, want both the plan and the output read to reach Redis", point.callers, planned)
	}
	for _, caller := range point.callers {
		if caller != redisfailure.CallerDirectoryRead {
			t.Fatalf("a point read named itself %q: %v", caller, point.callers)
		}
	}
}
