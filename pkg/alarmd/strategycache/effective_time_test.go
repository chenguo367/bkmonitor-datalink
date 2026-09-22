package strategycache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/strategy"
	"github.com/go-redis/redis/v8"
)

// Only background Redis reads are implemented. Any accidental Redis access
// from a provider lookup is observable through calls (or the embedded nil).
type effectiveCacheRedis struct {
	redis.Cmdable
	business, calendar string
	err                error
	calls              int
}

func (r *effectiveCacheRedis) HGet(_ context.Context, key, field string) *redis.StringCmd {
	r.calls++
	if key != "monitor.cache.cmdb.business" || field != "2" {
		return redis.NewStringResult("", fmt.Errorf("unexpected business key %s %s", key, field))
	}
	return redis.NewStringResult(r.business, r.err)
}
func (r *effectiveCacheRedis) GetRange(_ context.Context, key string, start, end int64) *redis.StringCmd {
	r.calls++
	if key != "monitor.cache.calendar.7" || start != 0 || end <= 0 {
		return redis.NewStringResult("", fmt.Errorf("unexpected calendar range %s %d %d", key, start, end))
	}
	return redis.NewStringResult(r.calendar, r.err)
}

func effectiveCacheFixture(t *testing.T) (*LegacyEffectiveTime, *effectiveCacheRedis, *time.Time) {
	t.Helper()
	now := time.Date(2026, 9, 22, 12, 0, 30, 0, time.UTC)
	source := &effectiveCacheRedis{business: `{"bk_tenant_id":"tenant-a","time_zone":"UTC"}`, calendar: fmt.Sprintf(`[{"bk_tenant_id":"tenant-a","start_time":%d,"end_time":%d}]`, now.Add(-24*time.Hour).Unix(), now.Add(24*time.Hour).Unix())}
	cache := NewLegacyEffectiveTime(source, source, "monitor", func() time.Time { return now }, 64, 64<<10)
	return cache, source, &now
}

func effectiveRequest(t *testing.T, tenant string, at int64, calendar bool) strategy.EffectiveTimeRequest {
	t.Helper()
	raw := `{"time_ranges":[{"start":"11:00","end":"13:00"}]}`
	if calendar {
		raw = `{"time_ranges":[{"start":"11:00","end":"13:00"}],"active_calendars":[7]}`
	}
	requirement, err := strategy.CompileUptime(json.RawMessage(raw))
	if err != nil {
		t.Fatal(err)
	}
	return strategy.EffectiveTimeRequest{TenantID: tenant, BusinessID: "2", EvaluationTime: at, Requirement: requirement}
}

func requireEffectiveStatus(t *testing.T, cache *LegacyEffectiveTime, request strategy.EffectiveTimeRequest, want string) {
	t.Helper()
	facts, err := cache.Provider().Resolve(context.Background(), []strategy.EffectiveTimeRequest{request})
	if err != nil {
		t.Fatalf("provider returned dependency error instead of %s: %v", want, err)
	}
	if len(facts) != 1 || facts[0].Status() != want {
		t.Fatalf("facts=%+v want status=%s", facts, want)
	}
}

func TestLegacyEffectiveTimeCurrentMinuteOnly(t *testing.T) {
	for _, calendar := range []bool{false, true} {
		t.Run(fmt.Sprintf("calendar=%v", calendar), func(t *testing.T) {
			cache, source, now := effectiveCacheFixture(t)
			if err := cache.Refresh(context.Background(), "tenant-a", "2", []int64{7}); err != nil {
				t.Fatal(err)
			}
			reads := source.calls
			for _, tt := range []struct {
				name string
				at   int64
				want string
			}{
				{"minute-start", now.Truncate(time.Minute).Unix(), strategy.EffectiveTimeActive},
				{"current-second", now.Unix(), strategy.EffectiveTimeActive},
				{"previous-minute", now.Truncate(time.Minute).Unix() - 1, strategy.EffectiveTimeUnknown},
				{"historical-covered-occurrence", now.Add(-time.Hour).Unix(), strategy.EffectiveTimeUnknown},
				{"future-second", now.Unix() + 1, strategy.EffectiveTimeUnknown},
			} {
				t.Run(tt.name, func(t *testing.T) {
					requireEffectiveStatus(t, cache, effectiveRequest(t, "tenant-a", tt.at, calendar), tt.want)
				})
			}
			if source.calls != reads {
				t.Fatal("provider performed Redis reads in execution path")
			}
		})
	}
}

func TestLegacyCalendarFactCannotProveHistoricalOrEmptyAbsence(t *testing.T) {
	for _, tt := range []struct {
		name, calendar string
		wantKnown      bool
	}{
		{"matching", `[{"bk_tenant_id":"tenant-a","start_time":1,"end_time":2000000000}]`, true},
		{"empty", `[]`, false},
		{"foreign-tenant", `[{"bk_tenant_id":"tenant-b","start_time":1,"end_time":2000000000}]`, false},
		{"not-matching", `[{"bk_tenant_id":"tenant-a","start_time":1,"end_time":2}]`, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cache, source, now := effectiveCacheFixture(t)
			source.calendar = tt.calendar
			if err := cache.Refresh(context.Background(), "tenant-a", "2", []int64{7}); err != nil {
				t.Fatal(err)
			}
			requests := []strategy.CalendarFactRequest{{TenantID: "tenant-a", CalendarID: 7, EvaluationTime: now.Unix()}, {TenantID: "tenant-a", CalendarID: 7, EvaluationTime: now.Add(-time.Hour).Unix()}, {TenantID: "tenant-b", CalendarID: 7, EvaluationTime: now.Unix()}}
			facts, err := cache.ResolveCalendarFacts(context.Background(), requests)
			if err != nil {
				t.Fatal(err)
			}
			if facts[0].Known != tt.wantKnown || facts[0].Matched != tt.wantKnown || facts[1].Known || facts[2].Known {
				t.Fatalf("facts=%+v", facts)
			}
			want := strategy.EffectiveTimeUnknown
			if tt.wantKnown {
				want = strategy.EffectiveTimeActive
			}
			requireEffectiveStatus(t, cache, effectiveRequest(t, "tenant-a", now.Unix(), true), want)
		})
	}
}

func TestLegacyEffectiveTimeTenantAndTimezone(t *testing.T) {
	t.Run("business-tenant-mismatch", func(t *testing.T) {
		cache, _, now := effectiveCacheFixture(t)
		if err := cache.Refresh(context.Background(), "tenant-b", "2", nil); err == nil {
			t.Fatal("foreign tenant business was loaded")
		}
		requireEffectiveStatus(t, cache, effectiveRequest(t, "tenant-b", now.Unix(), false), strategy.EffectiveTimeUnknown)
	})
	t.Run("extend-timezone-wins", func(t *testing.T) {
		cache, source, now := effectiveCacheFixture(t)
		source.business = `{"bk_tenant_id":"tenant-a","time_zone":"UTC","extend":{"time_zone":"Asia/Kolkata"}}`
		if err := cache.Refresh(context.Background(), "tenant-a", "2", nil); err != nil {
			t.Fatal(err)
		}
		requireEffectiveStatus(t, cache, effectiveRequest(t, "tenant-a", now.Unix(), false), strategy.EffectiveTimeInactive)
	})
	t.Run("missing-tenant-is-system", func(t *testing.T) {
		cache, source, now := effectiveCacheFixture(t)
		source.business = `{"time_zone":"UTC"}`
		source.calendar = `[{"start_time":1,"end_time":2000000000}]`
		if err := cache.Refresh(context.Background(), "system", "2", []int64{7}); err != nil {
			t.Fatal(err)
		}
		requireEffectiveStatus(t, cache, effectiveRequest(t, "system", now.Unix(), true), strategy.EffectiveTimeActive)
	})
	for _, zone := range []string{"", "Local", "invalid/zone"} {
		t.Run("invalid-zone-"+zone, func(t *testing.T) {
			cache, source, _ := effectiveCacheFixture(t)
			source.business = fmt.Sprintf(`{"bk_tenant_id":"tenant-a","time_zone":%q}`, zone)
			if err := cache.Refresh(context.Background(), "tenant-a", "2", nil); err == nil {
				t.Fatal("invalid timezone loaded")
			}
		})
	}
}

func TestLegacyEffectiveTimeFreshnessAndRefreshFailure(t *testing.T) {
	cache, source, now := effectiveCacheFixture(t)
	if err := cache.Refresh(context.Background(), "tenant-a", "2", []int64{7}); err != nil {
		t.Fatal(err)
	}
	reads := source.calls
	if err := cache.Refresh(context.Background(), "tenant-a", "2", []int64{7}); err != nil {
		t.Fatal(err)
	}
	if reads != source.calls {
		t.Fatal("same-minute refresh reread shared dependencies")
	}
	*now = now.Add(2*time.Minute + time.Second)
	source.err = errors.New("cache disconnected")
	if err := cache.Refresh(context.Background(), "tenant-a", "2", []int64{7}); err == nil {
		t.Fatal("dependency failure hidden")
	}
	requireEffectiveStatus(t, cache, effectiveRequest(t, "tenant-a", now.Unix(), true), strategy.EffectiveTimeUnknown)
	requireEffectiveStatus(t, cache, effectiveRequest(t, "tenant-a", now.Unix(), false), strategy.EffectiveTimeUnknown)
}

func TestLegacyEffectiveTimeMissingCalendarIsUnknown(t *testing.T) {
	cache, source, now := effectiveCacheFixture(t)
	source.calendar = ""
	if err := cache.Refresh(context.Background(), "tenant-a", "2", []int64{7}); err == nil {
		t.Fatal("missing calendar was accepted as authoritative empty")
	}
	requireEffectiveStatus(t, cache, effectiveRequest(t, "tenant-a", now.Unix(), true), strategy.EffectiveTimeUnknown)
}
