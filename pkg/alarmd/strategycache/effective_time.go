package strategycache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/strategy"
	"github.com/go-redis/redis/v8"
)

// LegacyEffectiveTime reads Python's existing business/calendar caches only
// from background maintenance. Its provider methods perform memory lookups.
// A legacy calendar contains currently expanded occurrences, not full rules:
// a matching occurrence proves presence within its explicit interval, whereas
// an empty/missing key never proves historical absence.
type LegacyEffectiveTime struct {
	calendar, cmdb       redis.Cmdable
	prefix               string
	now                  func() time.Time
	maxEntries, maxBytes int
	pruned               time.Time
	mu                   sync.RWMutex
	business             map[string]legacyBusiness
	calendars            map[string]legacyCalendar
}
type legacyBusiness struct {
	zone     string
	location *time.Location
	read     time.Time
}
type legacyOccurrence struct {
	Tenant string `json:"bk_tenant_id"`
	Start  int64  `json:"start_time"`
	End    int64  `json:"end_time"`
}
type legacyCalendar struct {
	items    []legacyOccurrence
	revision string
	read     time.Time
}

func NewLegacyEffectiveTime(calendar, cmdb redis.Cmdable, prefix string, now func() time.Time, maxEntries, maxBytes int) *LegacyEffectiveTime {
	return &LegacyEffectiveTime{calendar: calendar, cmdb: cmdb, prefix: prefix, now: now, maxEntries: maxEntries, maxBytes: maxBytes,
		business: make(map[string]legacyBusiness), calendars: make(map[string]legacyCalendar)}
}

func (c *LegacyEffectiveTime) Provider() strategy.EffectiveTimeProvider {
	return c
}

func (c *LegacyEffectiveTime) Resolve(ctx context.Context, requests []strategy.EffectiveTimeRequest) ([]strategy.EffectiveTimeFact, error) {
	provider := strategy.NewCalendarScheduleProvider(c, c)
	unknown := strategy.NewCalendarScheduleProvider(strategy.TimezoneResolverFunc(func(context.Context, string, string, string) (*time.Location, error) {
		return nil, strategy.ErrEffectiveTimeUnknown
	}), nil)
	facts := make([]strategy.EffectiveTimeFact, len(requests))
	now := c.now()
	for i, request := range requests {
		selected := provider
		// The old cache has no publication version or historical coverage.
		// Its explicit compatibility scope is the current wall-clock minute;
		// historical replay needs the full snapshot and remains UNKNOWN here.
		if request.EvaluationTime < now.Truncate(time.Minute).Unix() || request.EvaluationTime > now.Unix() {
			selected = unknown
		}
		resolved, err := selected.Resolve(ctx, []strategy.EffectiveTimeRequest{request})
		if err != nil {
			return nil, err
		}
		facts[i] = resolved[0]
	}
	return facts, nil
}

// Refresh is called for owned legacy plans. Identical dependencies are read at
// most once per minute and unused entries expire, with a hard memory bound.
func (c *LegacyEffectiveTime) Refresh(ctx context.Context, tenant, business string, ids []int64) error {
	if c == nil || c.cmdb == nil {
		return errors.New("legacy effective time cache is unavailable")
	}
	now := c.now()
	key := tenant + "\x00" + business
	c.mu.Lock()
	if now.Sub(c.pruned) >= time.Minute {
		for k, v := range c.business {
			if now.Sub(v.read) > 5*time.Minute {
				delete(c.business, k)
			}
		}
		for k, v := range c.calendars {
			if now.Sub(v.read) > 5*time.Minute {
				delete(c.calendars, k)
			}
		}
		c.pruned = now
	}
	b := c.business[key]
	c.mu.Unlock()
	if now.Sub(b.read) >= time.Minute {
		raw, err := c.cmdb.HGet(ctx, c.prefix+".cache.cmdb.business", business).Bytes()
		if err != nil {
			return err
		}
		if len(raw) > c.maxBytes {
			return errors.New("legacy business cache exceeds byte budget")
		}
		var value struct {
			Tenant string `json:"bk_tenant_id"`
			Zone   string `json:"time_zone"`
			Extend struct {
				Zone string `json:"time_zone"`
			} `json:"extend"`
		}
		if err := json.Unmarshal(raw, &value); err != nil {
			return err
		}
		if value.Tenant == "" {
			value.Tenant = "system"
		}
		if value.Tenant != tenant {
			return errors.New("legacy business cache tenant mismatch")
		}
		if value.Extend.Zone != "" {
			value.Zone = value.Extend.Zone
		}
		location, err := time.LoadLocation(value.Zone)
		if err != nil || value.Zone == "" || value.Zone == "Local" {
			return errors.New("legacy business timezone missing or invalid")
		}
		c.mu.Lock()
		if len(c.business) < c.maxEntries || !b.read.IsZero() {
			c.business[key] = legacyBusiness{zone: value.Zone, location: location, read: now}
		}
		c.mu.Unlock()
	}
	for _, id := range ids {
		key := tenant + "\x00" + strconv.FormatInt(id, 10)
		c.mu.RLock()
		prior := c.calendars[key]
		c.mu.RUnlock()
		if now.Sub(prior.read) < time.Minute {
			continue
		}
		if c.calendar == nil {
			return errors.New("legacy calendar connection is unavailable")
		}
		raw, err := c.calendar.GetRange(ctx, c.prefix+".cache.calendar."+strconv.FormatInt(id, 10), 0, int64(c.maxBytes)).Bytes()
		if err != nil {
			return err
		}
		if len(raw) == 0 || len(raw) > c.maxBytes {
			return errors.New("legacy calendar missing or exceeds byte budget")
		}
		var items []legacyOccurrence
		if err := json.Unmarshal(raw, &items); err != nil {
			return err
		}
		filtered := make([]legacyOccurrence, 0, len(items))
		for _, item := range items {
			if item.Tenant == "" {
				item.Tenant = "system"
			}
			if item.Tenant != tenant {
				continue
			}
			if item.Start <= 0 || item.End < item.Start {
				return errors.New("legacy calendar occurrence has invalid bounds")
			}
			filtered = append(filtered, item)
		}
		digest := sha256.Sum256(raw)
		c.mu.Lock()
		bytes := len(filtered) * 64
		for k, v := range c.calendars {
			if k != key {
				bytes += len(v.items) * 64
			}
		}
		if (len(c.calendars) < c.maxEntries || !prior.read.IsZero()) && bytes <= c.maxBytes {
			c.calendars[key] = legacyCalendar{items: filtered, revision: hex.EncodeToString(digest[:]), read: now}
		} else {
			c.mu.Unlock()
			return errors.New("legacy calendar memory budget exhausted")
		}
		c.mu.Unlock()
	}
	return nil
}

func (c *LegacyEffectiveTime) ResolveBusinessTimezone(_ context.Context, tenant, business string) (string, bool, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	v, ok := c.business[tenant+"\x00"+business]
	return v.zone, ok && c.now().Sub(v.read) <= 2*time.Minute, nil
}

func (c *LegacyEffectiveTime) ResolveTimezone(_ context.Context, ref, tenant, business string) (*time.Location, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	v, ok := c.business[tenant+"\x00"+business]
	if ref != "BUSINESS_LOCAL" || !ok || c.now().Sub(v.read) > 2*time.Minute {
		return nil, strategy.ErrEffectiveTimeUnknown
	}
	return v.location, nil
}

func (c *LegacyEffectiveTime) ResolveCalendarFacts(_ context.Context, requests []strategy.CalendarFactRequest) ([]strategy.CalendarFact, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	facts := make([]strategy.CalendarFact, len(requests))
	for i, r := range requests {
		facts[i].Request = r
		if r.EvaluationTime < c.now().Truncate(time.Minute).Unix() || r.EvaluationTime > c.now().Unix() {
			continue
		}
		v, ok := c.calendars[r.TenantID+"\x00"+strconv.FormatInt(r.CalendarID, 10)]
		if !ok || c.now().Sub(v.read) > 2*time.Minute {
			continue
		}
		for _, item := range v.items {
			if item.Start <= r.EvaluationTime && r.EvaluationTime <= item.End && item.End < int64(^uint64(0)>>1) {
				facts[i] = strategy.CalendarFact{Request: r, Known: true, Matched: true, Revision: v.revision, ValidFrom: item.Start, ValidUntil: item.End + 1}
				break
			}
		}
	}
	return facts, nil
}
