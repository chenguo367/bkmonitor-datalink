// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package openalerts

import (
	"sort"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
)

// episodeBackstopIntervals is how many of the strategy's detection
// intervals after an episode's first acknowledged RECOVERY this process's
// record of the send (added) is let go even though a Level of the episode
// never had its own RECOVERY: twice the longest spacing measured between the
// first RECOVERY at one Level and the first at another of the same alert
// (11 intervals, a two-Level strategy at 60 s over 17 alert ends).
const episodeBackstopIntervals = 22

// episodeLevels is how many Level ids an episode records, as bits: a Level
// id at or past it makes the episode one only the backstop lets go.
const episodeLevels = 64

// episode is this process's account of an alert's current ABNORMAL
// episode, from the envelopes the broker took: the Levels its ABNORMALs
// named abnormal, the Levels that have had their own RECOVERY since, the
// first RECOVERY of the episode, and the strategy's detection interval
// read off the envelopes' windows. The alert's own RECOVERY is at the Level
// the link holds it at, which may come intervals after another Level's
// (the warning recovers while the info alert stays open), so what this
// process sent is let go only once every Level of the episode has
// recovered, or past the backstop (released).
type episode struct {
	abnormal, recovered uint64
	// unaccounted is an episode whose Levels this process cannot know: it
	// first saw it in a RECOVERY, or in an ABNORMAL while its record of the
	// send outlived the episode's (let go to the bound or by a calibration),
	// or while the link already holds the alert with no send of this
	// process behind it - another replica opened it and the series moved
	// here mid-episode - or within a backstop of its own start - a restart
	// mid-episode - or a Level id past episodeLevels. Only the backstop
	// lets it go.
	unaccounted   bool
	firstRecovery time.Time
	step          time.Duration
	// at is the last envelope's, for the bound.
	at time.Time
}

// complete is whether every Level the episode named abnormal has had its
// own RECOVERY.
func (e episode) complete() bool {
	return !e.unaccounted && e.abnormal != 0 && e.abnormal&^e.recovered == 0
}

// backstop is episodeBackstopIntervals of the strategy's interval, or
// fallback where no envelope said the interval.
func (e episode) backstop(fallback time.Duration) time.Duration {
	if e.step <= 0 {
		return fallback
	}
	return episodeBackstopIntervals * e.step
}

// released is whether the episode is over as far as this process's sends
// say: it has had a RECOVERY, and every Level recovered or the backstop
// passed since its first.
func (e episode) released(now time.Time, fallback time.Duration) bool {
	if e.firstRecovery.IsZero() {
		return false
	}
	return e.complete() || now.Sub(e.firstRecovery) > e.backstop(fallback)
}

// levelStep is a Level result's detection interval, from its trigger
// window: window_size positions a step apart ending at window_end
// (trigger windowStartV2). Zero where the window says nothing.
func levelStep(result contract.LevelResultV1) time.Duration {
	window := result.DecisionWindow.Trigger
	if window.WindowSize == 0 || window.WindowEnd < window.WindowStart {
		return 0
	}
	return time.Duration((window.WindowEnd-window.WindowStart+1)/int64(window.WindowSize)) * time.Second
}

// noteEpisode takes one acknowledged envelope of m into its episode. An
// ABNORMAL after the episode was released starts the next one; one before
// adds its abnormal Levels, takes back their recoveries and restarts the
// wait for a first RECOVERY. A RECOVERY marks the Levels it recovered.
// Called with the lock held, before the ABNORMAL is taken into added.
func (cache *Cache) noteEpisode(m member, event contract.TriggerEventV1, now time.Time) {
	fallback := cache.index.options.ReconcileInterval
	e, known := cache.episodes[m]
	_, sent := cache.added[m]
	abnormal := event.EventKind == contract.TriggerEventAbnormal
	switch {
	case abnormal && (!known || e.released(now, fallback)):
		e = episode{unaccounted: !known && (sent || cache.linkHolds(m, now))}
	case !known:
		e = episode{unaccounted: true}
	}
	for _, result := range event.LevelResults {
		if step := levelStep(result); step > e.step {
			e.step = step
		}
		if result.LevelID >= episodeLevels {
			if result.Result == contract.LevelResultAbnormal || result.Result == contract.LevelResultRecovery {
				e.unaccounted = true
			}
			continue
		}
		bit := uint64(1) << result.LevelID
		switch result.Result {
		case contract.LevelResultAbnormal:
			e.abnormal |= bit
			e.recovered &^= bit
		case contract.LevelResultRecovery:
			e.recovered |= bit
		}
	}
	if abnormal {
		e.firstRecovery = time.Time{}
		if now.Sub(cache.gateSince) <= e.backstop(fallback) {
			// It may have begun before this process: the Levels named abnormal
			// before it are not in memory.
			e.unaccounted = true
		}
	} else if e.firstRecovery.IsZero() {
		e.firstRecovery = now
	}
	e.at = now
	cache.episodes[m] = e
}

// linkHolds is whether the link's own answer, read before this envelope -
// the set's fresh read, or a current calibration - already names m active.
// Called with the lock held.
func (cache *Cache) linkHolds(m member, now time.Time) bool {
	entry := cache.index.entries[m.key]
	if entry == nil || cache.stale(entry, now) || (entry.indexReadAt.IsZero() && !cache.calibrated(entry, now)) {
		return false
	}
	return cache.setCarries(entry, m.fingerprint, now)
}

// boundEpisodes keeps the episodes inside maxLocal by letting the oldest go,
// counted as evicted: such an alert's next envelope opens an unaccounted
// episode, which only the backstop lets go. Called with the lock held.
func (cache *Cache) boundEpisodes() {
	over := len(cache.episodes) - cache.maxLocal
	if over <= 0 {
		return
	}
	type aged struct {
		m  member
		at time.Time
	}
	all := make([]aged, 0, len(cache.episodes))
	for m, e := range cache.episodes {
		all = append(all, aged{m: m, at: e.at})
	}
	sort.Slice(all, func(i, j int) bool { return all[i].at.Before(all[j].at) })
	for _, entry := range all[:over] {
		delete(cache.episodes, entry.m)
		cache.evictions++
	}
}
