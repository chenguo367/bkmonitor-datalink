// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package openalerts

import (
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/linkdoutput"
)

// openRecord is one alert this process opened: when it first sent the
// ABNORMAL, and the severity the alert stands at after the latest message
// this process sent for it. An empty severity is one the copy could not
// tell (see nextStanding).
type openRecord struct {
	at       time.Time
	severity string
}

// consumerSeverityRank is the consumer's default severity table: a lower
// rank is more severe. The platform's three levels are written under these
// names (linkdoutput.SeverityFor), so for them the order is the level
// order. A severity outside the table cannot be ordered here, because the
// consumer orders it by a table this process does not read.
var consumerSeverityRank = map[string]int{"critical": 1, "warning": 2, "info": 3}

// compareSeverity is negative when a is more severe than b, zero when they
// are the same, positive when a is less severe; ok is false when the two
// cannot be ordered.
func compareSeverity(a, b string) (order int, ok bool) {
	if a == b {
		return 0, true
	}
	rankA, knownA := consumerSeverityRank[a]
	rankB, knownB := consumerSeverityRank[b]
	if !knownA || !knownB {
		return 0, false
	}
	return rankA - rankB, true
}

// outranksAll reports whether no severity in the table is more severe than
// name.
func outranksAll(name string) bool {
	rank, known := consumerSeverityRank[name]
	if !known {
		return false
	}
	for _, other := range consumerSeverityRank {
		if other < rank {
			return false
		}
	}
	return true
}

// nextStanding is what the consumer does with one message to the alert it
// holds on the series. It holds at most one active alert per fingerprint,
// at one severity, and reads the message's evaluations, not its kind:
//
//   - a resolved evaluation at the alert's severity closes it; one at any
//     other severity is an orphan and changes nothing;
//   - a trigger more severe than the alert moves the alert to it, and a
//     resolved evaluation in the same message is superseded; a trigger no
//     more severe leaves the alert where it is;
//   - with no alert left, the most severe trigger opens a new one.
//
// open and severity are the alert before the message; severity "" is one
// the copy does not know. Such an alert is never taken to be closed, since
// nothing says the resolved severity was its own, and a trigger the copy
// cannot order against it leaves its severity unknown. triggered says the
// message carried a trigger, after which the alert is open whatever else
// it said.
func nextStanding(open bool, severity string, levels []contract.LevelResultV1) (stillOpen bool, next string, triggered bool) {
	highest, ordered, closes := "", true, false
	for _, level := range levels {
		switch linkdoutput.LevelAction(level) {
		case linkdoutput.ActionTriggered:
			name := linkdoutput.SeverityFor(level)
			if !triggered {
				highest, triggered = name, true
				continue
			}
			order, ok := compareSeverity(name, highest)
			if !ok {
				ordered = false
			} else if order < 0 {
				highest = name
			}
		case linkdoutput.ActionResolved:
			if open && severity != "" && linkdoutput.SeverityFor(level) == severity {
				closes = true
			}
		}
	}
	if !ordered {
		highest = ""
	}
	switch {
	case !triggered && closes:
		return false, "", false
	case !triggered:
		return open, severity, false
	case !open:
		return true, highest, true
	case highest == "":
		return true, "", true
	case severity == "" && outranksAll(highest):
		// Whatever the alert stood at, this trigger is where it stands now:
		// nothing in the table is more severe, and an alert at a severity
		// outside the table is closed by the consumer before the trigger
		// opens a new one.
		return true, highest, true
	case severity == "":
		return true, "", true
	}
	order, ok := compareSeverity(highest, severity)
	switch {
	case !ok:
		return true, "", true
	case order < 0 || closes:
		return true, highest, true
	default:
		return true, severity, true
	}
}

// standing is what the copy knows of the alert on m before a message:
// whether it is open, and at which severity (empty when it cannot tell).
// Called with the lock held.
//
//   - This process's own record answers first: it follows every message
//     this process sent for the alert.
//   - A RECOVERY this process sent closed it, and the set still carries it:
//     that is the recovery not yet processed, so the same alert at the
//     same severity. Once the set has let it go, nothing is open.
//   - The latest calibration that named the alert's severity, current or
//     not. An alert's severity moves only on a trigger that outranks it,
//     and this process's own triggers are on its record above, so an old
//     calibration is rarely wrong; when it is, the cost is one retention's
//     hide, not a calibration interval, because a recovery at another
//     severity is never taken for the same recovery sent again.
//   - The set carries it, or this process sent its ABNORMAL without room
//     to record it: open, severity unknown.
func (cache *Cache) standing(m member, now time.Time) (bool, string) {
	if record, ok := cache.index.opened[m]; ok {
		return true, record.severity
	}
	entry := cache.index.entries[m.key]
	carried := entry != nil && cache.setCarries(entry, m.fingerprint, now)
	if removed, ok := cache.removed[m]; ok {
		if carried {
			return true, removed.severity
		}
		return false, ""
	}
	if entry != nil {
		if severity, ok := entry.severities[m.fingerprint]; ok {
			return true, severity
		}
	}
	if _, sent := cache.added[m]; sent {
		return true, ""
	}
	return carried, ""
}

// setCarries is the consumer's own word on the fingerprint as last read:
// in the set or found active by a calibration, and not found inactive by a
// current one. Called with the lock held.
func (cache *Cache) setCarries(entry *indexEntry, fingerprint string, now time.Time) bool {
	_, present := entry.index[fingerprint]
	if _, missing := entry.missing[fingerprint]; missing {
		present = true
	}
	if cache.calibrated(entry, now) {
		if _, suppressed := entry.suppressed[fingerprint]; suppressed {
			present = false
		}
	}
	return present
}
