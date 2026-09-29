// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package fleet

import "time"

// ReplicaPart is one replica's share of the numbers the first screen draws
// from rows, in the shape that adds across replicas: counts add, sets take
// their union, and a moment keeps the earliest. The part of a view of one
// replica's snapshot, merged with the others', gives the numbers the whole
// view gives -- which is what lets a replica publish its part and the leader
// read parts instead of every row (fleet-read-scale-design §6).
//
// A duration is never kept, only the moment it runs from: the reader turns
// it into a duration at its own time, so a part read later reads older.
type ReplicaPart struct {
	Replica string
	// Attribution counts the anomaly column's rows by who they are
	// attributed to: what the verdict and the per-replica split are read from.
	Attribution AttributionTally
	Impact      ImpactTally
	// EmptyEveryRound is the objects of KindEmptyEveryRound among the no-data
	// rows. An object is one replica's, so the count adds.
	EmptyEveryRound int
	// DemotedDue is the pooled objects whose cooldown has ended and
	// DemotedDueSince the earliest end among them, zero when none has.
	DemotedDue      int
	DemotedDueSince time.Time
}

// AttributionTally is the anomaly column's rows by attribution: Ours and
// External as attributed, Unknown with no evidence either way, and Other
// with the field not set at all -- which the per-replica split counts as
// unattributed and the verdict does not (Settle, UnattributedCount).
type AttributionTally struct {
	Ours     int
	External int
	Unknown  int
	Other    int
}

// ReplicaPartOf reads a view's part. A view of one replica's snapshot gives
// that replica's; the rows must have been decided at now (Decide).
func ReplicaPartOf(view View, now time.Time) ReplicaPart {
	part := ReplicaPart{Impact: ImpactTallyOf(view, now)}
	if len(view.PerReplica) == 1 {
		part.Replica = view.PerReplica[0].Replica
	}
	for _, anomaly := range view.Anomalies {
		switch anomaly.Attribution {
		case AttributionOurs:
			part.Attribution.Ours++
		case AttributionExternal:
			part.Attribution.External++
		case AttributionUnknown:
			part.Attribution.Unknown++
		default:
			part.Attribution.Other++
		}
	}
	part.EmptyEveryRound = countEmptyEveryRound(view.NoData)
	for _, demoted := range view.Demoted {
		if cooldown := demoted.QueryCooldown; cooldown != nil && !cooldown.Until.IsZero() && cooldown.Until.Before(now) {
			part.DemotedDue++
			if part.DemotedDueSince.IsZero() || cooldown.Until.Before(part.DemotedDueSince) {
				part.DemotedDueSince = cooldown.Until
			}
		}
	}
	return part
}

// MergeReplicaParts adds replicas' parts into the deployment's.
func MergeReplicaParts(parts ...ReplicaPart) ReplicaPart {
	merged := ReplicaPart{}
	tallies := make([]ImpactTally, 0, len(parts))
	for _, part := range parts {
		merged.Attribution.Ours += part.Attribution.Ours
		merged.Attribution.External += part.Attribution.External
		merged.Attribution.Unknown += part.Attribution.Unknown
		merged.Attribution.Other += part.Attribution.Other
		tallies = append(tallies, part.Impact)
		merged.EmptyEveryRound += part.EmptyEveryRound
		merged.DemotedDue += part.DemotedDue
		if !part.DemotedDueSince.IsZero() && (merged.DemotedDueSince.IsZero() || part.DemotedDueSince.Before(merged.DemotedDueSince)) {
			merged.DemotedDueSince = part.DemotedDueSince
		}
	}
	merged.Impact = MergeImpactTallies(tallies...)
	return merged
}

// DemotedDueOldestSeconds is how long the earliest due pooled object has
// waited at now, whole seconds, as the view counts it.
func (part ReplicaPart) DemotedDueOldestSeconds(now time.Time) int {
	if part.DemotedDueSince.IsZero() {
		return 0
	}
	return int(now.Sub(part.DemotedDueSince).Seconds())
}
