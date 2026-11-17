// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package segment

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// Mark is a Plan's mark: the round W up to whose position every open member
// of its pages has a point where one was expected, every Level valid and not
// anomalous, and the page renewal table (Renewed, one a page).
type Mark struct {
	// Version is the round's apply version; Position the source time of its
	// last point, the U an open segment is read up to.
	Version  execution.ApplyVersion
	Position int64
	Pages    int
	Renewed  []Renewal
}

// Renewal is a page's last renewal: At, the wall clock in milliseconds the
// worker read before its first segment was sent, and TTL, the lifetime in
// milliseconds that renewal gave the members' keys. Every open member's key
// expires no sooner than At + TTL (the invariant R the renewal rules keep).
type Renewal struct {
	At  int64 `json:"at"`
	TTL int64 `json:"ttl"`
}

// ErrMark is a mark refused, by what is wrong with it.
var ErrMark = errors.New("segment: not a mark")

const markSchema = "segment-mark-v1"

type markJSON struct {
	Schema   string                 `json:"schema"`
	Version  execution.ApplyVersion `json:"version"`
	Position int64                  `json:"position"`
	Pages    int                    `json:"pages"`
	Renewed  []Renewal              `json:"renewed"`
}

// EncodeMark is a mark's bytes.
func EncodeMark(mark Mark) ([]byte, error) {
	if err := mark.check(); err != nil {
		return nil, err
	}
	return json.Marshal(markJSON{Schema: markSchema, Version: mark.Version, Position: mark.Position,
		Pages: mark.Pages, Renewed: mark.Renewed})
}

// DecodeMark reads a mark back.
func DecodeMark(raw []byte) (Mark, error) {
	var decoded markJSON
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return Mark{}, fmt.Errorf("%w: %v", ErrMark, err)
	}
	if decoded.Schema != markSchema {
		return Mark{}, fmt.Errorf("%w: schema %q", ErrMark, decoded.Schema)
	}
	mark := Mark{Version: decoded.Version, Position: decoded.Position, Pages: decoded.Pages, Renewed: decoded.Renewed}
	if err := mark.check(); err != nil {
		return Mark{}, err
	}
	return mark, nil
}

func (mark Mark) check() error {
	if mark.Pages < 1 || mark.Pages&(mark.Pages-1) != 0 {
		return fmt.Errorf("%w: %d pages is not a power of two", ErrMark, mark.Pages)
	}
	if len(mark.Renewed) != mark.Pages {
		return fmt.Errorf("%w: %d renewals for %d pages", ErrMark, len(mark.Renewed), mark.Pages)
	}
	if mark.Position < 0 {
		return fmt.Errorf("%w: position %d", ErrMark, mark.Position)
	}
	for page, renewal := range mark.Renewed {
		if renewal.At < 0 || renewal.TTL <= 0 {
			return fmt.Errorf("%w: page %d renewal %+v", ErrMark, page, renewal)
		}
	}
	return nil
}

// Due is whether the page is renewed this round: a quarter of the shorter of
// the lifetime it was given and the one this round gives has passed since.
// The shorter, as a lifetime that grew is not yet on the keys.
func (renewal Renewal) Due(now, ttlNow int64) bool {
	return now-renewal.At >= min(renewal.TTL, ttlNow)/4
}

// Fallback is whether the page has gone half its lifetime unrenewed: its
// members are written whole this round and taken off it, a write that keeps
// each key alive as 0.6 did.
func (renewal Renewal) Fallback(now int64) bool {
	return now-renewal.At >= renewal.TTL/2
}

// Lowered is the renewal with its lifetime lowered to ttlNow when that is
// shorter: a member joining this round is written with ttlNow, and the
// invariant holds for it only against the shorter. Never raised: a longer
// lifetime is on the keys only once a renewal gave it.
func (renewal Renewal) Lowered(ttlNow int64) Renewal {
	renewal.TTL = min(renewal.TTL, ttlNow)
	return renewal
}

// Renewed is the page renewed at at - the wall clock read before its first
// segment - with ttl, once every segment of it was answered.
func Renewed(at, ttl int64) Renewal { return Renewal{At: at, TTL: ttl} }

// NewPage is a page's renewal when it is made: now, less a part of a quarter
// lifetime its Plan and number spread, so that the pages made together - a
// Plan's first, and every Plan's after a restart - fall due on different
// rounds. Earlier only, so it renews sooner and the invariant holds: every
// member it starts with was just written.
func NewPage(now int64, plan string, page int, ttl int64) Renewal {
	quarter := ttl / 4
	if quarter <= 0 {
		return Renewal{At: now, TTL: ttl}
	}
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(plan))
	var number [8]byte
	binary.BigEndian.PutUint64(number[:], uint64(page))
	_, _ = hash.Write(number[:])
	return Renewal{At: now - int64(hash.Sum64()%uint64(quarter)), TTL: ttl}
}

// Carried is the renewal of a page made from the pages sources were, as a
// count changed: the earliest of their times and the shortest of their
// lifetimes. Only ever sooner.
func Carried(sources []Renewal) Renewal {
	if len(sources) == 0 {
		return Renewal{}
	}
	carried := sources[0]
	for _, source := range sources[1:] {
		carried.At = min(carried.At, source.At)
		carried.TTL = min(carried.TTL, source.TTL)
	}
	return carried
}
