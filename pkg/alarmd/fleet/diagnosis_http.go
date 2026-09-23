// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package fleet

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// UniverseReader reads the source's active strategy set: the same key the
// control plane lists strategies from, read now. Its error is written to
// the response as it is, so the wiring classifies it first: a reason word
// and a detail, never a dependency's address.
type UniverseReader func(ctx context.Context) ([]string, error)

// DiagnosisCacheTTL is how long one diagnosis keeps the universe and the
// view it read on its first page. Every later page reads the same pair, so
// one diagnosis reads the fleet's snapshots once however many pages it
// takes; a page asked after the TTL rereads and says so.
const DiagnosisCacheTTL = 10 * time.Minute

// DiagnosisUniverse is the population the pages cover.
type DiagnosisUniverse struct {
	// Status is ok, or unreadable with Reason: then there are no rows and
	// the page does not hold, because a diagnosis with no population has
	// covered nothing -- it is not a diagnosis of nothing wrong.
	Status      string              `json:"status"`
	Reason      string              `json:"reason,omitempty"`
	Source      string              `json:"source"`
	Count       int                 `json:"count"`
	Digest      string              `json:"digest,omitempty"`
	ReadAt      *time.Time          `json:"read_at,omitempty"`
	Publication StrategyPublication `json:"publication"`
}

// UniverseChange says a later page found a different universe than the
// cursor was written against. The CLI reruns from the first page.
type UniverseChange struct {
	FromDigest string `json:"from_digest"`
	ToDigest   string `json:"to_digest"`
	Count      int    `json:"count"`
}

// DiagnosisResponse is one page of GET /api/diagnose.
type DiagnosisResponse struct {
	Diagnosis  string            `json:"diagnosis_id"`
	AnsweredBy string            `json:"answered_by"`
	Universe   DiagnosisUniverse `json:"universe"`
	Strategies []DiagnosisRow    `json:"strategies"`
	Page       DiagnosisPage     `json:"page"`
	// Verdicts and UnknownReasons are the closed lists, on every page.
	Verdicts       []StateWord `json:"verdicts"`
	UnknownReasons []string    `json:"unknown_reasons"`
	// Progress is read, not_wired, or unavailable: whether the Plans'
	// persisted progress could be read for this page.
	Progress        string          `json:"progress"`
	UniverseChanged *UniverseChange `json:"universe_changed,omitempty"`
	// SnapshotReread says a later page read the universe and the view
	// again, because the first page's had expired or the answering
	// process changed.
	SnapshotReread bool   `json:"snapshot_reread,omitempty"`
	NextCursor     string `json:"next_cursor,omitempty"`
}

type diagnosisEntry struct {
	id        string
	universe  []string
	digest    string
	readAt    time.Time
	view      *View
	readError string
	expires   time.Time
}

// WithDiagnosis serves GET /api/diagnose[?cursor=&limit=]. The page is
// answered where the catalog is: a process without one forwards the page to
// the Leader once, the way a strategy's standing is forwarded, so every
// page of one diagnosis is decided against one catalog and one cache.
func WithDiagnosis(next http.Handler, service *Service, lookup StrategyLookupFunc, forward LeaderForward,
	universe UniverseReader, progress ProgressReader, replica string, now func() time.Time, stallAfter time.Duration) http.Handler {
	if now == nil {
		now = time.Now
	}
	var mu sync.Mutex
	var cached *diagnosisEntry
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/api/diagnose" {
			next.ServeHTTP(response, request)
			return
		}
		if request.Method != http.MethodGet {
			response.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		query := request.URL.Query()
		var cursor DiagnosisCursor
		if raw := query.Get("cursor"); raw != "" {
			parsed, ok := ParseDiagnosisCursor(raw)
			if !ok {
				writeJSON(response, http.StatusBadRequest, map[string]string{"error": "INVALID_CURSOR"})
				return
			}
			cursor = parsed
		}
		limit := DiagnosisPageRows
		if raw := query.Get("limit"); raw != "" {
			n, err := strconv.Atoi(raw)
			if err != nil || n < 1 || n > DiagnosisPageRows {
				writeJSON(response, http.StatusBadRequest, map[string]any{"error": "INVALID_LIMIT", "max": DiagnosisPageRows})
				return
			}
			limit = n
		}
		if lookup == nil || universe == nil {
			writeJSON(response, http.StatusServiceUnavailable, map[string]string{"error": "DIAGNOSIS_NOT_WIRED"})
			return
		}
		if !lookup("0").Available && forward != nil && request.Header.Get(forwardedHeader) == "" {
			if forwarded, _ := forward(response, request); forwarded {
				return
			}
			// The Leader could not be reached: answer here, where every row
			// will say LOOKUP_UNAVAILABLE and the universe is still counted.
		}
		at := now()
		mu.Lock()
		entry := cached
		reread := false
		if entry == nil || cursor.Diagnosis == "" || entry.id != cursor.Diagnosis || at.After(entry.expires) {
			reread = cursor.Diagnosis != ""
			entry = readDiagnosisEntry(request.Context(), service, universe, at, stallAfter)
			if cursor.Diagnosis != "" {
				// A later page's reread keeps the diagnosis id, so the CLI's
				// cursor stays valid; the digest comparison below says whether
				// the population moved under it.
				entry.id = cursor.Diagnosis
			}
			cached = entry
		}
		mu.Unlock()
		body := DiagnosisResponse{Diagnosis: entry.id, AnsweredBy: replica, Strategies: []DiagnosisRow{},
			Verdicts: DiagnosisVerdicts(), UnknownReasons: append([]string(nil), DiagnosisUnknownReasons...),
			SnapshotReread: reread, Progress: "not_wired"}
		body.Universe = DiagnosisUniverse{Status: "ok", Source: "strategy_ids", Count: len(entry.universe), Digest: entry.digest}
		if !entry.readAt.IsZero() {
			readAt := entry.readAt
			body.Universe.ReadAt = &readAt
		}
		body.Universe.Publication = lookup("0").Publication
		if entry.readError != "" {
			body.Universe.Status, body.Universe.Reason = "unreadable", entry.readError
			body.Page = DiagnosisPage{ByVerdict: map[StateWord]int{}, Holds: false}
			writeJSON(response, http.StatusOK, body)
			return
		}
		if cursor.Digest != "" && cursor.Digest != entry.digest {
			body.UniverseChanged = &UniverseChange{FromDigest: cursor.Digest, ToDigest: entry.digest, Count: len(entry.universe)}
		}
		ctx := newDiagnosisContext(entry.view, replica, at)
		page := buildDiagnosisPage(entry.universe, cursor.After, limit, func(id string) DiagnosisRow {
			return diagnoseStrategy(id, lookup(id), ctx)
		})
		if progress != nil {
			facts, err := progress(pageQueryGroups(page.Rows))
			if err != nil {
				body.Progress = "unavailable"
				applyProgress(page.Rows, nil, "PROGRESS_UNREADABLE")
			} else {
				body.Progress = "read"
				applyProgress(page.Rows, facts, "")
			}
		}
		body.Strategies, body.Page = page.Rows, page
		if page.Last != "" {
			body.NextCursor = DiagnosisCursor{Diagnosis: entry.id, Digest: entry.digest, After: page.Last}.Encode()
		}
		writeJSON(response, http.StatusOK, body)
	})
}

func readDiagnosisEntry(ctx context.Context, service *Service, universe UniverseReader, at time.Time, stallAfter time.Duration) *diagnosisEntry {
	entry := &diagnosisEntry{id: newDiagnosisID(), readAt: at, expires: at.Add(DiagnosisCacheTTL)}
	ids, err := universe(ctx)
	if err != nil {
		entry.readError = err.Error()
		return entry
	}
	entry.universe, entry.digest = NormalizeUniverse(ids)
	if service != nil {
		view := service.View(ctx)
		Decide(&view, at, stallAfter)
		entry.view = &view
	}
	return entry
}

func newDiagnosisID() string {
	var raw [8]byte
	_, _ = rand.Read(raw[:])
	return hex.EncodeToString(raw[:])
}
