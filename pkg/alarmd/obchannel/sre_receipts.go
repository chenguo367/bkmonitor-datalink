// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License.

package obchannel

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
)

const (
	// OpSREReceipts reads the receipts of the SRE channel's actions.
	OpSREReceipts = "sre.receipts"
	// MaxSREReceipts is how many receipts are kept, newest first, and
	// SREReceiptRetention how long the list lives past its latest write.
	MaxSREReceipts      = 1024
	SREReceiptRetention = 7 * 24 * time.Hour
	// sreReceiptWriteTimeout bounds one write: a store that does not answer
	// does not hold the action's answer up for longer.
	sreReceiptWriteTimeout = 500 * time.Millisecond
	// maxSREReceiptsPage is the most receipts one read returns.
	maxSREReceiptsPage = 100
)

// SREExec is what a pod.exec receipt says about the run, without its
// output: how it was supervised, how it ended, and how its supervisor
// failed if it did.
type SREExec struct {
	RemoteState            string `json:"remote_state"`
	Supervision            string `json:"supervision,omitempty"`
	UnsupervisedReason     string `json:"unsupervised_reason,omitempty"`
	ExitCode               *int   `json:"exit_code,omitempty"`
	DeadlineRaced          bool   `json:"deadline_raced,omitempty"`
	StreamCut              bool   `json:"stream_cut,omitempty"`
	Truncated              bool   `json:"truncated,omitempty"`
	ScriptDigest           string `json:"script_digest,omitempty"`
	DeclaredTimeoutMS      int64  `json:"declared_timeout_ms,omitempty"`
	EnforcedTimeoutSeconds int64  `json:"enforced_timeout_seconds,omitempty"`
	BootstrapExitCode      *int   `json:"bootstrap_exit_code,omitempty"`
	BootstrapClass         string `json:"bootstrap_class,omitempty"`
	StartFailure           string `json:"start_failure,omitempty"`
}

// SREReceipts keeps the SRE channel's receipts in the shared store, one list
// for the deployment: every replica writes its actions and any replica
// answers sre.receipts. A write that fails is counted and does not hold the
// action up; the sre_action line is then the only record.
type SREReceipts struct {
	client  redis.Cmdable
	key     string
	now     func() time.Time
	written func(ok bool)
}

// NewSREReceipts keeps receipts under key; written hears each write's
// outcome and may be nil.
func NewSREReceipts(client redis.Cmdable, key string, now func() time.Time, written func(ok bool)) *SREReceipts {
	if now == nil {
		now = time.Now
	}
	return &SREReceipts{client: client, key: key, now: now, written: written}
}

// Record writes one action's receipt, the newest first, within its own
// bound.
func (s *SREReceipts) Record(action SREAction) {
	if s == nil {
		return
	}
	if action.At.IsZero() {
		action.At = s.now().UTC()
	}
	body, err := json.Marshal(action)
	if err == nil {
		ctx, cancel := context.WithTimeout(context.Background(), sreReceiptWriteTimeout)
		_, err = s.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.LPush(ctx, s.key, body)
			pipe.LTrim(ctx, s.key, 0, MaxSREReceipts-1)
			pipe.Expire(ctx, s.key, SREReceiptRetention)
			return nil
		})
		cancel()
	}
	if s.written != nil {
		s.written(err == nil)
	}
}

// SREReceiptFilter selects receipts: by time (since inclusive, until
// exclusive; zero is open), by namespace, by object name (the name, or a
// Pod's name before its /container), by request; Offset and Limit page the
// matches, newest first.
type SREReceiptFilter struct {
	Since, Until    time.Time
	Namespace, Name string
	RequestID       string
	Offset, Limit   int
}

// SREReceiptPage is one page of matching receipts, newest first: Matched is
// all that match, Stored all kept, NextOffset the next page's offset while
// there is one.
type SREReceiptPage struct {
	Receipts   []SREAction `json:"receipts"`
	Matched    int         `json:"matched"`
	Stored     int         `json:"stored"`
	NextOffset int         `json:"next_offset,omitempty"`
}

// List reads the kept receipts and returns the page that matches.
func (s *SREReceipts) List(ctx context.Context, filter SREReceiptFilter) (SREReceiptPage, error) {
	page := SREReceiptPage{Receipts: []SREAction{}}
	raw, err := s.client.LRange(ctx, s.key, 0, MaxSREReceipts-1).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return page, err
	}
	page.Stored = len(raw)
	limit := filter.Limit
	if limit <= 0 || limit > maxSREReceiptsPage {
		limit = maxSREReceiptsPage
	}
	matches := make([]SREAction, 0, len(raw))
	for _, entry := range raw {
		var action SREAction
		if json.Unmarshal([]byte(entry), &action) != nil || !filter.matches(action) {
			continue
		}
		matches = append(matches, action)
	}
	// Written newest first; two replicas' writes can cross, so order by
	// the receipts' own times.
	sort.SliceStable(matches, func(i, j int) bool { return matches[i].At.After(matches[j].At) })
	page.Matched = len(matches)
	if filter.Offset < len(matches) {
		end := min(len(matches), filter.Offset+limit)
		page.Receipts = matches[filter.Offset:end]
		if end < len(matches) {
			page.NextOffset = end
		}
	}
	return page, nil
}

func (f SREReceiptFilter) matches(action SREAction) bool {
	if !f.Since.IsZero() && action.At.Before(f.Since) {
		return false
	}
	if !f.Until.IsZero() && !action.At.Before(f.Until) {
		return false
	}
	if f.Namespace != "" && action.Namespace != f.Namespace {
		return false
	}
	if f.Name != "" && action.Name != f.Name && !strings.HasPrefix(action.Name, f.Name+"/") {
		return false
	}
	return f.RequestID == "" || action.RequestID == f.RequestID
}
