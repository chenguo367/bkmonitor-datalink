// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License.

package obchannel

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/go-redis/redis/v8"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/redisatomic"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/sre/pod"
)

const (
	// OpSREReceipts reads the receipts of the SRE channel's actions.
	OpSREReceipts = "sre.receipts"
	// SREReceiptRetention is how long each receipt list lives past its own
	// latest write.
	SREReceiptRetention = 7 * 24 * time.Hour
	// Receipts are kept in two lists by effect, so reads cannot push a
	// change out: a change (an image, an apply or delete, a Pod deleted, a
	// workload restarted or scaled, a dry run) and an access (an execution,
	// a Secret or ConfigMap read). One sre.receipts read takes both lists
	// whole inside its hold on the observation memory line
	// (MaxResponseBytes); decoding about doubles the raw bytes, so the
	// lists' raw bytes stay within half of it, a quarter each. Each receipt
	// is cut to its effect's size when it is written, and each list keeps
	// as many as fit its quarter.
	sreReceiptListBytes   = MaxResponseBytes / 4
	SREChangeReceiptBytes = 4 << 10
	SREAccessReceiptBytes = 1 << 10
	MaxSREChangeReceipts  = sreReceiptListBytes / SREChangeReceiptBytes
	MaxSREAccessReceipts  = sreReceiptListBytes / SREAccessReceiptBytes
	// MaxSREReceipts is how many receipts are kept in all.
	MaxSREReceipts = MaxSREChangeReceipts + MaxSREAccessReceipts
	// SREEffectChange and SREEffectAccess name the two lists.
	SREEffectChange = "change"
	SREEffectAccess = "access"
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

// SREReceipts keeps the SRE channel's receipts in the shared store, two
// lists for the deployment (changes under key+":change", accesses under
// key): every replica writes its actions and any replica answers
// sre.receipts. A write that fails is counted and does not hold the
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

// sreReceiptWrite adds a receipt, cuts the list to its bound and renews its
// life, as one script rather than a MULTI/EXEC (see redisatomic for why).
// One key, so it is in one slot on any deployment. ARGV[3] is seconds, as
// EXPIRE takes them.
var sreReceiptWrite = redisatomic.New(`
redis.call('LPUSH', KEYS[1], ARGV[1])
redis.call('LTRIM', KEYS[1], 0, tonumber(ARGV[2]))
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[3]))
return 1
`)

// sreEffect is the list an action's receipt goes to.
func sreEffect(op string) string {
	switch op {
	case OpSetImage, pod.OpApply, pod.OpDelete, pod.OpDeletePod, pod.OpRestartWorkload, pod.OpScaleWorkload, pod.OpDryRun:
		return SREEffectChange
	}
	return SREEffectAccess
}

func (s *SREReceipts) keyOf(effect string) string {
	if effect == SREEffectChange {
		return s.key + ":change"
	}
	return s.key
}

// Record writes one action's receipt, the newest first, within its own
// bound, and says whether it landed.
func (s *SREReceipts) Record(action SREAction) {
	if s == nil {
		return
	}
	err := s.record(action)
	if s.written != nil {
		s.written(err == nil)
	}
}

func (s *SREReceipts) record(action SREAction) error {
	if action.At.IsZero() {
		action.At = s.now().UTC()
	}
	effect := sreEffect(action.Op)
	size, kept := SREAccessReceiptBytes, MaxSREAccessReceipts
	if effect == SREEffectChange {
		size, kept = SREChangeReceiptBytes, MaxSREChangeReceipts
	}
	body, err := fitReceipt(action, size)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), sreReceiptWriteTimeout)
	defer cancel()
	return sreReceiptWrite.Run(ctx, s.client, []string{s.keyOf(effect)}, body, kept-1, int64(SREReceiptRetention/time.Second))
}

// fitReceipt encodes a receipt within size bytes. Past it, the object list
// gives up names from its end for a "+N more", and then the free texts are
// halved, longest first, until it fits.
func fitReceipt(action SREAction, size int) ([]byte, error) {
	body, err := json.Marshal(action)
	if err != nil || len(body) <= size {
		return body, err
	}
	names, more := strings.Split(action.Name, ","), 0
	if last := names[len(names)-1]; len(names) > 1 && strings.HasPrefix(last, "+") && strings.HasSuffix(last, " more") {
		if _, scanned := fmt.Sscanf(last, "+%d more", &more); scanned == nil {
			names = names[:len(names)-1]
		}
	}
	for len(names) > 1 {
		names, more = names[:len(names)-1], more+1
		action.Name = strings.Join(names, ",") + fmt.Sprintf(",+%d more", more)
		if body, err = json.Marshal(action); err != nil || len(body) <= size {
			return body, err
		}
	}
	texts := []*string{&action.Name, &action.Detail, &action.Namespace, &action.UID, &action.Operator}
	if action.Exec != nil {
		exec := *action.Exec
		action.Exec = &exec
		texts = append(texts, &exec.StartFailure, &exec.UnsupervisedReason, &exec.Supervision, &exec.BootstrapClass)
	}
	for len(body) > size {
		longest := texts[0]
		for _, text := range texts[1:] {
			if len(*text) > len(*longest) {
				longest = text
			}
		}
		if *longest == "" {
			break
		}
		*longest = halveUTF8(*longest)
		if body, err = json.Marshal(action); err != nil {
			return nil, err
		}
	}
	return body, nil
}

// halveUTF8 is the first half of text, cut at a character boundary, marked
// as cut; a text of one character or less becomes empty.
func halveUTF8(text string) string {
	cut := len(text) / 2
	for cut > 0 && !utf8.RuneStart(text[cut]) {
		cut--
	}
	if cut <= len("…") {
		return ""
	}
	return strings.TrimSuffix(text[:cut], "…") + "…"
}

// SREReceiptFilter selects receipts: by time (since inclusive, until
// exclusive; zero is open), by namespace, by object name (the name, or a
// Pod's name before its /container), by request; Offset and Limit page the
// matches, newest first.
type SREReceiptFilter struct {
	Since, Until    time.Time
	Namespace, Name string
	RequestID       string
	// Effect selects one list, change or access; empty reads both.
	Effect        string
	Offset, Limit int
}

// SREReceiptPage is one page of matching receipts, newest first: Matched is
// all that match, Stored all kept, NextOffset the next page's offset while
// there is one.
type SREReceiptPage struct {
	Receipts []SREAction `json:"receipts"`
	Matched  int         `json:"matched"`
	Stored   int         `json:"stored"`
	// StoredByEffect is what each list keeps.
	StoredByEffect map[string]int `json:"stored_by_effect"`
	NextOffset     int            `json:"next_offset,omitempty"`
}

// List reads the kept receipts and returns the page that matches.
func (s *SREReceipts) List(ctx context.Context, filter SREReceiptFilter) (SREReceiptPage, error) {
	page := SREReceiptPage{Receipts: []SREAction{}, StoredByEffect: map[string]int{}}
	limit := filter.Limit
	if limit <= 0 || limit > maxSREReceiptsPage {
		limit = maxSREReceiptsPage
	}
	var matches []SREAction
	for _, list := range []struct {
		effect string
		kept   int
	}{{SREEffectChange, MaxSREChangeReceipts}, {SREEffectAccess, MaxSREAccessReceipts}} {
		raw, err := s.client.LRange(ctx, s.keyOf(list.effect), 0, int64(list.kept-1)).Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			return page, err
		}
		page.Stored += len(raw)
		page.StoredByEffect[list.effect] = len(raw)
		if filter.Effect != "" && filter.Effect != list.effect {
			continue
		}
		for _, entry := range raw {
			var action SREAction
			if json.Unmarshal([]byte(entry), &action) != nil || !filter.matches(action) {
				continue
			}
			action.Effect = list.effect
			matches = append(matches, action)
		}
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
