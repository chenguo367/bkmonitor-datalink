// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package worker

import (
	"errors"
	"fmt"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// StateConflictError preserves a state version refusal through the Slot's
// error wrappers. It names the observation without changing retry behavior.
type StateConflictError struct {
	Stage  string
	Status string
	// RepeatedKey: the conflicting item was the later copy of a key the same
	// request had already written -- the producer made two different
	// statements for one series. The line has to say so, because from the
	// status alone this is indistinguishable from a race with another writer.
	RepeatedKey bool
	// Kind is which comparison refused a STATE_VERSION_CONFLICT, with the two
	// revisions it compared (StoredRevision is zero for a key not found) and
	// how the stored ApplyVersion ordered against the mutation's. Empty on a
	// STALE_VERSION, which has one way of being reached. When a chunk refuses
	// more than one item these are the first item's; the counts by kind
	// travel on the observation.
	Kind              execution.StateVersionConflictKind
	ExpectedRevision  uint64
	StoredRevision    uint64
	VersionComparison execution.ApplyVersionComparison
}

func (err *StateConflictError) Error() string {
	text := fmt.Sprintf("%s: %s", err.Stage, err.Status)
	if err.Kind != "" {
		text += fmt.Sprintf(" (%s: expected revision %d, stored revision %d", err.Kind, err.ExpectedRevision, err.StoredRevision)
		if err.VersionComparison != "" {
			text += fmt.Sprintf(", stored version %s", err.VersionComparison)
		}
		text += ")"
	}
	if err.RepeatedKey {
		text += " (repeated key in the same request)"
	}
	return text
}

// StateConflictReason names the statuses this build has a word for, from the
// status value rather than from the error's text.
//
// Reading the text is the thing this must never do: an error that merely says
// "STATE_VERSION_CONFLICT" in a sentence is not a state conflict, and naming it
// one would let any wrapped message anywhere claim the word. That rule is why
// the unnamed cases below are unnamed, and it is unchanged.
//
// What changed is which statuses have words. The preflight produces two; the
// apply path reuses this error with a wider set, and RETRYABLE_IO went unnamed
// only because it was not in the preflight's two - not because a transient
// write failure is unclassifiable. It has had a word in the vocabulary all
// along. CAS_CONFLICT is still unnamed: it needs a word of its own and there is
// no reading yet to say what that word should distinguish, and inventing one
// here would be guessing at a distinction rather than recording it.

func StateConflictReason(err error) (execution.ReasonCode, bool) {
	var conflict *StateConflictError
	if !errors.As(err, &conflict) || conflict == nil {
		return "", false
	}
	switch conflict.Status {
	case string(execution.StateVersionConflict):
		return execution.ReasonCode(contract.ReasonStateVersionConflict), true
	case string(execution.StateStaleVersion):
		return execution.ReasonCode(contract.ReasonStateStaleVersion), true
	// The apply path reaches here with statuses the preflight never produces.
	// RETRYABLE_IO had a name in the vocabulary already and still arrived as
	// internal_unknown, because nothing mapped it: a transient write failure
	// and a site that could not classify its own failure were the same word.
	case string(execution.StateApplyRetryable):
		return execution.ReasonCode(contract.ReasonStateWriteRetryable), true
	default:
		return "", false
	}
}
