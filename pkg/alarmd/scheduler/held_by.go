// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.

package scheduler

import (
	"context"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

type heldByContextKey struct{}

// withHeldBy carries what the previous round did into this one.
//
// A context value rather than a parameter because the two places that report
// it -- the Slot source, which decides to give a Slot up, and the executor,
// which reports the completion -- are reached through interfaces the Runner
// does not own. Threading it through both signatures would put a diagnostic
// field in two contracts that have nothing else to do with it, and every
// implementation of those interfaces would have to carry it whether or not it
// reports anything.
func withHeldBy(ctx context.Context, facts observability.HeldByFacts) context.Context {
	return context.WithValue(ctx, heldByContextKey{}, facts)
}

// HeldByFromContext is what the previous round did with this Query Group, or
// nil when nothing held it: either that round ran the Slot, or this is the
// first round.
//
// Exported because the runtime's executor reports the completion line and
// lives in another package. Returning nil rather than a zero value keeps the
// caller from having to know which word means "nothing"; the normalizer fills
// that in at the observer.
func HeldByFromContext(ctx context.Context) *observability.HeldByFacts {
	facts, ok := ctx.Value(heldByContextKey{}).(observability.HeldByFacts)
	if !ok || facts.Decision == "" || facts.Decision == observability.HeldByNothing {
		return nil
	}
	return &facts
}

// rememberHeldBy stores this round's decision for the next one.
//
// A round that executed held nothing back, so it is remembered as the word for
// that rather than as itself: the question the next Slot answers is "what kept
// me from running", and "the previous round ran" is not an answer to it. Every
// other word is kept exactly as run_one_return_total counts it, so the two
// readings share one vocabulary.
func (runner *Runner) rememberHeldBy(decision string) {
	if runner == nil {
		return
	}
	word := decision
	switch decision {
	case "execute", "execute_returned", "execution_returned", "query_readiness_deferred":
		word = observability.HeldByNothing
	}
	if !observability.ValidHeldByDecision(word) {
		// A decision that is not in the published vocabulary is reported as
		// the catch-all the run outcome uses for the same case, never dropped:
		// a word nobody can read is still evidence that a path exists.
		word = "other_error"
	}
	facts := observability.HeldByFacts{Decision: word, AtUnixMilli: runner.now().UnixMilli()}
	if word == "query_cooldown" {
		facts.QueryCooldownFailures = runner.queryCooldown.failures
		if until := runner.queryCooldown.until; !until.IsZero() {
			facts.QueryCooldownUntilMilli = until.UnixMilli()
		}
	}
	runner.heldBy = facts
}
