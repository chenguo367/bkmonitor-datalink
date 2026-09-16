// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package controlplane

import (
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// Every failure the cutover can return has a name.
//
// This is the whole point of the family. A cutover that says only that it
// failed leaves nothing to act on, and one failed about twice a minute for
// eleven hours while the fleet quietly stopped picking up published content.
func TestEveryCutoverFailureIsNamed(t *testing.T) {
	for name, test := range map[string]struct {
		err  error
		want string
	}{
		"the activation and the Segments disagree": {
			err:  fmt.Errorf("%w: a kept Query Group has a Plan without a current activation record", ErrActivationRecordMissing),
			want: CutoverReasonActivationRecordMissing,
		},
		"an open Segment is not in the state the cutover requires": {
			err: ErrScheduleConflict, want: CutoverReasonSegmentConflict,
		},
		"the compare-and-set lost": {
			err: ErrActivationConflict, want: CutoverReasonConflict,
		},
		"stored content does not hash to its name": {
			err: ErrCatalogObjectCorrupt, want: CutoverReasonDigestMismatch,
		},
		"a persisted snapshot will not decode": {
			err:  &PersistedSnapshotCorruptError{Err: errors.New("bad")},
			want: CutoverReasonDigestMismatch,
		},
		"content the cutover needs is not stored": {
			err: ErrCatalogObjectUnavailable, want: CutoverReasonUnavailable,
		},
		"there is no snapshot to advance from": {
			err: ErrSnapshotUnavailable, want: CutoverReasonUnavailable,
		},
		"the request is not a cutover": {
			err:  fmt.Errorf("%w: publication schedule activation is required", ErrCutoverRequest),
			want: CutoverReasonInvalidRequest,
		},
		"the store failed underneath": {
			err:  &ActivationDependencyIOError{Err: errors.New("dial tcp: connection refused")},
			want: CutoverReasonIO,
		},
		"a wrapped store failure": {
			err:  fmt.Errorf("persist schedule cutover: %w", &ActivationDependencyIOError{Err: errors.New("broken pipe")}),
			want: CutoverReasonIO,
		},
	} {
		t.Run(name, func(t *testing.T) {
			if got := cutoverFailureReason(test.err); got != test.want {
				t.Fatalf("reason = %q, want %q", got, test.want)
			}
		})
	}

	if got := cutoverFailureReason(nil); got != "" {
		t.Fatalf("a cutover that did not fail has reason %q, want none", got)
	}
	if got := cutoverFailureReason(errors.New("something nobody named")); got != CutoverReasonOther {
		t.Fatalf("an unnamed failure = %q, want %q; other is where a new failure path with no name "+
			"must land, so that it is visible rather than folded into one that has a meaning",
			got, CutoverReasonOther)
	}
}

// The reasons the metric bounds itself by are the ones this file defines.
func TestEveryCutoverReasonIsListed(t *testing.T) {
	listed := map[string]bool{}
	for _, reason := range CutoverReasons {
		if listed[reason] {
			t.Fatalf("reason %q is listed twice", reason)
		}
		listed[reason] = true
	}
	for _, reason := range []string{
		CutoverReasonActivationRecordMissing, CutoverReasonSegmentConflict, CutoverReasonDigestMismatch,
		CutoverReasonConflict, CutoverReasonUnavailable, CutoverReasonInvalidRequest,
		CutoverReasonIO, CutoverReasonOther,
	} {
		if !listed[reason] {
			t.Fatalf("reason %q is declared and not listed, so the metric never creates its label and "+
				"a failure landing there reports a series nobody can find", reason)
		}
	}
}

// No failure path in the cutover returns a bare error.
//
// A bare errors.New cannot be told apart from any other by the reporter, so it
// lands in other and says nothing -- and other is meant to stay at zero. This
// scans the function rather than trusting a list, because the failure this
// guards is a path added later by someone who did not read this file.
func TestTheCutoverReturnsNoUnnamedError(t *testing.T) {
	fileSet := token.NewFileSet()
	parsed, err := parser.ParseFile(fileSet, "redis_runtime.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var target *ast.FuncDecl
	for _, declaration := range parsed.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if ok && function.Name.Name == "CompareAndSetPublicationScheduleActivation" {
			target = function
		}
	}
	if target == nil {
		t.Fatal("CompareAndSetPublicationScheduleActivation is gone; this guard now scans nothing")
	}

	var bare []string
	ast.Inspect(target, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		pkg, ok := selector.X.(*ast.Ident)
		if !ok || pkg.Name != "errors" || selector.Sel.Name != "New" {
			return true
		}
		position := fileSet.Position(call.Pos())
		bare = append(bare, fmt.Sprintf("redis_runtime.go:%d", position.Line))
		return true
	})
	if len(bare) > 0 {
		t.Fatalf("the cutover returns %d unnamed error(s) at %s. A bare errors.New reaches the reporter "+
			"as other, which is the label that means 'a failure path nobody named' -- wrap it in one of "+
			"the sentinels this package defines, or add a sentinel for it",
			len(bare), strings.Join(bare, ", "))
	}
}

// A failed cutover reports its reason, the Query Group it stopped on, and the
// error itself.
//
// Computing the reason is not reporting it. The observation is the only thing
// a reader sees, and until this release it carried neither the reason nor the
// error -- a core action failing every round, visible as one label on a
// duration histogram.
func TestAFailedCutoverReportsItsReasonAndWhereItStopped(t *testing.T) {
	var observed []observability.Observation
	repository := &RedisCatalogRepository{}
	repository.ConfigureObserver(observability.ObserverFunc(
		func(_ context.Context, observation observability.Observation) {
			observed = append(observed, observation)
		}))
	facts := newCutoverFacts()
	facts.failedAt("qg-that-stopped-it")

	repository.observeCutover(context.Background(), facts,
		fmt.Errorf("%w: a kept Query Group has a Plan without a current activation record", ErrActivationRecordMissing))

	if len(observed) != 1 || observed[0].ScheduleCutover == nil {
		t.Fatalf("observed = %+v, want one cutover observation", observed)
	}
	reported := observed[0].ScheduleCutover
	if reported.Result != "failure" {
		t.Fatalf("result = %q, want failure", reported.Result)
	}
	if reported.Reason != CutoverReasonActivationRecordMissing {
		t.Fatalf("reason = %q, want %q", reported.Reason, CutoverReasonActivationRecordMissing)
	}
	if reported.QueryGroup != "qg-that-stopped-it" {
		t.Fatalf("query group = %q, want the one the cutover stopped on", reported.QueryGroup)
	}
	if observed[0].Err == nil {
		t.Fatal("the observation carries no error; the reason is bounded on purpose and the error is " +
			"what says which of that reason's several causes it was")
	}
}

// A cutover that worked reports no reason, because there is nothing to explain.
func TestASuccessfulCutoverReportsNoReason(t *testing.T) {
	var observed []observability.Observation
	repository := &RedisCatalogRepository{}
	repository.ConfigureObserver(observability.ObserverFunc(
		func(_ context.Context, observation observability.Observation) {
			observed = append(observed, observation)
		}))
	facts := newCutoverFacts()
	facts.started = time.Now()

	repository.observeCutover(context.Background(), facts, nil)

	if len(observed) != 1 || observed[0].ScheduleCutover == nil {
		t.Fatalf("observed = %+v, want one cutover observation", observed)
	}
	if reported := observed[0].ScheduleCutover; reported.Result != "success" || reported.Reason != "" {
		t.Fatalf("result = %q reason = %q, want success with no reason", reported.Result, reported.Reason)
	}
}
