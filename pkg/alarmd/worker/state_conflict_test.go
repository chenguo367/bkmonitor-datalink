// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package worker_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/worker"
)

func TestStateConflictReasonSurvivesWrappers(t *testing.T) {
	for _, stage := range []string{"state mutation preflight", "state apply did not complete"} {
		for _, test := range []struct{ status, want string }{
			{string(execution.StateVersionConflict), contract.ReasonStateVersionConflict},
			{string(execution.StateStaleVersion), contract.ReasonStateStaleVersion},
		} {
			t.Run(stage+"/"+test.status, func(t *testing.T) {
				cause := &worker.StateConflictError{Stage: stage, Status: test.status}
				wrapped := fmt.Errorf("execute Slot: %w", fmt.Errorf("alarmd worker: %w", cause))
				if got, ok := worker.StateConflictReason(wrapped); !ok || string(got) != test.want {
					t.Fatalf("reason = %q/%v, want %s/true", got, ok, test.want)
				}
				if cause.Error() != stage+": "+test.status || !errors.Is(wrapped, cause) {
					t.Fatalf("error text or error chain changed: %v", wrapped)
				}
			})
		}
	}
	for _, err := range []error{nil, errors.New("STATE_VERSION_CONFLICT"),
		&worker.StateConflictError{Status: string(execution.StateApplyCASConflict)},
		&worker.StateConflictError{Status: string(execution.StateApplyRetryable)}} {
		if got, ok := worker.StateConflictReason(err); ok || got != "" {
			t.Fatalf("unclassified error %v was named %q", err, got)
		}
	}
}

// A conflict on a repeated key says so in its text: the same status from a
// race with another writer and from a producer that made two statements for
// one series must not read alike.
func TestStateConflictErrorNamesARepeatedKey(t *testing.T) {
	plain := &worker.StateConflictError{Stage: "state apply did not complete", Status: "STATE_VERSION_CONFLICT"}
	repeated := &worker.StateConflictError{Stage: "state apply did not complete", Status: "STATE_VERSION_CONFLICT", RepeatedKey: true}
	if plain.Error() == repeated.Error() {
		t.Fatalf("a repeated-key conflict reads the same as a plain one: %q", plain.Error())
	}
	if want := "state apply did not complete: STATE_VERSION_CONFLICT (repeated key in the same request)"; repeated.Error() != want {
		t.Fatalf("repeated-key conflict = %q, want %q", repeated.Error(), want)
	}
	if reason, ok := worker.StateConflictReason(repeated); !ok || string(reason) != "STATE_VERSION_CONFLICT" {
		t.Fatalf("reason of a repeated-key conflict = %q/%v, want STATE_VERSION_CONFLICT: the code stays, the text carries the cause", reason, ok)
	}
}
