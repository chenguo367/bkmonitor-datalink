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
}

func (err *StateConflictError) Error() string {
	if err.RepeatedKey {
		return fmt.Sprintf("%s: %s (repeated key in the same request)", err.Stage, err.Status)
	}
	return fmt.Sprintf("%s: %s", err.Stage, err.Status)
}

// StateConflictReason recognizes only the two version refusals. Other state
// errors retain their existing classification rather than being guessed from text.
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
	default:
		return "", false
	}
}
