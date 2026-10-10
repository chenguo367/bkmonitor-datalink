// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License.

package config

import (
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/sre/pod"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/sre/uq"
)

// SREConfig registers deployment-owned target scopes and native query egresses.
// Omission preserves the legacy runtime and grants no Pod execution.
type SREConfig struct {
	Pods []pod.Scope `yaml:"pods,omitempty"`
	UQ   SREUQConfig `yaml:"uq,omitempty"`
}

type SREUQConfig struct {
	Egresses []uq.Egress `yaml:"egresses,omitempty"`
}

func (c SREConfig) Validate() error {
	if err := pod.ValidateScopes(c.Pods); err != nil {
		return err
	}
	return uq.Validate(c.UQ.Egresses)
}
