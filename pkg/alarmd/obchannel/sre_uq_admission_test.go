// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License.

package obchannel

import (
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/sre/uq"
)

// A UQ read waits for the egress as long as any read waits for its pool:
// the two waits are one admission timeout.
func TestTheUQEgressWaitIsTheChannelAdmissionTimeout(t *testing.T) {
	if uq.AdmissionWait != RequestTimeout {
		t.Fatalf("UQ egress wait %s, channel admission timeout %s; want them the same", uq.AdmissionWait, RequestTimeout)
	}
}
