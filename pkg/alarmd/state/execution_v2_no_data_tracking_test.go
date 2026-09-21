// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package state

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// The stored form has two encoders that list their fields rather than copying
// a struct - the delta builder above this layer and the hash encoder in it -
// so a field can reach one and not the other. This drives the bytes: what the
// encoder wrote is what the decoder is asked to read back.
func TestTheStoredFormCarriesTheTrackingFactsBothWays(t *testing.T) {
	identity := execution.PlanNoDataIdentity{
		Plan:            execution.PlanIdentity{TenantID: "tenant", BusinessID: "2", StrategyID: "7"},
		StateGeneration: "generation-1",
	}
	mutation := execution.PlanNoDataMutation{
		Set: []execution.NoDataGroupDelta{{
			GroupKey: "a",
			Absent:   &execution.NoDataGroupAbsence{LastSeen: 80, FirstAbsent: 85, SuppressedAt: 90},
		}},
	}
	set, _, err := encodeNoDataDelta(mutation)
	if err != nil {
		t.Fatal(err)
	}
	if len(set) != 1 {
		t.Fatalf("encoded %d fields, want one", len(set))
	}
	group, ok := decodeNoDataGroupValue("a", set[0].Value, 90)
	if !ok {
		t.Fatalf("the encoder's own bytes did not decode: %s", set[0].Value)
	}
	if group.SuppressedAt != 90 {
		t.Fatalf("stored group decodes with suppressed-at %d, want 90; the encoder dropped it "+
			"and the group reads back as still tracked", group.SuppressedAt)
	}
	if group.LastSeen != 80 || group.FirstAbsent != 85 {
		t.Fatalf("suppression displaced the timestamps beside it: %+v", group)
	}

	// The Plan-level fact travels in the header, which is the field a write
	// compares against. A header that dropped it would let the next round read
	// the roster as never exhausted and rebuild the whole-item absence.
	header, err := json.Marshal(noDataHashHeader{
		Schema: executionNoDataSchema, Version: execution.WrittenNoDataMemorySchema, Identity: identity,
		MarkerRevision: 1, PresentAsOf: 90, TrackingExhaustedAt: 90,
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, decoded, ok := decodeNoDataHashHeader(header, identity)
	if !ok {
		t.Fatalf("the header this build writes did not decode: %+v", snapshot)
	}
	if decoded.TrackingExhaustedAt != 90 || snapshot.TrackingExhaustedAt != 90 {
		t.Fatalf("header decodes with tracking-exhausted %d and reports %d, want 90 for both",
			decoded.TrackingExhaustedAt, snapshot.TrackingExhaustedAt)
	}
}

// A record written before these fields existed decodes with both at zero,
// which reads as still tracking and not exhausted. Nothing migrates it; the
// first round under this build decides, from the horizon, what it should be.
func TestARecordWrittenBeforeTheTrackingFactsDecodesAsStillTracking(t *testing.T) {
	identity := execution.PlanNoDataIdentity{
		Plan:            execution.PlanIdentity{TenantID: "tenant", BusinessID: "2", StrategyID: "7"},
		StateGeneration: "generation-1",
	}
	// Exactly the bytes a v2 build wrote: no suppressed_at, no
	// tracking_exhausted_at, and a version this build still reads.
	group, ok := decodeNoDataGroupValue("a", []byte(`{"last_seen":80,"first_absent":85}`), 90)
	if !ok {
		t.Fatal("a record written by the previous build did not decode")
	}
	if group.SuppressedAt != 0 {
		t.Fatalf("an old group decodes as suppressed at %d", group.SuppressedAt)
	}
	// Built by leaving the new fields unset rather than by hand-writing the
	// JSON: omitempty then produces exactly the bytes a v2 build wrote, and the
	// identity is encoded the one way this package encodes it.
	header, err := json.Marshal(noDataHashHeader{
		Schema: executionNoDataSchema, Version: execution.NoDataMemorySchemaV2, Identity: identity,
		MarkerRevision: 1, PresentAsOf: 90,
	})
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(header, []byte("tracking_exhausted_at")) {
		t.Fatalf("a header with no tracking fact still wrote the key: %s", header)
	}
	snapshot, decoded, ok := decodeNoDataHashHeader(header, identity)
	if !ok {
		t.Fatalf("a v2 header did not decode: %+v", snapshot)
	}
	if decoded.TrackingExhaustedAt != 0 || snapshot.TrackingExhaustedAt != 0 {
		t.Fatal("a record written before the horizon existed reads as having exhausted its roster")
	}
	if snapshot.SchemaVersion != execution.NoDataMemorySchemaV2 {
		t.Fatalf("v2 header reports schema %d", snapshot.SchemaVersion)
	}
}
