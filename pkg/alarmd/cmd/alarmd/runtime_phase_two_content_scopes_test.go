// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/controlplane"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/ownership"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/scheduler"
)

// noContentScopes is the content reader of a fixture whose rounds are not
// about the content contract: it knows no content, so a declaring round
// touches no record's scope.
func noContentScopes(context.Context) (map[execution.QueryGroupIdentity]string, error) {
	return map[execution.QueryGroupIdentity]string{}, nil
}

// The gate of the content contract, decided per round on the ready set: a
// fleet that declares throughout gets the content it is published with; a
// fleet with one member that does not gets every scope withdrawn; and a
// leader that cannot read the current content this round leaves scopes as
// they are -- unknown is not a withdrawal, and a Redis blip must not roll
// the contract back fleet-wide -- and says so.
func TestTheContentContractIsGatedOnTheWholeReadySet(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	declaring := func(id string) ownership.WorkerRegistration {
		return ownership.WorkerRegistration{
			WorkerID: id, AssignmentReadiness: ownership.WorkerReady, DependencyStatus: ownership.DependencyHealthy,
			DeploymentProfile: "standard", CapabilitiesDigest: "d", ExpiresAt: now.Add(time.Minute),
			Capabilities: []string{ownership.CapabilityContentScope},
		}
	}
	silent := declaring("old")
	silent.Capabilities = nil
	digests := map[execution.QueryGroupIdentity]string{"query-group-1": "qg-object-1"}
	cases := []struct {
		name       string
		workers    []ownership.WorkerRegistration
		readErr    error
		wantPolicy scheduler.ContentScopePolicy
		wantDigest bool
		wantReport bool
	}{
		{name: "whole fleet declares", workers: []ownership.WorkerRegistration{declaring("a"), declaring("b")}, wantPolicy: scheduler.ContentScopesDeclared, wantDigest: true},
		{name: "one member does not", workers: []ownership.WorkerRegistration{declaring("a"), silent}, wantPolicy: scheduler.ContentScopesWithdrawn},
		{name: "no ready worker", workers: nil, wantPolicy: scheduler.ContentScopesWithdrawn},
		{name: "content unreadable", workers: []ownership.WorkerRegistration{declaring("a")}, readErr: errors.New("manifest gone"), wantPolicy: scheduler.ContentScopesUntouched, wantReport: true},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			var reports []observability.Observation
			runtime := &productionPhaseTwoOwnership{dependencies: productionPhaseTwoOwnershipDependencies{
				Observer: observability.ObserverFunc(func(_ context.Context, observation observability.Observation) {
					reports = append(reports, observation)
				}),
				ContentScopes: func(context.Context) (map[execution.QueryGroupIdentity]string, error) {
					if test.readErr != nil {
						return nil, test.readErr
					}
					return digests, nil
				},
			}}
			scopes, err := runtime.contentScopesFor(context.Background(), test.workers)
			if err != nil {
				t.Fatalf("contentScopesFor() error = %v", err)
			}
			if scopes.Policy != test.wantPolicy {
				t.Fatalf("policy = %v, want %v", scopes.Policy, test.wantPolicy)
			}
			if test.wantDigest && scopes.Digests["query-group-1"] != "qg-object-1" {
				t.Fatalf("digests = %v, want the published content", scopes.Digests)
			}
			if !test.wantDigest && len(scopes.Digests) != 0 {
				t.Fatalf("digests = %v, want none", scopes.Digests)
			}
			if test.wantReport != (len(reports) == 1) {
				t.Fatalf("reports = %+v, want reported=%t", reports, test.wantReport)
			}
		})
	}
}

type fakeContentScopeSource struct {
	state    controlplane.ActivationState
	manifest controlplane.CatalogManifest
	err      error
	asked    []execution.SnapshotRevision
}

func (source *fakeContentScopeSource) LoadActivation(context.Context) (controlplane.ActivationState, error) {
	return source.state, source.err
}

func (source *fakeContentScopeSource) LoadCatalogManifest(_ context.Context, revision execution.SnapshotRevision) (controlplane.CatalogManifest, error) {
	source.asked = append(source.asked, revision)
	return source.manifest, source.err
}

// The content a Query Group is published with is the ObjectDigest the
// current activation's manifest names for it -- the digest its open Segment
// carries and its Slots declare -- read for the activation's own revision.
func TestCurrentContentScopesReadTheActivationsManifest(t *testing.T) {
	source := &fakeContentScopeSource{
		state: controlplane.ActivationState{Current: controlplane.SnapshotPublicationRef{SnapshotRevision: "snap-9", PublicationEpoch: 2}},
		manifest: controlplane.CatalogManifest{QueryGroups: []controlplane.ManifestQueryGroup{
			{QueryGroup: "qg-a", ObjectDigest: "da"}, {QueryGroup: "qg-b", ObjectDigest: "db"}, {QueryGroup: "", ObjectDigest: "dx"}, {QueryGroup: "qg-c", ObjectDigest: ""},
		}},
	}
	digests, err := currentContentScopes(source)(context.Background())
	if err != nil {
		t.Fatalf("currentContentScopes() error = %v", err)
	}
	if len(digests) != 2 || digests["qg-a"] != "da" || digests["qg-b"] != "db" {
		t.Fatalf("digests = %v, want the two complete entries", digests)
	}
	if len(source.asked) != 1 || source.asked[0] != "snap-9" {
		t.Fatalf("manifest read for %v, want the activation's revision snap-9", source.asked)
	}
	source.err = errors.New("activation unavailable")
	if _, err := currentContentScopes(source)(context.Background()); err == nil {
		t.Fatal("a failed activation read returned digests")
	}
}
