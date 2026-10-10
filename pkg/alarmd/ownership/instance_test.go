// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package ownership

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/roles"
)

func TestInstanceRegistrationIsIndependentOfWorkers(t *testing.T) {
	store := newIntegrationStore(t)
	ctx := context.Background()
	at := time.Now().UTC().Truncate(time.Millisecond)
	worker := WorkerRegistration{WorkerID: "shared", AssignmentReadiness: WorkerReady, DependencyStatus: DependencyHealthy,
		DeploymentProfile: "standard", CapabilitiesDigest: "legacy", ExecutionContractDigest: "execution-v1", ExpiresAt: at.Add(time.Minute)}
	if err := store.RegisterWorker(ctx, worker); err != nil {
		t.Fatal(err)
	}
	for _, instance := range []InstanceRegistration{
		{InstanceID: "shared", Roles: roles.Set{roles.Control, roles.Worker, roles.Channel}, Endpoint: "127.0.0.1:8080",
			Incarnation: "shared-process", InternalToken: "secret", ExpiresAt: at.Add(time.Minute), RoleStatus: map[roles.Role]string{roles.Control: "READY"}},
		{InstanceID: "control-only", Roles: roles.Set{roles.Control}, Incarnation: "control-process", ExpiresAt: at.Add(time.Minute)},
		{InstanceID: "channel-only", Roles: roles.Set{roles.Channel}, Incarnation: "channel-process", ExpiresAt: at.Add(time.Minute)},
	} {
		if err := store.RegisterInstance(ctx, instance); err != nil {
			t.Fatal(err)
		}
		got, found, err := store.ReadInstance(ctx, instance.InstanceID)
		if err != nil || !found || got.InstanceID != instance.InstanceID || got.Incarnation != instance.Incarnation ||
			got.InternalToken != instance.InternalToken || !got.ExpiresAt.Equal(instance.ExpiresAt) {
			t.Fatalf("ReadInstance() = (%+v, %v, %v)", got, found, err)
		}
		if ttl := store.client.PTTL(ctx, store.instanceKey(instance.InstanceID)).Val(); ttl <= 0 || ttl > time.Minute {
			t.Fatalf("instance TTL = %s", ttl)
		}
	}
	ready, _, err := store.ListReadyWorkers(ctx, at)
	if err != nil || len(ready) != 1 || ready[0].WorkerID != "shared" || ready[0].ExecutionContractDigest != "execution-v1" {
		t.Fatalf("ready workers = (%+v, %v)", ready, err)
	}
	if store.instanceKey("shared") == store.workerKey("shared") {
		t.Fatal("instance and worker namespaces collide")
	}
	got, found, err := store.ReadInstance(ctx, "not-registered")
	if err != nil || found || got.InstanceID != "" {
		t.Fatalf("missing instance = (%+v, %v, %v)", got, found, err)
	}
}

func TestInstanceRegistrationRejectsInvalidAndExpiredWrites(t *testing.T) {
	store := newIntegrationStore(t)
	valid := InstanceRegistration{InstanceID: "control", Roles: roles.Set{roles.Control}, Incarnation: "process", ExpiresAt: time.Now().Add(time.Minute)}
	for _, mutate := range []func(*InstanceRegistration){
		func(instance *InstanceRegistration) { instance.InstanceID = "" },
		func(instance *InstanceRegistration) { instance.Incarnation = "" },
		func(instance *InstanceRegistration) { instance.Roles = nil },
		func(instance *InstanceRegistration) { instance.Roles = roles.Set{"unsupported"} },
		func(instance *InstanceRegistration) { instance.Roles = roles.Set{roles.Control, roles.Control} },
		func(instance *InstanceRegistration) { instance.ExpiresAt = time.Now().Add(-time.Second) },
		func(instance *InstanceRegistration) {
			instance.RoleStatus = map[roles.Role]string{roles.Worker: "READY"}
		},
	} {
		instance := valid
		mutate(&instance)
		if err := store.RegisterInstance(context.Background(), instance); err == nil {
			t.Fatalf("invalid registration accepted: %+v", instance)
		}
	}
}

func TestInstanceReadReturnsExpiryAndRejectsCorruptIdentity(t *testing.T) {
	store := newIntegrationStore(t)
	ctx := context.Background()
	instance := InstanceRegistration{InstanceID: "control", Roles: roles.Set{roles.Control}, Incarnation: "process", ExpiresAt: time.Now().Add(-time.Second)}
	payload, _ := json.Marshal(instance)
	if err := store.client.Set(ctx, store.instanceKey(instance.InstanceID), payload, time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	got, found, err := store.ReadInstance(ctx, instance.InstanceID)
	if err != nil || !found || got.ExpiresAt.After(time.Now()) {
		t.Fatalf("expired stored record = (%+v, %v, %v)", got, found, err)
	}
	for _, payload := range []string{"broken-json", `{"instance_id":"control"}`} {
		if err := store.client.Set(ctx, store.instanceKey("control"), payload, time.Minute).Err(); err != nil {
			t.Fatal(err)
		}
		if _, _, err := store.ReadInstance(ctx, "control"); err == nil {
			t.Fatal("corrupt record was accepted")
		}
	}
	instance.InstanceID = "different"
	payload, _ = json.Marshal(instance)
	if err := store.client.Set(ctx, store.instanceKey("control"), payload, time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.ReadInstance(ctx, "control"); err == nil {
		t.Fatal("record under another identity was accepted")
	}
}
