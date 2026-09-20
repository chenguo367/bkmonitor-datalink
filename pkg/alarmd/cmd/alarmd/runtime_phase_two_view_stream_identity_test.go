// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package main

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/ownership"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/viewstream"
)

// The identity a process writes into its registration: a token minted once
// and never the same twice, and an endpoint on the HTTP listener's port at
// the address the listener binds -- or, for a wildcard bind, the address
// this process reaches Redis from. A listener that cannot be parsed leaves
// the endpoint empty and the token minted: the process serves the stream
// and is not advertised.
func TestTheStreamIdentityIsMintedOnceAndAdvertisedOnTheListenersPort(t *testing.T) {
	first, err := newViewStreamIdentity("10.1.2.3:8080", "127.0.0.1:6379")
	if err != nil {
		t.Fatal(err)
	}
	second, err := newViewStreamIdentity("10.1.2.3:8080", "127.0.0.1:6379")
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Token) != 64 || first.Token == second.Token {
		t.Fatalf("tokens = %q / %q, want 32 random bytes each, different", first.Token, second.Token)
	}
	if first.Endpoint != "10.1.2.3:8080" {
		t.Fatalf("bound listener endpoint = %q, want the bound address", first.Endpoint)
	}
	wildcard, err := newViewStreamIdentity(":8080", "127.0.0.1:6379")
	if err != nil {
		t.Fatal(err)
	}
	host, port, err := net.SplitHostPort(wildcard.Endpoint)
	if err != nil || port != "8080" || host == "" || host == "0.0.0.0" {
		t.Fatalf("wildcard listener endpoint = %q, want the outbound address toward Redis on port 8080", wildcard.Endpoint)
	}
	if outbound := outboundAddress("127.0.0.1:6379"); outbound != host {
		t.Fatalf("endpoint host %s, outbound toward Redis %s", host, outbound)
	}
	unparsable, err := newViewStreamIdentity("not-a-listener", "127.0.0.1:6379")
	if err != nil || unparsable.Endpoint != "" || unparsable.Token == "" {
		t.Fatalf("unparsable listener = %+v (%v), want no endpoint and a token", unparsable, err)
	}
}

type fakeRegistry struct {
	registrations map[string]ownership.WorkerRegistration
	err           error
}

func (registry fakeRegistry) ReadWorker(_ context.Context, workerID string) (ownership.WorkerRegistration, bool, error) {
	if registry.err != nil {
		return ownership.WorkerRegistration{}, false, registry.err
	}
	registration, found := registry.registrations[workerID]
	return registration, found, nil
}

// Admission reads the Worker's own registration: the token there admits,
// any other token or none is BAD_TOKEN, no live registration is
// UNKNOWN_WORKER, and a registry that cannot be read is an error the
// server turns into REGISTRY_UNAVAILABLE.
func TestAdmissionTrustsTheRegistrationAndNothingElse(t *testing.T) {
	now := time.Unix(1000, 0)
	live := ownership.WorkerRegistration{WorkerID: "w1", StreamToken: "secret", ExpiresAt: now.Add(time.Minute)}
	old := ownership.WorkerRegistration{WorkerID: "w2", StreamToken: "secret", ExpiresAt: now.Add(-time.Minute)}
	legacy := ownership.WorkerRegistration{WorkerID: "w3", ExpiresAt: now.Add(time.Minute)}
	admission := viewStreamAdmission{registry: fakeRegistry{registrations: map[string]ownership.WorkerRegistration{"w1": live, "w2": old, "w3": legacy}}, now: func() time.Time { return now }}
	for name, test := range map[string]struct {
		worker, token, want string
	}{
		"right token":                {"w1", "secret", ""},
		"wrong token":                {"w1", "guess", viewstream.RefusalBadToken},
		"empty token":                {"w1", "", viewstream.RefusalBadToken},
		"expired registration":       {"w2", "secret", viewstream.RefusalUnknownWorker},
		"unknown worker":             {"w9", "secret", viewstream.RefusalUnknownWorker},
		"registration without token": {"w3", "", viewstream.RefusalBadToken},
	} {
		got, err := admission.Admit(context.Background(), test.worker, test.token)
		if err != nil || got != test.want {
			t.Fatalf("%s: (%q, %v), want %q", name, got, err, test.want)
		}
	}
	if _, err := (viewStreamAdmission{registry: fakeRegistry{err: context.DeadlineExceeded}, now: time.Now}).Admit(context.Background(), "w1", "secret"); err == nil || !strings.Contains(err.Error(), "deadline") {
		t.Fatalf("registry error = %v, want it returned", err)
	}
}
