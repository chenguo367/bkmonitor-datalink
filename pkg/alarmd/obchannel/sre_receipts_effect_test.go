// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License.

package obchannel

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/internal/redistest"
)

const effectKey = "test.sre:{env}:receipts"

// Reads cannot push a change out: a burst of executions past their own
// list's bound leaves an image change's receipt where it was. Before, one
// list of 1024 kept both, and a reader's executions evicted the changes
// within hours.
func TestAccessReceiptsCannotEvictAChange(t *testing.T) {
	client := redistest.Start(t).Client()
	receipts := NewSREReceipts(client, effectKey, nil, nil)
	at := time.Unix(1_700_000_000, 0).UTC()
	receipts.Record(SREAction{Op: OpSetImage, Namespace: "ns", Name: "web", RequestID: "image", At: at})
	for index := 0; index < MaxSREAccessReceipts+50; index++ {
		receipts.Record(SREAction{Op: "pod.exec", Namespace: "ns", Name: "web-0/web", RequestID: fmt.Sprint(index), At: at.Add(time.Duration(index+1) * time.Second)})
	}
	page, err := receipts.List(context.Background(), SREReceiptFilter{RequestID: "image"})
	if err != nil || page.Matched != 1 || page.Receipts[0].Effect != SREEffectChange {
		t.Fatalf("%+v %v, want the image change kept", page, err)
	}
	if page.StoredByEffect[SREEffectAccess] != MaxSREAccessReceipts || page.StoredByEffect[SREEffectChange] != 1 || page.Stored != MaxSREAccessReceipts+1 {
		t.Fatalf("stored %d %v, want the access list at its bound and the change kept", page.Stored, page.StoredByEffect)
	}
}

// Each action goes to its own list: every change op to the change list,
// executions and reads to the access list; effect selects one.
func TestEachReceiptGoesToItsEffectsList(t *testing.T) {
	client := redistest.Start(t).Client()
	receipts := NewSREReceipts(client, effectKey, nil, nil)
	changes := []string{OpSetImage, "k8s.apply", "k8s.delete", "pod.delete", "workload.restart", "workload.scale", "k8s.dry_run"}
	accesses := []string{"pod.exec", "secret.get", "configmap.get", "k8s.list"}
	at := time.Unix(1_700_000_000, 0).UTC()
	for index, op := range append(append([]string{}, changes...), accesses...) {
		receipts.Record(SREAction{Op: op, RequestID: op, At: at.Add(time.Duration(index) * time.Second)})
	}
	for effect, want := range map[string][]string{SREEffectChange: changes, SREEffectAccess: accesses} {
		page, err := receipts.List(context.Background(), SREReceiptFilter{Effect: effect})
		if err != nil {
			t.Fatal(err)
		}
		var got []string
		for _, receipt := range page.Receipts {
			if receipt.Effect != effect {
				t.Errorf("%s receipt marked %q", receipt.Op, receipt.Effect)
			}
			got = append([]string{receipt.Op}, got...)
		}
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Errorf("effect %s read %v, want %v", effect, got, want)
		}
	}
	ctx := context.Background()
	for _, key := range []string{effectKey, effectKey + ":change"} {
		if ttl := client.TTL(ctx, key).Val(); ttl <= SREReceiptRetention-time.Minute || ttl > SREReceiptRetention {
			t.Errorf("%s ttl %v, want the retention", key, ttl)
		}
	}
	page, _ := receipts.List(ctx, SREReceiptFilter{})
	if page.Matched != len(changes)+len(accesses) || page.Receipts[0].Op != "k8s.list" || page.Receipts[len(page.Receipts)-1].Op != OpSetImage {
		t.Fatalf("both lists read %d, newest %s, want all merged newest first", page.Matched, page.Receipts[0].Op)
	}
}

// A change naming more objects than its size holds gives names up for a
// "+N more" that still counts every object, and fits.
func TestAnOversizedChangeIsShortenedToItsSize(t *testing.T) {
	client := redistest.Start(t).Client()
	receipts := NewSREReceipts(client, effectKey, nil, nil)
	names := make([]string, 20)
	for index := range names {
		names[index] = "ConfigMap/ns/" + strings.Repeat("n", 240) + fmt.Sprint(index)
	}
	receipts.Record(SREAction{Op: "k8s.apply", Name: strings.Join(names, ",") + ",+30 more", RequestID: "big", Detail: "created 50"})
	stored := client.LRange(context.Background(), effectKey+":change", 0, 0).Val()
	if len(stored) != 1 || len(stored[0]) > SREChangeReceiptBytes {
		t.Fatalf("stored %d bytes, want at most %d", len(stored[0]), SREChangeReceiptBytes)
	}
	page, _ := receipts.List(context.Background(), SREReceiptFilter{RequestID: "big"})
	name := page.Receipts[0].Name
	kept := strings.Split(name, ",")
	var more int
	if _, err := fmt.Sscanf(kept[len(kept)-1], "+%d more", &more); err != nil || len(kept)-1+more != 50 || page.Receipts[0].Detail != "created 50" {
		t.Fatalf("name %q detail %q, want the kept names and a +N more counting all 50", name, page.Receipts[0].Detail)
	}
}

// An access receipt whose texts alone pass its size is cut at character
// boundaries, marked, and fits.
func TestAnOversizedAccessIsCutAtCharacters(t *testing.T) {
	client := redistest.Start(t).Client()
	receipts := NewSREReceipts(client, effectKey, nil, nil)
	long := strings.Repeat("容", 300)
	receipts.Record(SREAction{Op: "pod.exec", Namespace: "ns", Name: long + "/c", RequestID: "cut",
		Exec: &SREExec{RemoteState: "remote_completed", StartFailure: long}})
	stored := client.LRange(context.Background(), effectKey, 0, 0).Val()
	if len(stored) != 1 || len(stored[0]) > SREAccessReceiptBytes {
		t.Fatalf("stored %d bytes, want at most %d", len(stored[0]), SREAccessReceiptBytes)
	}
	page, _ := receipts.List(context.Background(), SREReceiptFilter{RequestID: "cut"})
	receipt := page.Receipts[0]
	for _, text := range []string{receipt.Name, receipt.Exec.StartFailure} {
		// A cut inside a character would be encoded as U+FFFD.
		if !utf8.ValidString(text) || strings.ContainsRune(text, utf8.RuneError) {
			t.Fatalf("text %q was cut inside a character", text)
		}
	}
	if !strings.HasSuffix(receipt.Name, "…") || receipt.Exec.RemoteState != "remote_completed" || receipt.Namespace != "ns" {
		t.Fatalf("receipt %+v, want the long texts cut at characters and marked, the rest kept", receipt)
	}
}

// Both lists full of receipts at their sizes stay within half of a read's
// hold, which the decoding doubles.
func TestTwoFullListsFitHalfARead(t *testing.T) {
	client := redistest.Start(t).Client()
	receipts := NewSREReceipts(client, effectKey, nil, nil)
	big := strings.Repeat("x", 8<<10)
	for index := 0; index < MaxSREChangeReceipts+3; index++ {
		receipts.Record(SREAction{Op: "k8s.apply", Name: big, Detail: big})
	}
	for index := 0; index < MaxSREAccessReceipts+3; index++ {
		receipts.Record(SREAction{Op: "pod.exec", Name: big})
	}
	ctx := context.Background()
	total := 0
	for _, key := range []string{effectKey, effectKey + ":change"} {
		for _, entry := range client.LRange(ctx, key, 0, -1).Val() {
			total += len(entry)
		}
	}
	if total > MaxResponseBytes/2 {
		t.Fatalf("both lists hold %d bytes, want at most %d", total, MaxResponseBytes/2)
	}
}

// Cutting a receipt to its size leaves the caller's action as it was: the
// audit line and the receipt share the run's facts.
func TestCuttingAReceiptLeavesTheCallersActionAlone(t *testing.T) {
	client := redistest.Start(t).Client()
	long := strings.Repeat("f", 2<<10)
	exec := &SREExec{RemoteState: "remote_completed", StartFailure: long}
	NewSREReceipts(client, effectKey, nil, nil).Record(SREAction{Op: "pod.exec", Name: "p/c", Exec: exec})
	if exec.StartFailure != long {
		t.Fatalf("the caller's start failure became %d bytes, want it untouched", len(exec.StartFailure))
	}
}
