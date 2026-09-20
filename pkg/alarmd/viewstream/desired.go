// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package viewstream

import (
	"errors"
	"sort"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// Desired is the Leader's whole desired set at one moment: everything a
// reconcile round knows once it has read the activation, that publication's
// manifest and the Assignment records it wrote. Projecting it per Worker is
// this package's job; producing it is the round's.
type Desired struct {
	ControlEpoch uint64
	Publication  Publication
	// Content is what the current publication carries, by Query Group. A
	// Query Group assigned but absent here is draining.
	Content map[execution.QueryGroupIdentity]Content
	// Assignments is every Assignment record the Leader holds, by Query
	// Group, including draining ones.
	Assignments map[execution.QueryGroupIdentity]Assignment
}

// Project is the view of one Worker: every Query Group whose Assignment
// names it, with the content the publication carries for it. The revision
// and digest are filled by the caller that publishes it; Project computes
// the digest so the caller cannot name a different one.
//
// A Query Group is in exactly the view of the Worker its record names. A
// Worker still holding a lease on a Query Group whose record moved away
// does not see it: the record is the authority, the view a preview of it,
// and the Worker learns the move the way it always did, from its renewal.
func (desired Desired) Project(workerID string) (View, error) {
	if workerID == "" {
		return View{}, errors.New("alarmd viewstream: projection needs a worker")
	}
	entries := make([]Entry, 0)
	for queryGroup, assignment := range desired.Assignments {
		if assignment.DesiredWorkerID != workerID {
			continue
		}
		entry := Entry{QueryGroup: queryGroup, Assignment: assignment}
		if content, published := desired.Content[queryGroup]; published {
			copied := content
			copied.OutputContexts = append([]OutputContextRef(nil), content.OutputContexts...)
			entry.Content = &copied
		}
		entries = append(entries, entry)
	}
	digest, err := DigestOf(desired.Publication, entries)
	if err != nil {
		return View{}, err
	}
	return View{
		WorkerID: workerID, Version: Version{ControlEpoch: desired.ControlEpoch, Digest: digest},
		Publication: desired.Publication, Entries: entries,
	}, nil
}

// Workers lists every Worker the desired set names, sorted: the receivers a
// publication of it expects.
func (desired Desired) Workers() []string {
	seen := make(map[string]struct{})
	for _, assignment := range desired.Assignments {
		if assignment.DesiredWorkerID != "" {
			seen[assignment.DesiredWorkerID] = struct{}{}
		}
	}
	workers := make([]string, 0, len(seen))
	for worker := range seen {
		workers = append(workers, worker)
	}
	sort.Strings(workers)
	return workers
}
