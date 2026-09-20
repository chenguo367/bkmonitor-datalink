// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.

// Package scheduler selects Query Group ownership and starts frozen Slot
// execution. It does not contain Access, evaluation or completion logic.
package scheduler

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/ownership"
)

var ErrNoEligibleWorker = errors.New("alarmd scheduler: no eligible worker")

type WorkerEligibility interface {
	Eligible(execution.QueryGroupIdentity, ownership.WorkerRegistration, time.Time) bool
}

type staticWorkerEligibility struct {
	required ownership.WorkerCompatibility
}

func NewStaticWorkerEligibility(required ownership.WorkerCompatibility) (WorkerEligibility, error) {
	if err := required.Validate(); err != nil {
		return nil, err
	}
	return staticWorkerEligibility{required: required}, nil
}

func (eligibility staticWorkerEligibility) Eligible(
	_ execution.QueryGroupIdentity,
	worker ownership.WorkerRegistration,
	_ time.Time,
) bool {
	return worker.Compatibility() == eligibility.required
}

type Router struct {
	additionalEligibility WorkerEligibility
}

func NewRouter(additionalEligibility WorkerEligibility) *Router {
	return &Router{additionalEligibility: additionalEligibility}
}

func (router *Router) Select(
	queryGroup execution.QueryGroupIdentity,
	workers []ownership.WorkerRegistration,
	at time.Time,
) (ownership.WorkerRegistration, error) {
	if router == nil || queryGroup == "" || at.IsZero() {
		return ownership.WorkerRegistration{}, errors.New("alarmd scheduler: invalid routing request")
	}
	var selected ownership.WorkerRegistration
	var selectedScore [sha256.Size]byte
	found := false
	seen := make(map[string]struct{}, len(workers))
	for _, worker := range workers {
		if err := worker.Validate(); err != nil {
			return ownership.WorkerRegistration{}, err
		}
		if _, duplicate := seen[worker.WorkerID]; duplicate {
			return ownership.WorkerRegistration{}, errors.New("alarmd scheduler: duplicate worker identity")
		}
		seen[worker.WorkerID] = struct{}{}
		if worker.AssignmentReadiness != ownership.WorkerReady || !worker.ExpiresAt.After(at) {
			continue
		}
		if router.additionalEligibility != nil && !router.additionalEligibility.Eligible(queryGroup, worker, at) {
			continue
		}
		score := sha256.Sum256([]byte(string(queryGroup) + "\x00" + worker.WorkerID))
		if !found || bytes.Compare(score[:], selectedScore[:]) > 0 {
			selected, selectedScore, found = worker, score, true
		}
	}
	if !found {
		return ownership.WorkerRegistration{}, ErrNoEligibleWorker
	}
	return selected, nil
}

// incumbentEligible reports whether the Worker named by workerID is in the
// ready set under the same membership rules Select applies: a valid READY
// registration that has not expired at the given time and, when configured,
// the additional eligibility. Select itself is left untouched.
func (router *Router) incumbentEligible(
	queryGroup execution.QueryGroupIdentity,
	workerID string,
	workers []ownership.WorkerRegistration,
	at time.Time,
) bool {
	return router.incumbentEligibleIn(queryGroup, workerID, indexReadyWorkers(workers), at)
}

// readyWorkerIndex is one round's ready set, kept both ways.
//
// The set is read once per round and then consulted once per Query Group to
// ask whether that Query Group's incumbent is still in it. Walking the slice
// each time makes the round Q*N in a comparison a lookup answers, and the
// walk is not where any decision is taken -- the same worker is found either
// way.
//
// The slice is kept beside the map, and Select still takes the slice. Select
// is not a lookup: it validates every worker and refuses a duplicate worker
// identity, and both of those are statements about the list it was given.
// Handing it a list rebuilt from the map would drop the duplicate refusal
// entirely and make "which invalid worker is reported" depend on map order.
type readyWorkerIndex struct {
	ordered []ownership.WorkerRegistration
	byID    map[string]ownership.WorkerRegistration
}

func indexReadyWorkers(workers []ownership.WorkerRegistration) readyWorkerIndex {
	index := readyWorkerIndex{ordered: workers, byID: make(map[string]ownership.WorkerRegistration, len(workers))}
	for _, worker := range workers {
		// First wins, which is what scanning the slice in order did.
		if _, seen := index.byID[worker.WorkerID]; !seen {
			index.byID[worker.WorkerID] = worker
		}
	}
	return index
}

func (router *Router) incumbentEligibleIn(
	queryGroup execution.QueryGroupIdentity,
	workerID string,
	workers readyWorkerIndex,
	at time.Time,
) bool {
	if router == nil || workerID == "" {
		return false
	}
	worker, found := workers.byID[workerID]
	if !found {
		return false
	}
	if worker.Validate() != nil || worker.AssignmentReadiness != ownership.WorkerReady || !worker.ExpiresAt.After(at) {
		return false
	}
	return router.additionalEligibility == nil || router.additionalEligibility.Eligible(queryGroup, worker, at)
}

type AssignmentStore interface {
	ListReadyWorkers(context.Context, time.Time) ([]ownership.WorkerRegistration, ownership.ControlReadStats, error)
	ReadAssignment(context.Context, execution.QueryGroupIdentity) (ownership.AssignmentRecord, error)
	// ReadAssignments is how a round reads the records of many Query Groups.
	// It is in this interface rather than behind a type assertion because a
	// method the round depends on and only some implementations have is a
	// method that silently does not run: the fakes would all carry it, the
	// production store would be the one that quietly did not, and the round
	// would go on spending one round trip per Query Group with every test
	// green.
	ReadAssignments(
		context.Context,
		[]execution.QueryGroupIdentity,
	) (map[execution.QueryGroupIdentity]ownership.AssignmentRecord, ownership.ControlReadStats, error)
	PublishAssignment(
		context.Context,
		ownership.PublicationAuthority,
		ownership.AssignmentDecision,
	) (ownership.AssignmentRecord, error)
}

type Reconciler struct {
	router *Router
	store  AssignmentStore
}

func NewReconciler(router *Router, store AssignmentStore) (*Reconciler, error) {
	if router == nil || store == nil {
		return nil, errors.New("alarmd scheduler: router and Assignment store are required")
	}
	return &Reconciler{router: router, store: store}, nil
}

// ListReadyWorkers reads the ready set once for a reconcile round. A round
// reconciles every Query Group against this one set: listing per Query
// Group costs a registry read per Query Group and, worse, lets one round
// decide different Query Groups on different ready sets, a world state that
// never existed at any instant.
func (reconciler *Reconciler) ListReadyWorkers(
	ctx context.Context,
	at time.Time,
) ([]ownership.WorkerRegistration, ownership.ControlReadStats, error) {
	if reconciler == nil {
		return nil, ownership.ControlReadStats{}, errors.New("alarmd scheduler: initialized reconciler is required")
	}
	return reconciler.store.ListReadyWorkers(ctx, at)
}

// Reconcile settles one Query Group on its own, listing the ready set for
// it. A round over many Query Groups must list once and use ReconcileWith.
func (reconciler *Reconciler) Reconcile(
	ctx context.Context,
	authority ownership.PublicationAuthority,
	queryGroup execution.QueryGroupIdentity,
	at time.Time,
) (ownership.AssignmentRecord, error) {
	workers, _, err := reconciler.ListReadyWorkers(ctx, at)
	if err != nil {
		return ownership.AssignmentRecord{}, err
	}
	return reconciler.ReconcileWith(ctx, authority, queryGroup, workers, at)
}

// ReconcileWith settles one Query Group against a ready set the caller read
// for the round: a ready, eligible incumbent keeps its Assignment; otherwise
// rendezvous placement over that set publishes a new one under the given
// authority. It never lists workers itself.
func (reconciler *Reconciler) ReconcileWith(
	ctx context.Context,
	authority ownership.PublicationAuthority,
	queryGroup execution.QueryGroupIdentity,
	workers []ownership.WorkerRegistration,
	at time.Time,
) (ownership.AssignmentRecord, error) {
	if reconciler == nil {
		return ownership.AssignmentRecord{}, errors.New("alarmd scheduler: initialized reconciler is required")
	}
	current, err := reconciler.store.ReadAssignment(ctx, queryGroup)
	hasCurrent := err == nil
	if !hasCurrent && !errors.Is(err, ownership.ErrAssignmentAbsent) {
		return ownership.AssignmentRecord{}, err
	}
	return reconciler.settle(ctx, authority, queryGroup, current, hasCurrent, indexReadyWorkers(workers), at)
}

// ReconcileRound settles every Query Group of a round against one ready set,
// reading all of their Assignment records first.
//
// The records are read in one bounded batch rather than one at a time. That
// is the only difference from calling ReconcileWith in a loop: the same
// records, the same eligibility rule, the same expected-revision CAS on the
// same revision, the same publication authority. A Query Group the batch did
// not find is placed, exactly as an absent record was placed before.
//
// The batch read failing fails the round before anything is published. It has
// to: the records say who currently holds each Query Group, and a round that
// treated "could not read" as "nobody holds it" would rendezvous the entire
// population onto fresh owners in one go.
func (reconciler *Reconciler) ReconcileRound(
	ctx context.Context,
	authority ownership.PublicationAuthority,
	queryGroups []execution.QueryGroupIdentity,
	workers []ownership.WorkerRegistration,
	at time.Time,
) (map[execution.QueryGroupIdentity]ownership.AssignmentRecord, ownership.ControlReadStats, error) {
	if reconciler == nil {
		return nil, ownership.ControlReadStats{}, errors.New("alarmd scheduler: initialized reconciler is required")
	}
	current, stats, err := reconciler.store.ReadAssignments(ctx, queryGroups)
	if err != nil {
		return nil, stats, err
	}
	index := indexReadyWorkers(workers)
	settled := make(map[execution.QueryGroupIdentity]ownership.AssignmentRecord, len(queryGroups))
	for _, queryGroup := range queryGroups {
		existing, hasCurrent := current[queryGroup]
		record, settleErr := reconciler.settle(ctx, authority, queryGroup, existing, hasCurrent, index, at)
		if settleErr != nil {
			return nil, stats, settleErr
		}
		settled[queryGroup] = record
	}
	return settled, stats, nil
}

// settle is the placement decision itself, over a record the caller has
// already read. Both entry points route through it so that reading one record
// or reading a batch of them cannot come to different conclusions.
func (reconciler *Reconciler) settle(
	ctx context.Context,
	authority ownership.PublicationAuthority,
	queryGroup execution.QueryGroupIdentity,
	current ownership.AssignmentRecord,
	hasCurrent bool,
	workers readyWorkerIndex,
	at time.Time,
) (ownership.AssignmentRecord, error) {
	expectedRevision := uint64(0)
	if hasCurrent {
		expectedRevision = current.RecordRevision
	}
	if hasCurrent && reconciler.router.incumbentEligibleIn(queryGroup, current.DesiredWorkerID, workers, at) {
		return current, nil
	}
	selected, err := reconciler.router.Select(queryGroup, workers.ordered, at)
	if err != nil {
		return ownership.AssignmentRecord{}, err
	}
	return reconciler.store.PublishAssignment(
		ctx, authority, ownership.AssignmentDecision{
			QueryGroup: queryGroup, DesiredWorkerID: selected.WorkerID,
			ExpectedRecordRevision: expectedRevision, PlacementReason: ownership.PlacementRendezvous, DecidedAt: at,
		},
	)
}
