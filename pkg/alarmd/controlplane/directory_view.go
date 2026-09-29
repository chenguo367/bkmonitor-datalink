// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package controlplane

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/redisfailure"
)

// DirectoryView answers the strategy directory from what the control Leader
// already holds: the catalog it published (the strategy index its rounds
// build), the activation its repository has parsed for the version it runs,
// and the content of the publications that activation carries Plans on. It
// keeps no copy of its own and refreshes nothing. A row is built when a
// request asks for it, and only the rows of the page asked for.
//
// Only the Leader answers. A follower, a Leader before its first completed
// round, and a former Leader after it stepped down hold no publication of
// their own: Available is false there, and the reader is sent to the
// Leader rather than answered from a copy every replica would have to keep.
//
// What a row needs and where it comes from: the Plan, its Query Group, the
// Query Group's revisions and the object it is read from from the published
// catalog; the activation and the role from the activation; the output
// context from the publication's content. A Plan still active on an older
// publication - a Query Group draining after its query changed - is named
// from that publication's content, and its revisions from its object, read
// for the rows a request returns and no others. A publication whose manifest
// has expired names its Plans from the activation alone (ManifestExpired).
type DirectoryView struct {
	reconciler *SourceReconciler
	repository *RedisCatalogRepository
	timeout    time.Duration
}

// NewDirectoryView builds the view over one process's control plane. The
// timeout bounds the reads one request makes: the activation header, an
// older publication's content, the objects its rows name.
func NewDirectoryView(reconciler *SourceReconciler, repository *RedisCatalogRepository, timeout time.Duration) (*DirectoryView, error) {
	if reconciler == nil || repository == nil || timeout <= 0 {
		return nil, errors.New("alarmd controlplane: a directory view needs the reconciler, the repository and a timeout")
	}
	return &DirectoryView{reconciler: reconciler, repository: repository, timeout: timeout}, nil
}

// Available says this process holds a publication of its own to answer
// from: it leads and has completed a round.
func (view *DirectoryView) Available() bool {
	return view != nil && view.reconciler.publishedIndex() != nil
}

// directoryAnswer is what one request reads, once: the publication, the
// activation, and the rows it names in the order a page reads them.
type directoryAnswer struct {
	snapshot StrategyDirectorySnapshot
	index    *strategyIndex
	// active is every Plan the activation carries, by key, and carried the
	// publications other than the published one it carries them on, in the
	// order the activation lists them.
	active  map[execution.PlanKey]PlanActivationRecord
	carried []SnapshotPublicationRef
	// older is the content of each carried publication read so far, and
	// expired the carried publications whose manifest is gone.
	older   map[SnapshotPublicationRef]PublishedContent
	expired map[SnapshotPublicationRef]bool
	// objects are the Query Group objects read for this answer's rows.
	objects map[execution.ObjectDigest]QueryGroupObject
}

// answer reads what every row of this request is built from. It fails only
// when the view has nothing to answer with; a failed read of a carried
// publication is recorded on the snapshot and leaves its rows out.
func (view *DirectoryView) answer(ctx context.Context, at time.Time) (*directoryAnswer, error) {
	index := view.reconciler.publishedIndex()
	if index == nil {
		return nil, ErrSnapshotUnavailable
	}
	a := &directoryAnswer{index: index, active: map[execution.PlanKey]PlanActivationRecord{},
		older: map[SnapshotPublicationRef]PublishedContent{}, expired: map[SnapshotPublicationRef]bool{},
		objects: map[execution.ObjectDigest]QueryGroupObject{}}
	s := &a.snapshot
	s.ObservedAt, s.Published, s.SourceObservation = at, index.publication, index.observation
	s.GroupsTotal, s.GroupsKnown, s.Rows = len(index.groups), len(index.groups), []StrategyDirectoryRow{}
	activation, err := view.repository.LoadActivation(ctx)
	if err != nil {
		view.fail(s, "activation", "", nil, err)
		return a, nil
	}
	if validateActivationState(activation) != nil {
		s.Reason = "INVALID_ACTIVATION"
		return a, nil
	}
	s.Current, s.ActivationRevision = activation.Current, activation.RecordRevision
	s.Revision = fmt.Sprintf("%s:%d:%d", index.publication.SnapshotRevision, index.publication.PublicationEpoch, activation.RecordRevision)
	counts := map[SnapshotPublicationRef]int{}
	for _, record := range activation.Plans {
		a.active[record.Fact.Key()] = record
		counts[record.Publication]++
		if record.Publication != index.publication && counts[record.Publication] == 1 && record.Publication.validate() == nil {
			a.carried = append(a.carried, record.Publication)
		}
	}
	s.Complete = true
	s.Publications = append(s.Publications, DirectoryPublication{Publication: index.publication, Plans: counts[index.publication], Manifest: "memory"})
	for _, publication := range a.carried {
		read := DirectoryPublication{Publication: publication, Plans: counts[publication], Manifest: "store"}
		if _, remembered := view.repository.contentMemo.lookup(publication); remembered {
			read.Manifest = "memory"
		}
		content, err := view.repository.LoadPublishedContent(ctx, publication)
		switch {
		case errors.Is(err, ErrSnapshotUnavailable):
			// Its Plans run on objects the cutover renews while the manifest
			// that listed them is past its retention: named from the
			// activation, nothing guessed about the rest.
			read.Manifest = "expired"
			a.expired[publication] = true
		case err != nil:
			read.Manifest = "failed"
			failed := publication
			view.fail(s, "manifest", view.repository.catalogManifestKey(publication.SnapshotRevision), &failed, err)
		default:
			a.older[publication] = content
			s.GroupsTotal += len(content.Groups)
			s.GroupsKnown += len(content.Groups)
		}
		s.Publications = append(s.Publications, read)
	}
	return a, nil
}

// fail records the first read that failed and why.
func (view *DirectoryView) fail(s *StrategyDirectorySnapshot, step, key string, publication *SnapshotPublicationRef, err error) {
	s.Complete = false
	s.Reason = "DEPENDENCY_UNAVAILABLE"
	if s.FailedRead == "" && err != nil {
		s.FailedRead, s.Error, s.FailedKey, s.FailedPublication = step, observability.SanitizeErrorText(err.Error()), key, publication
	}
}

// rowsOf is every row of one Plan identity in this answer: its Plans in the
// published catalog, then those still active on a carried publication, then
// those on a carried publication whose manifest expired, each sorted as a
// page reads them - by Query Group, then by publication.
func (view *DirectoryView) rowsOf(ctx context.Context, a *directoryAnswer, identity execution.PlanIdentity, s *StrategyDirectorySnapshot) []StrategyDirectoryRow {
	var rows []StrategyDirectoryRow
	published := a.index.publication
	contexts := view.repository.rememberedContextRefs(published)
	for _, at := range a.index.plans[identity.StrategyID] {
		group := &a.index.groups[at.group]
		plan := &group.Plans[at.plan]
		if plan.Identity != identity {
			continue
		}
		row := StrategyDirectoryRow{Identity: plan.Identity, QueryGroup: group.Identity, Publication: published, Role: "PUBLISHED",
			QueryRevision: group.QueryPlan.QueryRevision, ScheduleRevision: group.ScheduleRevision, OutputContext: contexts[plan.Identity]}
		if digest, err := DeriveQueryGroupObjectDigest(*group); err == nil {
			row.ObjectDigest = digest
		}
		if record, active := a.active[plan.Key()]; active && record.Publication == published {
			fact := record.Fact
			row.Activation, row.Role = &fact, string(fact.Selection)
		}
		rows = append(rows, row)
	}
	for _, publication := range a.carried {
		// Expired and failed publications have no content read.
		content, read := a.older[publication]
		if !read {
			continue
		}
		carriedContexts := contextRefsOf(content)
		for groupIdentity, entry := range content.Groups {
			for _, key := range entry.Plans {
				if key.PlanIdentity != identity {
					continue
				}
				record, active := a.active[key]
				if !active || record.Publication != publication {
					continue
				}
				fact := record.Fact
				row := StrategyDirectoryRow{Identity: key.PlanIdentity, QueryGroup: groupIdentity, ObjectDigest: entry.Digest,
					Publication: publication, Role: string(fact.Selection), Activation: &fact, OutputContext: carriedContexts[key.PlanIdentity]}
				object, err := view.object(ctx, a, entry.Digest)
				if err != nil {
					// The row is named; the revisions only its object holds are
					// left empty rather than guessed, and the answer says so.
					failed := publication
					view.fail(s, "group_object", view.repository.queryGroupObjectKey(entry.Digest), &failed, err)
				} else {
					row.QueryRevision, row.ScheduleRevision = object.QueryPlan.QueryRevision, object.ScheduleRevision
				}
				rows = append(rows, row)
			}
		}
	}
	for _, publication := range a.carried {
		if !a.expired[publication] {
			continue
		}
		for _, record := range a.active {
			if record.Publication != publication || record.Fact.Plan != identity {
				continue
			}
			fact := record.Fact
			rows = append(rows, StrategyDirectoryRow{Identity: fact.Plan, Publication: publication, Role: string(fact.Selection),
				Activation: &fact, ManifestExpired: true})
		}
	}
	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].QueryGroup != rows[j].QueryGroup {
			return rows[i].QueryGroup < rows[j].QueryGroup
		}
		if rows[i].Publication.PublicationEpoch != rows[j].Publication.PublicationEpoch {
			return rows[i].Publication.PublicationEpoch < rows[j].Publication.PublicationEpoch
		}
		return shardOf(rows[i]) < shardOf(rows[j])
	})
	return rows
}

func shardOf(row StrategyDirectoryRow) int {
	if row.Activation == nil {
		return 0
	}
	return row.Activation.Key().ShardIndex
}

// contextRefsOf is a publication's Plan -> output context naming.
func contextRefsOf(content PublishedContent) map[execution.PlanIdentity]execution.OutputContextDigest {
	contexts := make(map[execution.PlanIdentity]execution.OutputContextDigest)
	for _, entry := range content.Groups {
		for _, ref := range entry.Refs {
			contexts[ref.Plan] = ref.Digest
		}
	}
	return contexts
}

// object reads one Query Group object once per answer.
func (view *DirectoryView) object(ctx context.Context, a *directoryAnswer, digest execution.ObjectDigest) (QueryGroupObject, error) {
	if object, read := a.objects[digest]; read {
		return object, nil
	}
	object, err := view.repository.LoadQueryGroupObject(ctx, digest)
	if err != nil {
		return QueryGroupObject{}, err
	}
	a.objects[digest] = object
	return object, nil
}

// identities is the Plan identities this answer lists: every one of a
// strategy, or every one the catalog and the activation name, in the order
// a page reads them.
func (a *directoryAnswer) identities(strategy string) []execution.PlanIdentity {
	seen := map[execution.PlanIdentity]bool{}
	var out []execution.PlanIdentity
	add := func(identity execution.PlanIdentity) {
		if !seen[identity] {
			seen[identity] = true
			out = append(out, identity)
		}
	}
	if strategy != "" {
		for _, at := range a.index.plans[strategy] {
			add(a.index.groups[at.group].Plans[at.plan].Identity)
		}
	} else {
		for _, identity := range a.index.allIdentities() {
			add(identity)
		}
	}
	// A Plan active only on a carried publication - its strategy left the
	// catalog's current Query Group, or the catalog no longer has it - is
	// still a row.
	for _, record := range a.active {
		if record.Publication == a.index.publication {
			continue
		}
		if strategy == "" || record.Fact.Plan.StrategyID == strategy {
			add(record.Fact.Plan)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return lessPlanIdentity(out[i], out[j]) })
	return out
}

// Page answers one page of the directory: the rows of one strategy, or of
// every strategy, from offset, at most limit of them. The source
// dispositions of the strategy asked for come with it; they carry no tenant
// and are not joined to a Plan.
func (view *DirectoryView) Page(ctx context.Context, at time.Time, tenant, business, strategy string, offset, limit int) StrategyDirectorySnapshot {
	ctx, cancel := context.WithTimeout(redisfailure.WithCaller(ctx, redisfailure.CallerDirectoryRead), view.timeout)
	defer cancel()
	a, err := view.answer(ctx, at)
	if err != nil {
		return StrategyDirectorySnapshot{ObservedAt: at, Reason: "LEADER_CATALOG_NOT_READY", Rows: []StrategyDirectoryRow{}}
	}
	s := a.snapshot
	s.SourceComplete = true
	for _, disposition := range a.index.dispositions[strategy] {
		if strategy == "" {
			break
		}
		s.SourceMatchedTotal++
		if len(s.Unattributed) < limit {
			s.Unattributed = append(s.Unattributed, disposition)
		}
	}
	s.SourceTruncated = s.SourceMatchedTotal > len(s.Unattributed)
	for _, identity := range a.identities(strategy) {
		if tenant != "" && identity.TenantID != tenant || business != "" && identity.BusinessID != business {
			continue
		}
		for _, row := range view.rowsOf(ctx, a, identity, &s) {
			if offset > 0 {
				offset--
				continue
			}
			if len(s.Rows) >= limit {
				return s
			}
			s.Rows = append(s.Rows, row)
		}
	}
	return s
}

// ResolveCurrent is the one row of a strategy running under the current
// activation, narrowed by tenant, business and Query Group where given. A
// strategy whose Plans name two identities, or with two current rows, is
// ambiguous; one with none is unavailable. expectedRevision, when given,
// holds the answer to the revision a page was read at.
func (view *DirectoryView) ResolveCurrent(ctx context.Context, at time.Time, tenant, business, strategy, group string,
	expectedRevision ...string) (StrategyDirectoryRow, error) {
	ctx, cancel := context.WithTimeout(redisfailure.WithCaller(ctx, redisfailure.CallerDirectoryRead), view.timeout)
	defer cancel()
	a, err := view.answer(ctx, at)
	if err != nil {
		return StrategyDirectoryRow{}, err
	}
	if len(expectedRevision) > 0 && expectedRevision[0] != a.snapshot.Revision {
		return StrategyDirectoryRow{}, ErrObservationChanged
	}
	if a.snapshot.Revision == "" {
		return StrategyDirectoryRow{}, ErrSnapshotUnavailable
	}
	var identity execution.PlanIdentity
	var selected StrategyDirectoryRow
	for _, candidate := range a.identities(strategy) {
		if tenant != "" && candidate.TenantID != tenant || business != "" && candidate.BusinessID != business {
			continue
		}
		for _, row := range view.rowsOf(ctx, a, candidate, &a.snapshot) {
			if identity != (execution.PlanIdentity{}) && identity != row.Identity {
				return StrategyDirectoryRow{}, ErrObservationAmbiguous
			}
			identity = row.Identity
			if group != "" && string(row.QueryGroup) != group || row.Role != string(execution.ActivationCurrent) {
				continue
			}
			if selected.QueryGroup != "" {
				return StrategyDirectoryRow{}, ErrObservationAmbiguous
			}
			selected = row
		}
	}
	if selected.QueryGroup == "" {
		return StrategyDirectoryRow{}, ErrSnapshotUnavailable
	}
	return selected, nil
}

// EffectivePlan is the Plan a row names, read from its Query Group object:
// the process's object cache first, the store otherwise. The object must be
// the row's Query Group and carry the Plan under the revisions the row's
// activation selected.
func (view *DirectoryView) EffectivePlan(ctx context.Context, row StrategyDirectoryRow) (QueryGroupPlanObject, error) {
	ctx, cancel := context.WithTimeout(redisfailure.WithCaller(ctx, redisfailure.CallerDirectoryRead), view.timeout)
	defer cancel()
	object, err := view.repository.LoadQueryGroupObject(ctx, row.ObjectDigest)
	if err != nil {
		return QueryGroupPlanObject{}, err
	}
	if object.Identity != row.QueryGroup {
		return QueryGroupPlanObject{}, ErrCatalogObjectCorrupt
	}
	for _, plan := range object.Plans {
		if plan.Identity == row.Identity {
			// Older objects omit the generation and let activation compilation
			// derive it. Never invent that field in the stored configuration.
			if row.Activation == nil || plan.StateGeneration != "" && plan.StateGeneration != row.Activation.Selected.StateGeneration ||
				plan.ScheduleRevision != row.Activation.Selected.ScheduleRevision {
				return QueryGroupPlanObject{}, ErrCatalogObjectCorrupt
			}
			return plan, nil
		}
	}
	return QueryGroupPlanObject{}, ErrCatalogObjectUnavailable
}

// EffectiveOutput reads the output context a row names and says what the
// Plan publishes as: one immutable object, verified against the digest that
// names it, from the object cache first. A row with no digest is answered
// as not retained.
func (view *DirectoryView) EffectiveOutput(ctx context.Context, row StrategyDirectoryRow) OutputFormatFacts {
	if row.OutputContext == "" {
		return OutputFormatFacts{Reason: OutputContextRefNotRetained}
	}
	ctx, cancel := context.WithTimeout(redisfailure.WithCaller(ctx, redisfailure.CallerDirectoryRead), view.timeout)
	defer cancel()
	contexts, err := view.repository.loadOutputContexts(ctx, []execution.OutputContextRef{{Plan: row.Identity, Digest: row.OutputContext}})
	switch {
	case errors.Is(err, ErrCatalogObjectUnavailable):
		return OutputFormatFacts{Reason: OutputContextUnavailable, OutputContextDigest: row.OutputContext}
	case errors.Is(err, ErrCatalogObjectCorrupt) || errors.Is(err, ErrCatalogObjectContractNewer):
		return OutputFormatFacts{Reason: OutputContextCorrupt, OutputContextDigest: row.OutputContext}
	case err != nil:
		return OutputFormatFacts{Reason: OutputContextDependency, OutputContextDigest: row.OutputContext}
	}
	object := contexts[row.OutputContext]
	if object.Identity != row.Identity {
		return OutputFormatFacts{Reason: OutputContextCorrupt, OutputContextDigest: row.OutputContext}
	}
	format, decidedBy := EffectiveWireFormat(object.WireFormat, object.StrategyRef.SnapshotRevision)
	return OutputFormatFacts{
		Known: true, OutputContextDigest: row.OutputContext,
		WireFormat: object.WireFormat, EffectiveWireFormat: format, DecidedBy: decidedBy,
		SnapshotRevision: object.StrategyRef.SnapshotRevision, SignalType: object.SignalType,
		CompatibilityContext: object.LegacyOutput != nil,
	}
}
