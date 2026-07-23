/*
Copyright (c) YugabyteDB, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package cmd

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	goerrors "github.com/go-errors/errors"
	log "github.com/sirupsen/logrus"

	reporter "github.com/yugabyte/yb-voyager/yb-voyager/src/reporter/stats"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/tgtdb"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils/sqlname"
)

// quiescePollInterval controls how often the barrier polls workers for idleness.
const quiescePollInterval = 5 * time.Millisecond

// tabletWorkerCountWarnThreshold triggers a one-time warning when the number of live
// tablet workers grows large (many goroutines/connections), so operators can notice
// pathological tablet counts. This is observability only; workers keep functioning.
const tabletWorkerCountWarnThreshold = 2000

type tabletWorkerKey struct {
	table    string // NameTuple.ForKey()
	tabletID string
}

// tabletWorker owns a single (table, tablet) apply pipeline: a buffered channel
// feeding a goroutine that batches and applies events for exactly one tablet.
type tabletWorker struct {
	key            tabletWorkerKey
	tableTup       sqlname.NameTuple
	ch             chan *tgtdb.Event
	done           chan bool
	lastAppliedVsn int64
	busy           atomic.Bool // true while a non-empty batch is being applied
}

// TabletWorkerRegistry manages the lifecycle of tablet-affine workers for the
// tablet CDC partitioning strategy. Workers are created on demand as tablets are
// first seen and persist across segments; the registry provides a routing barrier
// used to safely remap workers when tablets split.
type TabletWorkerRegistry struct {
	// routingLock is the barrier: Enqueue holds it in read mode, ApplySplit in
	// write mode so no new events are routed while workers are quiesced/remapped.
	routingLock sync.RWMutex

	mu             sync.Mutex
	workers        map[tabletWorkerKey]*tabletWorker
	state          *ImportDataState
	statsReporter  *reporter.StreamImportStatsReporter
	channelSize    int
	warnedHighLoad bool
}

func NewTabletWorkerRegistry(state *ImportDataState, statsReporter *reporter.StreamImportStatsReporter, channelSize int) *TabletWorkerRegistry {
	return &TabletWorkerRegistry{
		workers:       make(map[tabletWorkerKey]*tabletWorker),
		state:         state,
		statsReporter: statsReporter,
		channelSize:   channelSize,
	}
}

// RoutingBarrierRLock/RUnlock guard the routing critical section. The dispatcher
// holds the read side across route+enqueue so a split remap (write side) can never
// interleave between resolving a tablet and enqueuing to its worker.
func (r *TabletWorkerRegistry) RoutingBarrierRLock()   { r.routingLock.RLock() }
func (r *TabletWorkerRegistry) RoutingBarrierRUnlock() { r.routingLock.RUnlock() }

// Enqueue routes an event to the worker owning (table, tabletID), creating the
// worker (and its metadata row) on first use.
//
// The caller MUST already hold the routing barrier read lock (see
// RoutingBarrierRLock); Enqueue does not take it itself, both to keep route+enqueue
// atomic w.r.t. splits and to avoid non-reentrant RWMutex read-lock deadlocks.
func (r *TabletWorkerRegistry) Enqueue(tableTup sqlname.NameTuple, tabletID string, event *tgtdb.Event) error {
	w, err := r.getOrCreateWorker(tableTup, tabletID)
	if err != nil {
		return err
	}
	w.ch <- event
	log.Tracef("routed event %v to tablet worker %s/%s", event.Vsn, w.key.table, w.key.tabletID)
	return nil
}

func (r *TabletWorkerRegistry) getOrCreateWorker(tableTup sqlname.NameTuple, tabletID string) (*tabletWorker, error) {
	key := tabletWorkerKey{table: tableTup.ForKey(), tabletID: tabletID}

	r.mu.Lock()
	defer r.mu.Unlock()
	if w, ok := r.workers[key]; ok {
		return w, nil
	}

	// Ensure a metadata row exists (watermark defaults to -1 for a brand-new tablet)
	// and load the persisted watermark so we resume correctly.
	lastAppliedVsn, err := r.state.InitTabletWorker(migrationUUID, tableTup, tabletID)
	if err != nil {
		return nil, goerrors.Errorf("init tablet worker %s/%s: %v", key.table, tabletID, err)
	}

	w := &tabletWorker{
		key:            key,
		tableTup:       tableTup,
		ch:             make(chan *tgtdb.Event, r.channelSize),
		done:           make(chan bool, 1),
		lastAppliedVsn: lastAppliedVsn,
	}
	r.workers[key] = w
	go r.runWorker(w)
	log.Infof("started tablet worker for %s/%s (resume lastAppliedVsn=%d)", key.table, tabletID, lastAppliedVsn)
	if len(r.workers) >= tabletWorkerCountWarnThreshold && !r.warnedHighLoad {
		r.warnedHighLoad = true
		log.Warnf("tablet strategy: high live tablet-worker count (%d) — many goroutines/connections in use; "+
			"consider pk partitioning for very high-tablet-count tables", len(r.workers))
	}
	return w, nil
}

func (r *TabletWorkerRegistry) runWorker(w *tabletWorker) {
	endOfProcessing := false
	for !endOfProcessing {
		batch := []*tgtdb.Event{}
		timer := time.NewTimer(time.Duration(MAX_INTERVAL_BETWEEN_BATCHES) * time.Millisecond)
	Batching:
		for {
			select {
			case event := <-w.ch:
				if event == END_OF_QUEUE_SEGMENT_EVENT {
					endOfProcessing = true
					break Batching
				}
				if event == FLUSH_BATCH_EVENT {
					break Batching
				}
				if event.Vsn <= w.lastAppliedVsn {
					log.Tracef("tablet worker %s/%s ignoring event %v (vsn <= %v)", w.key.table, w.key.tabletID, event.Vsn, w.lastAppliedVsn)
					conflictDetectionCache.RemoveEvents(event)
					continue
				}
				if importerRole == SOURCE_DB_IMPORTER_ROLE && event.ExporterRole != TARGET_DB_EXPORTER_FB_ROLE {
					conflictDetectionCache.RemoveEvents(event)
					continue
				}
				w.busy.Store(true)
				batch = append(batch, event)
				if len(batch) >= MAX_EVENTS_PER_BATCH {
					break Batching
				}
			case <-timer.C:
				break Batching
			}
		}
		timer.Stop()

		if len(batch) == 0 {
			w.busy.Store(false)
			continue
		}

		eventBatch := tgtdb.NewTabletEventBatch(batch, w.key.table, w.key.tabletID)
		executeEventBatch(eventBatch, r.state, r.statsReporter, fmt.Sprintf("tablet %s (table %s)", w.key.tabletID, w.key.table))
		w.busy.Store(false)
	}
	w.done <- true
}

// FlushAllWorkers sends a non-blocking flush to every worker so pending batches are
// applied promptly. Used as the conflict-detection-cache flush hook.
func (r *TabletWorkerRegistry) FlushAllWorkers() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, w := range r.workers {
		select {
		case w.ch <- FLUSH_BATCH_EVENT:
		default:
		}
	}
}

// QuiesceAll drains all worker queues and waits until no worker is applying a batch.
// Callers must ensure no new events are being routed (e.g. hold routingLock write, or
// be at a segment boundary). It is used both at segment boundaries and split barriers.
func (r *TabletWorkerRegistry) QuiesceAll() {
	r.FlushAllWorkers()
	for {
		if r.allWorkersIdle() {
			return
		}
		time.Sleep(quiescePollInterval)
	}
}

func (r *TabletWorkerRegistry) allWorkersIdle() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, w := range r.workers {
		if len(w.ch) > 0 || w.busy.Load() {
			return false
		}
	}
	return true
}

// ApplySplit remaps workers after tablet splits/merges detected by the router.
// Under the routing barrier it: (1) quiesces workers so parent watermarks are final,
// (2) seeds each new (child) tablet's watermark from the max committed watermark of
// its parents, (3) marks removed (parent) tablets inactive, and (4) publishes the new
// tablet map so routing starts using the child tablets. Because seeding happens before
// the map is published (and routing is blocked throughout), child workers created after
// publish always read the inherited watermark, and no parent/child pair ever applies the
// same key concurrently.
func (r *TabletWorkerRegistry) ApplySplit(router *TabletRouter, newLayouts map[string]*tgtdb.TableTabletLayout, diffs map[string]*TabletSetDiff) error {
	if len(diffs) == 0 {
		router.Publish(newLayouts)
		return nil
	}
	// Barrier: block routing so parent and child tablets never apply the same key concurrently.
	r.routingLock.Lock()
	defer r.routingLock.Unlock()

	// Drain everything that is already enqueued so parent watermarks are final in the DB.
	r.QuiesceAll()

	for _, diff := range diffs {
		for _, child := range diff.AddedTablets {
			var inheritVsn int64 = -1
			for _, parent := range diff.AddedToParents[child.TabletID] {
				pv, err := r.state.GetTabletWorkerLastAppliedVsn(migrationUUID, diff.Table, parent.TabletID)
				if err != nil {
					log.Warnf("split remap: could not read parent %s watermark for table %s: %v", parent.TabletID, diff.Table.ForKey(), err)
					continue
				}
				if pv > inheritVsn {
					inheritVsn = pv
				}
			}
			if err := r.state.SeedTabletWorkerWatermark(migrationUUID, diff.Table, child.TabletID, inheritVsn); err != nil {
				return goerrors.Errorf("seed child tablet %s watermark for table %s: %v", child.TabletID, diff.Table.ForKey(), err)
			}
			log.Infof("split remap: seeded child tablet %s (table %s) with inherited watermark %d",
				child.TabletID, diff.Table.ForKey(), inheritVsn)
		}
		for _, parent := range diff.RemovedTablets {
			if err := r.state.MarkTabletWorkerInactive(migrationUUID, diff.Table, parent.TabletID); err != nil {
				log.Warnf("split remap: could not mark parent tablet %s inactive for table %s: %v", parent.TabletID, diff.Table.ForKey(), err)
			}
		}
	}

	// Publish the new tablet map only after children are seeded, while routing is still blocked.
	router.Publish(newLayouts)
	return nil
}

// CloseAll signals all workers to stop and waits for them to finish. Used at the
// end of the streaming phase.
func (r *TabletWorkerRegistry) CloseAll() {
	r.mu.Lock()
	workers := make([]*tabletWorker, 0, len(r.workers))
	for _, w := range r.workers {
		workers = append(workers, w)
	}
	r.mu.Unlock()

	for _, w := range workers {
		w.ch <- END_OF_QUEUE_SEGMENT_EVENT
	}
	for _, w := range workers {
		<-w.done
	}
	log.Infof("closed %d tablet workers", len(workers))
}

// NumWorkers returns the current number of live workers (diagnostics/guardrails).
func (r *TabletWorkerRegistry) NumWorkers() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.workers)
}

// startTabletMetadataRefreshLoop periodically re-polls tablet layouts and applies
// any detected splits to the registry until ctx is cancelled.
func startTabletMetadataRefreshLoop(ctx context.Context, router *TabletRouter, registry *TabletWorkerRegistry, interval time.Duration, tableToStrategy *utils.StructMap[sqlname.NameTuple, string]) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			newLayouts, diffs, err := router.PollLayouts()
			if err != nil {
				log.Warnf("tablet metadata refresh failed: %v", err)
				continue
			}
			if len(diffs) == 0 {
				// No tablet-set change (ids unchanged); safe to publish without a barrier.
				router.Publish(newLayouts)
				continue
			}
			for key, diff := range diffs {
				log.Infof("tablet metadata refresh: table %s changed (added=%d removed=%d)",
					key, len(diff.AddedTablets), len(diff.RemovedTablets))
			}
			if err := registry.ApplySplit(router, newLayouts, diffs); err != nil {
				log.Warnf("applying tablet split remap failed: %v", err)
			}
		}
	}
}
