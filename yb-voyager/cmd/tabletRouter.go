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
	"sort"
	"strings"
	"sync"

	goerrors "github.com/go-errors/errors"
	log "github.com/sirupsen/logrus"

	"github.com/yugabyte/yb-voyager/yb-voyager/src/tgtdb"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils/sqlname"
)

// TabletMetadataProvider abstracts the target-database queries needed to route
// CDC events to tablet-affine workers. It is implemented by *tgtdb.TargetYugabyteDB
// and faked in unit tests.
type TabletMetadataProvider interface {
	IsTabletMetadataSupported() bool
	IsTableColocated(table sqlname.NameTuple) (bool, error)
	GetTableTabletLayout(table sqlname.NameTuple) (*tgtdb.TableTabletLayout, error)
	ComputeHashCode(layout *tgtdb.TableTabletLayout, key map[string]*string) (int, error)
}

// maxHashCodeCacheSize bounds the per-router cache of (hash-key values -> hash code).
// The cache mainly helps update-heavy workloads that repeatedly touch the same rows.
const maxHashCodeCacheSize = 200000

// TabletSetDiff captures how a table's tablet set changed between two refreshes.
// It is used to seed watermarks for newly created (child) tablets from the tablets
// they replaced (parents), preserving idempotent resume across splits.
type TabletSetDiff struct {
	Table          sqlname.NameTuple
	AddedTablets   []tgtdb.TabletInfo
	RemovedTablets []tgtdb.TabletInfo
	// AddedToParents maps each added tablet id to the removed tablets whose hash
	// range it overlaps (its split parents).
	AddedToParents map[string][]tgtdb.TabletInfo
}

func (d *TabletSetDiff) HasChanges() bool {
	return len(d.AddedTablets) > 0 || len(d.RemovedTablets) > 0
}

// TabletRouter resolves which tablet owns a given CDC event, maintaining an
// immutable per-table layout snapshot that is atomically replaced on refresh.
type TabletRouter struct {
	mu       sync.RWMutex
	provider TabletMetadataProvider

	layouts map[string]*tgtdb.TableTabletLayout // key: table.ForKey()
	tables  map[string]sqlname.NameTuple        // key: table.ForKey()

	hashCache map[string]int
}

func NewTabletRouter(provider TabletMetadataProvider) *TabletRouter {
	return &TabletRouter{
		provider:  provider,
		layouts:   make(map[string]*tgtdb.TableTabletLayout),
		tables:    make(map[string]sqlname.NameTuple),
		hashCache: make(map[string]int),
	}
}

// AddTable fetches and caches the initial layout for a table and reports whether
// the table is eligible for tablet-affine routing. Ineligible tables (colocated,
// range-sharded, or missing a hash key) must fall back to pk/table partitioning.
func (r *TabletRouter) AddTable(table sqlname.NameTuple) (bool, string, error) {
	colocated, err := r.provider.IsTableColocated(table)
	if err != nil {
		return false, "", goerrors.Errorf("check colocation for %s: %v", table.ForKey(), err)
	}
	if colocated {
		return false, "table is colocated", nil
	}

	layout, err := r.provider.GetTableTabletLayout(table)
	if err != nil {
		return false, "", goerrors.Errorf("get tablet layout for %s: %v", table.ForKey(), err)
	}
	if layout.RangeSharded {
		return false, "table is range-sharded", nil
	}
	if len(layout.HashKeyColumns) == 0 {
		return false, "table has no hash key columns", nil
	}
	if len(layout.Tablets) == 0 {
		return false, "no tablets reported for table", nil
	}

	r.mu.Lock()
	r.layouts[table.ForKey()] = layout
	r.tables[table.ForKey()] = table
	r.mu.Unlock()
	return true, "", nil
}

// IsTableRouted reports whether the router is managing the given table.
func (r *TabletRouter) IsTableRouted(table sqlname.NameTuple) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.layouts[table.ForKey()]
	return ok
}

// RouteEvent returns the tablet id that owns the event's key.
func (r *TabletRouter) RouteEvent(event *tgtdb.Event) (string, error) {
	key := event.TableNameTup.ForKey()

	r.mu.RLock()
	layout, ok := r.layouts[key]
	r.mu.RUnlock()
	if !ok {
		return "", goerrors.Errorf("table %s is not managed by tablet router", key)
	}

	hashCode, err := r.hashCodeForEvent(layout, event)
	if err != nil {
		return "", goerrors.Errorf("compute hash code for event vsn(%d): %v", event.Vsn, err)
	}

	tablet, found := findTabletForHash(layout.Tablets, hashCode)
	if !found {
		return "", goerrors.Errorf("no tablet found for table %s hash code %d", key, hashCode)
	}
	return tablet.TabletID, nil
}

func (r *TabletRouter) hashCodeForEvent(layout *tgtdb.TableTabletLayout, event *tgtdb.Event) (int, error) {
	cacheKey := hashCacheKey(event.TableNameTup.ForKey(), layout.HashKeyColumns, event.Key)

	r.mu.RLock()
	cached, ok := r.hashCache[cacheKey]
	r.mu.RUnlock()
	if ok {
		return cached, nil
	}

	hashCode, err := r.provider.ComputeHashCode(layout, event.Key)
	if err != nil {
		return 0, err
	}

	r.mu.Lock()
	// Bound memory: drop the cache wholesale when it grows too large. Hash codes are
	// stable for given values, so a cold cache only costs recomputation, not correctness.
	if len(r.hashCache) >= maxHashCodeCacheSize {
		r.hashCache = make(map[string]int)
	}
	r.hashCache[cacheKey] = hashCode
	r.mu.Unlock()
	return hashCode, nil
}

// PollLayouts re-polls the layout for every tracked table WITHOUT publishing the
// result. It returns the freshly polled layouts and per-table diffs describing
// tablet-set changes (splits/merges). Publishing is a separate step so that, when a
// split is detected, the caller can quiesce and seed child watermarks under a barrier
// before the new map becomes visible to routing.
func (r *TabletRouter) PollLayouts() (map[string]*tgtdb.TableTabletLayout, map[string]*TabletSetDiff, error) {
	r.mu.RLock()
	tables := make(map[string]sqlname.NameTuple, len(r.tables))
	for k, v := range r.tables {
		tables[k] = v
	}
	oldLayouts := r.layouts
	r.mu.RUnlock()

	diffs := make(map[string]*TabletSetDiff)
	newLayouts := make(map[string]*tgtdb.TableTabletLayout, len(tables))

	for key, table := range tables {
		layout, err := r.provider.GetTableTabletLayout(table)
		if err != nil {
			// Keep the last-known layout for this table on a transient poll failure.
			log.Warnf("tablet router: failed to refresh layout for %s, keeping last-known: %v", key, err)
			if old, ok := oldLayouts[key]; ok {
				newLayouts[key] = old
			}
			continue
		}
		if layout.RangeSharded || len(layout.Tablets) == 0 {
			log.Warnf("tablet router: table %s reported range-sharded/empty on refresh, keeping last-known layout", key)
			if old, ok := oldLayouts[key]; ok {
				newLayouts[key] = old
			}
			continue
		}
		newLayouts[key] = layout

		if old, ok := oldLayouts[key]; ok {
			diff := computeTabletSetDiff(table, old.Tablets, layout.Tablets)
			if diff.HasChanges() {
				diffs[key] = diff
			}
		}
	}
	return newLayouts, diffs, nil
}

// Publish atomically installs a new set of per-table layouts computed by PollLayouts.
func (r *TabletRouter) Publish(newLayouts map[string]*tgtdb.TableTabletLayout) {
	r.mu.Lock()
	r.layouts = newLayouts
	r.mu.Unlock()
}

// Refresh polls and immediately publishes new layouts, returning detected diffs.
// It performs no barrier and is intended for cases where no split remap is needed
// (and for unit tests). Split handling uses PollLayouts + Publish under a barrier.
func (r *TabletRouter) Refresh() (map[string]*TabletSetDiff, error) {
	newLayouts, diffs, err := r.PollLayouts()
	if err != nil {
		return nil, err
	}
	r.Publish(newLayouts)
	return diffs, nil
}

// CurrentTabletIDs returns the current tablet ids for a table (for diagnostics/tests).
func (r *TabletRouter) CurrentTabletIDs(table sqlname.NameTuple) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	layout, ok := r.layouts[table.ForKey()]
	if !ok {
		return nil
	}
	ids := make([]string, 0, len(layout.Tablets))
	for _, t := range layout.Tablets {
		ids = append(ids, t.TabletID)
	}
	return ids
}

// findTabletForHash finds the tablet whose [StartHashCode, EndHashCode) range
// contains hashCode. tablets must be sorted ascending by StartHashCode.
func findTabletForHash(tablets []tgtdb.TabletInfo, hashCode int) (tgtdb.TabletInfo, bool) {
	// Find the last tablet whose StartHashCode <= hashCode via binary search.
	idx := sort.Search(len(tablets), func(i int) bool {
		return tablets[i].StartHashCode > hashCode
	}) - 1
	if idx < 0 || idx >= len(tablets) {
		return tgtdb.TabletInfo{}, false
	}
	t := tablets[idx]
	if hashCode >= t.StartHashCode && hashCode < t.EndHashCode {
		return t, true
	}
	return tgtdb.TabletInfo{}, false
}

// computeTabletSetDiff diffs two tablet sets for a table and maps each added
// tablet to the removed tablets whose hash range it overlaps (its parents).
func computeTabletSetDiff(table sqlname.NameTuple, oldTablets, newTablets []tgtdb.TabletInfo) *TabletSetDiff {
	oldByID := make(map[string]tgtdb.TabletInfo, len(oldTablets))
	for _, t := range oldTablets {
		oldByID[t.TabletID] = t
	}
	newByID := make(map[string]tgtdb.TabletInfo, len(newTablets))
	for _, t := range newTablets {
		newByID[t.TabletID] = t
	}

	diff := &TabletSetDiff{
		Table:          table,
		AddedToParents: make(map[string][]tgtdb.TabletInfo),
	}

	var removed []tgtdb.TabletInfo
	for id, t := range oldByID {
		if _, ok := newByID[id]; !ok {
			removed = append(removed, t)
		}
	}
	for id, t := range newByID {
		if _, ok := oldByID[id]; !ok {
			diff.AddedTablets = append(diff.AddedTablets, t)
			// A child's parents are the removed tablets whose range overlaps it.
			for _, rt := range removed {
				if rangesOverlap(t.StartHashCode, t.EndHashCode, rt.StartHashCode, rt.EndHashCode) {
					diff.AddedToParents[id] = append(diff.AddedToParents[id], rt)
				}
			}
		}
	}
	diff.RemovedTablets = removed
	return diff
}

func rangesOverlap(aStart, aEnd, bStart, bEnd int) bool {
	return aStart < bEnd && bStart < aEnd
}

// hashCacheKey builds a stable cache key from the table and the event's hash-key values.
func hashCacheKey(tableKey string, hashKeyColumns []string, key map[string]*string) string {
	var sb strings.Builder
	sb.WriteString(tableKey)
	for _, col := range hashKeyColumns {
		sb.WriteByte(0)
		if v, ok := key[col]; ok && v != nil {
			sb.WriteString(*v)
		} else {
			sb.WriteString("\x01nil")
		}
	}
	return sb.String()
}

// resolveTabletEligibleTables partitions the given tables into those eligible for
// tablet-affine routing (registered with the router) and the rest, returning the
// set of eligible table keys. It logs the fallback reason for ineligible tables.
func resolveTabletEligibleTables(router *TabletRouter, tableNames []sqlname.NameTuple) (*utils.StructMap[sqlname.NameTuple, bool], error) {
	eligible := utils.NewStructMap[sqlname.NameTuple, bool]()
	for _, t := range tableNames {
		ok, reason, err := router.AddTable(t)
		if err != nil {
			return nil, goerrors.Errorf("evaluating tablet eligibility for %s: %v", t.ForKey(), err)
		}
		if ok {
			eligible.Put(t, true)
			log.Infof("tablet strategy: table %s is eligible for tablet-affine apply", t.ForKey())
		} else {
			eligible.Put(t, false)
			log.Infof("tablet strategy: table %s NOT eligible (%s); falling back to pk partitioning", t.ForKey(), reason)
		}
	}
	return eligible, nil
}
