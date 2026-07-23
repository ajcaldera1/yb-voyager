//go:build unit

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
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/yugabyte/yb-voyager/yb-voyager/src/tgtdb"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils/sqlname"
)

func strPtr(s string) *string { return &s }

func newTableTuple(name string) sqlname.NameTuple {
	oname := sqlname.NewObjectName(YUGABYTEDB, "public", "public", name)
	return sqlname.NameTuple{CurrentName: oname, TargetName: oname}
}

// hashSharded6 returns the classic 6-tablet even hash split over [0,65536).
func hashSharded6() []tgtdb.TabletInfo {
	return []tgtdb.TabletInfo{
		{TabletID: "t0", StartHashCode: 0, EndHashCode: 10922},
		{TabletID: "t1", StartHashCode: 10922, EndHashCode: 21845},
		{TabletID: "t2", StartHashCode: 21845, EndHashCode: 32768},
		{TabletID: "t3", StartHashCode: 32768, EndHashCode: 43690},
		{TabletID: "t4", StartHashCode: 43690, EndHashCode: 54613},
		{TabletID: "t5", StartHashCode: 54613, EndHashCode: 65536},
	}
}

// fakeTabletProvider is an in-memory TabletMetadataProvider for unit tests.
type fakeTabletProvider struct {
	supported       bool
	colocatedTables map[string]bool
	layouts         map[string]*tgtdb.TableTabletLayout
	hashFn          func(key map[string]*string) int
	computeCalls    int
}

func (f *fakeTabletProvider) IsTabletMetadataSupported() bool { return f.supported }

func (f *fakeTabletProvider) IsTableColocated(table sqlname.NameTuple) (bool, error) {
	return f.colocatedTables[table.ForKey()], nil
}

func (f *fakeTabletProvider) GetTableTabletLayout(table sqlname.NameTuple) (*tgtdb.TableTabletLayout, error) {
	if l, ok := f.layouts[table.ForKey()]; ok {
		return l, nil
	}
	return &tgtdb.TableTabletLayout{}, nil
}

func (f *fakeTabletProvider) ComputeHashCode(layout *tgtdb.TableTabletLayout, key map[string]*string) (int, error) {
	f.computeCalls++
	return f.hashFn(key), nil
}

func idHashFn(key map[string]*string) int {
	v := key["id"]
	if v == nil {
		return 0
	}
	n, _ := strconv.Atoi(*v)
	return n % tgtdb.YB_HASH_CODE_MAX
}

func TestFindTabletForHash(t *testing.T) {
	tablets := hashSharded6()
	tests := []struct {
		hash       int
		wantTablet string
		wantFound  bool
	}{
		{0, "t0", true},     // inclusive start
		{8000, "t0", true},  // interior
		{10921, "t0", true}, // just below boundary
		{10922, "t1", true}, // exclusive end -> next tablet
		{30096, "t2", true}, // interior
		{65535, "t5", true}, // last valid
		{65536, "", false},  // == max, exclusive of last end
		{100000, "", false}, // out of range
	}
	for _, tt := range tests {
		got, found := findTabletForHash(tablets, tt.hash)
		assert.Equal(t, tt.wantFound, found, "hash=%d found", tt.hash)
		if tt.wantFound {
			assert.Equal(t, tt.wantTablet, got.TabletID, "hash=%d tablet", tt.hash)
		}
	}
}

func TestFindTabletForHashEmpty(t *testing.T) {
	_, found := findTabletForHash(nil, 100)
	assert.False(t, found)
}

func TestRangesOverlap(t *testing.T) {
	assert.True(t, rangesOverlap(0, 100, 50, 150))
	assert.True(t, rangesOverlap(0, 100, 0, 100))
	assert.True(t, rangesOverlap(0, 32768, 0, 65536))
	assert.False(t, rangesOverlap(0, 100, 100, 200)) // touching, not overlapping
	assert.False(t, rangesOverlap(0, 100, 200, 300))
}

func TestComputeTabletSetDiffNoChange(t *testing.T) {
	table := newTableTuple("users")
	diff := computeTabletSetDiff(table, hashSharded6(), hashSharded6())
	assert.False(t, diff.HasChanges())
	assert.Empty(t, diff.AddedTablets)
	assert.Empty(t, diff.RemovedTablets)
}

func TestComputeTabletSetDiffSplit(t *testing.T) {
	table := newTableTuple("users")
	// parent tablet p0 covering [0,32768) splits into c0 [0,16384) and c1 [16384,32768).
	old := []tgtdb.TabletInfo{
		{TabletID: "p0", StartHashCode: 0, EndHashCode: 32768},
		{TabletID: "p1", StartHashCode: 32768, EndHashCode: 65536},
	}
	new := []tgtdb.TabletInfo{
		{TabletID: "c0", StartHashCode: 0, EndHashCode: 16384},
		{TabletID: "c1", StartHashCode: 16384, EndHashCode: 32768},
		{TabletID: "p1", StartHashCode: 32768, EndHashCode: 65536},
	}
	diff := computeTabletSetDiff(table, old, new)
	require.True(t, diff.HasChanges())
	assert.Len(t, diff.AddedTablets, 2)
	require.Len(t, diff.RemovedTablets, 1)
	assert.Equal(t, "p0", diff.RemovedTablets[0].TabletID)

	// Both children should map to parent p0 (the removed tablet whose range they overlap).
	require.Len(t, diff.AddedToParents["c0"], 1)
	assert.Equal(t, "p0", diff.AddedToParents["c0"][0].TabletID)
	require.Len(t, diff.AddedToParents["c1"], 1)
	assert.Equal(t, "p0", diff.AddedToParents["c1"][0].TabletID)
}

func TestComputeTabletSetDiffMerge(t *testing.T) {
	table := newTableTuple("users")
	old := []tgtdb.TabletInfo{
		{TabletID: "c0", StartHashCode: 0, EndHashCode: 16384},
		{TabletID: "c1", StartHashCode: 16384, EndHashCode: 32768},
	}
	new := []tgtdb.TabletInfo{
		{TabletID: "m0", StartHashCode: 0, EndHashCode: 32768},
	}
	diff := computeTabletSetDiff(table, old, new)
	require.True(t, diff.HasChanges())
	require.Len(t, diff.AddedTablets, 1)
	assert.Equal(t, "m0", diff.AddedTablets[0].TabletID)
	assert.Len(t, diff.RemovedTablets, 2)
	// The merged tablet inherits from both parents.
	assert.Len(t, diff.AddedToParents["m0"], 2)
}

func TestRouterAddTableEligibility(t *testing.T) {
	usersKey := newTableTuple("users").ForKey()
	colocatedKey := newTableTuple("colo").ForKey()
	rangeKey := newTableTuple("rangetbl").ForKey()
	nohashKey := newTableTuple("nohash").ForKey()

	provider := &fakeTabletProvider{
		supported:       true,
		colocatedTables: map[string]bool{colocatedKey: true},
		layouts: map[string]*tgtdb.TableTabletLayout{
			usersKey:  {Tablets: hashSharded6(), HashKeyColumns: []string{"id"}, HashKeyColumnTypes: []string{"bigint"}},
			rangeKey:  {RangeSharded: true},
			nohashKey: {Tablets: hashSharded6()}, // no hash key columns
		},
		hashFn: idHashFn,
	}
	router := NewTabletRouter(provider)

	ok, _, err := router.AddTable(newTableTuple("users"))
	require.NoError(t, err)
	assert.True(t, ok)
	assert.True(t, router.IsTableRouted(newTableTuple("users")))

	ok, reason, err := router.AddTable(newTableTuple("colo"))
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Contains(t, reason, "colocated")

	ok, reason, err = router.AddTable(newTableTuple("rangetbl"))
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Contains(t, reason, "range-sharded")

	ok, reason, err = router.AddTable(newTableTuple("nohash"))
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Contains(t, reason, "hash key")
}

func TestRouterRouteEventAndCache(t *testing.T) {
	usersKey := newTableTuple("users").ForKey()
	provider := &fakeTabletProvider{
		supported: true,
		layouts: map[string]*tgtdb.TableTabletLayout{
			usersKey: {Tablets: hashSharded6(), HashKeyColumns: []string{"id"}, HashKeyColumnTypes: []string{"bigint"}},
		},
		hashFn: idHashFn,
	}
	router := NewTabletRouter(provider)
	ok, _, err := router.AddTable(newTableTuple("users"))
	require.NoError(t, err)
	require.True(t, ok)

	ev := &tgtdb.Event{
		Vsn:          1,
		Op:           "c",
		TableNameTup: newTableTuple("users"),
		Key:          map[string]*string{"id": strPtr("8000")},
	}
	tabletID, err := router.RouteEvent(ev)
	require.NoError(t, err)
	assert.Equal(t, "t0", tabletID)

	// Second route of the same key should hit the cache (no extra ComputeHashCode call).
	callsBefore := provider.computeCalls
	tabletID, err = router.RouteEvent(ev)
	require.NoError(t, err)
	assert.Equal(t, "t0", tabletID)
	assert.Equal(t, callsBefore, provider.computeCalls, "expected hash cache hit")

	// A key in a different range routes to the correct tablet.
	ev2 := &tgtdb.Event{
		Vsn:          2,
		Op:           "c",
		TableNameTup: newTableTuple("users"),
		Key:          map[string]*string{"id": strPtr("60000")},
	}
	tabletID, err = router.RouteEvent(ev2)
	require.NoError(t, err)
	assert.Equal(t, "t5", tabletID)
}

func TestRouterRouteEventUnknownTable(t *testing.T) {
	provider := &fakeTabletProvider{supported: true, hashFn: idHashFn}
	router := NewTabletRouter(provider)
	ev := &tgtdb.Event{Vsn: 1, TableNameTup: newTableTuple("missing"), Key: map[string]*string{"id": strPtr("1")}}
	_, err := router.RouteEvent(ev)
	assert.Error(t, err)
}

func TestRouterRefreshDetectsSplit(t *testing.T) {
	usersKey := newTableTuple("users").ForKey()
	provider := &fakeTabletProvider{
		supported: true,
		layouts: map[string]*tgtdb.TableTabletLayout{
			usersKey: {
				Tablets:            []tgtdb.TabletInfo{{TabletID: "p0", StartHashCode: 0, EndHashCode: 65536}},
				HashKeyColumns:     []string{"id"},
				HashKeyColumnTypes: []string{"bigint"},
			},
		},
		hashFn: idHashFn,
	}
	router := NewTabletRouter(provider)
	ok, _, err := router.AddTable(newTableTuple("users"))
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, []string{"p0"}, router.CurrentTabletIDs(newTableTuple("users")))

	// Simulate a split by swapping the provider's layout, then refresh.
	provider.layouts[usersKey] = &tgtdb.TableTabletLayout{
		Tablets: []tgtdb.TabletInfo{
			{TabletID: "c0", StartHashCode: 0, EndHashCode: 32768},
			{TabletID: "c1", StartHashCode: 32768, EndHashCode: 65536},
		},
		HashKeyColumns:     []string{"id"},
		HashKeyColumnTypes: []string{"bigint"},
	}
	diffs, err := router.Refresh()
	require.NoError(t, err)
	require.Contains(t, diffs, usersKey)
	assert.Len(t, diffs[usersKey].AddedTablets, 2)
	assert.Len(t, diffs[usersKey].RemovedTablets, 1)
	assert.ElementsMatch(t, []string{"c0", "c1"}, router.CurrentTabletIDs(newTableTuple("users")))
}

func TestHashCacheKeyStableAndDistinct(t *testing.T) {
	k1 := hashCacheKey("public.users", []string{"id"}, map[string]*string{"id": strPtr("5")})
	k2 := hashCacheKey("public.users", []string{"id"}, map[string]*string{"id": strPtr("5")})
	k3 := hashCacheKey("public.users", []string{"id"}, map[string]*string{"id": strPtr("6")})
	assert.Equal(t, k1, k2)
	assert.NotEqual(t, k1, k3)

	// Multi-column hash keys must be order-sensitive/positional.
	kA := hashCacheKey("public.t", []string{"a", "b"}, map[string]*string{"a": strPtr("1"), "b": strPtr("2")})
	kB := hashCacheKey("public.t", []string{"a", "b"}, map[string]*string{"a": strPtr("2"), "b": strPtr("1")})
	assert.NotEqual(t, kA, kB)
}
