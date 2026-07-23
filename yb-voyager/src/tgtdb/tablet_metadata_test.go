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
package tgtdb

import (
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils/sqlname"
)

func tabletTableTuple(name string) sqlname.NameTuple {
	oname := sqlname.NewObjectName(YUGABYTEDB, "public", "public", name)
	return sqlname.NameTuple{CurrentName: oname, TargetName: oname}
}

func mkEvent(vsn int64, op string) *Event {
	return &Event{
		Vsn:          vsn,
		Op:           op,
		TableNameTup: tabletTableTuple("users"),
		Key:          map[string]*string{"id": strPtrT("1")},
		Fields:       map[string]*string{"id": strPtrT("1")},
	}
}

func strPtrT(s string) *string { return &s }

func TestNewTabletEventBatchIdentity(t *testing.T) {
	batch := NewTabletEventBatch([]*Event{mkEvent(10, "c"), mkEvent(11, "u")}, "public.users", "tabletABC")
	assert.True(t, batch.IsTabletBatch())
	assert.NotNil(t, batch.TabletWorker)
	assert.Equal(t, "public.users", batch.TabletWorker.TableName)
	assert.Equal(t, "tabletABC", batch.TabletWorker.TabletID)
	assert.Equal(t, int64(11), batch.GetLastVsn())

	// A plain channel batch must not look like a tablet batch.
	chanBatch := NewEventBatch([]*Event{mkEvent(1, "c")}, 3)
	assert.False(t, chanBatch.IsTabletBatch())
	assert.Nil(t, chanBatch.TabletWorker)
}

func TestGetTabletWorkerMetadataUpdateQuery(t *testing.T) {
	migrationUUID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	batch := NewTabletEventBatch([]*Event{
		mkEvent(100, "c"),
		mkEvent(101, "u"),
		mkEvent(102, "d"),
	}, "public.users", "tabletXYZ")

	query := batch.GetTabletWorkerMetadataUpdateQuery(migrationUUID)

	assert.Contains(t, query, TABLET_WORKERS_METADATA_TABLE_NAME)
	assert.Contains(t, query, "last_applied_vsn=102")
	assert.Contains(t, query, "total_events = total_events + 3")
	assert.Contains(t, query, "num_inserts = num_inserts + 1")
	assert.Contains(t, query, "num_updates = num_updates + 1")
	assert.Contains(t, query, "num_deletes = num_deletes + 1")
	assert.Contains(t, query, "table_name='public.users'")
	assert.Contains(t, query, "tablet_id='tabletXYZ'")
	assert.Contains(t, query, migrationUUID.String())
	// Must not touch the channel metadata table.
	assert.False(t, strings.Contains(query, "channel_no"))
}
