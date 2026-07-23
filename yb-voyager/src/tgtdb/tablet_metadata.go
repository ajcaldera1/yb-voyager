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
	"database/sql"
	"fmt"
	"strings"

	goerrors "github.com/go-errors/errors"
	log "github.com/sirupsen/logrus"

	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils/sqlname"
)

// YB_HASH_CODE_MAX is the exclusive upper bound of the DocDB hash space.
// yb_hash_code() returns a value in [0, YB_HASH_CODE_MAX). Tablet hash ranges
// use [start_hash_code, end_hash_code) with end capped at YB_HASH_CODE_MAX.
const YB_HASH_CODE_MAX = 65536

// TabletInfo describes a single tablet's hash range and leader for a hash-sharded table.
// StartHashCode is inclusive, EndHashCode is exclusive.
type TabletInfo struct {
	TabletID      string
	StartHashCode int
	EndHashCode   int
	Leader        string
}

// TableTabletLayout bundles everything the CDC router needs to route an event
// of a table to a specific tablet worker: the current set of tablets (hash
// ranges) and the ordered hash-key columns (+ their types) used to compute
// yb_hash_code for a row.
type TableTabletLayout struct {
	Tablets            []TabletInfo
	HashKeyColumns     []string // ordered as they appear in the hash part of the primary key
	HashKeyColumnTypes []string // parallel to HashKeyColumns (format_type output)
	// RangeSharded is true when the table is range-sharded (tablets have NULL
	// hash codes) and therefore cannot be routed by yb_hash_code.
	RangeSharded bool
}

// IsTabletMetadataSupported reports whether the target cluster exposes the
// yb_tablet_metadata view (Early Access, ~2025.2+). Tablet-affine CDC apply
// requires it; callers must fall back to pk/table partitioning otherwise.
func (yb *TargetYugabyteDB) IsTabletMetadataSupported() bool {
	query := "SELECT 1 FROM pg_class WHERE relname='yb_tablet_metadata'"
	return yb.isQueryResultNonEmpty(query)
}

// GetTableTabletLayout returns the current tablet layout for a hash-sharded
// table. It returns RangeSharded=true (and no tablets) for range-sharded
// tables, which the caller must treat as ineligible for tablet affinity.
func (yb *TargetYugabyteDB) GetTableTabletLayout(table sqlname.NameTuple) (*TableTabletLayout, error) {
	tablets, rangeSharded, err := yb.getTabletsForTable(table)
	if err != nil {
		return nil, fmt.Errorf("get tablets for table %s: %w", table.ForKey(), err)
	}
	layout := &TableTabletLayout{
		Tablets:      tablets,
		RangeSharded: rangeSharded,
	}
	if rangeSharded {
		return layout, nil
	}

	hashCols, hashColTypes, err := yb.getHashKeyColumns(table)
	if err != nil {
		return nil, fmt.Errorf("get hash key columns for table %s: %w", table.ForKey(), err)
	}
	layout.HashKeyColumns = hashCols
	layout.HashKeyColumnTypes = hashColTypes
	return layout, nil
}

// getTabletsForTable polls yb_tablet_metadata for the given table.
// The second return value is true when the table is range-sharded (NULL hash codes).
func (yb *TargetYugabyteDB) getTabletsForTable(table sqlname.NameTuple) ([]TabletInfo, bool, error) {
	query := fmt.Sprintf(`SELECT tablet_id, start_hash_code, end_hash_code, COALESCE(leader, '')
		FROM yb_tablet_metadata
		WHERE oid = '%s'::regclass
		ORDER BY start_hash_code`, table.ForUserQuery())

	rows, err := yb.Query(query)
	if err != nil {
		return nil, false, fmt.Errorf("querying yb_tablet_metadata: %w", err)
	}
	defer rows.Close()

	var tablets []TabletInfo
	for rows.Next() {
		var tabletID, leader string
		var startHash, endHash sql.NullInt64
		if err := rows.Scan(&tabletID, &startHash, &endHash, &leader); err != nil {
			return nil, false, fmt.Errorf("scanning yb_tablet_metadata row: %w", err)
		}
		// NULL hash codes => range-sharded table; not routable by hash.
		if !startHash.Valid || !endHash.Valid {
			return nil, true, nil
		}
		tablets = append(tablets, TabletInfo{
			TabletID:      tabletID,
			StartHashCode: int(startHash.Int64),
			EndHashCode:   int(endHash.Int64),
			Leader:        leader,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("iterating yb_tablet_metadata rows: %w", err)
	}
	return tablets, false, nil
}

// getHashKeyColumns returns the ordered hash-key columns of the table's primary
// key along with their SQL types. The first num_hash_key_columns primary-key
// columns (in index order) form the hash key in YugabyteDB.
func (yb *TargetYugabyteDB) getHashKeyColumns(table sqlname.NameTuple) ([]string, []string, error) {
	numHashKeyCols, err := yb.getNumHashKeyColumns(table)
	if err != nil {
		return nil, nil, fmt.Errorf("get num hash key columns: %w", err)
	}
	if numHashKeyCols == 0 {
		// No hash key (range-sharded or single-tablet); not routable by hash.
		return nil, nil, nil
	}

	schemaName, tableName := table.ForCatalogQuery()
	query := fmt.Sprintf(`
		SELECT a.attname, format_type(a.atttypid, a.atttypmod)
		FROM pg_index i
		JOIN pg_class c ON c.oid = i.indrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		JOIN unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) ON true
		JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = k.attnum
		WHERE n.nspname = '%s' AND c.relname = '%s' AND i.indisprimary
		ORDER BY k.ord`, schemaName, tableName)

	rows, err := yb.Query(query)
	if err != nil {
		return nil, nil, fmt.Errorf("querying primary key columns: %w", err)
	}
	defer rows.Close()

	var cols, colTypes []string
	for rows.Next() {
		var col, colType string
		if err := rows.Scan(&col, &colType); err != nil {
			return nil, nil, fmt.Errorf("scanning pk column row: %w", err)
		}
		cols = append(cols, col)
		colTypes = append(colTypes, colType)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("iterating pk column rows: %w", err)
	}

	if numHashKeyCols > len(cols) {
		return nil, nil, goerrors.Errorf("num_hash_key_columns (%d) exceeds primary key column count (%d) for table %s",
			numHashKeyCols, len(cols), table.ForKey())
	}
	return cols[:numHashKeyCols], colTypes[:numHashKeyCols], nil
}

func (yb *TargetYugabyteDB) getNumHashKeyColumns(table sqlname.NameTuple) (int, error) {
	query := fmt.Sprintf("SELECT num_hash_key_columns FROM yb_table_properties('%s'::regclass)", table.ForUserQuery())
	var numHashKeyCols int
	err := yb.QueryRow(query).Scan(&numHashKeyCols)
	if err != nil {
		return 0, fmt.Errorf("querying num_hash_key_columns: %w", err)
	}
	return numHashKeyCols, nil
}

// ComputeHashCode computes yb_hash_code() for the hash-key portion of an event's
// key, using the target database so that the result matches DocDB's own hashing.
// Values are passed as text parameters cast to the hash columns' declared types.
func (yb *TargetYugabyteDB) ComputeHashCode(layout *TableTabletLayout, key map[string]*string) (int, error) {
	if layout == nil || len(layout.HashKeyColumns) == 0 {
		return 0, goerrors.Errorf("cannot compute hash code: table has no hash key columns")
	}

	castExprs := make([]string, 0, len(layout.HashKeyColumns))
	params := make([]interface{}, 0, len(layout.HashKeyColumns))
	for i, col := range layout.HashKeyColumns {
		val, ok := key[col]
		if !ok {
			return 0, goerrors.Errorf("hash key column %q not present in event key", col)
		}
		castExprs = append(castExprs, fmt.Sprintf("$%d::%s", i+1, layout.HashKeyColumnTypes[i]))
		if val == nil {
			params = append(params, nil)
		} else {
			params = append(params, *val)
		}
	}

	query := fmt.Sprintf("SELECT yb_hash_code(%s)", strings.Join(castExprs, ", "))
	var hashCode int
	// Uses the shared query connection; ComputeHashCode is called from the single
	// streaming dispatcher goroutine, so no additional locking is required here.
	err := yb.db.QueryRow(query, params...).Scan(&hashCode)
	if err != nil {
		return 0, fmt.Errorf("computing yb_hash_code via [%s]: %w", query, err)
	}
	log.Tracef("computed yb_hash_code=%d for hash columns %v", hashCode, layout.HashKeyColumns)
	return hashCode, nil
}
