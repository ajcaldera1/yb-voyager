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
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils"
)

var (
	metaDB                                     *MetaDB
	QUEUE_SEGMENT_META_TABLE_NAME              = "queue_segment_meta"
	EXPORTED_EVENTS_STATS_TABLE_NAME           = "exported_events_stats"
	EXPORTED_EVENTS_STATS_PER_TABLE_TABLE_NAME = "exported_events_stats_per_table"
	JSON_OBJECTS_TABLE_NAME                    = "json_objects"
)

const SQLITE_OPTIONS = "?_txlock=exclusive&_timeout=30000"

func getMetaDBPath(exportDir string) string {
	return filepath.Join(exportDir, "metainfo", "meta.db")
}

func createAndInitMetaDBIfRequired(exportDir string) error {
	metaDBPath := getMetaDBPath(exportDir)
	if utils.FileOrFolderExists(metaDBPath) {
		// already created and initied.
		return nil
	}
	err := createMetaDBFile(metaDBPath)
	if err != nil {
		return err
	}
	err = initMetaDB(metaDBPath)
	if err != nil {
		return err
	}
	return nil
}

func createMetaDBFile(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("not able to create meta db file :%w", err)
	}
	err = f.Close()
	if err != nil {
		return fmt.Errorf("error while closing meta db file: %w", err)
	}
	return nil
}

func initMetaDB(path string) error {
	conn, err := sql.Open("sqlite3", fmt.Sprintf("%s%s", path, SQLITE_OPTIONS))
	if err != nil {
		return fmt.Errorf("error while opening meta db :%w", err)
	}
	cmds := []string{

		fmt.Sprintf(`CREATE TABLE %s 
      (segment_no INTEGER PRIMARY KEY, 
       file_path TEXT, size_committed INTEGER, 
       imported_in_targetdb INTEGER DEFAULT 0, 
       imported_in_ffdb INTEGER DEFAULT 0, 
       archived INTEGER DEFAULT 0);`, QUEUE_SEGMENT_META_TABLE_NAME),
		fmt.Sprintf(`CREATE TABLE %s (
			run_id TEXT, 
			timestamp INTEGER, 
			num_total INTEGER, 
			num_inserts INTEGER, 
			num_updates INTEGER, 
			num_deletes INTEGER, 
			PRIMARY KEY(run_id, timestamp) );`, EXPORTED_EVENTS_STATS_TABLE_NAME),
		fmt.Sprintf(`CREATE TABLE %s (
			schema_name TEXT, 
			table_name TEXT, 
			num_total INTEGER, 
			num_inserts INTEGER, 
			num_updates INTEGER, 
			num_deletes INTEGER, 
			PRIMARY KEY(schema_name, table_name) );`, EXPORTED_EVENTS_STATS_PER_TABLE_TABLE_NAME),
		fmt.Sprintf(`CREATE TABLE %s (
			key TEXT PRIMARY KEY,
			json_text TEXT);`, JSON_OBJECTS_TABLE_NAME),
	}
	for _, cmd := range cmds {
		_, err = conn.Exec(cmd)
		if err != nil {
			return fmt.Errorf("error while initializating meta db with query-%s :%w", cmd, err)
		}
		log.Infof("Executed query on meta db - %s", cmd)
	}
	return nil
}

func truncateTablesInMetaDb(exportDir string, tableNames []string) error {
	conn, err := sql.Open("sqlite3", fmt.Sprintf("%s%s", getMetaDBPath(exportDir), SQLITE_OPTIONS))
	defer func() {
		err := conn.Close()
		if err != nil {
			log.Errorf("failed to close connection to meta db: %v", err)
		}
	}()
	if err != nil {
		return fmt.Errorf("error while opening meta db :%w", err)
	}
	for _, tableName := range tableNames {
		query := fmt.Sprintf(`DELETE FROM %s;`, tableName)
		_, err = conn.Exec(query)
		if err != nil {
			return fmt.Errorf("error while running query on meta db -%s :%w", query, err)
		}
		log.Infof("Executed query on meta db - %s", query)
	}
	return nil
}

// =====================================================================================================================

type MetaDB struct {
	db *sql.DB
}

func NewMetaDB(exportDir string) (*MetaDB, error) {
	db, err := sql.Open("sqlite3", fmt.Sprintf("%s%s", getMetaDBPath(exportDir), SQLITE_OPTIONS))
	if err != nil {
		return nil, fmt.Errorf("error while opening meta db :%w", err)
	}
	return &MetaDB{db: db}, nil
}

func (m *MetaDB) MarkEventQueueSegmentAsProcessed(segmentNum int64) error {
	var query string
	if importDestinationType == TARGET_DB {
		query = fmt.Sprintf(`UPDATE %s SET imported_in_targetdb = 1 WHERE segment_no = %d;`, QUEUE_SEGMENT_META_TABLE_NAME, segmentNum)
	} else if importDestinationType == FF_DB {
		query = fmt.Sprintf(`UPDATE %s SET imported_in_ffdb = 1 WHERE segment_no = %d;`, QUEUE_SEGMENT_META_TABLE_NAME, segmentNum)
	} else {
		return fmt.Errorf("invalid importer type: %s", importDestinationType)
	}

	result, err := m.db.Exec(query)
	if err != nil {
		return fmt.Errorf("error while running query on meta db -%s :%w", query, err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("error while getting rows updated -%s :%w", query, err)
	}

	if rowsAffected != 1 {
		return fmt.Errorf("expected 1 row to be updated, got %d", rowsAffected)
	}

	log.Infof("Executed query on meta db - %s", query)
	return nil
}

func (m *MetaDB) GetLastValidOffsetInSegmentFile(segmentNum int64) (int64, error) {
	query := fmt.Sprintf(`SELECT size_committed FROM %s WHERE segment_no = %d;`, QUEUE_SEGMENT_META_TABLE_NAME, segmentNum)
	row := m.db.QueryRow(query)
	var sizeCommitted int64
	err := row.Scan(&sizeCommitted)
	if err != nil {
		return -1, fmt.Errorf("error while running query on meta db - %s :%w", query, err)
	}
	return sizeCommitted, nil
}

func (m *MetaDB) GetTotalExportedEvents(runId string) (int64, int64, error) {
	var totalCount int64
	var totalCountRun int64

	query := fmt.Sprintf(`SELECT sum(num_total) from %s`, EXPORTED_EVENTS_STATS_TABLE_NAME)
	err := m.db.QueryRow(query).Scan(&totalCount)
	if err != nil {
		if !strings.Contains(err.Error(), "converting NULL to int64 is unsupported") {
			return 0, 0, fmt.Errorf("error while running query on meta db -%s :%w", query, err)
		}

	}

	query = fmt.Sprintf(`SELECT sum(num_total) from %s WHERE run_id = '%s'`, EXPORTED_EVENTS_STATS_TABLE_NAME, runId)
	err = m.db.QueryRow(query).Scan(&totalCountRun)
	if err != nil {
		if !strings.Contains(err.Error(), "converting NULL to int64 is unsupported") {
			return 0, 0, fmt.Errorf("error while running query on meta db -%s :%w", query, err)
		}
	}

	return totalCount, totalCountRun, nil
}

func (m *MetaDB) GetExportedEventsRateInLastNMinutes(runId string, n int) (int64, error) {
	var totalCount int64
	now := time.Now()
	startTimeStamp := now.Add(-time.Minute * time.Duration(n))
	query := fmt.Sprintf(`select sum(num_total) from %s WHERE run_id='%s' AND timestamp >= %d`,
		EXPORTED_EVENTS_STATS_TABLE_NAME, runId, startTimeStamp.Unix())
	err := m.db.QueryRow(query).Scan(&totalCount)
	if err != nil {
		if !strings.Contains(err.Error(), "converting NULL to int64 is unsupported") {
			return 0, fmt.Errorf("error while running query on meta db -%s :%w", query, err)
		}
	}
	return totalCount / int64(n*60), nil
}

func (m *MetaDB) InsertJsonObject(tx *sql.Tx, key string, obj any) error {
	jsonText, err := json.Marshal(obj)
	if err != nil {
		return fmt.Errorf("error while marshalling json: %w", err)
	}
	query := fmt.Sprintf(`INSERT INTO %s (key, json_text) VALUES (?, ?)`, JSON_OBJECTS_TABLE_NAME)
	if tx == nil {
		_, err = m.db.Exec(query, key, jsonText)
	} else {
		_, err = tx.Exec(query, key, jsonText)
	}
	if err != nil {
		return fmt.Errorf("error while running query on meta db - %s :%w", query, err)
	}
	return nil
}

func (m *MetaDB) GetJsonObject(tx *sql.Tx, key string, obj any) (bool, error) {
	query := fmt.Sprintf(`SELECT json_text FROM %s WHERE key = ?`, JSON_OBJECTS_TABLE_NAME)
	var row *sql.Row
	if tx == nil {
		row = m.db.QueryRow(query, key)
	} else {
		row = tx.QueryRow(query, key)
	}
	var jsonText string
	err := row.Scan(&jsonText)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, fmt.Errorf("error while running query on meta db - %s :%w", query, err)
	}
	err = json.Unmarshal([]byte(jsonText), obj)
	if err != nil {
		return true, fmt.Errorf("error while unmarshalling json: %w", err)
	}
	return true, nil
}

func UpdateJsonObjectInMetaDB[T any](m *MetaDB, key string, updateFn func(obj *T)) error {
	// Get a connection to the meta db.
	conn, err := m.db.Conn(context.Background())
	if err != nil {
		return fmt.Errorf("error while getting connection to meta db: %w", err)
	}
	defer func() {
		err := conn.Close()
		if err != nil {
			log.Errorf("failed to close connection to meta db: %v", err)
		}
	}()
	// Start a transaction.
	tx, err := conn.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return fmt.Errorf("error while starting transaction on meta db: %w", err)
	}
	defer func() {
		err := tx.Rollback()
		if err != nil {
			log.Errorf("failed to rollback transaction on meta db: %v", err)
		}
	}()
	// Get the json object.
	obj := new(T)
	found, err := m.GetJsonObject(tx, key, obj)
	if err != nil {
		return fmt.Errorf("error while getting json object from meta db: %w", err)
	}
	// Update the json object.
	updateFn(obj)
	// Update the json object in the meta db.
	newJsonText, err := json.Marshal(obj)
	if err != nil {
		return fmt.Errorf("error while marshalling json: %w", err)
	}
	if !found {
		err = m.InsertJsonObject(tx, key, obj)
		if err != nil {
			return fmt.Errorf("error while inserting json object into meta db: %w", err)
		}
	} else {
		query := fmt.Sprintf(`UPDATE %s SET json_text = ? WHERE key = ?`, JSON_OBJECTS_TABLE_NAME)
		_, err = tx.Exec(query, string(newJsonText), key)
		if err != nil {
			return fmt.Errorf("error while running query on meta db - %s :%w", query, err)
		}
	}
	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("error while commiting transaction on meta db: %w", err)
	}
	return nil
}

func (m *MetaDB) GetSegmentNumToResume() (int64, error) {
	query := fmt.Sprintf(`SELECT MIN(segment_no) FROM %s WHERE imported_in_%sdb = 0;`, QUEUE_SEGMENT_META_TABLE_NAME, importDestinationType)
	row := m.db.QueryRow(query)
	var segmentNum int64
	err := row.Scan(&segmentNum)
	if err != nil {
		return -1, fmt.Errorf("error while running query on meta db - %s : %w", query, err)
	}
	return segmentNum, nil
}

func (m *MetaDB) GetExportedEventsStatsForTable(schemaName string, tableName string) (int64, int64, int64, int64, error) {
	var totalCount int64
	var inserts int64
	var updates int64
	var deletes int64
	query := fmt.Sprintf(`select num_total, num_inserts, num_updates, num_deletes from %s WHERE schema_name='%s' AND table_name='%s'`,
		EXPORTED_EVENTS_STATS_PER_TABLE_TABLE_NAME, schemaName, tableName)

	err := m.db.QueryRow(query).Scan(&totalCount, &inserts, &updates, &deletes)
	if err != nil {
		return -1, -1, -1, -1, fmt.Errorf("error while running query on meta db -%s :%w", query, err)
	}
	return totalCount, inserts, updates, deletes, nil
}
