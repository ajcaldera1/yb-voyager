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
	"fmt"
	"strings"
)

type Event struct {
	Op         string            `json:"op"`
	SchemaName string            `json:"schema_name"`
	TableName  string            `json:"table_name"`
	Key        map[string]string `json:"key"`
	Fields     map[string]string `json:"fields"`
}

func (e *Event) GetSQLStmt(targetSchema string) string {
	switch e.Op {
	case "c":
		return e.getInsertStmt(targetSchema)
	case "u":
		return e.getUpdateStmt(targetSchema)
	case "d":
		return e.getDeleteStmt(targetSchema)
	default:
		panic("unknown op: " + e.Op)
	}
}

const insertTemplate = "INSERT INTO %s (%s) VALUES (%s);"
const updateTemplate = "UPDATE %s SET %s WHERE %s;"
const deleteTemplate = "DELETE FROM %s WHERE %s;"

func (event *Event) getInsertStmt(targetSchema string) string {
	tableName := event.SchemaName + "." + event.TableName
	if targetSchema != "" {
		tableName = targetSchema + "." + event.TableName
	}
	columnList := make([]string, 0, len(event.Fields))
	valueList := make([]string, 0, len(event.Fields))
	for column, value := range event.Fields {
		columnList = append(columnList, column)
		valueList = append(valueList, value)
	}
	columns := strings.Join(columnList, ", ")
	values := strings.Join(valueList, ", ")
	stmt := fmt.Sprintf(insertTemplate, tableName, columns, values)
	return stmt
}

func (event *Event) getUpdateStmt(targetSchema string) string {
	tableName := event.SchemaName + "." + event.TableName
	if targetSchema != "" {
		tableName = targetSchema + "." + event.TableName
	}
	var setClauses []string
	for column, value := range event.Fields {
		setClauses = append(setClauses, fmt.Sprintf("%s = %s", column, value))
	}
	setClause := strings.Join(setClauses, ", ")
	var whereClauses []string
	for column, value := range event.Key {
		whereClauses = append(whereClauses, fmt.Sprintf("%s = %s", column, value))
	}
	whereClause := strings.Join(whereClauses, " AND ")
	return fmt.Sprintf(updateTemplate, tableName, setClause, whereClause)
}

func (event *Event) getDeleteStmt(targetSchema string) string {
	tableName := event.SchemaName + "." + event.TableName
	if targetSchema != "" {
		tableName = targetSchema + "." + event.TableName
	}
	var whereClauses []string
	for column, value := range event.Key {
		whereClauses = append(whereClauses, fmt.Sprintf("%s = %s", column, value))
	}
	whereClause := strings.Join(whereClauses, " AND ")
	return fmt.Sprintf(deleteTemplate, tableName, whereClause)
}
