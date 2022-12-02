/*
Copyright (c) YugaByte, Inc.

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
	"strings"

	"github.com/jackc/pgx/v4"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/srcdb"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils"
	"golang.org/x/exp/slices"

	"github.com/yugabyte/yb-voyager/yb-voyager/src/callhome"
)

var importSchemaCmd = &cobra.Command{
	Use:   "schema",
	Short: "This command imports schema into the destination YugabyteDB database",
	Long:  ``,

	PreRun: func(cmd *cobra.Command, args []string) {
		validateImportFlags(cmd)
	},

	Run: func(cmd *cobra.Command, args []string) {
		target.ImportMode = true
		sourceDBType = ExtractMetaInfo(exportDir).SourceDBType
		importSchema()
	},
}

func init() {
	importCmd.AddCommand(importSchemaCmd)
	registerCommonGlobalFlags(importSchemaCmd)
	registerCommonImportFlags(importSchemaCmd)
	registerImportSchemaFlags(importSchemaCmd)
	target.Schema = strings.ToLower(target.Schema)
}

var flagPostImportData bool
var importObjectsInStraightOrder bool

func importSchema() {
	err := target.DB().Connect()
	if err != nil {
		utils.ErrExit("Failed to connect to target YB cluster: %s", err)
	}
	conn := target.DB().Conn()
	targetDBVersion := target.DB().GetVersion()
	utils.PrintAndLog("YugabyteDB version: %s\n", targetDBVersion)

	payload := callhome.GetPayload(exportDir)
	payload.TargetDBVersion = targetDBVersion

	if !flagPostImportData {
		createTargetSchemas(conn)
	}
	var objectList []string

	objectsToImportAfterData := []string{"INDEX", "FTS_INDEX", "PARTITION_INDEX", "TRIGGER"}
	if !flagPostImportData { // Pre data load.
		// This list also has defined the order to create object type in target YugabyteDB.
		objectList = utils.GetSchemaObjectList(sourceDBType)
		objectList = utils.SetDifference(objectList, objectsToImportAfterData)
		if len(objectList) == 0 {
			utils.ErrExit("No schema objects to import! Must import at least 1 of the supported schema object types: %v", utils.GetSchemaObjectList(sourceDBType))
		}
	} else { // Post data load.
		objectList = objectsToImportAfterData
	}
	objectList = applySchemaObjectFilterFlags(objectList)
	log.Infof("List of schema objects to import: %v", objectList)

	// Import some statements only after importing everything else
	isSkipStatement := func(objType, stmt string) bool {
		stmt = strings.ToUpper(strings.TrimSpace(stmt))
		switch objType {
		case "SEQUENCE":
			// ALTER TABLE table_name ALTER COLUMN column_name ... ('sequence_name');
			return strings.HasPrefix(stmt, "ALTER TABLE")
		case "TABLE":
			// skips the ALTER TABLE table_name ADD CONSTRAINT constraint_name FOREIGN KEY (column_name) REFERENCES another_table_name(another_column_name);
			return strings.Contains(stmt, "ALTER TABLE") && strings.Contains(stmt, "FOREIGN KEY")
		case "UNIQUE INDEX":
			// skips all the INDEX DDLs, Except CREATE UNIQUE INDEX index_name ON table ... (column_name);
			return !strings.Contains(stmt, objType)
		case "INDEX":
			// skips all the CREATE UNIQUE INDEX index_name ON table ... (column_name);
			return strings.Contains(stmt, "UNIQUE INDEX")
		}
		return false
	}
	skipFn := isSkipStatement
	importSchemaInternal(exportDir, objectList, skipFn)
	fmt.Printf("\nImporting deferred DDL statements.\n\n")
	// Import the skipped ALTER TABLE statements from sequence.sql and table.sql if it exists
	skipFn = func(objType, stmt string) bool {
		return !isSkipStatement(objType, stmt)
	}
	if slices.Contains(objectList, "SEQUENCE") {
		importSchemaInternal(exportDir, []string{"SEQUENCE"}, skipFn)
	}
	if slices.Contains(objectList, "TABLE") {
		importSchemaInternal(exportDir, []string{"TABLE"}, skipFn)
	}

	log.Info("Schema import is complete.")
	callhome.PackAndSendPayload(exportDir)
}

func createTargetSchemas(conn *pgx.Conn) {
	var targetSchemas []string
	target.Schema = strings.ToLower(strings.Trim(target.Schema, "\"")) //trim case sensitivity quotes if needed, convert to lowercase
	switch sourceDBType {
	case "postgresql": // in case of postgreSQL as source, there can be multiple schemas present in a database
		source = srcdb.Source{DBType: sourceDBType}
		targetSchemas = utils.GetObjectNameListFromReport(analyzeSchemaInternal(), "SCHEMA")
	case "oracle": // ORACLE PACKAGEs are exported as SCHEMAs
		source = srcdb.Source{DBType: sourceDBType}
		targetSchemas = append(targetSchemas, target.Schema)
		targetSchemas = append(targetSchemas, utils.GetObjectNameListFromReport(analyzeSchemaInternal(), "PACKAGE")...)
	case "mysql":
		source = srcdb.Source{DBType: sourceDBType}
		targetSchemas = append(targetSchemas, target.Schema)

	}
	targetSchemas = utils.ToCaseInsensitiveNames(targetSchemas)

	utils.PrintAndLog("schemas to be present in target database %q: %v\n", target.DBName, targetSchemas)

	for _, targetSchema := range targetSchemas {
		//check if target schema exists or not
		schemaExists := checkIfTargetSchemaExists(conn, targetSchema)
		dropSchemaQuery := fmt.Sprintf("DROP SCHEMA %s CASCADE", targetSchema)

		if schemaExists {
			if startClean {
				promptMsg := fmt.Sprintf("do you really want to drop the '%s' schema", targetSchema)
				if !utils.AskPrompt(promptMsg) {
					continue
				}

				utils.PrintAndLog("dropping schema '%s' in target database", targetSchema)
				_, err := conn.Exec(context.Background(), dropSchemaQuery)
				if err != nil {
					utils.ErrExit("Failed to drop schema %q: %s", targetSchema, err)
				}
			} else {
				utils.PrintAndLog("schema '%s' already present in target database, continuing with it..\n", targetSchema)
			}
		}
	}

	if sourceDBType != POSTGRESQL { // with the new schema list flag, pg_dump takes care of all schema creation DDLs
		schemaExists := checkIfTargetSchemaExists(conn, target.Schema)
		createSchemaQuery := fmt.Sprintf("CREATE SCHEMA %s", target.Schema)
		/* --target-db-schema(or target.Schema) flag valid for Oracle & MySQL
		only create target.Schema, other required schemas are created via .sql files */
		if !schemaExists {
			utils.PrintAndLog("creating schema '%s' in target database...", target.Schema)
			_, err := conn.Exec(context.Background(), createSchemaQuery)
			if err != nil {
				utils.ErrExit("Failed to create %q schema in the target DB: %s", target.Schema, err)
			}
		}

		if target.Schema == YUGABYTEDB_DEFAULT_SCHEMA &&
			!utils.AskPrompt("do you really want to import into 'public' schema") {
			utils.ErrExit("User selected not to import in the `public` schema. Exiting.")
		}
	}
}

func checkIfTargetSchemaExists(conn *pgx.Conn, targetSchema string) bool {
	checkSchemaExistQuery := fmt.Sprintf("SELECT schema_name FROM information_schema.schemata WHERE schema_name = '%s'", targetSchema)

	var fetchedSchema string
	err := conn.QueryRow(context.Background(), checkSchemaExistQuery).Scan(&fetchedSchema)
	log.Infof("check if schema %q exists: fetchedSchema: %q, err: %s", targetSchema, fetchedSchema, err)
	if err != nil && (strings.Contains(err.Error(), "no rows in result set") && fetchedSchema == "") {
		return false
	} else if err != nil {
		utils.ErrExit("Failed to check if schema %q exists: %s", targetSchema, err)
	}

	return fetchedSchema == targetSchema
}

func isAlreadyExists(errString string) bool {
	alreadyExistsErrors := []string{"already exists",
		"multiple primary keys",
		"already a partition"}
	for _, subStr := range alreadyExistsErrors {
		if strings.Contains(errString, subStr) {
			return true
		}
	}
	return false
}
