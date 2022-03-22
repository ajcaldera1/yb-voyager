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
	"os"
	"os/exec"
	"regexp"
	"strings"
	"time"

	"github.com/yugabyte/ybm/yb_migrate/src/migration"
	"github.com/yugabyte/ybm/yb_migrate/src/utils"

	"github.com/fatih/color"
	"github.com/spf13/cobra"
)

var exportDataCmd = &cobra.Command{
	Use:   "data",
	Short: "This command is used to export table's data from source database to *.sql files",
	Long:  ``,

	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		cmd.Parent().PersistentPreRun(cmd.Parent(), args)
	},

	Run: func(cmd *cobra.Command, args []string) {
		exportData()
	},
}

func init() {
	exportCmd.AddCommand(exportDataCmd)
}

func exportData() {
	fmt.Printf("export of data for source type as '%s'\n", source.DBType)
	exportDoneFlagPath := exportDir + "/metainfo/flags/exportDataDone"
	exportDataDirPath := exportDir + "/data"
	if startClean {
		//remove flag before start & clean existing data/tables
		os.Remove(exportDoneFlagPath)
		utils.CleanDir(exportDataDirPath)
	} else {
		if utils.FileOrFolderExists(exportDoneFlagPath) || !utils.IsDirectoryEmpty(exportDataDirPath) {
			fmt.Println("export already done or table data files already exists, use --start-clean flag to clean data and start again")
			os.Exit(0)
		}
	}

	var success bool
	if migrationMode == "offline" {
		success = exportDataOffline()
	} else {
		success = exportDataOnline()
	}

	if success {
		err := exec.Command("touch", exportDir+"/metainfo/flags/exportDataDone").Run() //to inform import data command
		utils.CheckError(err, "", "couldn't touch file exportDataDone in metainfo/flags folder", true)
		color.Green("Export of data complete \u2705")
	} else {
		color.Red("Export of data failed, retry!! \u274C")
	}
}

func exportDataOffline() bool {
	utils.CheckToolsRequiredInstalledOrNot(&source)

	migration.CheckSourceDBAccessibility(&source)

	utils.CreateMigrationProjectIfNotExists(&source, exportDir)

	ctx, cancel := context.WithCancel(context.Background())
	// defer cancel()

	var tableList []string
	if source.TableList != "" {
		userTableList := strings.Split(source.TableList, ",")

		if source.DBType == POSTGRESQL {
			// in postgres format should be schema.table, public is default and other parts of code assume schema.table format
			for _, table := range userTableList {
				parts := strings.Split(table, ".")
				if len(parts) == 1 {
					tableList = append(tableList, "public."+table)
				} else if len(parts) == 2 {
					tableList = append(tableList, table)
				} else {
					fmt.Println("invalid value for --table-list flag")
					os.Exit(1)
				}
			}
		} else {
			tableList = userTableList
		}

		if source.VerboseMode {
			fmt.Printf("table list flag values: %v\n", tableList)
		}
	} else {
		if source.DBType == "mysql" {
			tableList = migration.MySQLGetAllTableNames(&source)
		} else {
			tableList = utils.GetObjectNameListFromReport(generateReportHelper(), "TABLE")
		}
		fmt.Printf("Num tables to export: %d\n", len(tableList))
		fmt.Printf("table list for data export: %v\n", tableList)
	}
	if len(tableList) == 0 {
		fmt.Println("no tables present to export, exiting...")
		os.Exit(0)
	}

	exportDataStart := make(chan bool)
	quitChan := make(chan bool) //for checking failure/errors of the parallel goroutines
	go func() {
		q := <-quitChan
		if q {
			fmt.Println("cancel(), quitchan, main exportDataOffline()")
			cancel()                    //will cancel/stop both dump tool and progress bar
			time.Sleep(time.Second * 5) //give sometime for the cancel to complete before this function returns
			fmt.Println("Cancelled the context having dump command and progress bar")
		}
	}()

	tablesMetadata := initializeExportTableMetadataSlice(tableList)
	// fmt.Printf("tablesMetadata: %+v\n", tablesMetadata)
	migration.UpdateTableRowCount(&source, exportDir, tablesMetadata)

	switch source.DBType {
	case ORACLE:
		fmt.Printf("Preparing for data export from Oracle\n")
		utils.WaitGroup.Add(1)
		go migration.Ora2PgExportDataOffline(ctx, &source, exportDir, tableList, quitChan, exportDataStart)

	case POSTGRESQL:
		fmt.Printf("Preparing for data export from Postgres\n")
		utils.WaitGroup.Add(1)

		//need to export setval() calls to resume sequence value generation
		sequenceList := utils.GetObjectNameListFromReport(generateReportHelper(), "SEQUENCE")
		tableList = append(tableList, sequenceList...)

		go migration.PgDumpExportDataOffline(ctx, &source, exportDir, tableList, quitChan, exportDataStart)

	case MYSQL:
		fmt.Printf("Preparing for data export from MySQL\n")
		utils.WaitGroup.Add(1)
		go migration.Ora2PgExportDataOffline(ctx, &source, exportDir, tableList, quitChan, exportDataStart)

	}

	//wait for the export data to start
	// fmt.Println("passing the exportDataStart channel receiver")
	<-exportDataStart
	// fmt.Println("passed the exportDataStart channel receiver")

	migration.UpdateFilePaths(&source, exportDir, tablesMetadata)

	exportDataStatus(ctx, tablesMetadata, quitChan)

	utils.WaitGroup.Wait() //waiting for the dump to complete

	if ctx.Err() != nil {
		fmt.Printf("ctx error(exportData.go): %v\n", ctx.Err())
		return false
	}

	migration.ExportDataPostProcessing(exportDir, &tablesMetadata)

	return true
}

func exportDataOnline() bool {
	return true // empty function
}

func initializeExportTableMetadataSlice(tableList []string) []utils.TableProgressMetadata {
	numTables := len(tableList)
	tablesMetadata := make([]utils.TableProgressMetadata, numTables)

	for i := 0; i < numTables; i++ {
		tableInfo := strings.Split(tableList[i], ".")
		if source.DBType == POSTGRESQL { //format for every table: schema.tablename
			tablesMetadata[i].TableSchema = tableInfo[0]
			tablesMetadata[i].TableName = tableInfo[len(tableInfo)-1] //tableInfo[1]
			tablesMetadata[i].FullTableName = tablesMetadata[i].TableSchema + "." + tablesMetadata[i].TableName
		} else { //no schema.tablename format required for oracle & mysql
			tablesMetadata[i].TableName = tableInfo[len(tableInfo)-1] //tableInfo[0]
			tablesMetadata[i].FullTableName = tablesMetadata[i].TableName
		}

		//Initializing all the members of struct
		tablesMetadata[i].InProgressFilePath = ""
		tablesMetadata[i].FinalFilePath = ""        //file paths will be updated when status changes to IN-PROGRESS by other func
		tablesMetadata[i].CountTotalRows = int64(0) //will be updated by other func
		tablesMetadata[i].CountLiveRows = int64(0)
		tablesMetadata[i].Status = 0
		tablesMetadata[i].FileOffsetToContinue = int64(0)
	}

	return tablesMetadata
}

func checkTableListFlag() {
	tableList := strings.Split(source.TableList, ",")
	//TODO: update regexp once table name with double quotes are allowed/supported
	tableNameRegex := regexp.MustCompile("[a-zA-Z0-9_.]+")

	for _, table := range tableList {
		if !tableNameRegex.MatchString(table) {
			fmt.Printf("invalid table name '%s' with --table-list flag\n", table)
			os.Exit(1)
		}
	}
}
