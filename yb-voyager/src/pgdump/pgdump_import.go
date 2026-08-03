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

// Package pgdump normalizes an existing PostgreSQL pg_dump backup (directory,
// custom, or plain-SQL format) into the tab-delimited TEXT data files that
// yb-voyager's `import data file` path already understands. It is used to
// populate a YugabyteDB target from a pre-existing backup without connecting to
// a live source database.
package pgdump

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"

	log "github.com/sirupsen/logrus"
)

// DumpFormat identifies the on-disk shape of a pg_dump backup.
type DumpFormat string

const (
	DumpFormatDirectory DumpFormat = "directory" // pg_dump -Fd (a directory containing toc.dat)
	DumpFormatCustom    DumpFormat = "custom"    // pg_dump -Fc (a single archive file starting with the PGDMP magic)
	DumpFormatPlain     DumpFormat = "plain"     // pg_dump -Fp (plain SQL script with COPY ... FROM stdin blocks)
)

// pgDumpMagic is the magic prefix present at the start of pg_dump custom-format
// archives and directory-format toc.dat files.
const pgDumpMagic = "PGDMP"

// TableData describes a single normalized data file produced for one COPY block
// found in the dump.
type TableData struct {
	// QualifiedName is the table name exactly as it appears in the COPY
	// statement (e.g. `public.foo` or `public."Foo"`).
	QualifiedName string
	// Columns is the ordered column list from the COPY statement. It may be
	// empty when the dump emitted a column-less `COPY table FROM stdin;`.
	Columns []string
	// FilePath is the absolute path to the normalized tab-delimited TEXT file.
	FilePath string
}

// ConversionResult is the output of Normalize.
type ConversionResult struct {
	Format DumpFormat
	// Tables has one entry per COPY block found in the dump. Multiple entries
	// can reference the same table (for example, partition leaves routed to a
	// root table via --load-via-partition-root).
	Tables []TableData
	// SetvalStatements holds the `SELECT ... setval(...)` statements found in
	// the dump, used to restore sequence last-values after data import.
	SetvalStatements []string
}

var (
	// reCopy matches the start of a COPY block, capturing everything between
	// `COPY ` and ` FROM stdin;` so the table name and optional column list can
	// be parsed separately.
	reCopy = regexp.MustCompile(`(?i)^COPY\s+(.*?)\s+FROM\s+stdin;\s*$`)
	// reSetval matches a sequence setval statement line.
	reSetval = regexp.MustCompile(`(?i)^\s*SELECT\s+.*setval\s*\(`)
)

// DetectFormat inspects the given path and determines the pg_dump format.
func DetectFormat(dumpPath string) (DumpFormat, error) {
	info, err := os.Stat(dumpPath)
	if err != nil {
		return "", fmt.Errorf("stat dump path %q: %w", dumpPath, err)
	}

	if info.IsDir() {
		tocPath := filepath.Join(dumpPath, "toc.dat")
		if _, err := os.Stat(tocPath); err != nil {
			return "", fmt.Errorf("directory %q does not look like a pg_dump directory-format backup (missing toc.dat): %w", dumpPath, err)
		}
		return DumpFormatDirectory, nil
	}

	hasMagic, err := fileHasMagic(dumpPath, pgDumpMagic)
	if err != nil {
		return "", err
	}
	if hasMagic {
		return DumpFormatCustom, nil
	}
	return DumpFormatPlain, nil
}

func fileHasMagic(path string, magic string) (bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return false, fmt.Errorf("open dump file %q: %w", path, err)
	}
	defer f.Close()

	buf := make([]byte, len(magic))
	n, err := io.ReadFull(f, buf)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		// File shorter than the magic; treat as plain.
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read dump file %q: %w", path, err)
	}
	return string(buf[:n]) == magic, nil
}

// Normalize converts the dump at dumpPath into tab-delimited TEXT data files
// written under outDir, returning the discovered tables and sequence setval
// statements. For directory and custom formats, pgRestorePath must point to a
// usable pg_restore binary; it is unused for plain-SQL dumps.
func Normalize(dumpPath string, outDir string, pgRestorePath string) (*ConversionResult, error) {
	format, err := DetectFormat(dumpPath)
	if err != nil {
		return nil, err
	}
	log.Infof("detected pg_dump format %q for %q", format, dumpPath)

	if err := os.MkdirAll(outDir, 0755); err != nil {
		return nil, fmt.Errorf("create normalized data dir %q: %w", outDir, err)
	}

	var plainSQLPath string
	switch format {
	case DumpFormatPlain:
		plainSQLPath = dumpPath
	case DumpFormatDirectory, DumpFormatCustom:
		if pgRestorePath == "" {
			return nil, fmt.Errorf("pg_restore is required to convert a %s-format dump but no path was provided", format)
		}
		plainSQLPath = filepath.Join(outDir, "pgdump_plain.sql")
		if err := restoreToPlainSQL(pgRestorePath, dumpPath, plainSQLPath); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported pg_dump format %q", format)
	}

	result, err := parsePlainSQLFile(plainSQLPath, outDir)
	if err != nil {
		return nil, err
	}
	result.Format = format
	return result, nil
}

// restoreToPlainSQL shells out to pg_restore to convert a directory/custom-format
// archive into a plain-SQL script containing COPY data blocks and setval
// statements only (data section).
func restoreToPlainSQL(pgRestorePath string, dumpPath string, outSQLPath string) error {
	args := []string{
		"--data-only",
		"--no-owner",
		"--no-privileges",
		"-f", outSQLPath,
		dumpPath,
	}
	cmd := exec.Command(pgRestorePath, args...)
	log.Infof("running command: %s", cmd.String())
	output, err := cmd.CombinedOutput()
	if len(output) > 0 {
		log.Infof("pg_restore output: %s", string(output))
	}
	if err != nil {
		return fmt.Errorf("pg_restore failed to convert dump %q to plain SQL: %w (output: %s)", dumpPath, err, strings.TrimSpace(string(output)))
	}
	return nil
}

// parsePlainSQLFile scans a plain-SQL dump, writing each COPY block's rows into
// its own tab-delimited TEXT file under outDir and collecting setval statements.
func parsePlainSQLFile(sqlPath string, outDir string) (*ConversionResult, error) {
	f, err := os.Open(sqlPath)
	if err != nil {
		return nil, fmt.Errorf("open plain SQL dump %q: %w", sqlPath, err)
	}
	defer f.Close()

	result := &ConversionResult{}
	reader := bufio.NewReader(f)
	fileIndex := 0

	for {
		line, readErr := reader.ReadString('\n')
		if line != "" {
			trimmed := strings.TrimRight(line, "\r\n")
			if matches := reCopy.FindStringSubmatch(trimmed); matches != nil {
				tableName, columns := parseCopySpec(matches[1])
				td, err := writeCopyBlock(reader, outDir, fileIndex, tableName, columns)
				if err != nil {
					return nil, err
				}
				result.Tables = append(result.Tables, *td)
				fileIndex++
			} else if reSetval.MatchString(trimmed) {
				result.SetvalStatements = append(result.SetvalStatements, strings.TrimSpace(trimmed))
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return nil, fmt.Errorf("read plain SQL dump %q: %w", sqlPath, readErr)
		}
	}
	return result, nil
}

// writeCopyBlock consumes rows from the reader (until the `\.` terminator) and
// writes them verbatim into a new TEXT file. The rows are already in PostgreSQL
// COPY text format (tab-delimited, `\N` for null), which is exactly what the
// import path expects.
func writeCopyBlock(reader *bufio.Reader, outDir string, fileIndex int, tableName string, columns []string) (*TableData, error) {
	fileName := fmt.Sprintf("data_%d.tsv", fileIndex)
	filePath := filepath.Join(outDir, fileName)
	out, err := os.Create(filePath)
	if err != nil {
		return nil, fmt.Errorf("create normalized data file %q: %w", filePath, err)
	}
	writer := bufio.NewWriter(out)

	closeErr := func() error {
		if flushErr := writer.Flush(); flushErr != nil {
			out.Close()
			return fmt.Errorf("flush normalized data file %q: %w", filePath, flushErr)
		}
		return out.Close()
	}

	for {
		line, readErr := reader.ReadString('\n')
		trimmed := strings.TrimRight(line, "\r\n")
		if trimmed == `\.` {
			if err := closeErr(); err != nil {
				return nil, err
			}
			break
		}
		if line != "" {
			if _, werr := writer.WriteString(line); werr != nil {
				out.Close()
				return nil, fmt.Errorf("write normalized data file %q: %w", filePath, werr)
			}
		}
		if readErr == io.EOF {
			// Malformed dump: COPY block not terminated by `\.`. Persist what we
			// have and stop; the caller-level error handling will surface any
			// downstream mismatch.
			if err := closeErr(); err != nil {
				return nil, err
			}
			log.Warnf("COPY block for table %q in dump ended without a %q terminator", tableName, `\.`)
			break
		}
		if readErr != nil {
			out.Close()
			return nil, fmt.Errorf("read COPY block for table %q: %w", tableName, readErr)
		}
	}

	return &TableData{
		QualifiedName: tableName,
		Columns:       columns,
		FilePath:      filePath,
	}, nil
}

// parseCopySpec splits the text between `COPY ` and ` FROM stdin;` into the
// qualified table name and its optional column list.
// Examples:
//
//	`public.foo (a, b)`   -> "public.foo", ["a", "b"]
//	`public."Foo"`        -> `public."Foo"`, nil
func parseCopySpec(spec string) (string, []string) {
	spec = strings.TrimSpace(spec)
	openIdx := strings.Index(spec, "(")
	if openIdx == -1 {
		return spec, nil
	}
	tableName := strings.TrimSpace(spec[:openIdx])
	closeIdx := strings.LastIndex(spec, ")")
	if closeIdx == -1 || closeIdx < openIdx {
		return tableName, nil
	}
	colsPart := spec[openIdx+1 : closeIdx]
	var columns []string
	for _, col := range strings.Split(colsPart, ",") {
		col = strings.TrimSpace(col)
		if col != "" {
			columns = append(columns, col)
		}
	}
	return tableName, columns
}
