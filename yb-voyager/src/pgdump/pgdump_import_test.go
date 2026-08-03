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
package pgdump

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseCopySpec(t *testing.T) {
	tests := []struct {
		name        string
		spec        string
		wantTable   string
		wantColumns []string
	}{
		{
			name:        "qualified table with columns",
			spec:        "public.foo (a, b, c)",
			wantTable:   "public.foo",
			wantColumns: []string{"a", "b", "c"},
		},
		{
			name:        "quoted table without columns",
			spec:        `public."Foo"`,
			wantTable:   `public."Foo"`,
			wantColumns: nil,
		},
		{
			name:        "quoted columns preserved",
			spec:        `public.orders ("Id", "order date")`,
			wantTable:   "public.orders",
			wantColumns: []string{`"Id"`, `"order date"`},
		},
		{
			name:        "extra whitespace",
			spec:        "  schema1.tbl   (  x ,  y )  ",
			wantTable:   "schema1.tbl",
			wantColumns: []string{"x", "y"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			table, columns := parseCopySpec(tt.spec)
			assert.Equal(t, tt.wantTable, table)
			assert.Equal(t, tt.wantColumns, columns)
		})
	}
}

func TestDetectFormat(t *testing.T) {
	tmp := t.TempDir()

	// Directory format: a directory containing toc.dat.
	dirDump := filepath.Join(tmp, "dirdump")
	require.NoError(t, os.MkdirAll(dirDump, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(dirDump, "toc.dat"), []byte(pgDumpMagic+"\x01\x00"), 0644))

	format, err := DetectFormat(dirDump)
	require.NoError(t, err)
	assert.Equal(t, DumpFormatDirectory, format)

	// Directory without toc.dat should error.
	badDir := filepath.Join(tmp, "baddir")
	require.NoError(t, os.MkdirAll(badDir, 0755))
	_, err = DetectFormat(badDir)
	assert.Error(t, err)

	// Custom format: a file starting with the PGDMP magic.
	customDump := filepath.Join(tmp, "custom.dump")
	require.NoError(t, os.WriteFile(customDump, []byte(pgDumpMagic+"\x01\x0e\x00"), 0644))
	format, err = DetectFormat(customDump)
	require.NoError(t, err)
	assert.Equal(t, DumpFormatCustom, format)

	// Plain format: a text SQL file.
	plainDump := filepath.Join(tmp, "plain.sql")
	require.NoError(t, os.WriteFile(plainDump, []byte("--\n-- PostgreSQL database dump\n--\nCOPY public.foo (a) FROM stdin;\n1\n\\.\n"), 0644))
	format, err = DetectFormat(plainDump)
	require.NoError(t, err)
	assert.Equal(t, DumpFormatPlain, format)
}

func TestNormalizePlainSQL(t *testing.T) {
	tmp := t.TempDir()
	plainDump := filepath.Join(tmp, "plain.sql")

	// Two tables plus a setval statement. Note the tab-delimited data rows and
	// the \N null marker, exactly as pg_dump emits them.
	content := "" +
		"--\n-- PostgreSQL database dump\n--\n\n" +
		"SET statement_timeout = 0;\n\n" +
		"COPY public.foo (id, name) FROM stdin;\n" +
		"1\tapple\n" +
		"2\t\\N\n" +
		"\\.\n\n\n" +
		"COPY public.bar (x) FROM stdin;\n" +
		"10\n" +
		"\\.\n\n" +
		"SELECT pg_catalog.setval('public.foo_id_seq', 2, true);\n"

	require.NoError(t, os.WriteFile(plainDump, []byte(content), 0644))

	outDir := filepath.Join(tmp, "normalized")
	result, err := Normalize(plainDump, outDir, "")
	require.NoError(t, err)

	assert.Equal(t, DumpFormatPlain, result.Format)
	require.Len(t, result.Tables, 2)

	// First table: public.foo with columns id, name.
	foo := result.Tables[0]
	assert.Equal(t, "public.foo", foo.QualifiedName)
	assert.Equal(t, []string{"id", "name"}, foo.Columns)
	fooData, err := os.ReadFile(foo.FilePath)
	require.NoError(t, err)
	assert.Equal(t, "1\tapple\n2\t\\N\n", string(fooData))

	// Second table: public.bar.
	bar := result.Tables[1]
	assert.Equal(t, "public.bar", bar.QualifiedName)
	assert.Equal(t, []string{"x"}, bar.Columns)
	barData, err := os.ReadFile(bar.FilePath)
	require.NoError(t, err)
	assert.Equal(t, "10\n", string(barData))

	// Setval statement captured.
	require.Len(t, result.SetvalStatements, 1)
	assert.Equal(t, "SELECT pg_catalog.setval('public.foo_id_seq', 2, true);", result.SetvalStatements[0])
}

func TestNormalizePlainSQL_PartitionRootMultipleFilesSameTable(t *testing.T) {
	tmp := t.TempDir()
	plainDump := filepath.Join(tmp, "plain.sql")

	// Two COPY blocks targeting the same root table (as produced with
	// --load-via-partition-root when partition leaves are routed to the root).
	content := "" +
		"COPY public.events (id, ts) FROM stdin;\n" +
		"1\t2024-01-01\n" +
		"\\.\n\n" +
		"COPY public.events (id, ts) FROM stdin;\n" +
		"2\t2024-02-01\n" +
		"\\.\n"

	require.NoError(t, os.WriteFile(plainDump, []byte(content), 0644))

	outDir := filepath.Join(tmp, "normalized")
	result, err := Normalize(plainDump, outDir, "")
	require.NoError(t, err)

	require.Len(t, result.Tables, 2)
	assert.Equal(t, "public.events", result.Tables[0].QualifiedName)
	assert.Equal(t, "public.events", result.Tables[1].QualifiedName)
	assert.NotEqual(t, result.Tables[0].FilePath, result.Tables[1].FilePath)
}

func TestNormalizeDirectoryRequiresPgRestore(t *testing.T) {
	tmp := t.TempDir()
	dirDump := filepath.Join(tmp, "dirdump")
	require.NoError(t, os.MkdirAll(dirDump, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(dirDump, "toc.dat"), []byte(pgDumpMagic), 0644))

	_, err := Normalize(dirDump, filepath.Join(tmp, "out"), "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pg_restore is required")
}
