//go:build integration

package postgres

import (
	"context"
	"encoding/json"
	"os"
	"sort"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ulule/mover/dialect"
)

// Run with:
//
//	MOVER_TEST_DSN=postgres://user@localhost:5432/db MOVER_TEST_FIXTURE=1 \
//	  go test -tags integration ./dialect/postgres/ -run Equivalence -v
//
// MOVER_TEST_FIXTURE=1 creates (and drops at the end) the mvt_* tables, the mvt_view view and the mvt_other schema.
const equivalenceFixture = `
DROP SCHEMA IF EXISTS mvt_other CASCADE;
DROP VIEW IF EXISTS mvt_view;
DROP TABLE IF EXISTS mvt_child, mvt_tree, mvt_composite, mvt_multi_child, mvt_multi_parent, mvt_alone, mvt_remote_child, mvt_parent CASCADE;

CREATE SCHEMA mvt_other;
CREATE TABLE mvt_other.remote (id serial PRIMARY KEY);

-- simple FK
CREATE TABLE mvt_parent (id serial PRIMARY KEY, name varchar(10) NOT NULL, tags text[]);
CREATE TABLE mvt_child (id serial PRIMARY KEY, parent_id int REFERENCES mvt_parent(id));
-- FK to the same table
CREATE TABLE mvt_tree (id serial PRIMARY KEY, parent_id int REFERENCES mvt_tree(id));
-- composite PK, in an order different from the column order
CREATE TABLE mvt_composite (a int, b int, PRIMARY KEY (b, a));
-- multi-column FK
CREATE TABLE mvt_multi_parent (a int, b int, PRIMARY KEY (a, b));
CREATE TABLE mvt_multi_child (id serial PRIMARY KEY, a int, b int, FOREIGN KEY (a, b) REFERENCES mvt_multi_parent(a, b));
-- no FK
CREATE TABLE mvt_alone (id serial PRIMARY KEY);
-- FK to another schema, and from another schema
CREATE TABLE mvt_remote_child (id serial PRIMARY KEY, remote_id int REFERENCES mvt_other.remote(id));
CREATE TABLE mvt_other.back (id serial PRIMARY KEY, parent_id int REFERENCES public.mvt_parent(id));
-- view
CREATE VIEW mvt_view AS SELECT id FROM mvt_parent;
`

const equivalenceCleanup = `
DROP SCHEMA IF EXISTS mvt_other CASCADE;
DROP VIEW IF EXISTS mvt_view;
DROP TABLE IF EXISTS mvt_child, mvt_tree, mvt_composite, mvt_multi_child, mvt_multi_parent, mvt_alone, mvt_remote_child, mvt_parent CASCADE;
`

// queryCounter is a pgx tracer counting the queries.
type queryCounter struct {
	count atomic.Int64
}

func (c *queryCounter) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	c.count.Add(1)

	return ctx
}

func (c *queryCounter) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

type fkSummary struct{ Name, ColumnName, ReferencedTableName, ReferencedColumnName string }

type refSummary struct{ Name, TableName, ColumnName string }

func fkSummaries(fks dialect.ForeignKeys, tableNames map[string]bool) []fkSummary {
	res := make([]fkSummary, 0, len(fks))
	for i := range fks {
		fk := &fks[i]
		if tableNames != nil && !tableNames[fk.ReferencedTableName] {
			continue
		}

		res = append(res, fkSummary{fk.Name, fk.ColumnName, fk.ReferencedTableName, fk.ReferencedColumnName})
	}

	return res
}

func refSummaries(refs dialect.ReferenceKeys, tableNames map[string]bool) []refSummary {
	res := make([]refSummary, 0, len(refs))
	for i := range refs {
		ref := &refs[i]
		if tableNames != nil && !tableNames[ref.TableName] {
			continue
		}

		res = append(res, refSummary{ref.Name, ref.TableName, ref.ColumnName})
	}

	return res
}

func TestTablesEquivalence(t *testing.T) {
	dsn := os.Getenv("MOVER_TEST_DSN")
	if dsn == "" {
		t.Skip("MOVER_TEST_DSN is not set")
	}

	ctx := context.Background()

	cfg, err := pgx.ParseConfig(dsn)
	require.NoError(t, err)

	counter := &queryCounter{}
	cfg.Tracer = counter

	conn, err := pgx.ConnectConfig(ctx, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close(context.Background()) })

	d := &PGDialect{conn: conn}

	fixture := os.Getenv("MOVER_TEST_FIXTURE") == "1"
	if fixture {
		_, err = conn.Exec(ctx, equivalenceFixture)
		require.NoError(t, err)

		t.Cleanup(func() {
			_, err := conn.Exec(context.Background(), equivalenceCleanup)
			assert.NoError(t, err)
		})
	}

	counter.count.Store(0)
	tables, err := d.Tables(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, 4, counter.count.Load(), "Tables() must run 4 queries")
	if len(tables) == 0 {
		t.Skip("no table in the public schema")
	}

	// Foreign keys and reference keys involving a table outside of public are ignored by Tables().
	publicTables := make(map[string]bool, len(tables))
	for i := range tables {
		publicTables[tables[i].Name] = true
	}

	for i := range tables {
		table := tables[i]

		t.Run(table.Name, func(t *testing.T) {
			expected, err := d.Table(ctx, table.Name)
			if err != nil {
				// A multi-column foreign key made the former reference keys query fail: the reference keys
				// of the table cannot be compared, the other fields still are.
				require.Contains(t, err.Error(), "more than one row returned by a subquery", table.Name)
				assert.NotEmpty(t, table.ReferenceKeys, "only the first column of the constraint is kept")

				expected = dialect.Table{Name: table.Name}
				expected.Columns, err = d.Columns(ctx, table.Name)
				require.NoError(t, err)
				expected.ForeignKeys, err = d.ForeignKeys(ctx, table.Name)
				require.NoError(t, err)
				expected.PrimaryKeys, err = d.PrimaryKeys(ctx, table.Name)
				require.NoError(t, err)
				table.ReferenceKeys = nil
			}

			assert.Equal(t, []dialect.Column(expected.Columns), []dialect.Column(table.Columns))
			assert.ElementsMatch(t, expected.PrimaryKeys, table.PrimaryKeys)
			assert.Equal(t, fkSummaries(expected.ForeignKeys, publicTables), fkSummaries(table.ForeignKeys, nil))
			assert.ElementsMatch(t, refSummaries(expected.ReferenceKeys, publicTables), refSummaries(table.ReferenceKeys, nil))
		})
	}

	if !fixture {
		return
	}

	t.Run("fixture", func(t *testing.T) {
		byName := make(map[string]dialect.Table, len(tables))
		for i := range tables {
			byName[tables[i].Name] = tables[i]
		}

		// views and other schemas are excluded
		assert.NotContains(t, byName, "mvt_view")
		assert.NotContains(t, byName, "remote")
		assert.NotContains(t, byName, "back")

		// foreign keys from or to another schema are ignored
		assert.Empty(t, byName["mvt_remote_child"].ForeignKeys)
		assert.Len(t, byName["mvt_parent"].ReferenceKeys, 1, "mvt_child only")

		// the primary key follows the order of the index
		require.Len(t, byName["mvt_composite"].PrimaryKeys, 2)
		assert.Equal(t, "b", byName["mvt_composite"].PrimaryKeys[0].Name)
		assert.Equal(t, "a", byName["mvt_composite"].PrimaryKeys[1].Name)

		// self reference
		require.Len(t, byName["mvt_tree"].ForeignKeys, 1)
		assert.Equal(t, "mvt_tree", byName["mvt_tree"].ForeignKeys[0].ReferencedTable.Name)

		// multi-column foreign key: only the first column of the reference key is kept
		require.Len(t, byName["mvt_multi_parent"].ReferenceKeys, 1)
		assert.Equal(t, "a", byName["mvt_multi_parent"].ReferenceKeys[0].ColumnName)
		assert.True(t, strings.HasPrefix(byName["mvt_multi_child"].ForeignKeys[0].Definition, "FOREIGN KEY (a, b)"))
	})
}

const selectInFixture = `
DROP TABLE IF EXISTS mvt_in;
CREATE TABLE mvt_in (
	id serial PRIMARY KEY,
	int_col integer NOT NULL,
	big_col bigint NOT NULL,
	uuid_col uuid NOT NULL,
	vc_col varchar(30) NOT NULL,
	num_col numeric(10, 2) NOT NULL
);
INSERT INTO mvt_in (int_col, big_col, uuid_col, vc_col, num_col)
SELECT i, i * 10000000000, md5(i::text)::uuid, 'key-' || i, i / 4.0 FROM generate_series(1, 20000) AS i;
CREATE INDEX mvt_in_int_col ON mvt_in (int_col);
CREATE INDEX mvt_in_uuid_col ON mvt_in (uuid_col);
ANALYZE mvt_in;
`

// TestSelectIn runs against the table of a throwaway fixture. Run with:
//
//	MOVER_TEST_DSN=postgres://user@localhost:5432/db go test -tags integration ./dialect/postgres/ -run SelectIn -v
func TestSelectIn(t *testing.T) {
	dsn := os.Getenv("MOVER_TEST_DSN")
	if dsn == "" {
		t.Skip("MOVER_TEST_DSN is not set")
	}

	ctx := context.Background()

	cfg, err := pgx.ParseConfig(dsn)
	require.NoError(t, err)

	counter := &queryCounter{}
	cfg.Tracer = counter

	conn, err := pgx.ConnectConfig(ctx, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close(context.Background()) })

	_, err = conn.Exec(ctx, selectInFixture)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, err := conn.Exec(context.Background(), "DROP TABLE IF EXISTS mvt_in")
		assert.NoError(t, err)
	})

	d := &PGDialect{conn: conn}

	tables, err := d.Tables(ctx)
	require.NoError(t, err)

	var table dialect.Table
	for i := range tables {
		if tables[i].Name == "mvt_in" {
			table = tables[i]
		}
	}
	require.Equal(t, "mvt_in", table.Name)

	all, err := d.ResultSet(ctx, "SELECT * FROM mvt_in ORDER BY id LIMIT 3000")
	require.NoError(t, err)

	ids := func(rows []map[string]interface{}) []int {
		res := make([]int, len(rows))
		for i := range rows {
			res[i] = int(rows[i]["id"].(int32))
		}
		sort.Ints(res)

		return res
	}

	for _, column := range []string{"int_col", "big_col", "uuid_col", "vc_col", "num_col", "id"} {
		t.Run(column, func(t *testing.T) {
			// 1500 values: two batches
			values := make([]interface{}, 0, 1500)
			for i := 0; i < 3000; i += 2 {
				values = append(values, all[i][column])
			}

			counter.count.Store(0)
			rows, err := d.SelectIn(ctx, table, column, values)
			require.NoError(t, err)

			expected := make([]int, 0, 1500)
			for i := 0; i < 3000; i += 2 {
				expected = append(expected, int(all[i]["id"].(int32)))
			}
			assert.Equal(t, expected, ids(rows))

			if column == "num_col" {
				// numeric values are float64, and use the fallback: one query by value
				assert.EqualValues(t, len(values), counter.count.Load())
			} else {
				assert.EqualValues(t, 2, counter.count.Load(), "two batches of at most %d values", selectInBatchSize)
			}
		})
	}

	t.Run("empty", func(t *testing.T) {
		counter.count.Store(0)
		rows, err := d.SelectIn(ctx, table, "int_col", nil)
		require.NoError(t, err)
		assert.Empty(t, rows)
		assert.EqualValues(t, 0, counter.count.Load())
	})

	t.Run("no match", func(t *testing.T) {
		rows, err := d.SelectIn(ctx, table, "vc_col", []interface{}{"nope"})
		require.NoError(t, err)
		assert.Empty(t, rows)
	})

	t.Run("plan", func(t *testing.T) {
		for _, tc := range []struct {
			column string
			args   []string
		}{
			{"int_col", []string{"3", "17", "4242"}},
			{"uuid_col", []string{"c81e728d-9d4c-2f63-6f06-7f89cc14862c"}},
		} {
			rows, err := conn.Query(ctx, "EXPLAIN "+selectInQuery("mvt_in", tc.column, table.Columns.Get(tc.column).DataType), tc.args)
			require.NoError(t, err)

			var plan []string
			for rows.Next() {
				var line string
				require.NoError(t, rows.Scan(&line))
				plan = append(plan, line)
			}
			require.NoError(t, rows.Err())

			t.Logf("%s:\n%s", tc.column, strings.Join(plan, "\n"))
			assert.Contains(t, strings.Join(plan, "\n"), "Index Scan", tc.column)
		}
	})
}

// TestBulkInsert runs against a throwaway table. Run with:
//
//	MOVER_TEST_DSN=postgres://user@localhost:5432/db go test -tags integration ./dialect/postgres/ -run BulkInsert -v
func TestBulkInsert(t *testing.T) {
	dsn := os.Getenv("MOVER_TEST_DSN")
	if dsn == "" {
		t.Skip("MOVER_TEST_DSN is not set")
	}

	ctx := context.Background()

	conn, err := pgx.Connect(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close(context.Background()) })

	_, err = conn.Exec(ctx, `
DROP TABLE IF EXISTS mvt_bulk;
CREATE TABLE mvt_bulk (id serial PRIMARY KEY, name varchar(10) NOT NULL);`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, err := conn.Exec(context.Background(), "DROP TABLE IF EXISTS mvt_bulk")
		assert.NoError(t, err)
	})

	d := &PGDialect{conn: conn}

	table := dialect.Table{
		Name:        "mvt_bulk",
		PrimaryKeys: []dialect.PrimaryKey{{Name: "id"}},
		Columns: dialect.Columns{
			{Name: "id", DataType: "integer"},
			{Name: "name", DataType: "character varying(10)"},
		},
	}

	count := func() int {
		var n int
		require.NoError(t, conn.QueryRow(ctx, "SELECT count(*) FROM mvt_bulk").Scan(&n))

		return n
	}

	// 1201 rows: two full batches and a partial one
	rows := make([]map[string]interface{}, 0, 2*insertBatchSize+201)
	for i := 1; i <= 2*insertBatchSize+201; i++ {
		rows = append(rows, map[string]interface{}{"id": int32(i), "name": "row"})
	}

	require.NoError(t, d.BulkInsert(ctx, table, rows))
	assert.Equal(t, len(rows), count())

	// idempotent thanks to ON CONFLICT DO NOTHING
	require.NoError(t, d.BulkInsert(ctx, table, rows))
	assert.Equal(t, len(rows), count())

	// a failing row aborts the whole transaction
	failing := []map[string]interface{}{
		{"id": int32(5000), "name": "ok"},
		{"id": int32(5001), "name": "this name is too long"},
	}
	err = d.BulkInsert(ctx, table, failing)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "row 1")
	assert.Contains(t, err.Error(), "mvt_bulk")
	assert.Equal(t, len(rows), count())

	// the connection is still usable
	require.NoError(t, d.BulkInsert(ctx, table, failing[:1]))
	assert.Equal(t, len(rows)+1, count())
}

// TestBulkInsertArrays inserts rows decoded from JSON, as the loader does, into array columns.
func TestBulkInsertArrays(t *testing.T) {
	dsn := os.Getenv("MOVER_TEST_DSN")
	if dsn == "" {
		t.Skip("MOVER_TEST_DSN is not set")
	}

	ctx := context.Background()

	conn, err := pgx.Connect(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close(context.Background()) })

	_, err = conn.Exec(ctx, `
DROP TABLE IF EXISTS mvt_arrays;
CREATE TABLE mvt_arrays (id serial PRIMARY KEY, events text[], names varchar(20)[], counts int4[], bigs int8[]);`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, err := conn.Exec(context.Background(), "DROP TABLE IF EXISTS mvt_arrays")
		assert.NoError(t, err)
	})

	table := dialect.Table{
		Name:        "mvt_arrays",
		PrimaryKeys: []dialect.PrimaryKey{{Name: "id"}},
		Columns: dialect.Columns{
			{Name: "id", DataType: "integer"},
			{Name: "events", DataType: "text[]"},
			{Name: "names", DataType: "character varying(20)[]"},
			{Name: "counts", DataType: "integer[]"},
			{Name: "bigs", DataType: "bigint[]"},
		},
	}

	var rows []map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(`[
{"id": 1, "events": ["order.completed", "order.cancelled", "subscription.cancelled"], "names": ["a", null], "counts": [1, 2, 3], "bigs": [9007199254740992]},
{"id": 2, "events": [], "names": [], "counts": [], "bigs": []},
{"id": 3, "events": null, "names": null, "counts": null, "bigs": null}
]`), &rows))

	require.NoError(t, (&PGDialect{conn: conn}).BulkInsert(ctx, table, rows))

	var (
		events []string
		counts []int32
		bigs   []int64
	)
	require.NoError(t, conn.QueryRow(ctx, "SELECT events, counts, bigs FROM mvt_arrays WHERE id = 1").Scan(&events, &counts, &bigs))
	assert.Equal(t, []string{"order.completed", "order.cancelled", "subscription.cancelled"}, events)
	assert.Equal(t, []int32{1, 2, 3}, counts)
	assert.Equal(t, []int64{9007199254740992}, bigs)

	var nullNames []*string
	require.NoError(t, conn.QueryRow(ctx, "SELECT names FROM mvt_arrays WHERE id = 1").Scan(&nullNames))
	require.Len(t, nullNames, 2)
	assert.Equal(t, "a", *nullNames[0])
	assert.Nil(t, nullNames[1])

	// an empty array is stored as an empty array, not as NULL
	var isNull bool
	require.NoError(t, conn.QueryRow(ctx, "SELECT events IS NULL FROM mvt_arrays WHERE id = 2").Scan(&isNull))
	assert.False(t, isNull)
	require.NoError(t, conn.QueryRow(ctx, "SELECT cardinality(events) = 0 AND cardinality(counts) = 0 FROM mvt_arrays WHERE id = 2").Scan(&isNull))
	assert.True(t, isNull)

	require.NoError(t, conn.QueryRow(ctx, "SELECT events IS NULL AND counts IS NULL FROM mvt_arrays WHERE id = 3").Scan(&isNull))
	assert.True(t, isNull)
}
