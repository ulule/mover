//go:build integration

package postgres

import (
	"context"
	"os"
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
