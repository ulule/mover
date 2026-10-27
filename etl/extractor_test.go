package etl

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/ulule/mover/config"
	"github.com/ulule/mover/dialect"
)

// fakeDialect is an in-memory dialect: tables contains the rows by table and queries the result of the raw queries.
type fakeDialect struct {
	tables  map[string][]map[string]interface{}
	queries map[string][]map[string]interface{}
	// selectIn contains the number of values requested by SelectIn call, by table.
	selectIn   map[string][]int
	resultSets int
}

func (f *fakeDialect) Close(context.Context) error { return nil }
func (f *fakeDialect) ReferenceKeys(context.Context, string) (dialect.ReferenceKeys, error) {
	return nil, nil
}
func (f *fakeDialect) ForeignKeys(context.Context, string) (dialect.ForeignKeys, error) {
	return nil, nil
}
func (f *fakeDialect) PrimaryKeyConstraint(context.Context, string) (string, error) { return "", nil }
func (f *fakeDialect) Tables(context.Context) (dialect.Tables, error)               { return nil, nil }
func (f *fakeDialect) Table(context.Context, string) (dialect.Table, error) {
	return dialect.Table{}, nil
}
func (f *fakeDialect) Columns(context.Context, string) ([]dialect.Column, error) { return nil, nil }
func (f *fakeDialect) BulkInsert(context.Context, dialect.Table, []map[string]interface{}) error {
	return nil
}

func (f *fakeDialect) ResultSet(_ context.Context, query string, _ ...interface{}) ([]map[string]interface{}, error) {
	f.resultSets++

	rows, ok := f.queries[query]
	if !ok {
		return nil, fmt.Errorf("unexpected query %s", query)
	}

	return rows, nil
}

func (f *fakeDialect) SelectIn(_ context.Context, table dialect.Table, column string, values []interface{}) ([]map[string]interface{}, error) {
	f.selectIn[table.Name] = append(f.selectIn[table.Name], len(values))

	wanted := make(map[string]struct{}, len(values))
	for i := range values {
		wanted[fmt.Sprint(values[i])] = struct{}{}
	}

	rows := make([]map[string]interface{}, 0)
	for _, row := range f.tables[table.Name] {
		if _, ok := wanted[fmt.Sprint(row[column])]; ok {
			rows = append(rows, row)
		}
	}

	return rows, nil
}

func (f *fakeDialect) selectInCalls() int {
	total := 0
	for _, calls := range f.selectIn {
		total += len(calls)
	}

	return total
}

// testSchema describes:
//
//	parent <- child (parent_id) <- grandchild (child_id)
//	tree (parent_id -> tree)
//	extra
func testSchema() map[string]config.Schema {
	var (
		pk = func(name string) []dialect.PrimaryKey { return []dialect.PrimaryKey{{Name: "id", TableName: name}} }

		parent = dialect.Table{Name: "parent", PrimaryKeys: pk("parent")}
		child  = dialect.Table{Name: "child", PrimaryKeys: pk("child")}
		grand  = dialect.Table{Name: "grandchild", PrimaryKeys: pk("grandchild")}
		tree   = dialect.Table{Name: "tree", PrimaryKeys: pk("tree")}
		extra  = dialect.Table{Name: "extra", PrimaryKeys: pk("extra")}
	)

	child.ForeignKeys = dialect.ForeignKeys{{Name: "child_parent_fk", ColumnName: "parent_id", ReferencedTable: parent, ReferencedColumnName: "id"}}
	grand.ForeignKeys = dialect.ForeignKeys{{Name: "grandchild_child_fk", ColumnName: "child_id", ReferencedTable: child, ReferencedColumnName: "id"}}
	tree.ForeignKeys = dialect.ForeignKeys{{Name: "tree_parent_fk", ColumnName: "parent_id", ReferencedTable: tree, ReferencedColumnName: "id"}}

	parent.ReferenceKeys = dialect.ReferenceKeys{{Name: "child_parent_fk", Table: child, ColumnName: "parent_id"}}
	child.ReferenceKeys = dialect.ReferenceKeys{{Name: "grandchild_child_fk", Table: grand, ColumnName: "child_id"}}
	tree.ReferenceKeys = dialect.ReferenceKeys{{Name: "tree_parent_fk", Table: tree, ColumnName: "parent_id"}}

	schema := make(map[string]config.Schema)
	for _, table := range []dialect.Table{parent, child, grand, tree, extra} {
		schema[table.Name] = config.Schema{TableName: table.Name, Table: table}
	}

	return schema
}

func newTestDialect() *fakeDialect {
	row := func(values ...interface{}) map[string]interface{} {
		res := make(map[string]interface{})
		for i := 0; i < len(values); i += 2 {
			res[values[i].(string)] = values[i+1]
		}

		return res
	}

	d := &fakeDialect{
		selectIn: make(map[string][]int),
		tables: map[string][]map[string]interface{}{
			"parent": {row("id", int32(10)), row("id", int32(11))},
			"child": {
				row("id", int32(1), "parent_id", int32(10)),
				row("id", int32(2), "parent_id", int32(10)),
				row("id", int32(3), "parent_id", int32(10)),
				row("id", int32(4), "parent_id", int32(10)),
				row("id", int32(5), "parent_id", int32(10)),
				row("id", int32(6), "parent_id", int32(11)),
				row("id", int32(7), "parent_id", nil),
			},
			"grandchild": {
				row("id", int32(20), "child_id", int32(1)),
				row("id", int32(21), "child_id", int32(1)),
				row("id", int32(22), "child_id", int32(6)),
			},
			"tree": {
				row("id", int32(1), "parent_id", nil),
				row("id", int32(2), "parent_id", int32(1)),
				row("id", int32(3), "parent_id", int32(2)),
				row("id", int32(4), "parent_id", int32(3)),
				// self reference
				row("id", int32(5), "parent_id", int32(5)),
			},
			"extra": {row("id", int32(100)), row("id", int32(101))},
		},
	}

	d.queries = map[string][]map[string]interface{}{
		"SELECT * FROM child WHERE id = 1":   d.tables["child"][:1],
		"SELECT * FROM child WHERE id <= 5":  d.tables["child"][:5],
		"SELECT * FROM tree WHERE id = 3":    d.tables["tree"][2:3],
		"SELECT * FROM tree WHERE id = 5":    d.tables["tree"][4:5],
		"SELECT * FROM parent WHERE id = 10": d.tables["parent"][:1],
		`SELECT * FROM "parent"`:             d.tables["parent"],
		`SELECT * FROM "extra"`:              d.tables["extra"],
		"SELECT * FROM extra WHERE id = 100": d.tables["extra"][:1],
	}

	return d
}

func newTestExtractor(d dialect.Dialect, schema map[string]config.Schema) *extractor {
	return &extractor{
		extract:   make(extract),
		schema:    schema,
		dialect:   d,
		logger:    zap.NewNop(),
		processed: make(map[string]struct{}),
		fetched:   make(map[fetchKey]map[string]struct{}),
	}
}

// extracted returns the sorted set of "<table>(<id>)".
func extracted(e *extractor) []string {
	set := make(map[string]struct{})
	for tableName := range e.extract {
		for _, rows := range e.extract[tableName] {
			for _, row := range rows {
				set[fmt.Sprintf("%s(%v)", tableName, row["id"])] = struct{}{}
			}
		}
	}

	res := make([]string, 0, len(set))
	for k := range set {
		res = append(res, k)
	}
	sort.Strings(res)

	return res
}

func TestExtractorForeignAndReferenceKeys(t *testing.T) {
	var (
		d      = newTestDialect()
		schema = testSchema()
		e      = newTestExtractor(d, schema)
	)

	require.NoError(t, e.Handle(context.Background(), schema["child"], "SELECT * FROM child WHERE id = 1"))

	// the parent through the foreign key, the grandchildren through the reference key of the root. The other children
	// of the parent are not reference keys of the root.
	assert.Equal(t, []string{"child(1)", "grandchild(20)", "grandchild(21)", "parent(10)"}, extracted(e))
}

func TestExtractorSelfReference(t *testing.T) {
	var (
		d      = newTestDialect()
		schema = testSchema()
		e      = newTestExtractor(d, schema)
	)

	require.NoError(t, e.Handle(context.Background(), schema["tree"], "SELECT * FROM tree WHERE id = 5"))
	assert.Equal(t, []string{"tree(5)"}, extracted(e))

	// the chain of parents is followed up to the top, and the children of the root are retrieved
	e = newTestExtractor(d, schema)
	require.NoError(t, e.Handle(context.Background(), schema["tree"], "SELECT * FROM tree WHERE id = 3"))
	assert.Equal(t, []string{"tree(1)", "tree(2)", "tree(3)", "tree(4)"}, extracted(e))
}

func TestExtractorOmitAndConfigReferenceKeys(t *testing.T) {
	t.Run("omit", func(t *testing.T) {
		var (
			d      = newTestDialect()
			schema = testSchema()
		)

		child := schema["child"]
		child.OmitReferenceKeys = true
		schema["child"] = child

		e := newTestExtractor(d, schema)
		require.NoError(t, e.Handle(context.Background(), child, "SELECT * FROM child WHERE id = 1"))
		assert.Equal(t, []string{"child(1)", "parent(10)"}, extracted(e))
	})

	t.Run("config reference key on a deeper level", func(t *testing.T) {
		var (
			d      = newTestDialect()
			schema = testSchema()
		)

		child := schema["child"]
		child.OmitReferenceKeys = true
		schema["child"] = child

		parent := schema["parent"]
		parent.ReferenceKeys = []string{"child_parent_fk"}
		schema["parent"] = parent

		e := newTestExtractor(d, schema)
		require.NoError(t, e.Handle(context.Background(), child, "SELECT * FROM child WHERE id = 1"))

		// parent(10) is not a root, but its config reference key brings all its children. They do not bring their
		// grandchildren as they are not roots.
		assert.Equal(t, []string{"child(1)", "child(2)", "child(3)", "child(4)", "child(5)", "parent(10)"}, extracted(e))
	})
}

func TestExtractorConfigQueries(t *testing.T) {
	var (
		d      = newTestDialect()
		schema = testSchema()
	)

	child := schema["child"]
	child.OmitReferenceKeys = true
	child.Queries = []config.Query{{TableName: "extra", Table: schema["extra"].Table, Query: "SELECT * FROM extra WHERE id = {parent_id}0"}}
	schema["child"] = child

	// {attr} is replaced by the value of the row: 10 -> 100
	e := newTestExtractor(d, schema)
	require.NoError(t, e.Handle(context.Background(), child, "SELECT * FROM child WHERE id = 1"))
	assert.Equal(t, []string{"child(1)", "extra(100)", "parent(10)"}, extracted(e))
	assert.Equal(t, 1, d.selectInCalls(), "only the parent goes through SelectIn")
	assert.Equal(t, 2, d.resultSets, "root and raw query")
}

func TestExtractorSharedParent(t *testing.T) {
	var (
		d      = newTestDialect()
		schema = testSchema()
	)

	child := schema["child"]
	child.OmitReferenceKeys = true
	schema["child"] = child

	// five roots share parent 10
	e := newTestExtractor(d, schema)
	require.NoError(t, e.Handle(context.Background(), child, "SELECT * FROM child WHERE id <= 5"))

	assert.Equal(t, []string{"child(1)", "child(2)", "child(3)", "child(4)", "child(5)", "parent(10)"}, extracted(e))
	assert.Equal(t, map[string][]int{"parent": {1}}, d.selectIn, "a single call, with a single value")
}

func TestExtractorExtra(t *testing.T) {
	var (
		d      = newTestDialect()
		schema = testSchema()
	)

	// the last extra table is already cached: Handle used to return nil, and the whole output was lost
	cfg := config.Config{Extra: []config.Schema{{TableName: "extra"}, {TableName: "extra"}}}
	engine := &Engine{schema: schema, dialect: d, config: cfg, logger: zap.NewNop()}

	output := t.TempDir()
	require.NoError(t, engine.Extract(context.Background(), output, "SELECT * FROM parent WHERE id = 10"))

	for _, name := range []string{"parent.json", "extra.json"} {
		content, err := os.ReadFile(filepath.Join(output, name))
		require.NoError(t, err, name)
		assert.NotContains(t, string(content), `"count": 0`, name)
	}

	// the child of the root comes with it
	_, err := os.Stat(filepath.Join(output, "child.json"))
	assert.NoError(t, err)
}
