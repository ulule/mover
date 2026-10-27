package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"sort"

	"github.com/georgysavva/scany/v2/pgxscan"
	"github.com/jackc/pgx/v5"
	lk "github.com/ulule/loukoum/v3"
	"github.com/ulule/loukoum/v3/types"

	"github.com/ulule/mover/dialect"
)

var fkRegexp = regexp.MustCompile(`FOREIGN KEY \((.*?)\) REFERENCES (?:(.*?)\.)?(.*?)\((.*?)\)`)

// NewPGDialect initializes a new PGDialect instance.
func NewPGDialect(ctx context.Context, dsn string) (dialect.Dialect, error) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to database with dsn %s: %w", dsn, err)
	}

	return &PGDialect{
		conn: conn,
	}, nil
}

// PGDialect manages a connection with PostgreSQL.
type PGDialect struct {
	conn *pgx.Conn
}

// Close closes a connection.
func (d *PGDialect) Close(ctx context.Context) error {
	return d.conn.Close(ctx)
}

// ResultSet executes a query and converts Rows in map[string]interface{}.
func (d *PGDialect) ResultSet(ctx context.Context, query string, args ...interface{}) ([]map[string]interface{}, error) {
	rows, err := d.query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("unable to retrieve results : %w", err)
	}

	results := make([]map[string]interface{}, 0)
	for rows.Next() {
		result, err := marshalRows(rows)
		if err != nil {
			return nil, err
		}

		results = append(results, result)
	}

	return results, nil
}

// selectInBatchSize is the maximum number of values sent in a single SelectIn query.
const selectInBatchSize = 1000

// selectInQuery returns the query of SelectIn. The data type comes from format_type, so it is not quoted.
func selectInQuery(tableName, column, dataType string) string {
	return fmt.Sprintf("SELECT * FROM %s WHERE %s = ANY($1::text[]::%s[])",
		pgx.Identifier{tableName}.Sanitize(), pgx.Identifier{column}.Sanitize(), dataType)
}

// SelectIn returns the rows of a table whose column is one of the values. Keys of a known type are sent as a single
// text array cast to the column type, so the planner can still use an index on the column. Other types fall back to
// one query by value.
func (d *PGDialect) SelectIn(ctx context.Context, table dialect.Table, column string, values []interface{}) ([]map[string]interface{}, error) {
	var (
		results  = make([]map[string]interface{}, 0)
		dataType = table.Columns.Get(column).DataType
		texts    = make([]string, 0, len(values))
	)

	for i := range values {
		text, ok := keyToText(values[i])
		if !ok || dataType == "" {
			texts = nil
			break
		}

		texts = append(texts, text)
	}

	if texts == nil {
		query := fmt.Sprintf("SELECT * FROM %s WHERE %s = $1", pgx.Identifier{table.Name}.Sanitize(), pgx.Identifier{column}.Sanitize())

		for i := range values {
			rows, err := d.ResultSet(ctx, query, values[i])
			if err != nil {
				return nil, err
			}

			results = append(results, rows...)
		}

		return results, nil
	}

	query := selectInQuery(table.Name, column, dataType)

	for start := 0; start < len(texts); start += selectInBatchSize {
		end := min(start+selectInBatchSize, len(texts))

		rows, err := d.query(ctx, query, texts[start:end])
		if err != nil {
			return nil, err
		}

		for rows.Next() {
			result, err := marshalRows(rows)
			if err != nil {
				rows.Close()
				return nil, err
			}

			results = append(results, result)
		}

		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("unable to read rows of table %s: %w", table.Name, err)
		}
	}

	return results, nil
}

// insertBatchSize is the maximum number of insert queries sent in a single batch.
const insertBatchSize = 500

// BulkInsert inserts multiple data a single database transaction. It disables triggers to avoid conflicts on
// foreign constraints. Rows are sent by batches of insertBatchSize queries.
func (d *PGDialect) BulkInsert(ctx context.Context, table dialect.Table, data []map[string]interface{}) error {
	tx, err := d.conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("unable to begin transaction on table %s: %w", table.Name, err)
	}

	defer func() { _ = tx.Rollback(ctx) }()

	if err := d.disableTriggers(ctx, tx, table, func(ctx context.Context) error {
		for start := 0; start < len(data); start += insertBatchSize {
			end := min(start+insertBatchSize, len(data))
			b := &pgx.Batch{}

			for i := start; i < end; i++ {
				query, args, err := insertQuery(table, data[i])
				if err != nil {
					return fmt.Errorf("unable to build insert query of row %d on table %s: %w", i, table.Name, err)
				}

				b.Queue(query, args...)
			}

			br := tx.SendBatch(ctx, b)

			for i := start; i < end; i++ {
				if _, err := br.Exec(); err != nil {
					_ = br.Close()
					return fmt.Errorf("unable to insert row %d on table %s: %w", i, table.Name, err)
				}
			}

			if err := br.Close(); err != nil {
				return fmt.Errorf("unable to close batch on table %s: %w", table.Name, err)
			}
		}

		return nil
	}); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("unable to commit transaction on table %s: %w", table.Name, err)
	}

	if err := d.resetSequence(ctx, table); err != nil {
		return fmt.Errorf("unable to reset sequence on table %s: %w", table.Name, err)
	}

	return nil
}

// ReferenceKeys returns the "Referenced by" constraints of a table.
func (d *PGDialect) ReferenceKeys(ctx context.Context, tableName string) (dialect.ReferenceKeys, error) {
	oid, err := d.getTableOID(ctx, tableName)
	if err != nil {
		return nil, err
	}

	builder := lk.Select(
		"conname",
		lk.Raw("c2.relname AS table"),
		lk.Raw("(SELECT attname FROM pg_attribute WHERE attrelid = r.conrelid AND ARRAY[attnum] <@ r.conkey) AS column"),
	).From(lk.Raw("pg_constraint r, pg_class c, pg_class c2")).
		Where(lk.Condition("r.confrelid").Equal(oid)).
		And(lk.Raw("c.oid = r.confrelid")).
		And(lk.Raw("c2.oid = r.conrelid")).
		Comment("reference keys")
	query, args := builder.Query()
	var results []struct {
		Conname string `db:"conname"`
		Table   string `db:"table"`
		Column  string `db:"column"`
	}

	if err := d.execQuery(ctx, &results, query, args...); err != nil {
		return nil, fmt.Errorf("unable to retrieve table %s reference keys: %w", tableName, err)
	}
	referenceKeys := make(dialect.ReferenceKeys, len(results))
	for i := range referenceKeys {
		referenceKeys[i] = dialect.ReferenceKey{
			Name:       results[i].Conname,
			TableName:  results[i].Table,
			ColumnName: results[i].Column,
		}
	}
	return referenceKeys, nil
}

// ForeignKeys returns the foreign keys of a table.
func (d *PGDialect) ForeignKeys(ctx context.Context, tableName string) (dialect.ForeignKeys, error) {
	oid, err := d.getTableOID(ctx, tableName)
	if err != nil {
		return nil, err
	}

	builder := lk.Select("r.conname", lk.Raw("pg_catalog.pg_get_constraintdef(r.oid, true) AS condef")).
		From(lk.Raw("pg_catalog.pg_constraint r, pg_namespace n, pg_class c")).
		Where(lk.Condition("r.conrelid").Equal(oid)).
		And(lk.Raw("r.contype = 'f'")).
		And(lk.Raw("c.oid = confrelid")).
		And(lk.Raw("n.oid = c.relnamespace")).
		OrderBy(lk.Order("1")).
		Comment("foreign keys")

	query, args := builder.Query()
	var results []struct {
		Conname string `db:"conname"`
		Condef  string `db:"condef"`
	}

	if err := d.execQuery(ctx, &results, query, args...); err != nil {
		return nil, fmt.Errorf("unable to retrieve table %s foreign keys: %w", tableName, err)
	}

	foreignKeys := make(dialect.ForeignKeys, len(results))
	for i := range foreignKeys {
		matches := fkRegexp.FindStringSubmatch(results[i].Condef)

		foreignKeys[i] = dialect.ForeignKey{
			Name:                 results[i].Conname,
			Definition:           results[i].Condef,
			ColumnName:           matches[1],
			ReferencedTableName:  matches[3],
			ReferencedColumnName: matches[4],
		}
	}

	return foreignKeys, nil
}

// PrimaryKeyConstraint returns the primary key constraint of a table.
func (d *PGDialect) PrimaryKeyConstraint(ctx context.Context, tableName string) (string, error) {
	oid, err := d.getTableOID(ctx, tableName)
	if err != nil {
		return "", err
	}

	builder := lk.Select("conname").
		From(lk.Table("pg_catalog.pg_constraint").As("r")).
		Where(lk.Condition("r.conrelid").Equal(oid)).
		And(lk.Raw("r.contype = 'p'")).
		OrderBy(lk.Order("1"))
	query, args := builder.Query()

	var result string
	if err := d.queryRow(ctx, &result, query, args...); err != nil {
		return "", fmt.Errorf("unable to retrieve table %s primary key constraint: %w", tableName, err)
	}
	return result, nil
}

// PrimaryKeys returns primary keys of a table.
func (d *PGDialect) PrimaryKeys(ctx context.Context, tableName string) ([]dialect.PrimaryKey, error) {
	oid, err := d.getTableOID(ctx, tableName)
	if err != nil {
		return nil, err
	}

	builder := lk.Select(
		lk.Raw("pg_attribute.attname AS name"),
		lk.Raw("format_type(pg_attribute.atttypid, pg_attribute.atttypmod) AS data_type"),
	).
		From(lk.Raw("pg_index, pg_class, pg_attribute, pg_namespace")).
		Where(lk.Condition("pg_class.oid").Equal(oid)).
		And(lk.Raw("indrelid = pg_class.oid")).
		And(lk.Raw("nspname = 'public'")).
		And(lk.Raw("pg_class.relnamespace = pg_namespace.oid")).
		And(lk.Raw("pg_attribute.attrelid = pg_class.oid")).
		And(lk.Raw("pg_attribute.attnum = any(pg_index.indkey)")).
		And(lk.Raw("indisprimary")).
		Comment("primary keys")

	query, args := builder.Query()
	var results []struct {
		Name     string `db:"name"`
		DataType string `db:"data_type"`
	}

	if err := d.execQuery(ctx, &results, query, args...); err != nil {
		return nil, fmt.Errorf("unable to retrieve table %s primary keys: %w", tableName, err)
	}

	primaryKeys := make([]dialect.PrimaryKey, len(results))
	for i := range results {
		primaryKeys[i] = dialect.PrimaryKey{
			Name:      results[i].Name,
			DataType:  results[i].DataType,
			TableName: tableName,
		}
	}

	return primaryKeys, err
}

// Columns returns sorted columns with types of a table.
func (d *PGDialect) Columns(ctx context.Context, tableName string) ([]dialect.Column, error) {
	builder := lk.Select(
		lk.Raw("a.attname AS column_name"),
		lk.Raw("pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type"),
		lk.Raw(`(
    SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid)
    FROM pg_catalog.pg_attrdef d
    WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum
    AND a.atthasdef
  ) AS default`),
		lk.Raw("a.attnotnull AS is_nullable"),
		lk.Raw("c.relname AS table_name"),
		lk.Raw("a.attnum as ordinal_position"),
	).
		From(lk.Table("pg_catalog.pg_attribute").As("a")).
		Join(lk.Table("pg_catalog.pg_class").As("c"), lk.On("a.attrelid", "c.oid"), lk.LeftJoin).
		Where(lk.Condition("a.attnum").GreaterThan(0)).
		And(lk.Condition("a.attisdropped").Equal(false)).
		OrderBy(lk.Order("a.attnum"))

	if tableName != "" {
		oid, err := d.getTableOID(ctx, tableName)
		if err != nil {
			return nil, err
		}

		builder = builder.Where(lk.Condition("a.attrelid").Equal(oid))
	} else {
		builder = builder.
			Join(lk.Table("pg_catalog.pg_namespace").As("n"), lk.On("n.oid", "c.relnamespace")).
			And(lk.Condition("n.nspname").Equal("public")).
			And(lk.Raw("c.relkind = 'r'"))
	}

	query, args := builder.Query()

	var results []struct {
		ColumnName      string         `db:"column_name"`
		IsNullable      bool           `db:"is_nullable"`
		DataType        string         `db:"data_type"`
		Default         sql.NullString `db:"default"`
		OrdinalPosition int64          `db:"ordinal_position"`
		TableName       string         `db:"table_name"`
	}

	if err := d.execQuery(ctx, &results, query, args...); err != nil {
		return nil, err
	}

	columns := make([]dialect.Column, len(results))
	for i := range results {
		result := results[i]

		columns[i] = dialect.Column{
			Name:      result.ColumnName,
			DataType:  result.DataType,
			TableName: result.TableName,
			Position:  result.OrdinalPosition,
			Nullable:  result.IsNullable,
		}
	}

	return columns, nil
}

// Table returns a table with its reference keys, foreign keys, columns and primary keys.
func (d *PGDialect) Table(ctx context.Context, tableName string) (dialect.Table, error) {
	columns, err := d.Columns(ctx, tableName)
	if err != nil {
		return dialect.Table{}, err
	}

	table := dialect.Table{
		Name:    tableName,
		Columns: columns,
	}
	table.ReferenceKeys, err = d.ReferenceKeys(ctx, tableName)
	if err != nil {
		return dialect.Table{}, err
	}

	table.ForeignKeys, err = d.ForeignKeys(ctx, tableName)
	if err != nil {
		return dialect.Table{}, err
	}

	table.PrimaryKeys, err = d.PrimaryKeys(ctx, tableName)
	if err != nil {
		return dialect.Table{}, err
	}

	return table, nil
}

// Tables returns all the tables from the database. It runs 4 queries, whatever the number of tables.
func (d *PGDialect) Tables(ctx context.Context) (dialect.Tables, error) {
	builder := lk.Select("c.relname").
		From(lk.Table("pg_catalog.pg_class").As("c")).
		Join(lk.Table("pg_namespace").As("n"), lk.On("n.oid", "c.relnamespace")).
		Where(lk.Raw("relkind = 'r'")).
		And(lk.Condition("n.nspname").Equal("public")).
		Comment("tables")

	query, args := builder.Query()

	var tableNames []string
	if err := d.execQuery(ctx, &tableNames, query, args...); err != nil {
		return nil, fmt.Errorf("unable to execute query %s: %w", builder.String(), err)
	}

	columns, err := d.Columns(ctx, "")
	if err != nil {
		return nil, fmt.Errorf("unable to retrieve columns: %w", err)
	}

	sortedColumns := make(map[string]dialect.Columns)
	for i := range columns {
		tableName := columns[i].TableName
		sortedColumns[tableName] = append(sortedColumns[tableName], columns[i])
	}

	foreignKeys, referenceKeys, err := d.allForeignKeys(ctx)
	if err != nil {
		return nil, err
	}

	primaryKeys, err := d.allPrimaryKeys(ctx)
	if err != nil {
		return nil, err
	}

	tables := make(dialect.Tables, len(tableNames))
	for i := range tableNames {
		name := tableNames[i]
		sort.Sort(sortedColumns[name])

		tables[i] = dialect.Table{
			Name:          name,
			Columns:       sortedColumns[name],
			PrimaryKeys:   primaryKeys[name],
			ForeignKeys:   foreignKeys[name],
			ReferenceKeys: referenceKeys[name],
		}
		if tables[i].PrimaryKeys == nil {
			tables[i].PrimaryKeys = make([]dialect.PrimaryKey, 0)
		}
		if tables[i].ForeignKeys == nil {
			tables[i].ForeignKeys = make(dialect.ForeignKeys, 0)
		}
		if tables[i].ReferenceKeys == nil {
			tables[i].ReferenceKeys = make(dialect.ReferenceKeys, 0)
		}
	}

	tablesMap := make(map[string]dialect.Table, len(tables))
	for i := range tables {
		tablesMap[tables[i].Name] = tables[i]
	}

	for i := range tables {
		for j := range tables[i].ReferenceKeys {
			tables[i].ReferenceKeys[j].Table = tablesMap[tables[i].ReferenceKeys[j].TableName]
		}

		for j := range tables[i].ForeignKeys {
			tables[i].ForeignKeys[j].ReferencedTable = tablesMap[tables[i].ForeignKeys[j].ReferencedTableName]
		}
	}

	return tables, nil
}

// allForeignKeys returns, in a single query, the foreign keys and the reference keys of all the tables of the public
// schema, indexed by table name. Constraints with a table outside of the public schema are ignored, and for a
// multi-column constraint the reference key only contains the first column.
func (d *PGDialect) allForeignKeys(ctx context.Context) (fks map[string]dialect.ForeignKeys, refs map[string]dialect.ReferenceKeys, err error) {
	query := `SELECT r.conname,
       child.relname  AS child_table,
       parent.relname AS parent_table,
       pg_catalog.pg_get_constraintdef(r.oid, true) AS condef,
       (SELECT a.attname FROM pg_attribute a
         WHERE a.attrelid = r.conrelid AND a.attnum = r.conkey[1]) AS child_column
FROM pg_catalog.pg_constraint r
JOIN pg_catalog.pg_class child      ON child.oid = r.conrelid
JOIN pg_catalog.pg_namespace cn     ON cn.oid = child.relnamespace
JOIN pg_catalog.pg_class parent     ON parent.oid = r.confrelid
JOIN pg_catalog.pg_namespace pn     ON pn.oid = parent.relnamespace
WHERE r.contype = 'f' AND cn.nspname = 'public' AND pn.nspname = 'public'
ORDER BY child.relname, r.conname`

	var results []struct {
		Conname     string `db:"conname"`
		ChildTable  string `db:"child_table"`
		ParentTable string `db:"parent_table"`
		Condef      string `db:"condef"`
		ChildColumn string `db:"child_column"`
	}

	if err := d.execQuery(ctx, &results, query); err != nil {
		return nil, nil, fmt.Errorf("unable to retrieve foreign keys: %w", err)
	}

	foreignKeys := make(map[string]dialect.ForeignKeys)
	referenceKeys := make(map[string]dialect.ReferenceKeys)
	for i := range results {
		matches := fkRegexp.FindStringSubmatch(results[i].Condef)
		if matches == nil {
			return nil, nil, fmt.Errorf("unable to parse foreign key definition %s", results[i].Condef)
		}

		foreignKeys[results[i].ChildTable] = append(foreignKeys[results[i].ChildTable], dialect.ForeignKey{
			Name:                 results[i].Conname,
			Definition:           results[i].Condef,
			ColumnName:           matches[1],
			ReferencedTableName:  matches[3],
			ReferencedColumnName: matches[4],
		})

		referenceKeys[results[i].ParentTable] = append(referenceKeys[results[i].ParentTable], dialect.ReferenceKey{
			Name:       results[i].Conname,
			TableName:  results[i].ChildTable,
			ColumnName: results[i].ChildColumn,
		})
	}

	return foreignKeys, referenceKeys, nil
}

// allPrimaryKeys returns, in a single query, the primary keys of all the tables of the public schema, indexed by
// table name and ordered as in the index.
func (d *PGDialect) allPrimaryKeys(ctx context.Context) (map[string][]dialect.PrimaryKey, error) {
	query := `SELECT c.relname AS table_name, a.attname AS name,
       format_type(a.atttypid, a.atttypmod) AS data_type
FROM pg_index i
JOIN pg_class c      ON c.oid = i.indrelid
JOIN pg_namespace n  ON n.oid = c.relnamespace
JOIN pg_attribute a  ON a.attrelid = c.oid AND a.attnum = ANY(i.indkey)
WHERE i.indisprimary AND n.nspname = 'public'
ORDER BY c.relname, array_position(i.indkey::int2[], a.attnum)`

	var results []struct {
		TableName string `db:"table_name"`
		Name      string `db:"name"`
		DataType  string `db:"data_type"`
	}

	if err := d.execQuery(ctx, &results, query); err != nil {
		return nil, fmt.Errorf("unable to retrieve primary keys: %w", err)
	}

	primaryKeys := make(map[string][]dialect.PrimaryKey)
	for i := range results {
		primaryKeys[results[i].TableName] = append(primaryKeys[results[i].TableName], dialect.PrimaryKey{
			Name:      results[i].Name,
			DataType:  results[i].DataType,
			TableName: results[i].TableName,
		})
	}

	return primaryKeys, nil
}

func (d *PGDialect) execQuery(ctx context.Context, result interface{}, query string, args ...interface{}) error {
	if err := pgxscan.Select(ctx, d.conn, result, query, args...); err != nil {
		return fmt.Errorf("unable to execute query %s with args %v: %w", query, args, err)
	}

	return nil
}

func (d *PGDialect) queryRow(ctx context.Context, result interface{}, query string, args ...interface{}) error {
	if err := d.conn.QueryRow(ctx, query, args...).Scan(result); err != nil {
		return fmt.Errorf("unable to execute query %s with args %v: %w", query, args, err)
	}

	return nil
}

func (d *PGDialect) query(ctx context.Context, query string, args ...interface{}) (pgx.Rows, error) {
	rows, err := d.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("unable to execute query %s with args %v: %w", query, args, err)
	}

	return rows, nil
}

func (d *PGDialect) getTableOID(ctx context.Context, tableName string) (int64, error) {
	builder := lk.Select("c.oid").
		From(lk.Table("pg_catalog.pg_class").As("c")).
		Join(lk.Table("pg_catalog.pg_namespace").As("n"), lk.On("n.oid", "c.relnamespace"), lk.LeftJoin).
		Where(lk.Raw("pg_catalog.pg_table_is_visible(c.oid)")).
		And(lk.Condition("c.relname").Equal(tableName)).
		And(lk.Raw("c.relkind IN ('r', 'v', 'm', 'f', 'p')")).
		Comment("table oid")
	query, args := builder.Query()

	var result uint32
	if err := d.queryRow(ctx, &result, query, args...); err != nil {
		return 0, fmt.Errorf("unable to retrieve table %s oid: %w", tableName, err)
	}

	return int64(result), nil
}

func (d *PGDialect) exec(ctx context.Context, query string, args ...interface{}) error {
	if _, err := d.conn.Exec(ctx, query, args...); err != nil {
		return fmt.Errorf("unable to execute query %s with args %v: %w", query, args, err)
	}

	return nil
}

// insertQuery returns the query and the arguments inserting a row, ignoring primary key conflicts.
func insertQuery(table dialect.Table, data map[string]interface{}) (query string, args []interface{}, err error) {
	pairs, err := valuesToPairs(table, data)
	if err != nil {
		return "", nil, fmt.Errorf("unable to convert %v to pairs: %w", data, err)
	}

	// loukoum panics on the values it does not support
	defer func() {
		if r := recover(); r != nil {
			query, args, err = "", nil, fmt.Errorf("unable to build insert query on table %s%s: %v", table.Name, unsupportedColumn(pairs), r)
		}
	}()

	query, args = lk.Insert(table.Name).
		Set(pairs...).
		OnConflict(table.PrimaryKeyColumnName(), lk.DoNothing()).
		Query()

	return query, args, nil
}

// unsupportedColumn returns the first column whose value loukoum does not support, with its Go type.
func unsupportedColumn(pairs []interface{}) (found string) {
	for _, raw := range pairs {
		pair, ok := raw.(types.Pair)
		if !ok {
			continue
		}

		func() {
			defer func() {
				if r := recover(); r != nil {
					found = fmt.Sprintf(", column %v (Go type %T)", pair.Key, pair.Value)
				}
			}()

			lk.Insert("t").Set(raw)
		}()
		if found != "" {
			return found
		}
	}

	return ""
}

func (d *PGDialect) disableTriggers(ctx context.Context, tx pgx.Tx, table dialect.Table, f func(ctx context.Context) error) error {
	if _, err := tx.Exec(ctx, fmt.Sprintf("ALTER TABLE %s DISABLE TRIGGER ALL;", table.Name)); err != nil {
		return fmt.Errorf("unable to disable triggers on table %s: %w", table.Name, err)
	}

	if err := f(ctx); err != nil {
		return err
	}

	if _, err := tx.Exec(ctx, fmt.Sprintf("ALTER TABLE %s ENABLE TRIGGER ALL;", table.Name)); err != nil {
		return fmt.Errorf("unable to enable triggers on table %s: %w", table.Name, err)
	}

	return nil
}

func (d *PGDialect) resetSequence(ctx context.Context, table dialect.Table) error {
	tableSeqName := fmt.Sprintf("%s_id_seq", table.Name)

	var rawNextval interface{}
	if err := d.queryRow(ctx, &rawNextval, fmt.Sprintf("SELECT nextval('%s')", tableSeqName)); err != nil {
		return err
	}
	var rawMaxval interface{}
	if err := d.queryRow(ctx, &rawMaxval, fmt.Sprintf("SELECT MAX(%s) FROM %s", table.PrimaryKeyColumnName(), table.Name)); err != nil {
		return err
	}

	if rawMaxval != nil && rawNextval != nil {
		nextval := interfaceToInt64(rawNextval)
		maxval := interfaceToInt64(rawMaxval)

		if maxval > nextval {
			if err := d.exec(ctx, fmt.Sprintf("SELECT setval('%s', COALESCE((SELECT MAX(id)+1 FROM %s), 1), false);", tableSeqName, table.Name)); err != nil {
				return err
			}
		}
	}

	return nil
}

var _ dialect.Dialect = (*PGDialect)(nil)
