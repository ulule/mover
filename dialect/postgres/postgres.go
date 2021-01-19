package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"sort"

	"github.com/georgysavva/scany/pgxscan"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	lk "github.com/ulule/loukoum/v3"

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

// BulkInsert inserts multiple data a single database transaction. It disables triggers to avoid conflicts on
// foreign constraints.
func (d *PGDialect) BulkInsert(ctx context.Context, table dialect.Table, data []map[string]interface{}) error {
	var err error

	tx, err := d.conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("unable to begin transaction on table %s: %w", table.Name, err)
	}

	defer func() {
		err = tx.Rollback(ctx)
	}()

	if err := d.disableTriggers(ctx, table, func(ctx context.Context) error {
		for i := range data {
			if err := d.insert(ctx, table, data[i]); err != nil {
				return err
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

	return err
}

// ReferenceKeys returns the "Referenced by" constraints of a table.
func (d *PGDialect) ReferenceKeys(ctx context.Context, tableName string) (dialect.ReferenceKeys, error) {
	oid, err := d.getTableOID(ctx, tableName)
	if err != nil {
		return nil, err
	}

	builder := lk.Select(
		"conname",
		"c2.relname AS table",
		"(SELECT attname FROM pg_attribute WHERE attrelid = r.conrelid AND ARRAY[attnum] <@ r.conkey) AS column",
	).From("pg_constraint r, pg_class c, pg_class c2").
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

	builder := lk.Select("r.conname", "pg_catalog.pg_get_constraintdef(r.oid, true) AS condef").
		From("pg_catalog.pg_constraint r, pg_namespace n, pg_class c").
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
		From("pg_catalog.pg_constraint r").
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

	builder := lk.Select("pg_attribute.attname AS name", "format_type(pg_attribute.atttypid, pg_attribute.atttypmod) AS data_type").
		From("pg_index, pg_class, pg_attribute, pg_namespace").
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
		"a.attname AS column_name",
		"pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type",
		`(
    SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid)
    FROM pg_catalog.pg_attrdef d
    WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum
    AND a.atthasdef
  ) AS default`,
		"a.attnotnull AS is_nullable",
		"c.relname AS table_name",
		"a.attnum as ordinal_position",
	).
		From("pg_catalog.pg_attribute a").
		Join("pg_catalog.pg_class c", lk.On("a.attrelid", "c.oid"), lk.LeftJoin).
		Join("pg_catalog.pg_description pgd", lk.On("pgd.objoid = a.attrelid AND pgd.objsubid", "a.attnum"), lk.LeftJoin).
		Where(lk.Condition("a.attnum").GreaterThan(0)).
		And(lk.Condition("a.attisdropped").Equal(false)).
		OrderBy(lk.Order("a.attnum"))

	if tableName != "" {
		oid, err := d.getTableOID(ctx, tableName)
		if err != nil {
			return nil, err
		}

		builder = builder.Where(lk.Condition("a.attrelid").Equal(oid))
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

// Tables returns all the tables from the database.
func (d *PGDialect) Tables(ctx context.Context) (dialect.Tables, error) {
	builder := lk.Select("c.relname").
		From("pg_catalog.pg_class c").
		Join("pg_namespace n", lk.On("n.oid", "c.relnamespace")).
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
		_, ok := sortedColumns[tableName]
		if !ok {
			sortedColumns[tableName] = make(dialect.Columns, 0)
		}
		sortedColumns[tableName] = append(sortedColumns[tableName], columns[i])
	}

	tables := make(dialect.Tables, len(tableNames))
	for i := range tableNames {
		sort.Sort(sortedColumns[tableNames[i]])
		tables[i] = dialect.Table{
			Name:    tableNames[i],
			Columns: sortedColumns[tableNames[i]],
		}
		tables[i].ReferenceKeys, err = d.ReferenceKeys(ctx, tableNames[i])
		if err != nil {
			return nil, err
		}

		tables[i].ForeignKeys, err = d.ForeignKeys(ctx, tableNames[i])
		if err != nil {
			return nil, err
		}

		tables[i].PrimaryKeys, err = d.PrimaryKeys(ctx, tableNames[i])
		if err != nil {
			return nil, err
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
		From("pg_catalog.pg_class c").
		Join(lk.Table("pg_catalog.pg_namespace n"), lk.On("n.oid", "c.relnamespace"), lk.LeftJoin).
		Where(lk.Condition("pg_catalog.pg_table_is_visible(c.oid)")).
		And(lk.Condition("c.relname").Equal(tableName)).
		And(lk.Raw("c.relkind IN ('r', 'v', 'm', 'f', 'p')")).
		Comment("table oid")
	query, args := builder.Query()

	var result pgtype.OID
	if err := d.queryRow(ctx, &result, query, args...); err != nil {
		return 0, fmt.Errorf("unable to retrieve table %s oid: %w", tableName, err)
	}

	raw, err := result.Value()
	if err != nil {
		return 0, fmt.Errorf("unable to retrieve table %s oid from driver value: %w", tableName, err)
	}

	val, ok := raw.(int64)
	if ok {
		return val, nil
	}

	return 0, fmt.Errorf("unable to cast %v to int64", val)
}

func (d *PGDialect) exec(ctx context.Context, query string, args ...interface{}) error {
	if _, err := d.conn.Exec(ctx, query, args...); err != nil {
		return fmt.Errorf("unable to execute query %s with args %v: %w", query, args, err)
	}

	return nil
}

func (d *PGDialect) insert(ctx context.Context, table dialect.Table, data map[string]interface{}) error {
	pairs, err := valuesToPairs(table, data)
	if err != nil {
		return fmt.Errorf("unable to convert %v to pairs: %w", data, err)
	}

	query, args := lk.Insert(table.Name).
		Set(pairs...).
		OnConflict(table.PrimaryKeyColumnName(), lk.DoUpdate(pairs...)).
		Query()
	if err := d.exec(ctx, query, args...); err != nil {
		return fmt.Errorf("unable to insert %v+ to %s:%w", pairs, table.Name, err)
	}

	return nil
}

func (d *PGDialect) disableTriggers(ctx context.Context, table dialect.Table, f func(ctx context.Context) error) error {
	if err := d.exec(ctx, fmt.Sprintf("ALTER TABLE %s DISABLE TRIGGER ALL;", table.Name)); err != nil {
		return err
	}

	if err := f(ctx); err != nil {
		return err
	}

	if err := d.exec(ctx, fmt.Sprintf("ALTER TABLE %s ENABLE TRIGGER ALL;", table.Name)); err != nil {
		return err
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
