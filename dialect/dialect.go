package dialect

import (
	"context"
	"fmt"
)

// Tables contains a set of tables.
type Tables []Table

// Get returns a Table with its name.
func (t Tables) Get(tableName string) Table {
	for i := range t {
		if t[i].Name == tableName {
			return t[i]
		}
	}

	return Table{}
}

// Table contains the definition of a database table.
type Table struct {
	Name          string
	PrimaryKeys   []PrimaryKey
	Columns       Columns
	ForeignKeys   ForeignKeys
	ReferenceKeys ReferenceKeys
}

// PrimaryKeyColumnName returns the primary key column name.
func (t Table) PrimaryKeyColumnName() string {
	return t.PrimaryKeys[0].Name
}

// Columns contains a set of columns.
type Columns []Column

// Get returns a Column with its name.
func (c Columns) Get(name string) Column {
	for i := range c {
		if c[i].Name == name {
			return c[i]
		}
	}

	return Column{}
}

func (c Columns) Len() int           { return len(c) }
func (c Columns) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }
func (c Columns) Less(i, j int) bool { return c[i].Position < c[j].Position }

// Column contains the definition of a column table.
type Column struct {
	Name      string
	Nullable  bool
	DataType  string
	TableName string
	Position  int64
}

// PrimaryKey contains the defintiion of a primary key column.
type PrimaryKey struct {
	Name      string
	DataType  string
	TableName string
}

// String returns the string representation of a Primarykey.
func (f PrimaryKey) String() string {
	return fmt.Sprintf("%s(%s)", f.TableName, f.Name)
}

// ForeignKey contains the definition of a foreign key column.
type ForeignKey struct {
	Name                 string
	Definition           string
	ColumnName           string
	ReferencedTableName  string
	ReferencedTable      Table
	ReferencedColumnName string
}

// String returns the string representation of a ForeignKey.
func (f ForeignKey) String() string {
	return fmt.Sprintf("%s(%s)", f.ReferencedTable.Name, f.ReferencedColumnName)
}

// ForeignKeys contains a set of ForeignKey.
type ForeignKeys []ForeignKey

// ReferenceKey contains the definition of a reference key.
type ReferenceKey struct {
	Name       string
	Table      Table
	TableName  string
	ColumnName string
}

// String returns the string representation of a ReferenceKey.
func (f ReferenceKey) String() string {
	return fmt.Sprintf("%s(%s)", f.Table.Name, f.ColumnName)
}

// ReferenceKeys contains a set of ReferenceKey.
type ReferenceKeys []ReferenceKey

// Dialect is the main interface to interact with RDMS.
type Dialect interface {
	Close(context.Context) error
	ReferenceKeys(context.Context, string) (ReferenceKeys, error)
	ForeignKeys(context.Context, string) (ForeignKeys, error)
	PrimaryKeyConstraint(context.Context, string) (string, error)
	Tables(context.Context) (Tables, error)
	Table(context.Context, string) (Table, error)
	Columns(context.Context, string) ([]Column, error)
	BulkInsert(context.Context, Table, []map[string]interface{}) error
	ResultSet(context.Context, string, ...interface{}) ([]map[string]interface{}, error)
}
