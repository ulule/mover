package dialect

import (
	"context"
	"fmt"
)

type Tables []Table

func (t Tables) Get(tableName string) Table {
	for i := range t {
		if t[i].Name == tableName {
			return t[i]
		}
	}

	return Table{}
}

type Table struct {
	Name          string
	PrimaryKeys   []PrimaryKey
	Columns       Columns
	ForeignKeys   ForeignKeys
	ReferenceKeys ReferenceKeys
}

type Columns []Column

func (c Columns) Get(name string) Column {
	for i := range c {
		if c[i].Name == name {
			return c[i]
		}
	}

	return Column{}
}

func (a Columns) Len() int           { return len(a) }
func (a Columns) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a Columns) Less(i, j int) bool { return a[i].Position < a[j].Position }

type Column struct {
	Name      string
	Nullable  bool
	DataType  string
	TableName string
	Position  int64
}

type PrimaryKey struct {
	Name      string
	DataType  string
	TableName string
}

func (f PrimaryKey) String() string {
	return fmt.Sprintf("%s(%s)", f.TableName, f.Name)
}

type ForeignKey struct {
	Name                 string
	Definition           string
	ColumnName           string
	ReferencedTableName  string
	ReferencedColumnName string
}

func (f ForeignKey) String() string {
	return fmt.Sprintf("%s(%s)", f.ReferencedTableName, f.ReferencedColumnName)
}

type ForeignKeys []ForeignKey

type ReferenceKey struct {
	Name       string
	TableName  string
	ColumnName string
}

func (f ReferenceKey) String() string {
	return fmt.Sprintf("%s(%s)", f.TableName, f.ColumnName)
}

type ReferenceKeys []ReferenceKey

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
