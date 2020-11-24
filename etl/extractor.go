package etl

import (
	"context"
	"fmt"
	"log"
	"strings"

	lk "github.com/ulule/loukoum/v3"

	"github.com/ulule/mover/dialect"
	"github.com/ulule/mover/etl/config"
)

type resultSet []map[string]interface{}
type extract map[string]entry
type entry map[string]resultSet

type extractor struct {
	extract      extract
	dialect      dialect.Dialect
	tables       dialect.Tables
	dependencies []config.Dependency
}

func (e *extractor) handleRow(ctx context.Context, depth int, tableName string, row map[string]interface{}) error {
	table := e.tables.Get(tableName)
	primaryKeys := table.PrimaryKeys

	foreignKeys := make(map[string]dialect.ForeignKey, len(table.ForeignKeys))
	for i := range table.ForeignKeys {
		foreignKeys[table.ForeignKeys[i].ColumnName] = table.ForeignKeys[i]
	}

	primaryKey := primaryKeys[0]
	log.Println(strings.Repeat("\t", depth), "Retrieve", fmt.Sprintf("%s = %v", primaryKey, row[primaryKey.Name]))

	for k, v := range row {
		if v == nil {
			continue
		}

		if foreignKey, ok := foreignKeys[k]; ok {
			builder := lk.Select("*").
				From(foreignKey.ReferencedTableName).
				Where(lk.Condition(foreignKey.ReferencedColumnName).Equal(v))
			query, args := builder.Query()

			log.Println(strings.Repeat("\t", depth+1), "Fetch foreign key:", k, "=>", fmt.Sprintf("%s = %v", foreignKey, v))

			if _, err := e.handle(ctx, depth+2, builder.String(), foreignKey.ReferencedTableName, query, args...); err != nil {
				return err
			}
		}
	}

	referenceKeys := make(dialect.ReferenceKeys, 0)
	queries := make([]config.Query, 0)
	if depth == 0 {
		referenceKeys = table.ReferenceKeys
	}

	for i := range e.dependencies {
		if e.dependencies[i].TableName != table.Name {
			continue
		}

		for _, referenceKey := range e.dependencies[i].ReferenceKeys {
			for j := range table.ReferenceKeys {
				if referenceKey == table.ReferenceKeys[j].Name {
					referenceKeys = append(referenceKeys, table.ReferenceKeys[j])
				}
			}
		}

		queries = e.dependencies[i].Queries
	}

	for _, referenceKey := range referenceKeys {
		value := row[primaryKey.Name]

		builder := lk.Select("*").
			From(referenceKey.TableName).
			Where(lk.Condition(referenceKey.ColumnName).Equal(value))

		query, args := builder.Query()

		log.Println(strings.Repeat("\t", depth+1), "Fetch reference key:", fmt.Sprintf("%s = %v", referenceKey, value))

		if _, err := e.handle(ctx, depth+2, builder.String(), referenceKey.TableName, query, args...); err != nil {
			return err
		}
	}

	for _, query := range queries {
		exec := fmt.Sprintf(query.Query, row[primaryKey.Name])
		log.Println(strings.Repeat("\t", depth), "Execute query", exec)

		if _, err := e.handle(ctx, depth+1, exec, query.TableName, exec); err != nil {
			return err
		}
	}

	return nil
}

func (e *extractor) Handle(ctx context.Context, tableName string, query string, args ...interface{}) (extract, error) {
	return e.handle(ctx, 0, query, tableName, query, args...)
}

func (e *extractor) handle(ctx context.Context, depth int, cacheKey string, tableName string, query string, args ...interface{}) (extract, error) {
	if _, ok := e.extract[tableName]; !ok {
		e.extract[tableName] = make(entry)
	}

	if _, ok := e.extract[tableName][cacheKey]; ok {
		log.Println(strings.Repeat("\t", depth), "Already cached")
		return nil, nil
	}

	results, err := e.dialect.ResultSet(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	e.extract[tableName][cacheKey] = results

	for i := range results {
		if err := e.handleRow(ctx, depth, tableName, results[i]); err != nil {
			return nil, err
		}
	}

	return e.extract, nil
}
