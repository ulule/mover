package etl

import (
	"context"
	"fmt"
	"strings"

	lk "github.com/ulule/loukoum/v3"
	"go.uber.org/zap"

	"github.com/ulule/mover/config"
	"github.com/ulule/mover/dialect"
)

type (
	resultSet []map[string]interface{}
	extract   map[string]entry
	entry     map[string]resultSet
	extractor struct {
		extract            extract
		dialect            dialect.Dialect
		schema             map[string]config.Schema
		logger             *zap.Logger
		processedRelations map[string]struct{}
	}
)

func depthF(depth int, msg string) string {
	return strings.Repeat("\t", depth+1) + msg
}

func (e *extractor) handleReferenceKeys(ctx context.Context, depth int, table dialect.Table, row map[string]interface{}) error {
	var (
		referenceKeys = make(dialect.ReferenceKeys, 0)
		primaryKeys   = table.PrimaryKeys
		primaryKey    = primaryKeys[0]
		schema        = e.schema[table.Name]
	)

	if depth == 0 && !schema.OmitReferenceKeys {
		referenceKeys = table.ReferenceKeys
	}

	for _, referenceKey := range schema.ReferenceKeys {
		for j := range table.ReferenceKeys {
			if referenceKey == table.ReferenceKeys[j].Name {
				referenceKeys = append(referenceKeys, table.ReferenceKeys[j])
			}
		}
	}

	for i := range referenceKeys {
		value := row[primaryKey.Name]
		referenceKey := referenceKeys[i]

		query, args := lk.Select(lk.Raw("*")).
			From(referenceKey.Table.Name).
			Where(lk.Condition(referenceKey.ColumnName).Equal(value)).
			Query()

		e.logger.Debug(depthF(depth+1, fmt.Sprintf("Fetch reference key %s = %v", referenceKey, value)),
			zap.String("table_name", table.Name),
		)

		if _, err := e.handle(ctx, depth+2, e.schema[referenceKey.Table.Name], query, args...); err != nil {
			return fmt.Errorf("unable to handle table %s (query: %s, args: %v): %w", referenceKey.Table.Name, query, args, err)
		}
	}

	for i := range schema.Queries {
		query := schema.Queries[i]
		exec := replaceVar(query.Query, row)
		e.logger.Debug(depthF(depth+1, "Execute query"),
			zap.String("query", exec))

		if _, err := e.handle(ctx, depth+1, e.schema[query.Table.Name], exec); err != nil {
			return fmt.Errorf("unable to handle table %s (query %s): %w", query.Table.Name, exec, err)
		}
	}

	return nil
}

func (e *extractor) handleRow(ctx context.Context, depth int, table dialect.Table, row map[string]interface{}) error {
	var (
		primaryKeys = table.PrimaryKeys
		foreignKeys = make(map[string]dialect.ForeignKey, len(table.ForeignKeys))
	)

	for i := range table.ForeignKeys {
		foreignKeys[table.ForeignKeys[i].ColumnName] = table.ForeignKeys[i]
	}

	primaryKey := primaryKeys[0]

	relationKey := fmt.Sprintf("%s = %v", primaryKey, row[primaryKey.Name])

	if _, ok := e.processedRelations[relationKey]; ok {
		e.logger.Debug(depthF(depth, fmt.Sprintf("Relation %s already processed", relationKey)))
		return nil
	}

	e.processedRelations[relationKey] = struct{}{}
	e.logger.Debug(depthF(depth, fmt.Sprintf("Retrieve relation %s", relationKey)))

	for k, v := range row {
		if v == nil {
			continue
		}

		if foreignKey, ok := foreignKeys[k]; ok {
			foreignRelationKey := fmt.Sprintf("%s = %v", foreignKey, v)

			if _, ok := e.processedRelations[foreignRelationKey]; ok {
				e.logger.Debug(depthF(depth, fmt.Sprintf("Foreign relation %s already processed", foreignRelationKey)))
				continue
			}

			e.logger.Debug(depthF(depth+1, fmt.Sprintf("Fetch foreign key %s = %v", foreignKey, v)))
			query, args := lk.Select(lk.Raw("*")).
				From(foreignKey.ReferencedTable.Name).
				Where(lk.Condition(foreignKey.ReferencedColumnName).Equal(v)).
				Query()

			if _, err := e.handle(ctx, depth+2, e.schema[foreignKey.ReferencedTable.Name], query, args...); err != nil {
				return fmt.Errorf("unable to handle table %s from foreign key %s: %w", foreignKey.ReferencedTable.Name, foreignKey.Name, err)
			}
		}
	}

	if err := e.handleReferenceKeys(ctx, depth, table, row); err != nil {
		return err
	}

	return nil
}

func (e *extractor) Handle(ctx context.Context, schema config.Schema, query string, args ...interface{}) (extract, error) {
	return e.handle(ctx, 0, schema, query, args...)
}

func (e *extractor) handle(ctx context.Context, depth int, schema config.Schema, query string, args ...interface{}) (extract, error) {
	var (
		table     = schema.Table
		tableName = table.Name
		cacheKey  = cacheKey(query, args)
	)

	if _, ok := e.extract[tableName]; !ok {
		e.extract[tableName] = make(entry)
	}

	if _, ok := e.extract[tableName][cacheKey]; ok {
		e.logger.Debug(depthF(depth, "Already cached"))
		return nil, nil
	}

	results, err := e.dialect.ResultSet(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("unable to retrieve results: %w", err)
	}

	e.logger.Debug(depthF(depth, fmt.Sprintf("-> %d results", len(results))))

	e.extract[tableName][cacheKey] = results

	for i := range results {
		if err := e.handleRow(ctx, depth, table, results[i]); err != nil {
			return nil, fmt.Errorf("unable to handle row %v from table %s: %w", table, results[i], err)
		}
	}

	return e.extract, nil
}
