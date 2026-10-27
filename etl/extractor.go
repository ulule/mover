package etl

import (
	"context"
	"fmt"
	"sort"

	"go.uber.org/zap"

	"github.com/ulule/mover/config"
	"github.com/ulule/mover/dialect"
)

type (
	resultSet []map[string]interface{}
	extract   map[string]entry
	entry     map[string]resultSet
	// fetchKey identifies a column of a table to look values up in.
	fetchKey struct{ table, column string }
	// rawQuery is a query from the configuration, executed as is.
	rawQuery struct{ table, query string }
	// pending contains what the next level has to fetch.
	pending struct {
		in  map[fetchKey][]interface{}
		raw []rawQuery
		// rawSeen avoids executing twice the same query in a level.
		rawSeen map[rawQuery]struct{}
	}
	extractor struct {
		extract   extract
		dialect   dialect.Dialect
		schema    map[string]config.Schema
		logger    *zap.Logger
		processed map[string]struct{}
		// fetched contains the values already requested, by table and column.
		fetched map[fetchKey]map[string]struct{}
		// queries counts the calls to the dialect.
		queries int
		batches int
	}
)

func newPending() *pending {
	return &pending{
		in:      make(map[fetchKey][]interface{}),
		rawSeen: make(map[rawQuery]struct{}),
	}
}

func (p *pending) empty() bool {
	return len(p.in) == 0 && len(p.raw) == 0
}

// keys returns the fetch keys in a deterministic order.
func (p *pending) keys() []fetchKey {
	keys := make([]fetchKey, 0, len(p.in))
	for key := range p.in {
		keys = append(keys, key)
	}

	sort.Slice(keys, func(i, j int) bool {
		if keys[i].table != keys[j].table {
			return keys[i].table < keys[j].table
		}

		return keys[i].column < keys[j].column
	})

	return keys
}

// request adds a value to look up at the next level, unless it has already been requested.
func (e *extractor) request(next *pending, key fetchKey, value interface{}) {
	id := fmt.Sprintf("%v", value)

	values, ok := e.fetched[key]
	if !ok {
		values = make(map[string]struct{})
		e.fetched[key] = values
	}

	if _, ok := values[id]; ok {
		return
	}

	values[id] = struct{}{}
	next.in[key] = append(next.in[key], value)
}

// visit applies the relations rules to rows of a table, and accumulates in next what has to be fetched.
func (e *extractor) visit(rows resultSet, table dialect.Table, root bool, next *pending) {
	var (
		schema        = e.schema[table.Name]
		primaryKey    = table.PrimaryKeys[0]
		foreignKeys   = make(map[string]dialect.ForeignKey, len(table.ForeignKeys))
		referenceKeys = make(dialect.ReferenceKeys, 0)
	)

	for i := range table.ForeignKeys {
		foreignKeys[table.ForeignKeys[i].ColumnName] = table.ForeignKeys[i]
	}

	if root && !schema.OmitReferenceKeys {
		referenceKeys = append(referenceKeys, table.ReferenceKeys...)
	}

	for _, name := range schema.ReferenceKeys {
		for j := range table.ReferenceKeys {
			if name == table.ReferenceKeys[j].Name {
				referenceKeys = append(referenceKeys, table.ReferenceKeys[j])
			}
		}
	}

	for _, row := range rows {
		relationKey := fmt.Sprintf("%s = %v", primaryKey, row[primaryKey.Name])
		if _, ok := e.processed[relationKey]; ok {
			continue
		}

		e.processed[relationKey] = struct{}{}

		for i := range table.ForeignKeys {
			foreignKey := foreignKeys[table.ForeignKeys[i].ColumnName]

			v := row[foreignKey.ColumnName]
			if v == nil {
				continue
			}

			if _, ok := e.processed[fmt.Sprintf("%s = %v", foreignKey, v)]; ok {
				continue
			}

			e.request(next, fetchKey{foreignKey.ReferencedTable.Name, foreignKey.ReferencedColumnName}, v)
		}

		for i := range referenceKeys {
			e.request(next, fetchKey{referenceKeys[i].Table.Name, referenceKeys[i].ColumnName}, row[primaryKey.Name])
		}

		for i := range schema.Queries {
			raw := rawQuery{
				table: schema.Queries[i].Table.Name,
				query: replaceVar(schema.Queries[i].Query, row),
			}

			if _, ok := next.rawSeen[raw]; ok {
				continue
			}

			next.rawSeen[raw] = struct{}{}
			next.raw = append(next.raw, raw)
		}
	}
}

// Handle executes the query as a root, then extracts the relations level by level.
func (e *extractor) Handle(ctx context.Context, schema config.Schema, query string, args ...interface{}) error {
	var (
		table     = schema.Table
		tableName = table.Name
		cacheKey  = cacheKey(query, args...)
	)

	if _, ok := e.extract[tableName]; !ok {
		e.extract[tableName] = make(entry)
	}

	if _, ok := e.extract[tableName][cacheKey]; ok {
		e.logger.Debug("Already cached", zap.String("table", tableName))
		return nil
	}

	results, err := e.dialect.ResultSet(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("unable to retrieve results: %w", err)
	}

	e.queries++
	e.extract[tableName][cacheKey] = results

	e.logger.Debug("Root query", zap.String("table", tableName), zap.Int("rows", len(results)))

	next := newPending()
	e.visit(results, table, true, next)

	for level := 1; !next.empty(); level++ {
		var err error

		if next, err = e.fetchLevel(ctx, level, next); err != nil {
			return err
		}
	}

	return nil
}

// fetchLevel fetches what is pending and returns what the next level has to fetch.
func (e *extractor) fetchLevel(ctx context.Context, level int, current *pending) (*pending, error) {
	next := newPending()

	for _, key := range current.keys() {
		var (
			values = current.in[key]
			table  = e.schema[key.table].Table
		)

		rows, err := e.dialect.SelectIn(ctx, table, key.column, values)
		if err != nil {
			return nil, fmt.Errorf("unable to handle table %s from column %s: %w", key.table, key.column, err)
		}

		e.queries++
		e.batches++

		if _, ok := e.extract[key.table]; !ok {
			e.extract[key.table] = make(entry)
		}

		e.extract[key.table][fmt.Sprintf("%s.%s IN batch#%d", key.table, key.column, e.batches)] = rows

		e.logger.Debug("Fetch level",
			zap.Int("level", level),
			zap.String("table", key.table),
			zap.String("column", key.column),
			zap.Int("values", len(values)),
			zap.Int("rows", len(rows)))

		e.visit(rows, table, false, next)
	}

	for _, raw := range current.raw {
		var (
			table    = e.schema[raw.table].Table
			cacheKey = cacheKey(raw.query)
		)

		if _, ok := e.extract[raw.table]; !ok {
			e.extract[raw.table] = make(entry)
		}

		if _, ok := e.extract[raw.table][cacheKey]; ok {
			e.logger.Debug("Already cached", zap.String("table", raw.table))
			continue
		}

		rows, err := e.dialect.ResultSet(ctx, raw.query)
		if err != nil {
			return nil, fmt.Errorf("unable to handle table %s (query %s): %w", raw.table, raw.query, err)
		}

		e.queries++
		e.extract[raw.table][cacheKey] = rows

		e.logger.Debug("Execute query",
			zap.Int("level", level),
			zap.String("table", raw.table),
			zap.String("query", raw.query),
			zap.Int("rows", len(rows)))

		e.visit(rows, table, false, next)
	}

	return next, nil
}

// logSummary logs the number of queries and of rows by table.
func (e *extractor) logSummary() {
	rows := make(map[string]int, len(e.extract))
	for tableName := range e.extract {
		for _, results := range e.extract[tableName] {
			rows[tableName] += len(results)
		}
	}

	e.logger.Info("Extraction done",
		zap.Int("queries", e.queries),
		zap.Any("rows", rows))
}
