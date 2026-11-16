package etl

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"strings"

	"go.uber.org/zap"

	lk "github.com/ulule/loukoum/v3"
	"github.com/ulule/mover/config"
	dialectpkg "github.com/ulule/mover/dialect"
	"github.com/ulule/mover/dialect/postgres"
)

// copySchemaTables copies tables from database to schema configuration.
func copySchemaTables(schema []config.Schema, tables []dialectpkg.Table) map[string]config.Schema {
	schemas := make(map[string]config.Schema, len(tables))
	for i := range tables {
		tableName := tables[i].Name
		found := false
		for j := range schema {
			if tables[i].Name == schema[j].TableName {
				found = true
				schema[j].Table = tables[i]
				schemas[tableName] = schema[j]
			}
		}

		if !found {
			schemas[tableName] = config.Schema{
				TableName: tables[i].Name,
				Table:     tables[i],
			}
		}

		for k := range schemas[tableName].Queries {
			for j := range tables {
				if tables[j].Name == schemas[tableName].Queries[k].TableName {
					schemas[tableName].Queries[k].Table = tables[j]
				}
			}
		}
	}

	return schemas
}

// warnUnknownTables logs the schema and extra entries whose table does not exist in the database.
func warnUnknownTables(logger *zap.Logger, cfg config.Config, tables []dialectpkg.Table) {
	known := make(map[string]struct{}, len(tables))
	for i := range tables {
		known[tables[i].Name] = struct{}{}
	}

	sections := []struct {
		name    string
		entries []config.Schema
	}{{"schema", cfg.Schema}, {"extra", cfg.Extra}}

	for _, section := range sections {
		for i := range section.entries {
			if _, ok := known[section.entries[i].TableName]; !ok {
				logger.Warn("Unknown table in configuration",
					zap.String("section", section.name),
					zap.String("table", section.entries[i].TableName))
			}
		}
	}
}

// Engine extracts and loads data from database with specific dialect.
type Engine struct {
	schema  map[string]config.Schema
	dialect dialectpkg.Dialect
	config  config.Config
	logger  *zap.Logger
}

type jsonPayload struct {
	TableName string                   `json:"table_name"`
	Count     int                      `json:"count"`
	Data      []map[string]interface{} `json:"data"`
}

// NewEngine returns a new Engine instance.
func NewEngine(ctx context.Context, cfg config.Config, dsn string, logger *zap.Logger) (*Engine, error) {
	dialect, err := postgres.NewPGDialect(ctx, dsn)
	if err != nil {
		return nil, err
	}

	tables, err := dialect.Tables(ctx)
	if err != nil {
		return nil, err
	}

	warnUnknownTables(logger, cfg, tables)

	schema := copySchemaTables(cfg.Schema, tables)

	return &Engine{
		config:  cfg,
		logger:  logger,
		dialect: dialect,
		schema:  schema,
	}, nil
}

// Describe returns a table from its name.
func (e *Engine) Describe(ctx context.Context, tableName string) (dialectpkg.Table, error) {
	schema, ok := e.schema[tableName]
	if !ok {
		return schema.Table, fmt.Errorf("table %s does not exist", tableName)
	}

	return schema.Table, nil
}

// Load loads data from an output directory.
func (e *Engine) Load(ctx context.Context, outputPath string) error {
	return e.newLoader().Load(ctx, outputPath)
}

// Extract extracts data to an output directory with a table name and its query.
func (e *Engine) Extract(ctx context.Context, outputPath, query string) error {
	extractor := e.newExtractor()

	tableName := getQueryTable(query)
	if tableName == "" {
		return fmt.Errorf("unable to retrieve table from query: %s", query)
	}

	if err := extractor.Handle(ctx, e.schema[tableName], query); err != nil {
		return fmt.Errorf("unable to extract %s (query %s): %w", tableName, query, err)
	}

	for i := range e.config.Extra {
		tableName := e.config.Extra[i].TableName
		if e.schema[tableName].Exclude {
			e.logger.Warn("Skip excluded extra table", zap.String("table", tableName))
			continue
		}

		query, _ := lk.Select(lk.Raw("*")).
			From(tableName).Query()
		if err := extractor.Handle(ctx, e.schema[tableName], query); err != nil {
			return fmt.Errorf("unable to extract %s (query %s): %w", tableName, query, err)
		}
	}

	extractor.logSummary()

	for tableName := range extractor.extract {
		// safety net: an excluded table must never be written
		if e.schema[tableName].Exclude {
			continue
		}

		if err := e.extract(ctx, outputPath, e.schema[tableName], extractor.extract[tableName]); err != nil {
			return fmt.Errorf("unable to extract rows from table %s: %w", tableName, err)
		}
	}

	return nil
}

// Shutdown shutdowns the Engine.
func (e *Engine) Shutdown(ctx context.Context) error {
	return e.dialect.Close(ctx)
}

func (e *Engine) extract(ctx context.Context, outputPath string, schema config.Schema, rows entry) error {
	var (
		table   = schema.Table
		results = e.newSanitizer().sanitize(table, rows)
		payload = jsonPayload{
			TableName: table.Name,
			Count:     len(results),
			Data:      results,
		}
	)

	output, err := json.MarshalIndent(payload, "", "\t")
	if err != nil {
		return fmt.Errorf("unable to encode in JSON: %w", err)
	}

	filePath := path.Join(outputPath, table.Name+extensionFormat)
	if err := os.WriteFile(filePath, output, 0644); err != nil {
		return fmt.Errorf("unable to write JSON output to %s: %w", filePath, err)
	}

	e.logger.Info(fmt.Sprintf("Export %d results", len(results)),
		zap.String("table", table.Name),
		zap.String("path", filePath))

	filenames := extractFilenames(schema, rows)
	if len(filenames) > 0 {
		e.logger.Debug("Download files",
			zap.String("files", strings.Join(filenames, " ")),
			zap.String("output_path", outputPath))

		if err := downloadFiles(ctx, filenames, path.Join(outputPath, "media"), 10); err != nil {
			e.logger.Error("unable to download files", zap.Error(err))
		}
	}

	return nil
}

func (e *Engine) newLoader() *loader {
	return &loader{
		dialect: e.dialect,
		schema:  e.schema,
		logger:  e.logger,
	}
}

func (e *Engine) newExtractor() *extractor {
	return &extractor{
		extract:   make(extract),
		schema:    e.schema,
		dialect:   e.dialect,
		logger:    e.logger,
		processed: make(map[string]struct{}),
		fetched:   make(map[fetchKey]map[string]struct{}),
	}
}

func (e *Engine) newSanitizer() *sanitizer {
	return newSanitizer(e.config.Locale, e.schema)
}
