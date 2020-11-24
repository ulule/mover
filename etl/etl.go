package etl

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"

	"go.uber.org/zap"

	lk "github.com/ulule/loukoum/v3"
	"github.com/ulule/mover/config"
	"github.com/ulule/mover/dialect"
	"github.com/ulule/mover/dialect/postgres"
)

// Engine extracts and loads data from database with specific dialect.
type Engine struct {
	schema  map[string]config.Schema
	dialect dialect.Dialect
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
	etl := &Engine{
		config: cfg,
		logger: logger,
	}

	dialect, err := etl.newDialect(ctx, dsn)
	if err != nil {
		return nil, err
	}

	etl.dialect = dialect

	tables, err := dialect.Tables(ctx)
	if err != nil {
		return nil, err
	}

	schema := make(map[string]config.Schema, len(tables))
	for i := range tables {
		tableName := tables[i].Name
		found := false
		for j := range cfg.Schema {
			if tables[i].Name == cfg.Schema[j].TableName {
				found = true
				cfg.Schema[j].Table = tables[i]
				schema[tableName] = cfg.Schema[j]

			}
		}

		if !found {
			schema[tableName] = config.Schema{
				TableName: tables[i].Name,
				Table:     tables[i],
			}
		}

		for k := range schema[tableName].Queries {
			for j := range tables {
				if tables[j].Name == schema[tableName].Queries[k].TableName {
					schema[tableName].Queries[k].Table = tables[j]
				}
			}
		}
	}

	etl.schema = schema

	return etl, nil
}

// Describe returns a table from its name.
func (e *Engine) Describe(ctx context.Context, tableName string) (dialect.Table, error) {
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
func (e *Engine) Extract(ctx context.Context, outputPath string, tableName string, query string) error {
	extractor := e.newExtractor()

	cache, err := extractor.Handle(ctx, e.schema[tableName], query)
	if err != nil {
		return fmt.Errorf("unable to extract %s (query %s): %w", tableName, query, err)
	}

	for i := range e.config.Extra {
		tableName := e.config.Extra[i].TableName
		query, _ := lk.Select("*").
			From(tableName).Query()
		cache, err = extractor.Handle(ctx, e.schema[tableName], query)
		if err != nil {
			return fmt.Errorf("unable to extract %s (query %s): %w", tableName, query, err)
		}
	}

	for tableName := range cache {
		if err := e.extract(ctx, outputPath, e.schema[tableName], cache[tableName]); err != nil {
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
	if err := ioutil.WriteFile(filePath, output, os.ModePerm); err != nil {
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

		if err := downloadFiles(ctx, filenames, path.Join(outputPath, "media")); err != nil {
			e.logger.Error("unable to download files", zap.Error(err))
		}
	}

	return nil
}

func (e *Engine) newDialect(ctx context.Context, dsn string) (dialect.Dialect, error) {
	return postgres.NewPGDialect(ctx, dsn)
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
		extract: make(extract),
		schema:  e.schema,
		dialect: e.dialect,
		logger:  e.logger,
	}
}

func (e *Engine) newSanitizer() *sanitizer {
	return newSanitizer(e.config.Locale, e.schema)
}
