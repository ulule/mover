package etl

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"

	"github.com/jackc/pgx/v4"

	"github.com/ulule/mover/dialect"
	"github.com/ulule/mover/dialect/postgres"
	"github.com/ulule/mover/etl/config"
)

func NewEngine(ctx context.Context, cfg config.Config, dsn string) (*Engine, error) {
	etl := &Engine{
		config: cfg,
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

	etl.tables = tables

	return etl, nil
}

type Engine struct {
	path    string
	conn    *pgx.Conn
	tables  dialect.Tables
	dialect dialect.Dialect
	config  config.Config
}

type jsonPayload struct {
	TableName string                   `json:"table_name"`
	Count     int                      `json:"count"`
	Data      []map[string]interface{} `json:"data"`
}

func (e *Engine) newDialect(ctx context.Context, dsn string) (dialect.Dialect, error) {
	return postgres.NewPGDialect(ctx, dsn)
}

func (e *Engine) newExtractor() *extractor {
	return &extractor{
		extract:      make(extract),
		dependencies: e.config.Dependencies,
		dialect:      e.dialect,
		tables:       e.tables,
	}
}

func (e *Engine) Load(ctx context.Context, outputPath string) error {
	var files []string

	if err := filepath.Walk(outputPath, func(path string, info os.FileInfo, err error) error {
		files = append(files, path)
		return nil
	}); err != nil {
		return err
	}

	for _, file := range files[1:] {
		log.Println("Load file", file)

		if err := e.loadFile(ctx, file); err != nil {
			return err
		}
	}

	return nil
}

func (e *Engine) loadFile(ctx context.Context, filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	content, err := ioutil.ReadAll(file)
	if err != nil {
		return err
	}

	var payload jsonPayload
	if err := json.Unmarshal(content, &payload); err != nil {
		return err
	}

	return e.loadJSON(ctx, payload)
}

func (e *Engine) loadJSON(ctx context.Context, payload jsonPayload) error {
	table := e.tables.Get(payload.TableName)
	return e.dialect.BulkInsert(ctx, table, payload.Data)
}

func (e *Engine) Extract(ctx context.Context, outputPath string, tableName string, query string) error {
	cache, err := e.newExtractor().Handle(ctx, tableName, query)
	if err != nil {
		return err
	}

	for tableName := range cache {
		table := e.tables.Get(tableName)
		primaryKeys := table.PrimaryKeys
		index := make(map[interface{}]struct{})

		results := make([]map[string]interface{}, 0)
		for _, values := range cache[tableName] {
			for j := range values {
				value := values[j]
				primaryKey := value[primaryKeys[0].Name]
				if _, ok := index[primaryKey]; ok {
					continue
				}

				results = append(results, value)
				index[primaryKey] = struct{}{}
			}
		}

		payload := jsonPayload{
			TableName: tableName,
			Count:     len(results),
			Data:      results,
		}

		output, err := json.MarshalIndent(payload, "", "\t")
		if err != nil {
			return err
		}

		path := path.Join(outputPath, fmt.Sprintf("%s.json", tableName))
		if err := ioutil.WriteFile(path, output, os.ModePerm); err != nil {
			return err
		}

		log.Println("Export", len(results), "results from", tableName, "to", path)
	}

	return nil
}

func (e *Engine) Shutdown(ctx context.Context) error {
	return e.dialect.Close(ctx)
}
