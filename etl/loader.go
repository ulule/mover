package etl

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"go.uber.org/zap"

	"github.com/ulule/mover/config"
	"github.com/ulule/mover/dialect"
)

type loader struct {
	dialect dialect.Dialect
	schema  map[string]config.Schema
	logger  *zap.Logger
}

// Load loads data from an output directory.
func (l *loader) Load(ctx context.Context, outputPath string) error {
	var files []string

	l.logger.Info("Loading files from directory", zap.String("output_path", outputPath))

	if _, err := os.Stat(outputPath); os.IsNotExist(err) {
		return fmt.Errorf("unable to open directory %s: %w", outputPath, err)
	}

	if err := filepath.Walk(outputPath, func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() && strings.HasSuffix(info.Name(), extensionFormat) {
			files = append(files, path)
		}
		return nil
	}); err != nil {
		return fmt.Errorf("unable to walk path %s: %w", outputPath, err)
	}

	for _, file := range files {
		l.logger.Info("Load file", zap.String("file", file))

		if err := l.loadFile(ctx, file); err != nil {
			return fmt.Errorf("unable to load file %s: %w", file, err)
		}
	}

	return nil
}

func (l *loader) loadFile(ctx context.Context, filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("unable to open file %s: %w", filePath, err)
	}
	defer file.Close()

	content, err := ioutil.ReadAll(file)
	if err != nil {
		return fmt.Errorf("unable to read file %s: %w", filePath, err)
	}

	var payload jsonPayload
	if err := json.Unmarshal(content, &payload); err != nil {
		return fmt.Errorf("unable to decode %s: %w", content, err)
	}

	return l.loadJSON(ctx, l.schema[payload.TableName], payload)
}

func (l *loader) loadJSON(ctx context.Context, schema config.Schema, payload jsonPayload) error {
	return l.dialect.BulkInsert(ctx, schema.Table, payload.Data)
}
