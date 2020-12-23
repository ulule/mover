package etl

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sync/errgroup"

	"github.com/ulule/mover/config"
)

func extractFilenames(schema config.Schema, rows entry) []string {
	filenames := make([]string, 0)
	for i := range schema.Columns {
		if schema.Columns[i].Download == nil {
			continue
		}

		for j := range rows {
			for _, row := range rows[j] {
				for k, v := range row {
					if k != schema.Columns[i].Name {
						continue
					}

					v, ok := v.(string)
					if ok && v != "" {
						filenames = append(filenames, schema.Columns[i].Download.HTTP.URL(v))
					}
				}
			}
		}
	}

	return filenames
}

func chunkStrings(slice []string, chunkSize int) [][]string {
	var chunks [][]string
	for i := 0; i < len(slice); i += chunkSize {
		end := i + chunkSize

		// necessary check to avoid slicing beyond
		// slice capacity
		if end > len(slice) {
			end = len(slice)
		}

		chunks = append(chunks, slice[i:end])
	}

	return chunks
}

func downloadFiles(ctx context.Context, filenames []string, outputPath string, chunkSize int) error {
	g, _ := errgroup.WithContext(ctx)
	var chunks [][]string
	if chunkSize == 0 {
		chunks = [][]string{filenames}
	} else {
		chunks = chunkStrings(filenames, chunkSize)
	}
	for i := range chunks {
		for j := range chunks[i] {
			g.Go((func(filename string) func() error {
				return func() error {
					if err := downloadFile(filename, outputPath); err != nil {
						return err
					}

					return nil
				}
			})(chunks[i][j]))
			if err := g.Wait(); err != nil {
				return err
			}
		}
	}

	return nil
}

func downloadFile(absoluteURL, outputDir string) error {
	res, err := http.Get(absoluteURL)
	if err != nil {
		return fmt.Errorf("unable to retrieve %s: %w", absoluteURL, err)
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		return fmt.Errorf("unable to download %s to %s: received %d HTTP code", absoluteURL, outputDir, res.StatusCode)
	}

	u, err := url.Parse(absoluteURL)
	if err != nil {
		return fmt.Errorf("unable to parse %s: %w", absoluteURL, err)
	}

	path := filepath.Join(outputDir, filepath.Dir(u.Path))
	if err := os.MkdirAll(path, os.ModePerm); err != nil {
		return fmt.Errorf("unable to create directory %s: %w", path, err)
	}

	file, err := os.Create(filepath.Join(outputDir, u.Path))
	if err != nil {
		return fmt.Errorf("unable to create file %s: %w", path, err)
	}
	defer file.Close()

	if _, err = io.Copy(file, res.Body); err != nil {
		return fmt.Errorf("unable to copy bytes to file: %w", err)
	}

	return nil
}

func cacheKey(query string, args ...interface{}) string {
	cacheKey := query
	if len(args) > 0 {
		parts := make([]string, len(args))
		for i := range args {
			parts[i] = fmt.Sprintf("%v", args[i])
		}
		cacheKey += strings.Join(parts, ".")
	}

	return cacheKey
}
