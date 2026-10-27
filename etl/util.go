package etl

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/ulule/mover/config"
)

const defaultDownloadConcurrency = 10

var httpClient = &http.Client{Timeout: 60 * time.Second}

var sqlSelectRegexp = regexp.MustCompile(`^(?i)SELECT (?P<columns>.*[^T]) FROM (?P<table>\w+).*`)

func regexpNamedParams(reg *regexp.Regexp, value string) map[string]string {
	match := reg.FindStringSubmatch(value)
	if match == nil {
		return nil
	}

	captures := make(map[string]string)
	for i, name := range sqlSelectRegexp.SubexpNames() {
		if i == 0 {
			continue
		}
		captures[name] = match[i]
	}

	return captures
}

func getQueryTable(query string) string {
	matches := regexpNamedParams(sqlSelectRegexp, query)

	if val, ok := matches["table"]; ok {
		return val
	}

	return ""
}

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

func downloadFiles(ctx context.Context, filenames []string, outputPath string, concurrency int) error {
	if concurrency <= 0 {
		concurrency = defaultDownloadConcurrency
	}

	// two rows can point to the same media: download each file once
	seen := make(map[string]struct{}, len(filenames))

	var (
		g    errgroup.Group
		mu   sync.Mutex
		errs []error
	)
	g.SetLimit(concurrency)

	for _, filename := range filenames {
		if _, ok := seen[filename]; ok {
			continue
		}
		seen[filename] = struct{}{}

		g.Go(func() error {
			if err := downloadFile(ctx, httpClient, filename, outputPath); err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}

			return nil
		})
	}

	_ = g.Wait()

	return errors.Join(errs...)
}

func downloadFile(ctx context.Context, client *http.Client, absoluteURL, outputDir string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, absoluteURL, http.NoBody)
	if err != nil {
		return fmt.Errorf("unable to create request for %s: %w", absoluteURL, err)
	}

	res, err := client.Do(req)
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
