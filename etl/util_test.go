package etl

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetQueryTable(t *testing.T) {
	assert.Equal(t, "ulule_project", getQueryTable("select * from ulule_project"))
	assert.Equal(t, "ulule_project", getQueryTable("SELECT * FROM ulule_project"))
	assert.Equal(t, "ulule_project", getQueryTable("SELECT one, two, three FROM ulule_project"))
}

func TestDownloadFiles(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		if r.URL.Path == "/media/file-7.txt" {
			http.NotFound(w, r)
			return
		}
		fmt.Fprint(w, "content of ", r.URL.Path)
	}))
	defer srv.Close()

	filenames := make([]string, 0, 21)
	for i := 0; i < 20; i++ {
		filenames = append(filenames, fmt.Sprintf("%s/media/file-%d.txt", srv.URL, i))
	}
	// duplicate must be downloaded only once
	filenames = append(filenames, filenames[0])

	dir := t.TempDir()

	start := time.Now()
	err := downloadFiles(context.Background(), filenames, dir, 10)
	elapsed := time.Since(start)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "404")
	assert.Less(t, elapsed, time.Second)

	for i := 0; i < 20; i++ {
		_, statErr := os.Stat(filepath.Join(dir, "media", fmt.Sprintf("file-%d.txt", i)))
		if i == 7 {
			assert.True(t, os.IsNotExist(statErr))
			continue
		}
		assert.NoError(t, statErr)
	}
}
