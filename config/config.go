package config

import (
	"encoding/json"
	"log"
	"os"

	"github.com/ulule/mover/dialect"
)

type Query struct {
	TableName string        `json:"table_name"`
	Query     string        `json:"query"`
	Table     dialect.Table `json:"-"`
}

type DownloadHTTP struct {
	BaseURL string `json:"base_url"`
}

func (d DownloadHTTP) URL(path string) string {
	return d.BaseURL + path
}

type Download struct {
	Type string       `json:"type"`
	HTTP DownloadHTTP `json:"http"`
}

type Column struct {
	Name     string    `json:"name"`
	Fake     string    `json:"fake"`
	Unique   bool      `json:"unique"`
	Replace  *string   `json:"replace"`
	Sanitize bool      `json:"sanitize"`
	Download *Download `json:"download"`
}

type Schema struct {
	TableName         string        `json:"table_name"`
	OmitReferenceKeys bool          `json:"omit_reference_keys"`
	ReferenceKeys     []string      `json:"reference_keys"`
	Queries           []Query       `json:"queries"`
	Columns           []Column      `json:"columns"`
	Table             dialect.Table `json:"-"`
}

type Config struct {
	Locale string   `json:"locale"`
	Schema []Schema `json:"schema"`
	Extra  []Schema `json:"extra"`
}

// Load loads the configuration from configuration file path.
func Load(path string, out interface{}) error {
	var err error

	f, err := os.Open(path)
	if f != nil {
		defer func() {
			ferr := f.Close()
			if ferr != nil {
				log.Println(ferr)
			}
		}()
	}

	if err != nil {
		return err
	}

	dec := json.NewDecoder(f)
	err = dec.Decode(out)
	if err != nil {
		return err
	}

	return err
}
