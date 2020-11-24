package config

import (
	"encoding/json"
	"log"
	"os"
)

type Query struct {
	TableName string `json:"table_name"`
	Query     string `json:"query"`
}

type Dependency struct {
	TableName     string   `json:"table_name"`
	ReferenceKeys []string `json:"reference_keys"`
	Queries       []Query  `json:"queries"`
}

type Config struct {
	DatabaseURL  string       `json:"database_url"`
	Dependencies []Dependency `json:"dependencies"`
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
