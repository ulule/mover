package etl

import (
	"fmt"
	"regexp"
	"strings"

	"syreclabs.com/go/faker"
	"syreclabs.com/go/faker/locales"

	"github.com/ulule/mover/config"
	"github.com/ulule/mover/dialect"
)

var attrReg = regexp.MustCompile(`\{(?P<attr>\w+)\}`)

type sanitizer struct {
	schema map[string]config.Schema
	cache  map[string]map[interface{}]struct{}
}

var sanitizerLocales = map[string]map[string]interface{}{
	"fr": locales.Fr,
}

func newSanitizer(localeKey string, schema map[string]config.Schema) *sanitizer {
	locale, ok := sanitizerLocales[localeKey]
	if ok {
		faker.Locale = locale
	}

	return &sanitizer{
		schema: schema,
		cache:  make(map[string]map[interface{}]struct{}),
	}
}

func (s *sanitizer) sanitize(table dialect.Table, rows entry) []map[string]interface{} {
	var (
		results = make([]map[string]interface{}, 0)
		index   = make(map[interface{}]struct{})
		schema  = s.schema[table.Name]
	)

	for _, values := range rows {
		for j := range values {
			value := values[j]
			primaryKey := value[table.PrimaryKeyColumnName()]
			if _, ok := index[primaryKey]; ok {
				continue
			}

			if len(schema.Columns) == 0 {
				results = append(results, value)
			} else {
				results = append(results, s.sanitizeValues(schema, value))
			}

			index[primaryKey] = struct{}{}
		}
	}

	return results
}

func (s *sanitizer) fakeValue(column config.Column, value interface{}) interface{} {
	switch column.Fake {
	case "last_name":
		value = faker.Name().LastName()
	case "first_name":
		value = faker.Name().FirstName()
	case "email":
		value = faker.Internet().Email()
	case "street_address":
		value = faker.Address().StreetAddress()
	case "phone_number":
		value = faker.PhoneNumber().PhoneNumber()
	}

	if column.Unique {
		if _, ok := s.cache[column.Name]; !ok {
			s.cache[column.Name] = make(map[interface{}]struct{})
		}

		if _, ok := s.cache[column.Name][value]; ok {
			value = s.fakeValue(column, value)
		}

		s.cache[column.Name][value] = struct{}{}
	}

	return value
}

func (s *sanitizer) sanitizeValues(schema config.Schema, values map[string]interface{}) map[string]interface{} {
	for i := range schema.Columns {
		column := schema.Columns[i]
		if column.Replace != nil {
			values[column.Name] = replaceVar(*column.Replace, values)
		} else if column.Fake != "" {
			values[column.Name] = s.fakeValue(column, values[column.Name])
		} else if column.Sanitize {
			values[column.Name] = nil
		}
	}

	return values
}

func replaceVar(replace string, values map[string]interface{}) string {
	results := attrReg.FindAllStringSubmatch(replace, -1)
	for i := range results {
		parts := results[i]
		switch value := values[parts[1]].(type) {
		case string:
			replace = strings.ReplaceAll(replace, parts[0], value)
		case int:
			replace = strings.ReplaceAll(replace, parts[0], fmt.Sprintf("%d", value))
		default:
			replace = strings.ReplaceAll(replace, parts[0], fmt.Sprintf("%v", value))
		}
	}
	return replace
}
