package postgres

import (
	"encoding/json"
	"fmt"
	"net"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	lk "github.com/ulule/loukoum/v3"

	"github.com/ulule/mover/dialect"
)

func interfaceToInt64(raw interface{}) int64 {
	var val int64

	switch raw := raw.(type) {
	case int32:
		val = int64(raw)
	case int64:
		val = raw
	}

	return val
}

func valuesToPairs(table dialect.Table, data map[string]interface{}) ([]interface{}, error) {
	var (
		pairs = make([]interface{}, len(data))
		i     = 0
	)
	for k, v := range data {
		switch v := v.(type) {
		case map[string]interface{}:
			res, err := json.Marshal(v)
			if err != nil {
				return nil, fmt.Errorf("unable to encode %v to JSON: %w", v, err)
			}

			pairs[i] = lk.Pair(k, &pgtype.JSONB{Bytes: res, Status: pgtype.Present})
		case []interface{}:
			column := table.Columns.Get(k)

			switch column.DataType {
			case "smallint[]":
				elements := make([]int16, len(v))
				for i := range v {
					elements[i] = int16(v[i].(float64))
				}
				values := &pgtype.Int2Array{}
				if err := values.Set(elements); err != nil {
					return nil, fmt.Errorf("unable to encode %v to pgtype.Int2Array: %w", elements, err)
				}
				pairs[i] = lk.Pair(k, values)
			case "integer[]":
				elements := make([]int32, len(v))
				for i := range v {
					elements[i] = int32(v[i].(float64))
				}
				values := &pgtype.Int4Array{}
				if err := values.Set(elements); err != nil {
					return nil, fmt.Errorf("unable to encode %v to pgtype.Int4Array: %w", elements, err)
				}
				pairs[i] = lk.Pair(k, values)
			case "character varying[]":
				elements := make([]string, len(v))
				for i := range v {
					elements[i] = v[i].(string)
				}
				values := &pgtype.VarcharArray{}
				if err := values.Set(elements); err != nil {
					return nil, fmt.Errorf("unable to encode %v to pgtype.VarcharArray: %w", elements, err)
				}
				pairs[i] = lk.Pair(k, values)
			default:
				pairs[i] = lk.Pair(k, v)
			}
		default:
			pairs[i] = lk.Pair(k, v)
		}

		i++
	}
	return pairs, nil
}

func rowsToMap(rows pgx.Rows) (map[string]interface{}, error) {
	values, err := rows.Values()
	if err != nil {
		return nil, err
	}
	result := make(map[string]interface{}, len(values))
	fields := rows.FieldDescriptions()
	for i := range fields {
		value := values[i]
		result[string(fields[i].Name)] = value
	}

	return result, nil
}

func marshalRows(rows pgx.Rows) (map[string]interface{}, error) {
	results, err := rowsToMap(rows)
	if err != nil {
		return nil, err
	}

	for k, v := range results {
		switch v := v.(type) {
		case pgtype.Int4range:
			value, err := v.Value()
			if err != nil {
				return nil, fmt.Errorf("unable to decode %v+: %w", v, err)
			}

			results[k] = value
		case *net.IPNet:
			results[k] = v.String()
		case pgtype.VarcharArray:
			var values []string
			if v.Elements != nil {
				if err := v.AssignTo(&values); err != nil {
					return nil, fmt.Errorf("unable to decode %v+: %w", v, err)
				}
			}

			results[k] = values
		case pgtype.Int2Array:
			var values []int8
			if v.Elements != nil {
				if err := v.AssignTo(&values); err != nil {
					return nil, fmt.Errorf("unable to decode %v+: %w", v, err)
				}
			}

			results[k] = values
		case pgtype.Int4Array:
			var values []int32
			if v.Elements != nil {
				if err := v.AssignTo(&values); err != nil {
					return nil, fmt.Errorf("unable to decode %v+: %w", v, err)
				}
			}

			results[k] = values
		case pgtype.Numeric:
			var res float64
			if err := v.AssignTo(&res); err != nil {
				return nil, fmt.Errorf("unable to decode %v+: %w", v, err)
			}
			results[k] = res
		}
	}

	return results, nil
}
