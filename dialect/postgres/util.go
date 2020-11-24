package postgres

import (
	"encoding/json"
	"net"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	lk "github.com/ulule/loukoum/v3"

	"github.com/ulule/mover/dialect"
)

func interfaceToInt64(raw interface{}) int64 {
	var val int64

	switch raw.(type) {
	case int32:
		val = int64(raw.(int32))
	case int64:
		val = raw.(int64)
	}

	return val
}

func valuesToPairs(table dialect.Table, data map[string]interface{}) ([]interface{}, error) {
	pairs := make([]interface{}, len(data))
	i := 0
	for k, v := range data {
		switch v.(type) {
		case map[string]interface{}:
			res, err := json.Marshal(v)
			if err != nil {
				return nil, err
			}

			v = &pgtype.JSONB{Bytes: res, Status: pgtype.Present}
		case []interface{}:
			column := table.Columns.Get(k)

			switch column.DataType {
			case "smallint[]":
				raw := v.([]interface{})
				elements := make([]pgtype.Int2, len(raw))
				for i := range raw {
					elements[i] = pgtype.Int2{Int: int16(raw[i].(float64)), Status: pgtype.Present}
				}
				v = &pgtype.Int2Array{
					Elements:   elements,
					Status:     pgtype.Present,
					Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: int32(len(raw))}},
				}
			case "integer[]":
				raw := v.([]interface{})
				elements := make([]pgtype.Int4, len(raw))
				for i := range raw {
					elements[i] = pgtype.Int4{Int: int32(raw[i].(float64)), Status: pgtype.Present}
				}
				v = &pgtype.Int4Array{
					Elements:   elements,
					Status:     pgtype.Present,
					Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: int32(len(raw))}},
				}
			case "character varying[]":
				raw := v.([]interface{})
				elements := make([]pgtype.Varchar, len(raw))
				for i := range raw {
					elements[i] = pgtype.Varchar{String: raw[i].(string), Status: pgtype.Present}
				}
				v = &pgtype.VarcharArray{
					Elements:   elements,
					Status:     pgtype.Present,
					Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: int32(len(raw))}},
				}
			}
		}

		pairs[i] = lk.Pair(k, v)
		i++
	}
	return pairs, nil
}

func rowsToMap(rows pgx.Rows) (map[string]interface{}, error) {
	values, err := rows.Values()
	if err != nil {
		return nil, nil
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
		switch v.(type) {
		case *net.IPNet:
			val := v.(*net.IPNet)
			results[k] = val.String()
		case pgtype.VarcharArray:
			var values []string
			arr := v.(pgtype.VarcharArray)
			if arr.Elements != nil {
				arr.AssignTo(&values)
			}

			results[k] = values
		case pgtype.Int2Array:
			var values []int8
			arr := v.(pgtype.Int2Array)
			if arr.Elements != nil {
				arr.AssignTo(&values)
			}

			results[k] = values
		case pgtype.Int4Array:
			var values []int32
			arr := v.(pgtype.Int4Array)
			if arr.Elements != nil {
				arr.AssignTo(&values)
			}

			results[k] = values
		case pgtype.Numeric:
			num := v.(pgtype.Numeric)
			results[k] = num.Int.Int64()
		}
	}

	return results, nil
}
