package postgres

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
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

			pairs[i] = lk.Pair(k, jsonArg(res))
		case []interface{}:
			column := table.Columns.Get(k)

			switch column.DataType {
			case "smallint[]":
				elements := make([]int16, len(v))
				for i := range v {
					elements[i] = int16(v[i].(float64))
				}
				pairs[i] = lk.Pair(k, newArray(elements))
			case "integer[]":
				elements := make([]int32, len(v))
				for i := range v {
					elements[i] = int32(v[i].(float64))
				}
				pairs[i] = lk.Pair(k, newArray(elements))
			case "character varying[]":
				elements := make([]string, len(v))
				for i := range v {
					elements[i] = v[i].(string)
				}
				pairs[i] = lk.Pair(k, newArray(elements))
			case "timestamp without time zone[]":
				elements := make([]time.Time, len(v))
				for i := range v {
					t, err := time.Parse(time.RFC3339, v[i].(string))
					if err != nil {
						return nil, fmt.Errorf("unable to parse %v to time.Time: %w", v[i], err)
					}
					elements[i] = t
				}
				pairs[i] = lk.Pair(k, newArray(elements))
			case "jsonb":
				b, err := json.Marshal(v)
				if err != nil {
					return nil, fmt.Errorf("unable to encode %v to JSON: %w", v, err)
				}
				pairs[i] = lk.Pair(k, jsonArg(b))
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
		result[fields[i].Name] = convertValue(fields[i].DataTypeOID, values[i])
	}

	return result, nil
}

func marshalRows(rows pgx.Rows) (map[string]interface{}, error) {
	return rowsToMap(rows)
}

// typeMap is only used to know if an OID is an array type.
var typeMap = pgtype.NewMap()

func isArrayOID(oid uint32) bool {
	t, ok := typeMap.TypeForOID(oid)
	if !ok {
		return false
	}

	_, ok = t.Codec.(*pgtype.ArrayCodec)

	return ok
}

// legacyInterval keeps the JSON representation of pgtype.Interval from pgx v4 (Status 2 means present).
type legacyInterval struct {
	Microseconds int64
	Days         int32
	Months       int32
	Status       byte
}

// convertValue converts a value returned by pgx v5 to the representation produced by pgx v4, to keep
// the JSON output unchanged.
func convertValue(oid uint32, value interface{}) interface{} {
	switch v := value.(type) {
	case netip.Prefix:
		return v.String()
	case pgtype.Numeric:
		f, err := v.Float64Value()
		if err != nil || !f.Valid {
			return nil
		}

		return f.Float64
	case pgtype.Time:
		if !v.Valid {
			return nil
		}

		return v.Microseconds
	case pgtype.Interval:
		if !v.Valid {
			return nil
		}

		return legacyInterval{Microseconds: v.Microseconds, Days: v.Days, Months: v.Months, Status: 2}
	case pgtype.Range[interface{}]:
		return rangeToString(v)
	case []interface{}:
		// an empty array was encoded as null, a JSON array is left untouched
		if len(v) == 0 && isArrayOID(oid) {
			return nil
		}
	}

	return value
}

// rangeToString formats a range as PostgreSQL does, for instance "[1,5)".
func rangeToString(r pgtype.Range[interface{}]) string {
	if !r.Valid || r.LowerType == pgtype.Empty {
		return "empty"
	}

	var b strings.Builder
	if r.LowerType == pgtype.Inclusive {
		b.WriteString("[")
	} else {
		b.WriteString("(")
	}
	if r.LowerType == pgtype.Inclusive || r.LowerType == pgtype.Exclusive {
		fmt.Fprint(&b, r.Lower)
	}
	b.WriteString(",")
	if r.UpperType == pgtype.Inclusive || r.UpperType == pgtype.Exclusive {
		fmt.Fprint(&b, r.Upper)
	}
	if r.UpperType == pgtype.Inclusive {
		b.WriteString("]")
	} else {
		b.WriteString(")")
	}

	return b.String()
}

// loukoum only accepts a few kinds of arguments, including driver.Valuer, and pgx v5 does not need
// pgtype values to implement it anymore. The two types below fulfill both.

// jsonArg is a raw JSON document.
type jsonArg []byte

// Value implements driver.Valuer.
func (j jsonArg) Value() (driver.Value, error) {
	return string(j), nil
}

// arrayArg is an array encoded by pgx through pgtype.ArrayGetter.
type arrayArg[T any] struct {
	pgtype.Array[T]
}

// Value implements driver.Valuer. It is never used as pgx prefers pgtype.ArrayGetter.
func (arrayArg[T]) Value() (driver.Value, error) {
	return nil, errors.New("array must be encoded by pgx")
}

func newArray[T any](elements []T) arrayArg[T] {
	return arrayArg[T]{pgtype.Array[T]{
		Elements: elements,
		Dims:     []pgtype.ArrayDimension{{Length: int32(len(elements)), LowerBound: 1}},
		Valid:    true,
	}}
}
