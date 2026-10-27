package postgres

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"regexp"
	"strconv"
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

			switch dataType := normalizeDataType(column.DataType); dataType {
			case "jsonb":
				b, err := json.Marshal(v)
				if err != nil {
					return nil, fmt.Errorf("unable to encode %v to JSON: %w", v, err)
				}
				pairs[i] = lk.Pair(k, jsonArg(b))
			default:
				array, err := arrayToArg(dataType, v)
				if err != nil {
					return nil, fmt.Errorf("unable to convert column %s of table %s (%s): %w", k, table.Name, column.DataType, err)
				}
				pairs[i] = lk.Pair(k, array)
			}
		default:
			pairs[i] = lk.Pair(k, v)
		}

		i++
	}
	return pairs, nil
}

// typmodRegexp matches the type modifiers of format_type, such as (10) or (10,2).
var typmodRegexp = regexp.MustCompile(`\(\d+(,\d+)?\)`)

// normalizeDataType removes the type modifiers from a format_type data type.
func normalizeDataType(dataType string) string {
	return strings.Join(strings.Fields(typmodRegexp.ReplaceAllString(dataType, "")), " ")
}

// arrayToArg converts a JSON array to the value of an array column, depending on its normalized data type. A JSON
// null element is a NULL element and an empty JSON array is an empty array.
func arrayToArg(dataType string, v []interface{}) (interface{}, error) {
	switch dataType {
	case "text[]", "character varying[]", "character[]", "name[]", "citext[]":
		return buildArray(v, toString)
	case "smallint[]":
		return buildArray(v, toInt[int16])
	case "integer[]":
		return buildArray(v, toInt[int32])
	case "bigint[]":
		return buildArray(v, toInt[int64])
	case "boolean[]":
		return buildArray(v, func(e interface{}) (bool, error) {
			b, ok := e.(bool)
			if !ok {
				return false, fmt.Errorf("unable to convert %v (%T) to bool", e, e)
			}

			return b, nil
		})
	case "real[]":
		return buildArray(v, func(e interface{}) (float32, error) {
			f, err := toFloat64(e)

			return float32(f), err
		})
	case "double precision[]":
		return buildArray(v, toFloat64)
	case "numeric[]":
		return buildArray(v, func(e interface{}) (pgtype.Numeric, error) {
			var (
				n    pgtype.Numeric
				text string
			)
			switch e := e.(type) {
			case string:
				text = e
			case json.Number:
				text = e.String()
			case float64:
				text = strconv.FormatFloat(e, 'f', -1, 64)
			default:
				return n, fmt.Errorf("unable to convert %v (%T) to numeric", e, e)
			}
			err := n.Scan(text)

			return n, err
		})
	case "uuid[]":
		return buildArray(v, func(e interface{}) (pgtype.UUID, error) {
			var u pgtype.UUID
			s, ok := e.(string)
			if !ok {
				return u, fmt.Errorf("unable to convert %v (%T) to uuid", e, e)
			}

			err := u.Scan(s)

			return u, err
		})
	case "timestamp without time zone[]", "timestamp with time zone[]", "date[]":
		return buildArray(v, func(e interface{}) (time.Time, error) {
			s, ok := e.(string)
			if !ok {
				return time.Time{}, fmt.Errorf("unable to convert %v (%T) to time.Time", e, e)
			}
			t, err := time.Parse(time.RFC3339, s)
			if err != nil && dataType == "date[]" {
				t, err = time.Parse(time.DateOnly, s)
			}
			if err != nil {
				return t, fmt.Errorf("unable to parse %v to time.Time: %w", e, err)
			}

			return t, nil
		})
	}

	return nil, fmt.Errorf("unsupported array data type %q", dataType)
}

// buildArray converts the elements of a JSON array, a nil element being a NULL element.
func buildArray[T any](v []interface{}, convert func(interface{}) (T, error)) (interface{}, error) {
	elements := make([]*T, len(v))
	for i := range v {
		if v[i] == nil {
			continue
		}

		element, err := convert(v[i])
		if err != nil {
			return nil, fmt.Errorf("element %d: %w", i, err)
		}
		elements[i] = &element
	}

	return newArray(elements), nil
}

func toString(e interface{}) (string, error) {
	s, ok := e.(string)
	if !ok {
		return "", fmt.Errorf("unable to convert %v (%T) to string", e, e)
	}

	return s, nil
}

func toFloat64(e interface{}) (float64, error) {
	switch e := e.(type) {
	case float64:
		return e, nil
	case json.Number:
		return e.Float64()
	}

	return 0, fmt.Errorf("unable to convert %v (%T) to float64", e, e)
}

// toInt converts a JSON number, a float64 unless the decoder uses json.Number, to an integer type.
func toInt[T int16 | int32 | int64](e interface{}) (T, error) {
	var n int64
	switch e := e.(type) {
	case float64:
		n = int64(e)
		if float64(n) != e {
			return 0, fmt.Errorf("%v is not an integer", e)
		}
	case json.Number:
		var err error
		if n, err = e.Int64(); err != nil {
			return 0, fmt.Errorf("unable to convert %v to integer: %w", e, err)
		}
	default:
		return 0, fmt.Errorf("unable to convert %v (%T) to integer", e, e)
	}

	if T(n) < 0 != (n < 0) || int64(T(n)) != n {
		return 0, fmt.Errorf("%d overflows %T", n, T(0))
	}

	return T(n), nil
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

// keyToText returns the text representation of a key value, as pgx v5 and convertValue return it. The second
// value is false for types that cannot safely be sent as text.
func keyToText(value interface{}) (string, bool) {
	switch v := value.(type) {
	case int:
		return strconv.FormatInt(int64(v), 10), true
	case int8:
		return strconv.FormatInt(int64(v), 10), true
	case int16:
		return strconv.FormatInt(int64(v), 10), true
	case int32:
		return strconv.FormatInt(int64(v), 10), true
	case int64:
		return strconv.FormatInt(v, 10), true
	case uint:
		return strconv.FormatUint(uint64(v), 10), true
	case uint8:
		return strconv.FormatUint(uint64(v), 10), true
	case uint16:
		return strconv.FormatUint(uint64(v), 10), true
	case uint32:
		return strconv.FormatUint(uint64(v), 10), true
	case uint64:
		return strconv.FormatUint(v, 10), true
	case string:
		return v, true
	case [16]byte:
		return uuidToText(v), true
	case pgtype.UUID:
		if !v.Valid {
			return "", false
		}

		return uuidToText(v.Bytes), true
	}

	return "", false
}

func uuidToText(b [16]byte) string {
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
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
