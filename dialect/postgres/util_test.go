package postgres

import (
	"encoding/json"
	"net/netip"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/ulule/loukoum/v3/types"

	"github.com/ulule/mover/dialect"
)

// decode encodes a Go value with the given PostgreSQL type, then decodes it the way pgx.Rows.Values() does
// and applies convertValue.
func decode(t *testing.T, typeName string, value interface{}) interface{} {
	t.Helper()

	m := pgtype.NewMap()
	typ, ok := m.TypeForName(typeName)
	require.True(t, ok, typeName)

	buf, err := m.Encode(typ.OID, pgtype.BinaryFormatCode, value, nil)
	require.NoError(t, err)

	var decoded interface{}
	if buf == nil {
		return convertValue(typ.OID, decoded)
	}

	// same plan as pgx.Rows.Values()
	v, err := typ.Codec.DecodeValue(m, typ.OID, pgtype.BinaryFormatCode, buf)
	require.NoError(t, err)

	return convertValue(typ.OID, v)
}

func toJSON(t *testing.T, v interface{}) string {
	t.Helper()

	b, err := json.Marshal(v)
	require.NoError(t, err)

	return string(b)
}

func TestConvertValueNumeric(t *testing.T) {
	var n pgtype.Numeric
	require.NoError(t, n.Scan("12.50"))

	assert.Equal(t, "12.5", toJSON(t, decode(t, "numeric", n)))
	assert.Equal(t, "null", toJSON(t, convertValue(0, nil)))
}

func TestConvertValueTimestamp(t *testing.T) {
	ts := time.Date(2025, 12, 9, 15, 23, 14, 123000000, time.UTC)

	assert.Equal(t, `"2025-12-09T15:23:14.123Z"`, toJSON(t, decode(t, "timestamp", ts)))
	// timestamptz is returned in the local time zone, as with pgx v4
	got, ok := decode(t, "timestamptz", ts).(time.Time)
	require.True(t, ok)
	assert.True(t, ts.Equal(got))
}

func TestConvertValueArrays(t *testing.T) {
	ts := time.Date(2025, 12, 9, 15, 23, 14, 0, time.UTC)

	assert.Equal(t, `["2025-12-09T15:23:14Z","2025-12-09T15:23:14Z"]`, toJSON(t, decode(t, "_timestamp", []time.Time{ts, ts})))
	assert.Equal(t, `[1,2,3]`, toJSON(t, decode(t, "_int4", []int32{1, 2, 3})))
	assert.Equal(t, `[1,2]`, toJSON(t, decode(t, "_int2", []int16{1, 2})))
	assert.Equal(t, `["a","b"]`, toJSON(t, decode(t, "_varchar", []string{"a", "b"})))
	assert.Equal(t, `["a","b"]`, toJSON(t, decode(t, "_text", []string{"a", "b"})))

	// an empty array was encoded as null with pgx v4, a jsonb array is left untouched
	assert.Equal(t, `null`, toJSON(t, decode(t, "_int4", []int32{})))
	assert.Equal(t, `null`, toJSON(t, decode(t, "_text", []string{})))
	assert.Equal(t, `[]`, toJSON(t, convertValue(3802, []interface{}{})))
}

func TestConvertValueScalars(t *testing.T) {
	uuid := [16]byte{0xa0, 0xee, 0xbc, 0x99, 0x9c, 0x0b, 0x4e, 0xf8, 0xbb, 0x6d, 0x6b, 0xb9, 0xbd, 0x38, 0x0a, 0x11}

	// pgx v4 returned [16]byte, JSON encoded as an array of numbers
	assert.Equal(t, `[160,238,188,153,156,11,78,248,187,109,107,185,189,56,10,17]`, toJSON(t, decode(t, "uuid", uuid)))
	assert.Equal(t, `"AQID"`, toJSON(t, decode(t, "bytea", []byte{1, 2, 3})))
	assert.Equal(t, `{"a":[1,"b"],"c":null}`, toJSON(t, decode(t, "jsonb", map[string]interface{}{"a": []interface{}{1, "b"}, "c": nil})))
	assert.Equal(t, `{"a":1}`, toJSON(t, decode(t, "json", `{"a":1}`)))
	assert.Equal(t, `[]`, toJSON(t, decode(t, "jsonb", []interface{}{})))
	assert.Equal(t, `"10.0.0.1/32"`, toJSON(t, decode(t, "inet", netip.MustParsePrefix("10.0.0.1/32"))))
	assert.Equal(t, `"10.0.0.0/8"`, toJSON(t, decode(t, "cidr", netip.MustParsePrefix("10.0.0.0/8"))))
}

func TestConvertValueTimeInterval(t *testing.T) {
	// pgx v4 returned the microseconds for time and a struct with a Status field for interval
	assert.Equal(t, `36672000000`, toJSON(t, decode(t, "time", pgtype.Time{Microseconds: 36672000000, Valid: true})))
	assert.Equal(t, `{"Microseconds":0,"Days":1,"Months":0,"Status":2}`, toJSON(t, decode(t, "interval", pgtype.Interval{Days: 1, Valid: true})))
}

func TestConvertValueRange(t *testing.T) {
	r := pgtype.Range[int32]{Lower: 1, Upper: 5, LowerType: pgtype.Inclusive, UpperType: pgtype.Exclusive, Valid: true}
	assert.Equal(t, `"[1,5)"`, toJSON(t, decode(t, "int4range", r)))

	r = pgtype.Range[int32]{Lower: 1, LowerType: pgtype.Inclusive, UpperType: pgtype.Unbounded, Valid: true}
	assert.Equal(t, `"[1,)"`, toJSON(t, decode(t, "int4range", r)))
}

func TestValuesToPairs(t *testing.T) {
	table := dialect.Table{Name: "t", Columns: dialect.Columns{
		{Name: "ts", DataType: "timestamp without time zone[]"},
		{Name: "ints", DataType: "integer[]"},
		{Name: "smalls", DataType: "smallint[]"},
		{Name: "names", DataType: "character varying[]"},
		{Name: "doc", DataType: "jsonb"},
	}}

	pairs, err := valuesToPairs(table, map[string]interface{}{
		"ts":     []interface{}{"2025-12-09T15:23:14Z"},
		"ints":   []interface{}{float64(1), float64(2)},
		"smalls": []interface{}{float64(1)},
		"names":  []interface{}{"a"},
		"doc":    []interface{}{"x", float64(1)},
	})
	require.NoError(t, err)
	assert.Len(t, pairs, 5)

	_, err = valuesToPairs(table, map[string]interface{}{"ts": []interface{}{"nope"}})
	assert.Error(t, err)
}

func TestKeyToText(t *testing.T) {
	uuid := [16]byte{0x12, 0x3e, 0x45, 0x67, 0xe8, 0x9b, 0x12, 0xd3, 0xa4, 0x56, 0x42, 0x66, 0x14, 0x17, 0x40, 0x00}

	for value, expected := range map[interface{}]string{
		int16(-3):                             "-3",
		int32(42):                             "42",
		int64(9007199254740993):               "9007199254740993",
		uint32(7):                             "7",
		uint64(18446744073709551615):          "18446744073709551615",
		"abc":                                 "abc",
		uuid:                                  "123e4567-e89b-12d3-a456-426614174000",
		pgtype.UUID{Bytes: uuid, Valid: true}: "123e4567-e89b-12d3-a456-426614174000",
	} {
		text, ok := keyToText(value)
		assert.True(t, ok, value)
		assert.Equal(t, expected, text, value)
	}

	for _, value := range []interface{}{nil, 1.5, true, []byte("a"), []interface{}{1}, pgtype.UUID{}, time.Now(), map[string]interface{}{}} {
		_, ok := keyToText(value)
		assert.False(t, ok, value)
	}
}

func TestInsertQuery(t *testing.T) {
	table := dialect.Table{
		Name:        "users",
		PrimaryKeys: []dialect.PrimaryKey{{Name: "id"}},
		Columns:     dialect.Columns{{Name: "id", DataType: "integer"}},
	}

	query, args, err := insertQuery(table, map[string]interface{}{"id": int32(1)})
	require.NoError(t, err)
	assert.Equal(t, `INSERT INTO "users" ("id") VALUES ($1) ON CONFLICT ("id") DO NOTHING`, query)
	assert.Equal(t, []interface{}{int32(1)}, args)
}

func TestValuesToPairsArrays(t *testing.T) {
	table := dialect.Table{Name: "t", Columns: dialect.Columns{
		{Name: "events", DataType: "text[]"},
		{Name: "ints", DataType: "integer[]"},
		{Name: "bigs", DataType: "bigint[]"},
		{Name: "names", DataType: "character varying(10)[]"},
		{Name: "prices", DataType: "numeric(10,2)[]"},
		{Name: "ids", DataType: "uuid[]"},
		{Name: "days", DataType: "date[]"},
		{Name: "weird", DataType: "inet[]"},
	}}

	value := func(data map[string]interface{}) interface{} {
		pairs, err := valuesToPairs(table, data)
		require.NoError(t, err)
		require.Len(t, pairs, 1)

		return pairs[0].(types.Pair).Value
	}

	events := value(map[string]interface{}{"events": []interface{}{"order.completed", "order.cancelled", "subscription.cancelled"}})
	assert.Equal(t, []string{"order.completed", "order.cancelled", "subscription.cancelled"}, derefAll(t, events.(arrayArg[*string]).Elements))

	ints := value(map[string]interface{}{"ints": []interface{}{float64(1), float64(2)}})
	assert.Equal(t, []int32{1, 2}, derefAll(t, ints.(arrayArg[*int32]).Elements))

	bigs := value(map[string]interface{}{"bigs": []interface{}{float64(1), json.Number("9007199254740993")}})
	assert.Equal(t, []int64{1, 9007199254740993}, derefAll(t, bigs.(arrayArg[*int64]).Elements))

	empty := value(map[string]interface{}{"events": []interface{}{}}).(arrayArg[*string])
	assert.True(t, empty.Valid)
	assert.Empty(t, empty.Elements)

	withNull := value(map[string]interface{}{"names": []interface{}{"a", nil}}).(arrayArg[*string])
	require.Len(t, withNull.Elements, 2)
	assert.Equal(t, "a", *withNull.Elements[0])
	assert.Nil(t, withNull.Elements[1])

	_ = value(map[string]interface{}{"prices": []interface{}{float64(1.5), "2.25"}})
	_ = value(map[string]interface{}{"ids": []interface{}{"123e4567-e89b-12d3-a456-426614174000"}})
	_ = value(map[string]interface{}{"days": []interface{}{"2025-12-09", "2025-12-09T00:00:00Z"}})

	for _, data := range []map[string]interface{}{
		{"ints": []interface{}{1.5}},
		{"ints": []interface{}{float64(1 << 40)}},
		{"ints": []interface{}{"1"}},
		{"events": []interface{}{float64(1)}},
		{"ids": []interface{}{"nope"}},
		{"weird": []interface{}{"127.0.0.1"}},
	} {
		_, err := valuesToPairs(table, data)
		assert.Error(t, err, "%v", data)
	}
}

func derefAll[T any](t *testing.T, elements []*T) []T {
	t.Helper()

	values := make([]T, len(elements))
	for i := range elements {
		require.NotNil(t, elements[i])
		values[i] = *elements[i]
	}

	return values
}

func TestInsertQueryUnsupportedValue(t *testing.T) {
	table := dialect.Table{Name: "mvt_t", PrimaryKeys: []dialect.PrimaryKey{{Name: "id"}}}

	assert.NotPanics(t, func() {
		_, _, err := insertQuery(table, map[string]interface{}{"id": 1, "bad": []string{"a"}})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "mvt_t")
		assert.Contains(t, err.Error(), "bad")
		assert.Contains(t, err.Error(), "[]string")
	})
}
