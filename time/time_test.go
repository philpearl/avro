package time

import (
	"encoding/binary"
	"strconv"
	"testing"
	"time"
	"unsafe"

	"github.com/google/go-cmp/cmp"
	"github.com/philpearl/avro"
)

func TestTime(t *testing.T) {
	now := time.Now()
	ts := now.Format(time.RFC3339Nano)
	data := []byte{byte(len(ts) << 1)}
	data = append(data, ts...)

	b := avro.NewReadBuf(data)
	c := StringCodec{}

	var out time.Time
	if err := c.Read(b, unsafe.Pointer(&out)); err != nil {
		t.Fatal(err)
	}

	if !out.Equal(now) {
		t.Fatalf("times %s & %s differ by %s", now, out, now.Sub(out))
	}
}

func TestTimeEmpty(t *testing.T) {
	b := avro.NewReadBuf([]byte{0})
	c := StringCodec{}

	var out time.Time
	if err := c.Read(b, unsafe.Pointer(&out)); err != nil {
		t.Fatal(err)
	}

	if !out.IsZero() {
		t.Fatalf("times %s but expected zero", out)
	}
}

func TestTimePtr(t *testing.T) {
	now := time.Now()
	ts := now.Format(time.RFC3339Nano)
	data := []byte{byte(len(ts) << 1)}
	data = append(data, ts...)

	b := avro.NewReadBuf(data)

	c := avro.PointerCodec{
		Codec: StringCodec{},
	}

	var out *time.Time
	if err := c.Read(b, unsafe.Pointer(&out)); err != nil {
		t.Fatal(err)
	}

	if !out.Equal(now) {
		t.Fatalf("times %s & %s differ by %s", now, out, now.Sub(*out))
	}
}

func TestTimeLong(t *testing.T) {
	now := time.Now()
	data := make([]byte, binary.MaxVarintLen64)
	l := binary.PutVarint(data, now.UnixNano())
	data = data[:l]

	b := avro.NewReadBuf(data)
	c := LongCodec{mult: 1}

	var out time.Time
	if err := c.Read(b, unsafe.Pointer(&out)); err != nil {
		t.Fatal(err)
	}

	if !out.Equal(now) {
		t.Fatalf("times %s & %s differ by %s", now, out, now.Sub(out))
	}
}

func TestDate(t *testing.T) {
	t0 := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, l := range []int{0, 1, 573} {
		t.Run(strconv.Itoa(l), func(t *testing.T) {
			exp := t0.AddDate(0, 0, l)
			data := make([]byte, binary.MaxVarintLen64)
			l := binary.PutVarint(data, int64(l))
			data = data[:l]

			b := avro.NewReadBuf(data)
			c := DateCodec{}

			var out time.Time
			if err := c.Read(b, unsafe.Pointer(&out)); err != nil {
				t.Fatal(err)
			}

			if !out.Equal(exp) {
				t.Fatalf("times %s & %s differ by %s", exp, out, exp.Sub(out))
			}
		})
	}
}

func TestTimeLongPtr(t *testing.T) {
	now := time.Now()
	data := make([]byte, binary.MaxVarintLen64)
	l := binary.PutVarint(data, now.UnixNano())
	data = data[:l]

	b := avro.NewReadBuf(data)

	c := avro.PointerCodec{
		Codec: LongCodec{mult: 1},
	}

	var out *time.Time
	if err := c.Read(b, unsafe.Pointer(&out)); err != nil {
		t.Fatal(err)
	}

	if !out.Equal(now) {
		t.Fatalf("times %s & %s differ by %s", now, out, now.Sub(*out))
	}
}

func BenchmarkTime(b *testing.B) {
	now := time.Now().UTC()
	ts := now.Format(time.RFC3339Nano)
	data := []byte{byte(len(ts) << 1)}
	data = append(data, ts...)

	buf := avro.NewReadBuf(data)
	c := StringCodec{}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		buf.Reset(data)

		var out time.Time
		if err := c.Read(buf, unsafe.Pointer(&out)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLongTime(b *testing.B) {
	now := time.Now()
	data := make([]byte, binary.MaxVarintLen64)
	l := binary.PutVarint(data, now.UnixNano())
	data = data[:l]

	buf := avro.NewReadBuf(data)
	c := LongCodec{mult: 1}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		buf.Reset(data)

		var out time.Time
		if err := c.Read(buf, unsafe.Pointer(&out)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkParseTime(b *testing.B) {
	ts := time.Now().UTC().Format(time.RFC3339Nano)
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := time.Parse(time.RFC3339, ts)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkParseTimeOurselves(b *testing.B) {
	ts := time.Now().UTC().Format(time.RFC3339Nano)
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := parseTime(ts)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestASchema(t *testing.T) {
	RegisterCodecs()

	s, err := avro.SchemaFromString(`{
  "type": "record",
  "name": "Root",
  "fields": [
    {
      "name": "timestamp",
      "type": [
        "null",
        {
          "type": "long",
          "logicalType": "timestamp-micros"
        }
      ],
      "default": null
    }
	]
}
	`)
	if err != nil {
		t.Fatal(err)
	}

	if diff := cmp.Diff(avro.Schema{
		Type: "record",
		Object: &avro.SchemaObject{
			Name: "Root",
			Fields: []avro.SchemaRecordField{
				{
					Name: "timestamp",
					Type: avro.Schema{
						Type: "union",
						Union: []avro.Schema{
							{Type: "null"},
							{
								Type:   "long",
								Object: &avro.SchemaObject{LogicalType: "timestamp-micros"},
							},
						},
					},
				},
			},
		},
	}, s); diff != "" {
		t.Fatal(diff)
	}

	type Thing struct {
		Timestamp time.Time `json:"timestamp"`
	}

	if _, err := s.Codec(Thing{}); err != nil {
		t.Fatal(err)
	}
}
