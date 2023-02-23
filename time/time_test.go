package time

import (
	"encoding/binary"
	"testing"
	"time"
	"unsafe"

	"github.com/philpearl/avro"
)

func TestTime(t *testing.T) {
	now := time.Now()
	ts := now.Format(time.RFC3339Nano)
	data := []byte{byte(len(ts) << 1)}
	data = append(data, ts...)

	b := avro.NewBuffer(data)
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
	b := avro.NewBuffer([]byte{0})
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

	b := avro.NewBuffer(data)

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

	b := avro.NewBuffer(data)
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
	data := make([]byte, binary.MaxVarintLen64)
	l := binary.PutVarint(data, 573)
	data = data[:l]

	b := avro.NewBuffer(data)
	c := DateCodec{}

	var out time.Time
	if err := c.Read(b, unsafe.Pointer(&out)); err != nil {
		t.Fatal(err)
	}

	exp := time.Date(1971, 7, 27, 0, 0, 0, 0, time.UTC)
	if !out.Equal(exp) {
		t.Fatalf("times %s & %s differ by %s", exp, out, exp.Sub(out))
	}
}

func TestTimeLongPtr(t *testing.T) {
	now := time.Now()
	data := make([]byte, binary.MaxVarintLen64)
	l := binary.PutVarint(data, now.UnixNano())
	data = data[:l]

	b := avro.NewBuffer(data)

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

	buf := avro.NewBuffer(data)
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

	buf := avro.NewBuffer(data)
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
