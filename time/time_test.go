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
	now := time.Now()
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
