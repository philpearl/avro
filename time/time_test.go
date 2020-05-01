package time

import (
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

func TestTimeBufio(t *testing.T) {
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
