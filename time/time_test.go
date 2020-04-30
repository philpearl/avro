package time

import (
	"bufio"
	"bytes"
	"io"
	"testing"
	"time"
	"unsafe"
)

func TestTime(t *testing.T) {
	now := time.Now()
	ts := now.Format(time.RFC3339Nano)
	data := []byte{byte(len(ts) << 1)}
	data = append(data, ts...)

	b := bytes.NewReader(data)
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

	b := bytes.NewReader(data)
	c := StringCodec{}

	var out time.Time
	if err := c.Read(bufio.NewReader(b), unsafe.Pointer(&out)); err != nil {
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

	buf := bytes.NewReader(data)
	c := StringCodec{}
	r := bufio.NewReader(buf)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		buf.Seek(io.SeekStart, 0)
		r.Reset(buf)

		var out time.Time
		if err := c.Read(r, unsafe.Pointer(&out)); err != nil {
			b.Fatal(err)
		}
	}
}
