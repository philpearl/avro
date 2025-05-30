package avro

import (
	"bytes"
	"testing"
	"unsafe"
)

func TestBoolCodec(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		exp  bool
	}{
		{
			name: "true",
			data: []byte{1},
			exp:  true,
		},
		{
			name: "false",
			data: []byte{0},
			exp:  false,
		},
	}

	c := BoolCodec{}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			var actual bool
			r := NewReadBuf(test.data)
			if err := c.Read(r, unsafe.Pointer(&actual)); err != nil {
				t.Fatal(err)
			}
			if actual != test.exp {
				t.Fatalf("got %t, expected %t", actual, test.exp)
			}
			if r.Len() != 0 {
				t.Fatalf("%d bytes left", r.Len())
			}
		})

		t.Run(test.name+" skip", func(t *testing.T) {
			t.Parallel()
			r := NewReadBuf(test.data)
			if err := c.Skip(r); err != nil {
				t.Fatal(err)
			}
			if r.Len() != 0 {
				t.Fatalf("%d bytes left", r.Len())
			}
		})
	}
}

func BenchmarkBoolPointer(b *testing.B) {
	data := bytes.Repeat([]byte{1}, 1000)
	r := NewReadBuf(data)

	c := PointerCodec{BoolCodec{}}
	b.ReportAllocs()

	for b.Loop() {
		r.Reset(data)
		for range 1000 {
			var out *bool
			if err := c.Read(r, unsafe.Pointer(&out)); err != nil {
				b.Fatal(err)
			}
			if !*out {
				b.Fatal("wrong bool")
			}
		}
		r.ExtractResourceBank().Close()
	}
}

func TestBoolCodecRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		data bool
	}{
		{
			name: "true",
			data: true,
		},
		{
			name: "false",
			data: false,
		},
	}

	c := BoolCodec{}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			var actual bool
			w := NewWriteBuf(nil)
			c.Write(w, unsafe.Pointer(&test.data))
			r := NewReadBuf(w.Bytes())
			if err := c.Read(r, unsafe.Pointer(&actual)); err != nil {
				t.Fatal(err)
			}
			if actual != test.data {
				t.Fatalf("got %t, expected %t", actual, test.data)
			}
		})
	}
}
