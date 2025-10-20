package avro

import (
	"testing"
	"unsafe"
)

func TestUnionCodec(t *testing.T) {
	c := unionCodec{
		codecs: []Codec{nullCodec{}, StringCodec{}},
	}

	tests := []struct {
		name string
		data []byte
		exp  string
	}{
		{
			name: "null",
			data: []byte{0},
			exp:  "",
		},
		{
			name: "string",
			data: []byte{2, 6, 'f', 'o', 'o'},
			exp:  "foo",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			r := NewReadBuf(test.data)
			var actual string
			if err := c.Read(r, unsafe.Pointer(&actual)); err != nil {
				t.Fatal(err)
			}
			if actual != test.exp {
				t.Fatalf("result %q does not match expected %q", actual, test.exp)
			}
			if r.Len() != 0 {
				t.Fatalf("%d bytes unread", r.Len())
			}
		})
		t.Run(test.name+" skip", func(t *testing.T) {
			r := NewReadBuf(test.data)
			if err := c.Skip(r); err != nil {
				t.Fatal(err)
			}
			if r.Len() != 0 {
				t.Fatalf("%d bytes unread", r.Len())
			}
		})

	}
}

func TestUnionOneCodec(t *testing.T) {
	c := unionOneAndNullCodec{
		codec:   StringCodec{},
		nonNull: 1,
	}

	tests := []struct {
		name string
		data []byte
		exp  string
	}{
		{
			name: "null",
			data: []byte{0},
			exp:  "",
		},
		{
			name: "string",
			data: []byte{2, 6, 'f', 'o', 'o'},
			exp:  "foo",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			r := NewReadBuf(test.data)
			var actual string
			if err := c.Read(r, unsafe.Pointer(&actual)); err != nil {
				t.Fatal(err)
			}
			if actual != test.exp {
				t.Fatalf("result %q does not match expected %q", actual, test.exp)
			}
			if r.Len() != 0 {
				t.Fatalf("%d bytes unread", r.Len())
			}
		})
		t.Run(test.name+" skip", func(t *testing.T) {
			r := NewReadBuf(test.data)
			if err := c.Skip(r); err != nil {
				t.Fatal(err)
			}
			if r.Len() != 0 {
				t.Fatalf("%d bytes unread", r.Len())
			}
		})
		t.Run(test.name+" roundtrip", func(t *testing.T) {
			w := NewWriteBuf(nil)
			c.Write(w, unsafe.Pointer(&test.exp))
			var actual string
			r := NewReadBuf(w.Bytes())
			if err := c.Read(r, unsafe.Pointer(&actual)); err != nil {
				t.Fatal(err)
			}
			if actual != test.exp {
				t.Fatalf("result %q does not match expected %q", actual, test.exp)
			}
		})
	}
}

func TestUnionStringCodec(t *testing.T) {
	c := unionNullString{
		nonNull: 1,
	}

	tests := []struct {
		name string
		data []byte
		exp  string
	}{
		{
			name: "null",
			data: []byte{0},
			exp:  "",
		},
		{
			name: "string",
			data: []byte{2, 6, 'f', 'o', 'o'},
			exp:  "foo",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			r := NewReadBuf(test.data)
			var actual string
			if err := c.Read(r, unsafe.Pointer(&actual)); err != nil {
				t.Fatal(err)
			}
			if actual != test.exp {
				t.Fatalf("result %q does not match expected %q", actual, test.exp)
			}
			if r.Len() != 0 {
				t.Fatalf("%d bytes unread", r.Len())
			}
		})
		t.Run(test.name+" skip", func(t *testing.T) {
			r := NewReadBuf(test.data)
			if err := c.Skip(r); err != nil {
				t.Fatal(err)
			}
			if r.Len() != 0 {
				t.Fatalf("%d bytes unread", r.Len())
			}
		})
		t.Run(test.name+" roundtrip", func(t *testing.T) {
			w := NewWriteBuf(nil)
			c.Write(w, unsafe.Pointer(&test.exp))
			var actual string
			r := NewReadBuf(w.Bytes())
			if err := c.Read(r, unsafe.Pointer(&actual)); err != nil {
				t.Fatal(err)
			}
			if actual != test.exp {
				t.Fatalf("result %q does not match expected %q", actual, test.exp)
			}
		})
		t.Run(test.name+" roundtrip omitempty", func(t *testing.T) {
			c := unionNullString{
				nonNull: 1,
				codec:   StringCodec{omitEmpty: true},
			}

			w := NewWriteBuf(nil)
			c.Write(w, unsafe.Pointer(&test.exp))
			var actual string
			r := NewReadBuf(w.Bytes())
			if err := c.Read(r, unsafe.Pointer(&actual)); err != nil {
				t.Fatal(err)
			}
			if actual != test.exp {
				t.Fatalf("result %q does not match expected %q", actual, test.exp)
			}
		})

	}
}

func BenchmarkUnionStringCodec(b *testing.B) {
	c := unionNullString{
		nonNull: 1,
	}
	data := []byte{2, 6, 'f', 'o', 'o'}

	b.Run("read", func(b *testing.B) {
		b.ReportAllocs()
		r := NewReadBuf(nil)
		var actual string
		for b.Loop() {
			r.Reset(data)
			if err := c.Read(r, unsafe.Pointer(&actual)); err != nil {
				b.Fatal(err)
			}
			r.ExtractResourceBank().Close()
		}
	})
	b.Run("skip", func(b *testing.B) {
		b.ReportAllocs()
		r := NewReadBuf(nil)
		for b.Loop() {
			r.Reset(data)
			if err := c.Skip(r); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkUnionOneCodec(b *testing.B) {
	c := unionOneAndNullCodec{
		codec:   StringCodec{},
		nonNull: 1,
	}
	data := []byte{2, 6, 'f', 'o', 'o'}

	b.Run("read", func(b *testing.B) {
		b.ReportAllocs()
		r := NewReadBuf(nil)
		var actual string
		for b.Loop() {
			r.Reset(data)
			if err := c.Read(r, unsafe.Pointer(&actual)); err != nil {
				b.Fatal(err)
			}
			r.ExtractResourceBank().Close()
		}
	})
	b.Run("skip", func(b *testing.B) {
		b.ReportAllocs()
		r := NewReadBuf(nil)
		for b.Loop() {
			r.Reset(data)
			if err := c.Skip(r); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkUnionCodec(b *testing.B) {
	c := unionCodec{
		codecs: []Codec{nullCodec{}, StringCodec{}},
	}
	data := []byte{2, 6, 'f', 'o', 'o'}

	b.Run("read", func(b *testing.B) {
		b.ReportAllocs()
		r := NewReadBuf(nil)
		var actual string
		for b.Loop() {
			r.Reset(data)
			if err := c.Read(r, unsafe.Pointer(&actual)); err != nil {
				b.Fatal(err)
			}
			r.ExtractResourceBank().Close()
		}
	})
	b.Run("skip", func(b *testing.B) {
		b.ReportAllocs()
		r := NewReadBuf(nil)
		for b.Loop() {
			r.Reset(data)
			if err := c.Skip(r); err != nil {
				b.Fatal(err)
			}
		}
	})
}
