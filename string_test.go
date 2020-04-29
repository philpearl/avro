package avro

import (
	"bytes"
	"testing"
	"unsafe"
)

func TestStringCodec(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		exp  string
	}{
		{
			name: "empty",
			data: []byte{0},
			exp:  "",
		},
		{
			name: "hello",

			data: []byte{10, 'h', 'e', 'l', 'l', 'o'},
			exp:  "hello",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			r := bytes.NewReader(test.data)
			c := stringCodec{}

			var actual string
			if err := c.Read(r, unsafe.Pointer(&actual)); err != nil {
				t.Fatal(err)
			}
			if test.exp != actual {
				t.Fatalf("%q does not match expected %q", actual, test.exp)
			}
			if r.Len() != 0 {
				t.Fatalf("%d bytes left", r.Len())
			}
		})

		t.Run(test.name+" skip", func(t *testing.T) {
			r := bytes.NewReader(test.data)
			c := stringCodec{}

			if err := c.Skip(r); err != nil {
				t.Fatal(err)
			}
			if r.Len() != 0 {
				t.Fatalf("%d bytes left", r.Len())
			}
		})

	}
}
