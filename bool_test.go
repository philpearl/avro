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
			r := bytes.NewReader(test.data)
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
			r := bytes.NewReader(test.data)
			if err := c.Skip(r); err != nil {
				t.Fatal(err)
			}
			if r.Len() != 0 {
				t.Fatalf("%d bytes left", r.Len())
			}
		})
	}
}
