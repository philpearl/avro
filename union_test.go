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
			r := NewBuffer(test.data)
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
			r := NewBuffer(test.data)
			if err := c.Skip(r); err != nil {
				t.Fatal(err)
			}
			if r.Len() != 0 {
				t.Fatalf("%d bytes unread", r.Len())
			}
		})

	}
}
