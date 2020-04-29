package avro

import (
	"bytes"
	"testing"
	"unsafe"

	"github.com/google/go-cmp/cmp"
)

func TestFixed(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		exp  [3]byte
	}{
		{
			name: "basic",
			data: []byte{1, 2, 3},
			exp:  [3]byte{1, 2, 3},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := fixedCodec{Size: 3}
			b := bytes.NewReader(test.data)
			var actual [3]byte
			if err := c.Read(b, unsafe.Pointer(&actual)); err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(test.exp, actual); diff != "" {
				t.Fatalf("result differs: %s", diff)
			}
			if b.Len() != 0 {
				t.Fatalf("Not all data read: %d", b.Len())
			}
		})
		t.Run(test.name+" skip", func(t *testing.T) {
			t.Parallel()
			c := fixedCodec{Size: 3}
			b := bytes.NewReader(test.data)
			if err := c.Skip(b); err != nil {
				t.Fatal(err)
			}
			if b.Len() != 0 {
				t.Fatalf("Not all data read: %d", b.Len())
			}
		})

	}
}
