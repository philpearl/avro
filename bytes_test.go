package avro

import (
	"bytes"
	"testing"
	"unsafe"

	"github.com/google/go-cmp/cmp"
)

func TestBytesCodec(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		exp  []byte
	}{
		{
			name: "empty",
			data: []byte{0},
		},
		{
			name: "small", // 10 is 5
			data: []byte{10, 1, 2, 3, 4, 5, 6, 7},
			exp:  []byte{1, 2, 3, 4, 5},
		},
		{
			name: "small end of", // 10 is 5
			data: []byte{10, 1, 2, 3, 4, 5},
			exp:  []byte{1, 2, 3, 4, 5},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var c bytesCodec
			r := bytes.NewReader(test.data)
			var actual []byte
			if err := c.Read(r, unsafe.Pointer(&actual)); err != nil {
				t.Fatal(err)
			}

			if diff := cmp.Diff(test.exp, actual); diff != "" {
				t.Fatalf("result not as expected. %s", diff)
			}
		})
	}
}
