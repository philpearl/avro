package avro

import (
	"testing"
	"unsafe"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
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
			data: []byte{10, 1, 2, 3, 4, 5},
			exp:  []byte{1, 2, 3, 4, 5},
		},
	}
	var c BytesCodec
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			r := NewBuffer(test.data)
			var actual []byte
			if err := c.Read(r, unsafe.Pointer(&actual)); err != nil {
				t.Fatal(err)
			}

			if diff := cmp.Diff(test.exp, actual); diff != "" {
				t.Fatalf("result not as expected. %s", diff)
			}
			if r.Len() != 0 {
				t.Fatalf("unread data %d", r.Len())
			}
		})
		t.Run(test.name+" skip", func(t *testing.T) {
			t.Parallel()
			r := NewBuffer(test.data)
			if err := c.Skip(r); err != nil {
				t.Fatal(err)
			}
			if r.Len() != 0 {
				t.Fatalf("unread data %d", r.Len())
			}
		})

	}
}

func TestBytesRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		in   []byte
	}{
		{
			name: "empty",
			in:   []byte{},
		},
		{
			name: "zero",
			in:   []byte{0},
		},

		{
			name: "hello",
			in:   []byte("hello"),
		},
	}

	var c BytesCodec
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			buf := NewWriteBuf(nil)
			c.Write(buf, unsafe.Pointer(&test.in))

			var actual []byte
			if err := c.Read(NewBuffer(buf.Bytes()), unsafe.Pointer(&actual)); err != nil {
				t.Fatal(err)
			}

			if diff := cmp.Diff(test.in, actual, cmpopts.EquateEmpty()); diff != "" {
				t.Fatalf("output not as expected. %s", diff)
			}
		})
	}
}
