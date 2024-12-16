package avro

import (
	"reflect"
	"testing"
	"unsafe"

	"github.com/google/go-cmp/cmp"
)

func TestMapCodec(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		exp  map[string][]byte
	}{
		{
			name: "1 simple block",
			data: []byte{
				// block count
				2, // meaning 1
				// no block size for positive count
				// key
				6, 'f', 'o', 'o',
				// value
				8, 1, 2, 3, 4,
				// zero block
				0,
			},
			exp: map[string][]byte{
				"foo": {1, 2, 3, 4},
			},
		},
		{
			name: "block with size",
			data: []byte{
				// block count
				1,
				18,
				// key
				6, 'f', 'o', 'o',
				// value
				8, 1, 2, 3, 4,
				// zero block
				0,
			},
			exp: map[string][]byte{
				"foo": {1, 2, 3, 4},
			},
		},

		{
			name: "1 simple block, 2 vals",
			data: []byte{
				// block count
				4, // meaning 2
				// no block size for positive count
				// key
				6, 'f', 'o', 'o',
				// value
				8, 1, 2, 3, 4,
				// key
				6, 'b', 'a', 'r',
				// value
				8, 4, 3, 2, 1,
				// zero block
				0,
			},
			exp: map[string][]byte{
				"foo": {1, 2, 3, 4},
				"bar": {4, 3, 2, 1},
			},
		},
		{
			name: "2 simple blocks",
			data: []byte{
				// block count
				2, // meaning 1
				// no block size for positive count
				// key
				6, 'f', 'o', 'o',
				// value
				8, 1, 2, 3, 4,
				// Next block
				2, // meaning 1
				// no block size for positive count
				// key
				6, 'b', 'a', 'r',
				// value
				8, 4, 3, 2, 1,
				// zero block
				0,
			},
			exp: map[string][]byte{
				"foo": {1, 2, 3, 4},
				"bar": {4, 3, 2, 1},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var m map[string][]byte
			typ := reflect.TypeOf(m)
			c := MapCodec{rtype: typ, valueCodec: BytesCodec{}}

			r := NewBuffer(test.data)

			if err := c.Read(r, unsafe.Pointer(&m)); err != nil {
				t.Fatal(err)
			}

			if diff := cmp.Diff(test.exp, m); diff != "" {
				t.Fatalf("map not as expected. %s", diff)
			}

			if r.Len() != 0 {
				t.Fatalf("unread bytes. %d", r.Len())
			}
		})

		t.Run(test.name+" skip", func(t *testing.T) {
			c := MapCodec{valueCodec: BytesCodec{}}
			r := NewBuffer(test.data)
			if err := c.Skip(r); err != nil {
				t.Fatal(err)
			}
			if r.Len() != 0 {
				t.Fatalf("unread bytes. %d", r.Len())
			}
		})

		t.Run(test.name+" roundtrip", func(t *testing.T) {
			typ := reflect.TypeOf(test.exp)
			c := MapCodec{rtype: typ, valueCodec: BytesCodec{}}
			w := NewWriter(nil)

			if err := c.Write(w, (unsafe.Pointer)(&test.exp)); err != nil {
				t.Fatal(err)
			}
			var actual map[string][]byte
			r := NewBuffer(w.Bytes())
			if err := c.Read(r, unsafe.Pointer(&actual)); err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(test.exp, actual); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}
