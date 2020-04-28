package avro

import (
	"bytes"
	"reflect"
	"testing"
	"unsafe"

	"github.com/google/go-cmp/cmp"
)

func TestArrayCodec(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		exp  []string
		out  []string
	}{
		{
			name: "empty",
			data: []byte{0},
		},
		{
			name: "one",
			data: []byte{
				2,
				6, 'o', 'n', 'e',
				0,
			},
			exp: []string{"one"},
		},
		{
			name: "one append",
			data: []byte{
				2,
				6, 'o', 'n', 'e',
				0,
			},
			out: []string{"two"},
			exp: []string{"two", "one"},
		},
		{
			name: "two",
			data: []byte{
				4,
				6, 'o', 'n', 'e',
				6, 't', 'w', 'o',
				0,
			},
			exp: []string{"one", "two"},
		},
		{
			name: "two blocks",
			data: []byte{
				2,
				6, 'o', 'n', 'e',
				2,
				6, 't', 'w', 'o',
				0,
			},
			exp: []string{"one", "two"},
		},
		{
			name: "two blocks with size",
			data: []byte{
				1,
				8,
				6, 'o', 'n', 'e',
				1,
				8,
				6, 't', 'w', 'o',
				0,
			},
			exp: []string{"one", "two"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := arrayCodec{
				itemCodec: stringCodec{},
				itemType:  reflect.TypeOf(""),
			}

			buf := bytes.NewReader(test.data)

			if err := c.Read(buf, unsafe.Pointer(&test.out)); err != nil {
				t.Fatal(err)
			}

			if diff := cmp.Diff(test.exp, test.out); diff != "" {
				t.Fatalf("output not as expected. %s", diff)
			}
			if buf.Len() != 0 {
				t.Fatalf("unread data (%d)", buf.Len())
			}
		})
		t.Run(test.name+"_skip", func(t *testing.T) {
			c := arrayCodec{
				itemCodec: stringCodec{},
			}

			buf := bytes.NewReader(test.data)

			if err := c.Skip(buf); err != nil {
				t.Fatal(err)
			}

			if buf.Len() != 0 {
				t.Fatalf("unread data (%d)", buf.Len())
			}
		})

	}
}
