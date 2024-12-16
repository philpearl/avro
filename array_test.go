package avro

import (
	"reflect"
	"testing"
	"unsafe"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
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

	c := arrayCodec{
		itemCodec: StringCodec{},
		itemType:  reflect.TypeOf(""),
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			buf := NewBuffer(test.data)

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
			buf := NewBuffer(test.data)

			if err := c.Skip(buf); err != nil {
				t.Fatal(err)
			}

			if buf.Len() != 0 {
				t.Fatalf("unread data (%d)", buf.Len())
			}
		})

	}
}

func TestArrayCodecInt(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		exp  []int32
		out  []int32
	}{
		{
			name: "empty",
			data: []byte{0},
		},
		{
			name: "one",
			data: []byte{
				2,
				2,
				0,
			},
			exp: []int32{1},
		},
		{
			name: "one append",
			data: []byte{
				2,
				2,
				0,
			},
			out: []int32{2},
			exp: []int32{2, 1},
		},
		{
			name: "more",
			data: []byte{
				8,
				1,
				2,
				3,
				4,
				0,
			},
			exp: []int32{-1, 1, -2, 2},
		},
		{
			name: "two blocks",
			data: []byte{
				2,
				2,
				2,
				4,
				0,
			},
			exp: []int32{1, 2},
		},
	}

	c := arrayCodec{
		itemCodec: Int32Codec{},
		itemType:  reflect.TypeOf(int32(0)),
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			buf := NewBuffer(test.data)

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
			t.Parallel()
			buf := NewBuffer(test.data)

			if err := c.Skip(buf); err != nil {
				t.Fatal(err)
			}

			if buf.Len() != 0 {
				t.Fatalf("unread data (%d)", buf.Len())
			}
		})

	}
}

func TestArrayCodecRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		data []string
	}{
		{
			name: "empty",
			data: []string{},
		},
		{
			name: "one",
			data: []string{"one"},
		},
		{
			name: "two",
			data: []string{"one", "two"},
		},
		{
			name: "three",
			data: []string{"one", "two", "three"},
		},
	}

	c := arrayCodec{
		itemCodec: StringCodec{},
		itemType:  reflect.TypeOf(""),
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			w := NewWriteBuf(nil)

			c.Write(w, unsafe.Pointer(&test.data))

			var out []string
			r := NewBuffer(w.Bytes())
			if err := c.Read(r, unsafe.Pointer(&out)); err != nil {
				t.Fatal(err)
			}

			if diff := cmp.Diff(test.data, out, cmpopts.EquateEmpty()); diff != "" {
				t.Fatalf("output not as expected. %s", diff)
			}
		})
	}
}
