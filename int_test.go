package avro

import (
	"math"
	"testing"
	"unsafe"

	"github.com/google/go-cmp/cmp"
)

func TestInt64Codec(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		exp  int64
	}{
		{
			name: "zero",
			data: []byte{0},
		},
		{
			name: "something",
			data: []byte{46},
			exp:  23,
		},
		{
			name: "-something",
			data: []byte{45},
			exp:  -23,
		},
		{
			name: "max",
			data: []byte{254, 255, 255, 255, 255, 255, 255, 255, 255, 1},
			exp:  math.MaxInt64,
		},
		{
			name: "min",
			data: []byte{255, 255, 255, 255, 255, 255, 255, 255, 255, 1},
			exp:  math.MinInt64,
		},
	}
	var c Int64Codec
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			r := NewBuffer(test.data)
			var actual int64
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

func TestInt32Codec(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		exp  int32
	}{
		{
			name: "zero",
			data: []byte{0},
		},
		{
			name: "something",
			data: []byte{46},
			exp:  23,
		},
		{
			name: "-something",
			data: []byte{45},
			exp:  -23,
		},
		{
			name: "max",
			data: []byte{254, 255, 255, 255, 15},
			exp:  math.MaxInt32,
		},
		{
			name: "min",
			data: []byte{255, 255, 255, 255, 15},
			exp:  math.MinInt32,
		},
	}
	var c Int32Codec
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			r := NewBuffer(test.data)
			var actual int32
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

func TestInt16TooBig(t *testing.T) {
	var c Int16Codec
	r := NewBuffer([]byte{128, 128, 4})
	var actual int16
	err := c.Read(r, unsafe.Pointer(&actual))
	if err == nil {
		t.Fatal("expected an error")
	}
	if s := err.Error(); s != "value 32768 will not fit in int16" {
		t.Fatalf("error not as expected: %q", s)
	}
}

func TestInt16Codec(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		exp  int16
	}{
		{
			name: "zero",
			data: []byte{0},
		},
		{
			name: "something",
			data: []byte{46},
			exp:  23,
		},
		{
			name: "-something",
			data: []byte{45},
			exp:  -23,
		},
		{
			name: "max",
			data: []byte{254, 255, 3},
			exp:  math.MaxInt16,
		},
		{
			name: "min",
			data: []byte{255, 255, 3},
			exp:  math.MinInt16,
		},
	}
	var c Int16Codec
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			r := NewBuffer(test.data)
			var actual int16
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
