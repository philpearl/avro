package avro

import (
	"bytes"
	"testing"
	"unsafe"

	"github.com/google/go-cmp/cmp"
)

func TestFloatCodec(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		exp  float32
	}{
		{
			name: "zero",
			data: []byte{0, 0, 0, 0},
		},
		{
			name: "something",
			data: []byte{0, 1, 0, 0},
			exp:  3.587324068671532e-43,
		},
	}
	var c FloatCodec
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			r := bytes.NewReader(test.data)
			var actual float32
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
			r := bytes.NewReader(test.data)
			if err := c.Skip(r); err != nil {
				t.Fatal(err)
			}
			if r.Len() != 0 {
				t.Fatalf("unread data %d", r.Len())
			}
		})
	}
}

func TestDoubleCodec(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		exp  float64
	}{
		{
			name: "zero",
			data: []byte{0, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			name: "something",
			data: []byte{0, 1, 0, 0, 0, 0, 0, 0},
			exp:  1.265e-321,
		},
	}
	var c DoubleCodec
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			r := bytes.NewReader(test.data)
			var actual float64
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
			r := bytes.NewReader(test.data)
			if err := c.Skip(r); err != nil {
				t.Fatal(err)
			}
			if r.Len() != 0 {
				t.Fatalf("unread data %d", r.Len())
			}
		})
	}
}

func TestFloat32DoubleCodec(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		exp  float32
	}{
		{
			name: "zero",
			data: []byte{0, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			name: "something",
			data: []byte{0, 1, 0, 0, 0, 0, 0, 0},
			exp:  1.265e-321,
		},
	}
	var c Float32DoubleCodec
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			r := bytes.NewReader(test.data)
			var actual float32
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
			r := bytes.NewReader(test.data)
			if err := c.Skip(r); err != nil {
				t.Fatal(err)
			}
			if r.Len() != 0 {
				t.Fatalf("unread data %d", r.Len())
			}
		})
	}
}
