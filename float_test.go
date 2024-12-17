package avro

import (
	"math"
	"testing"
	"unsafe"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
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
			r := NewReadBuf(test.data)
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
			r := NewReadBuf(test.data)
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
			r := NewReadBuf(test.data)
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
			r := NewReadBuf(test.data)
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
			r := NewReadBuf(test.data)
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
			r := NewReadBuf(test.data)
			if err := c.Skip(r); err != nil {
				t.Fatal(err)
			}
			if r.Len() != 0 {
				t.Fatalf("unread data %d", r.Len())
			}
		})
	}
}

func TestFloatRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		val  float32
	}{
		{
			name: "zero",
			val:  0,
		},
		{
			name: "something",
			val:  3.587324068671532e-43,
		},
		{
			name: "negative",
			val:  -3.587324068671532e-43,
		},

		{
			name: "max",
			val:  3.4028234663852886e+38,
		},
		{
			name: "NAN",
			val:  float32(math.NaN()),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			w := NewWriteBuf(nil)
			var c FloatCodec
			c.Write(w, unsafe.Pointer(&test.val))
			r := NewReadBuf(w.Bytes())
			var actual float32
			if err := c.Read(r, unsafe.Pointer(&actual)); err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(test.val, actual, cmpopts.EquateNaNs()); diff != "" {
				t.Fatalf("result not as expected. %s", diff)
			}
		})
	}
}

func TestDoubleRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		val  float64
	}{
		{
			name: "zero",
			val:  0,
		},
		{
			name: "something",
			val:  3.587324068671532e-43,
		},
		{
			name: "negative",
			val:  -3.587324068671532e-43,
		},

		{
			name: "max",
			val:  3.4028234663852886e+38,
		},
		{
			name: "NAN",
			val:  math.NaN(),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			w := NewWriteBuf(nil)
			var c DoubleCodec
			c.Write(w, unsafe.Pointer(&test.val))
			r := NewReadBuf(w.Bytes())
			var actual float64
			if err := c.Read(r, unsafe.Pointer(&actual)); err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(test.val, actual, cmpopts.EquateNaNs()); diff != "" {
				t.Fatalf("result not as expected. %s", diff)
			}
		})
	}
}

func TestFloat32DoubleRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		val  float32
	}{
		{
			name: "zero",
			val:  0,
		},
		{
			name: "something",
			val:  3.587324068671532e-43,
		},
		{
			name: "negative",
			val:  -3.587324068671532e-43,
		},

		{
			name: "max",
			val:  3.4028234663852886e+38,
		},
		{
			name: "NAN",
			val:  float32(math.NaN()),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			w := NewWriteBuf(nil)
			var c Float32DoubleCodec
			c.Write(w, unsafe.Pointer(&test.val))
			r := NewReadBuf(w.Bytes())
			var actual float32
			if err := c.Read(r, unsafe.Pointer(&actual)); err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(test.val, actual, cmpopts.EquateNaNs()); diff != "" {
				t.Fatalf("result not as expected. %s", diff)
			}
		})
	}
}
