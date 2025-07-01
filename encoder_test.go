package avro_test

import (
	"bytes"
	"testing"
	"unsafe"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/philpearl/avro"
)

func TestEncoder(t *testing.T) {
	type myStruct struct {
		Name  string `json:"name"`
		Hat   string `json:",omitempty"`
		V     int
		Q     float64
		Bytes []byte
		La    []int  `json:"la"`
		W     int32  `json:"w,omitempty"`
		Z     *int64 `json:"z"`
		Mmm   map[string]string
		N     uint64
	}

	buf := bytes.NewBuffer(nil)

	enc, err := avro.NewEncoderFor[myStruct](buf, avro.CompressionSnappy, 10_000)
	if err != nil {
		t.Fatal(err)
	}

	contents := []myStruct{
		{
			Name:  "jim",
			Hat:   "cat",
			V:     31,
			Q:     3.14,
			Bytes: []byte{1, 2, 3, 4},
			La:    []int{1, 2, 3, 4},
			W:     0,
			Z:     new(int64),
			Mmm:   map[string]string{"foo": "bar", "baz": "qux"},
			N:     99,
		},
		{
			Name:  "jim",
			Hat:   "cat",
			V:     31,
			Q:     3.14,
			Bytes: []byte{1, 2, 3, 4},
			La:    []int{1, 2, 3, 4},
			W:     0,
			Z:     nil,
			Mmm:   map[string]string{"foo": "bar", "baz": "qux"},
			N:     0x7F_FF_FF_FF_FF_FF_FF_FF,
		},
		{
			Name:  "jim",
			Hat:   "cat",
			V:     31,
			Q:     0,
			Bytes: []byte{1, 2, 3, 4},
			W:     0,
			Z:     new(int64),
			Mmm:   map[string]string{"foo": "bar", "baz": "qux"},
		},

		{
			Name:  "jim",
			Hat:   "cat",
			V:     31,
			Q:     0,
			Bytes: []byte{1, 2, 3, 4},
			W:     0,
			Z:     new(int64),
		},
		{},
	}

	for i := range contents {
		if err := enc.Encode(&contents[i]); err != nil {
			t.Fatal(err)
		}
	}

	if err := enc.Flush(); err != nil {
		t.Fatal(err)
	}

	var actual []myStruct
	if err := avro.ReadFile(buf, myStruct{}, func(val unsafe.Pointer, rb *avro.ResourceBank) error {
		v := *(*myStruct)(val)
		actual = append(actual, v)
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if diff := cmp.Diff(contents, actual, cmpopts.EquateEmpty()); diff != "" {
		t.Fatalf("result not as expected. %s", diff)
	}
}
