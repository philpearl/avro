package avroplenc_test

import (
	"bytes"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/philpearl/avro"
	"github.com/philpearl/avro/avroplenc"
	"github.com/philpearl/plenc/plenccodec"
)

func TestEncodeDecode(t *testing.T) {
	type yyStruct struct {
		A int
		B string
	}
	type testStruct struct {
		X int
		Y plenccodec.Optional[yyStruct]
	}

	if err := avroplenc.RegisterOptionalCodecFor[yyStruct](); err != nil {
		t.Fatal(err)
	}

	v := testStruct{
		X: 42,
		Y: plenccodec.OptionalOf(yyStruct{
			A: 7,
			B: "hello",
		}),
	}

	b := bytes.NewBuffer(nil)

	enc, err := avro.NewEncoderFor[testStruct](b, avro.CompressionSnappy, 1000)
	if err != nil {
		t.Fatalf("creating encoder: %v", err)
	}
	if err := enc.Encode(&v); err != nil {
		t.Fatalf("encode: %v", err)
	}
	if err := enc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	if err := avro.ReadFileFor[testStruct](b, func(val *testStruct, rb *avro.ResourceBank) error {
		if diff := cmp.Diff(v, *val); diff != "" {
			t.Fatalf("read value differs (-want +got):\n%s", diff)
		}
		return nil
	}); err != nil {
		t.Fatalf("read file: %v", err)
	}
}
