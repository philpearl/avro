package null

import (
	"bufio"
	"bytes"
	"os"
	"testing"
	"time"
	"unsafe"

	"github.com/google/go-cmp/cmp"
	"github.com/philpearl/avro"
	"github.com/unravelin/null"
)

func TestNullThings(t *testing.T) {
	RegisterCodecs()

	type mystruct struct {
		String null.String `json:"string,omitempty"`
		Int    null.Int    `json:"int,omitempty"`
		Bool   null.Bool   `json:"bool,omitempty"`
		Float  null.Float  `json:"float,omitempty"`
	}

	f, err := os.Open("./testdata/nullavro")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	var actual []mystruct
	var sbs []*avro.ResourceBank
	if err := avro.ReadFile(bufio.NewReader(f), mystruct{}, func(val unsafe.Pointer, sb *avro.ResourceBank) error {
		actual = append(actual, *(*mystruct)(val))
		sbs = append(sbs, sb)
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	exp := []mystruct{
		{
			String: null.StringFrom("String"),
			Int:    null.IntFrom(42),
			Bool:   null.BoolFrom(false),
			Float:  null.FloatFrom(13.37),
		},
		{},
	}

	if diff := cmp.Diff(exp, actual); diff != "" {
		t.Fatalf("result differs. %s", diff)
	}
	for _, sb := range sbs {
		sb.Close()
	}
}

func TestNullRoundTrip(t *testing.T) {
	RegisterCodecs()

	type mystruct struct {
		String null.String `json:"string,omitempty"`
		Int    null.Int    `json:"int,omitempty"`
		Bool   null.Bool   `json:"bool,omitempty"`
		Float  null.Float  `json:"float,omitempty"`
		Time   null.Time   `json:"time,omitempty"`
	}

	var buf bytes.Buffer

	enc, err := avro.NewEncoderFor[mystruct](&buf, avro.CompressionSnappy, 1024)
	if err != nil {
		t.Fatal(err)
	}

	if err := enc.Encode(mystruct{
		String: null.StringFrom("String"),
		Int:    null.IntFrom(42),
		Bool:   null.BoolFrom(true),
		Float:  null.FloatFrom(13.37),
		Time:   null.TimeFrom(time.Date(1970, 3, 15, 13, 37, 42, 0, time.UTC)),
	}); err != nil {
		t.Fatal(err)
	}

	if err := enc.Encode(mystruct{}); err != nil {
		t.Fatal(err)
	}

	if err := enc.Encode(mystruct{
		String: null.StringFrom(""),
		Int:    null.IntFrom(0),
		Bool:   null.BoolFrom(false),
		Float:  null.FloatFrom(0.0),
		Time:   null.TimeFrom(time.Time{}),
	}); err != nil {
		t.Fatal(err)
	}

	if err := enc.Flush(); err != nil {
		t.Fatal(err)
	}

	var actual []mystruct
	var sbs []*avro.ResourceBank
	if err := avro.ReadFile(&buf, mystruct{}, func(val unsafe.Pointer, sb *avro.ResourceBank) error {
		actual = append(actual, *(*mystruct)(val))
		sbs = append(sbs, sb)
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	exp := []mystruct{
		{
			String: null.StringFrom("String"),
			Int:    null.IntFrom(42),
			Bool:   null.BoolFrom(true),
			Float:  null.FloatFrom(13.37),
			Time:   null.TimeFrom(time.Date(1970, 3, 15, 13, 37, 42, 0, time.UTC)),
		},
		{},
		{
			String: null.StringFrom(""),
			Int:    null.IntFrom(0),
			Bool:   null.BoolFrom(false),
			Float:  null.FloatFrom(0.0),
			Time:   null.TimeFrom(time.Time{}),
		},
	}

	if diff := cmp.Diff(exp, actual); diff != "" {
		t.Fatalf("result differs. %s", diff)
	}
	for _, sb := range sbs {
		sb.Close()
	}
}
