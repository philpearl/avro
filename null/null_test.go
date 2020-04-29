package null

import (
	"bufio"
	"os"
	"testing"
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
	if err := avro.ReadFile(bufio.NewReader(f), mystruct{}, func(val unsafe.Pointer) error {
		actual = append(actual, *(*mystruct)(val))
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
}
