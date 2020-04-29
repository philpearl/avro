package avro

import (
	"bufio"
	"os"
	"testing"
	"unsafe"

	"github.com/google/go-cmp/cmp"
)

func TestReadFile(t *testing.T) {
	f, err := os.Open("./testdata/avro1")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	type obj struct {
		Typ  string  `json:"typ,omitempty"`
		Size float64 `json:"size,omitempty"`
	}
	type entry struct {
		Name   string `json:"name,omitempty"`
		Number int64  `json:"number"`
		Owns   []obj  `json:"owns,omitempty"`
	}

	var actual []entry
	if err := ReadFile(bufio.NewReader(f), entry{}, func(val unsafe.Pointer) error {
		actual = append(actual, *(*entry)(val))
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	exp := []entry{
		{
			Name:   "jim",
			Number: 1,
			Owns: []obj{
				{
					Typ:  "hat",
					Size: 1,
				},
				{
					Typ:  "shoe",
					Size: 42,
				},
			},
		},
		{
			Name:   "fred",
			Number: 1,
			Owns: []obj{
				{
					Typ:  "bag",
					Size: 3.7,
				},
			},
		},
	}

	if diff := cmp.Diff(exp, actual); diff != "" {
		t.Fatalf("result differs. %s", diff)
	}
}

func TestReadFileAlt(t *testing.T) {
	f, err := os.Open("./testdata/avro1")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	type obj struct {
		Typ  string  `json:"typ,omitempty"`
		Size float32 `json:"size,omitempty"`
	}
	type entry struct {
		Name   string `json:"name,omitempty"`
		Number *int32 `json:"number"`
		Owns   []*obj `json:"owns,omitempty"`
	}

	var actual []entry
	if err := ReadFile(bufio.NewReader(f), entry{}, func(val unsafe.Pointer) error {
		actual = append(actual, *(*entry)(val))
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	intptr := func(v int32) *int32 {
		return &v
	}

	exp := []entry{
		{
			Name:   "jim",
			Number: intptr(1),
			Owns: []*obj{
				{
					Typ:  "hat",
					Size: 1,
				},
				{
					Typ:  "shoe",
					Size: 42,
				},
			},
		},
		{
			Name:   "fred",
			Number: intptr(1),
			Owns: []*obj{
				{
					Typ:  "bag",
					Size: 3.7,
				},
			},
		},
	}

	if diff := cmp.Diff(exp, actual); diff != "" {
		t.Fatalf("result differs. %s", diff)
	}
}
