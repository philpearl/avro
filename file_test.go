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
	if err := ReadFile(bufio.NewReader(f), entry{}, func(val unsafe.Pointer, sb *ResourceBank) error {
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
		Typ  string   `json:"typ,omitempty"`
		Size *float32 `json:"size,omitempty"`
	}
	type entry struct {
		Name   *string `json:"name,omitempty"`
		Number **int32 `json:"number"`
		Owns   *[]*obj `json:"owns,omitempty"`
	}

	var actual []entry
	var sbs []*ResourceBank
	if err := ReadFile(bufio.NewReader(f), entry{}, func(val unsafe.Pointer, sb *ResourceBank) error {
		actual = append(actual, *(*entry)(val))
		sbs = append(sbs, sb)
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	strptr := func(v string) *string {
		return &v
	}
	floatptr := func(v float32) *float32 {
		return &v
	}
	var one int32 = 1
	oneptr := &one

	exp := []entry{
		{
			Name:   strptr("jim"),
			Number: &oneptr,
			Owns: &[]*obj{
				{
					Typ:  "hat",
					Size: floatptr(1),
				},
				{
					Typ:  "shoe",
					Size: floatptr(42),
				},
			},
		},
		{
			Name:   strptr("fred"),
			Number: &oneptr,
			Owns: &[]*obj{
				{
					Typ:  "bag",
					Size: floatptr(3.7),
				},
			},
		},
	}

	if diff := cmp.Diff(exp, actual); diff != "" {
		t.Fatalf("result differs. %s", diff)
	}
	for _, sb := range sbs {
		sb.Close()
	}
}

func TestFileSchema(t *testing.T) {
	schema, err := FileSchema("./testdata/avro1")
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(Schema{
		Type: "record",
		Object: &SchemaObject{
			Name: "Root",
			Fields: []SchemaRecordField{
				{
					Name: "name",
					Type: Schema{Type: "union", Union: []Schema{{Type: "null"}, {Type: "string"}}},
				},
				{
					Name: "number",
					Type: Schema{Type: "union", Union: []Schema{{Type: "null"}, {Type: "long"}}},
				},
				{
					Name: "owns",
					Type: Schema{
						Type: "array",
						Object: &SchemaObject{
							Items: Schema{
								Type: "record",
								Object: &SchemaObject{
									Name:      "Owns",
									Namespace: "root",
									Fields: []SchemaRecordField{
										{
											Name: "typ",
											Type: Schema{Type: "union", Union: []Schema{{Type: "null"}, {Type: "string"}}},
										},
										{
											Name: "size",
											Type: Schema{Type: "union", Union: []Schema{{Type: "null"}, {Type: "double"}}},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}, schema); diff != "" {
		t.Fatalf("not as expected: %s", diff)
	}
}
