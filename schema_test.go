package avro

import (
	"testing"

	"github.com/go-json-experiment/json"
	"github.com/google/go-cmp/cmp"
)

func TestSchemaEncoding(t *testing.T) {
	data, err := avroFileSchema.Marshal()
	if err != nil {
		t.Fatal(err)
	}

	var out Schema
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(avroFileSchema, out); diff != "" {
		t.Fatalf("results differ. %s", diff)
	}

	out2, err := SchemaFromString(string(data))
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(avroFileSchema, out2); diff != "" {
		t.Fatalf("results differ. %s", diff)
	}
}

func TestUnmarshal(t *testing.T) {
	// This tests we can unmarshal loads of different schemas correctly.
	tests := []struct {
		schema string
		want   Schema
	}{
		{
			schema: `{"type":"record","name":"test","fields":[{"name":"a","type":"int"}]}`,
			want: Schema{
				Type: "record",
				Object: &SchemaObject{
					Name: "test",
					Fields: []SchemaRecordField{
						{
							Name: "a",
							Type: Schema{
								Type: "int",
							},
						},
					},
				},
			},
		},
		{
			schema: `{"type":"enum","name":"test","symbols":["a","b"]}`,
			want: Schema{
				Type: "enum",
				Object: &SchemaObject{
					Name:    "test",
					Symbols: []string{"a", "b"},
				},
			},
		},
		{
			schema: `{"type":"fixed","name":"test","size":4}`,
			want: Schema{
				Type: "fixed",
				Object: &SchemaObject{
					Name: "test",
					Size: 4,
				},
			},
		},
		{
			schema: `{"type":"array","items":"int"}`,
			want: Schema{
				Type: "array",
				Object: &SchemaObject{
					Items: Schema{
						Type: "int",
					},
				},
			},
		},
		{
			schema: `{"type":"map","values":"int"}`,
			want: Schema{
				Type: "map",
				Object: &SchemaObject{
					Values: Schema{
						Type: "int",
					},
				},
			},
		},
		{
			schema: `"null"`,
			want: Schema{
				Type: "null",
			},
		},
		{
			schema: `"boolean"`,
			want: Schema{
				Type: "boolean",
			},
		},
		{
			schema: `"int"`,
			want: Schema{
				Type: "int",
			},
		},
		{
			schema: `"long"`,
			want: Schema{
				Type: "long",
			},
		},
		{
			schema: `"float"`,
			want: Schema{
				Type: "float",
			},
		},
		{
			schema: `"double"`,
			want: Schema{
				Type: "double",
			},
		},
		{
			schema: `"bytes"`,
			want: Schema{
				Type: "bytes",
			},
		},
		{
			schema: `"string"`,
			want: Schema{
				Type: "string",
			},
		},

		{
			schema: `["null","int"]`,
			want: Schema{
				Type: "union",
				Union: []Schema{
					{
						Type: "null",
					},
					{
						Type: "int",
					},
				},
			},
		},
	}

	for _, test := range tests {
		var got Schema
		if err := json.Unmarshal([]byte(test.schema), &got); err != nil {
			t.Fatalf("failed to unmarshal %s. %v", test.schema, err)
		}
		if diff := cmp.Diff(test.want, got); diff != "" {
			t.Fatalf("results differ. %s", diff)
		}

		data, err := json.Marshal(&test.want)
		if err != nil {
			t.Fatalf("failed to marshal. %v", err)
		}
		if string(data) != test.schema {
			t.Fatalf("expected %s got %s", test.schema, string(data))
		}
	}
}
