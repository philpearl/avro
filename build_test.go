package avro

import (
	"reflect"
	"testing"
)

func TestBuildCodec(t *testing.T) {
	t.Parallel()

	type some struct {
		I []int32
	}

	type all struct {
		A bool
		B int32
		C int64
		D float32
		E float64
		F []byte
		G string
		H some
		J map[string]int
		K [4]byte
		L int16
	}

	allSchema := Schema{
		Type: "record",
		Object: &SchemaObject{
			Fields: []SchemaRecordField{
				{
					Name: "A",
					Type: Schema{Type: "boolean"},
				},
				{
					Name: "B",
					Type: Schema{Type: "int"},
				},
				{
					Name: "C",
					Type: Schema{Type: "long"},
				},
				{
					Name: "D",
					Type: Schema{Type: "float"},
				},
				{
					Name: "E",
					Type: Schema{Type: "double"},
				},
				{
					Name: "F",
					Type: Schema{Type: "bytes"},
				},
				{
					Name: "G",
					Type: Schema{Type: "string"},
				},
				{
					Name: "H",
					Type: Schema{
						Type: "record",
						Object: &SchemaObject{
							Name: "some",
							Fields: []SchemaRecordField{
								{
									Name: "I",
									Type: Schema{
										Type: "array",
										Object: &SchemaObject{
											Items: Schema{Type: "int"},
										},
									},
								},
							},
						},
					},
				},
				{
					Name: "J",
					Type: Schema{
						Type: "map",
						Object: &SchemaObject{
							Values: Schema{Type: "long"},
						},
					},
				},
				{
					Name: "K",
					Type: Schema{
						Type: "fixed",
						Object: &SchemaObject{
							Size: 4,
						},
					},
				},
				{
					Name: "L",
					Type: Schema{
						Type: "long",
					},
				},
			},
		},
	}

	c, err := buildCodec(allSchema, reflect.TypeOf(all{}), false)
	if err != nil {
		t.Fatal(err)
	}

	_ = c
}
