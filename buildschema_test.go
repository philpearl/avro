package avro_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/philpearl/avro"
)

func TestBuildSchema(t *testing.T) {
	tests := []struct {
		name string
		in   any
		exp  avro.Schema
	}{
		{
			name: "int",
			in: struct {
				A int `json:"aaa"`
			}{},
			exp: avro.Schema{
				Type: "record",
				Object: &avro.SchemaObject{
					Fields: []avro.SchemaRecordField{
						{
							Name: "aaa",
							Type: avro.Schema{
								Type: "long",
							},
						},
					},
				},
			},
		},
		{
			name: "int omitempty",
			in: struct {
				A int `json:"aaa,omitempty"`
			}{},
			exp: avro.Schema{
				Type: "record",
				Object: &avro.SchemaObject{
					Fields: []avro.SchemaRecordField{
						{
							Name: "aaa",
							Type: avro.Schema{
								Type: "union",
								Union: []avro.Schema{
									{Type: "null"},
									{Type: "long"},
								},
							},
						},
					},
				},
			},
		},

		{
			name: "int skip unexported",
			in: struct {
				A int `json:"aaa"`
				b int
			}{},
			exp: avro.Schema{
				Type: "record",
				Object: &avro.SchemaObject{
					Fields: []avro.SchemaRecordField{
						{
							Name: "aaa",
							Type: avro.Schema{
								Type: "long",
							},
						},
					},
				},
			},
		},
		{
			name: "int skip json",
			in: struct {
				A int `json:"aaa"`
				B int `json:"-"`
			}{},
			exp: avro.Schema{
				Type: "record",
				Object: &avro.SchemaObject{
					Fields: []avro.SchemaRecordField{
						{
							Name: "aaa",
							Type: avro.Schema{
								Type: "long",
							},
						},
					},
				},
			},
		},
		{
			name: "int skip bq",
			in: struct {
				A int `json:"aaa"`
				B int `json:"bbb" bq:"-"`
			}{},
			exp: avro.Schema{
				Type: "record",
				Object: &avro.SchemaObject{
					Fields: []avro.SchemaRecordField{
						{
							Name: "aaa",
							Type: avro.Schema{
								Type: "long",
							},
						},
					},
				},
			},
		},
		{
			name: "bool",
			in: struct {
				A bool `json:"aaa"`
			}{},
			exp: avro.Schema{
				Type: "record",
				Object: &avro.SchemaObject{
					Fields: []avro.SchemaRecordField{
						{
							Name: "aaa",
							Type: avro.Schema{
								Type: "boolean",
							},
						},
					},
				},
			},
		},
		{
			name: "float32",
			in: struct {
				A float32 `json:"aaa"`
			}{},
			exp: avro.Schema{
				Type: "record",
				Object: &avro.SchemaObject{
					Fields: []avro.SchemaRecordField{
						{
							Name: "aaa",
							Type: avro.Schema{
								Type: "double",
							},
						},
					},
				},
			},
		},
		{
			name: "float64",
			in: struct {
				A float64 `json:"aaa"`
			}{},
			exp: avro.Schema{
				Type: "record",
				Object: &avro.SchemaObject{
					Fields: []avro.SchemaRecordField{
						{
							Name: "aaa",
							Type: avro.Schema{
								Type: "double",
							},
						},
					},
				},
			},
		},
		{
			name: "string",
			in: struct {
				A string `json:"aaa"`
			}{},
			exp: avro.Schema{
				Type: "record",
				Object: &avro.SchemaObject{
					Fields: []avro.SchemaRecordField{
						{
							Name: "aaa",
							Type: avro.Schema{
								Type: "string",
							},
						},
					},
				},
			},
		},
		{
			name: "bytes",
			in: struct {
				A []byte `json:"aaa"`
			}{},
			exp: avro.Schema{
				Type: "record",
				Object: &avro.SchemaObject{
					Fields: []avro.SchemaRecordField{
						{
							Name: "aaa",
							Type: avro.Schema{
								Type: "bytes",
							},
						},
					},
				},
			},
		},
		{
			name: "map",
			in: struct {
				A map[string]int `json:"aaa"`
			}{},
			exp: avro.Schema{
				Type: "record",
				Object: &avro.SchemaObject{
					Fields: []avro.SchemaRecordField{
						{
							Name: "aaa",
							Type: avro.Schema{
								Type: "map",
								Object: &avro.SchemaObject{
									Values: avro.Schema{
										Type: "long",
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "pointer to int",
			in: struct {
				A *int `json:"aaa"`
			}{},
			exp: avro.Schema{
				Type: "record",
				Object: &avro.SchemaObject{
					Fields: []avro.SchemaRecordField{
						{
							Name: "aaa",
							Type: avro.Schema{
								Type:  "union",
								Union: []avro.Schema{{Type: "null"}, {Type: "long"}},
							},
						},
					},
				},
			},
		},
		{
			name: "struct",
			in: struct {
				A struct {
					B int `json:"bbb"`
				} `json:"aaa"`
			}{},
			exp: avro.Schema{
				Type: "record",
				Object: &avro.SchemaObject{
					Fields: []avro.SchemaRecordField{
						{
							Name: "aaa",
							Type: avro.Schema{
								Type: "record",
								Object: &avro.SchemaObject{
									Fields: []avro.SchemaRecordField{
										{
											Name: "bbb",
											Type: avro.Schema{
												Type: "long",
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "struct ptr",
			in: struct {
				A *struct {
					B int `json:"bbb"`
				} `json:"aaa"`
			}{},
			exp: avro.Schema{
				Type: "record",
				Object: &avro.SchemaObject{
					Fields: []avro.SchemaRecordField{
						{
							Name: "aaa",
							Type: avro.Schema{
								Type: "union",
								Union: []avro.Schema{
									{Type: "null"},
									{
										Type: "record",
										Object: &avro.SchemaObject{
											Fields: []avro.SchemaRecordField{
												{
													Name: "bbb",
													Type: avro.Schema{
														Type: "long",
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},

		{
			name: "struct slice",
			in: struct {
				A []struct {
					B int `json:"bbb"`
				} `json:"aaa"`
			}{},
			exp: avro.Schema{
				Type: "record",
				Object: &avro.SchemaObject{
					Fields: []avro.SchemaRecordField{
						{
							Name: "aaa",
							Type: avro.Schema{
								Type: "array",
								Object: &avro.SchemaObject{
									Items: avro.Schema{
										Type: "record",
										Object: &avro.SchemaObject{
											Fields: []avro.SchemaRecordField{
												{
													Name: "bbb",
													Type: avro.Schema{
														Type: "long",
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := avro.SchemaForType(tt.in)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tt.exp, got); diff != "" {
				t.Errorf("BuildSchema() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
