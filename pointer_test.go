package avro

import (
	"testing"
	"unsafe"

	"github.com/google/go-cmp/cmp"
)

func TestPointerCodec(t *testing.T) {
	type inStruct struct {
		A string
		B int
	}
	type myStruct struct {
		P *inStruct `json:",omitempty"`
		B int
	}

	s, err := SchemaForType(myStruct{})
	if err != nil {
		t.Fatal(err)
	}

	if diff := cmp.Diff(Schema{
		Type: "record",
		Object: &SchemaObject{
			Name: "myStruct",
			Fields: []SchemaRecordField{
				{
					Name: "P",
					Type: Schema{
						Type: "union",
						Union: []Schema{
							{Type: "null"},
							{
								Type: "record",
								Object: &SchemaObject{
									Name: "inStruct",
									Fields: []SchemaRecordField{
										{Name: "A", Type: Schema{Type: "string"}},
										{Name: "B", Type: Schema{Type: "long"}},
									},
								},
							},
						},
					},
				},
				{
					Name: "B",
					Type: Schema{Type: "long"},
				},
			},
		},
	}, s); diff != "" {
		t.Fatal(diff)
	}

	c, err := s.Codec(myStruct{})
	if err != nil {
		t.Fatal(err)
	}

	w := NewWriteBuf(nil)
	c.Write(w, unsafe.Pointer(&myStruct{}))

	if diff := cmp.Diff([]byte{0x00, 0x00}, w.Bytes()); diff != "" {
		t.Fatal(diff)
	}

	var out myStruct
	if err := c.Read(NewReadBuf(w.Bytes()), unsafe.Pointer(&out)); err != nil {
		t.Fatal(err)
	}

	if diff := cmp.Diff(myStruct{}, out); diff != "" {
		t.Fatal(diff)
	}
}
