package avro

import (
	"reflect"
	"testing"
	"unsafe"

	"github.com/google/go-cmp/cmp"
)

func TestRecordCodec(t *testing.T) {
	type record struct {
		Name string `json:"name"`
		Hat  string `json:"-"`
	}

	schema := Schema{
		Type: "record",
		Object: &SchemaObject{
			Name: "Record",
			Fields: []SchemaRecordField{
				{
					Name: "name",
					Type: Schema{
						Type: "string",
					},
				},
				{
					Name: "Hat",
					Type: Schema{
						Type: "string",
					},
				},
			},
		},
	}

	data := []byte{
		6, 'j', 'i', 'm',
		6, 'c', 'a', 't',
	}

	var r record
	c, err := buildRecordCodec(schema, reflect.TypeOf(r))
	if err != nil {
		t.Fatal(err)
	}

	buf := NewBuffer(data)
	if err := c.Read(buf, unsafe.Pointer(&r)); err != nil {
		t.Fatal(err)
	}

	if diff := cmp.Diff(record{Name: "jim"}, r); diff != "" {
		t.Fatalf("record differs. %s", diff)
	}

	if buf.Len() != 0 {
		t.Fatalf("unread data (%d)", buf.Len())
	}

	// Now test skip
	buf.Reset(data)
	if err := c.Skip(buf); err != nil {
		t.Fatal(err)
	}
	if buf.Len() != 0 {
		t.Fatalf("unread data (%d)", buf.Len())
	}

}
