package avro

import (
	"reflect"
	"testing"
	"unsafe"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
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

	buf := NewReadBuf(data)
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

func TestRecordRoundTrip(t *testing.T) {
	type mustruct struct {
		Name  string `json:"name"`
		Hat   string `json:",omitempty"`
		V     int
		Q     float64
		Bytes []byte
		La    []int  `json:"la"`
		W     int32  `json:"w,omitempty"`
		Z     *int64 `json:"z"`
		Mmm   map[string]string
	}

	var zval int64 = 1020202

	tests := []struct {
		name string
		data mustruct
	}{
		{
			name: "basic",
			data: mustruct{
				Name:  "jim",
				Hat:   "cat",
				V:     31,
				Q:     3.14,
				Bytes: []byte{1, 2, 3, 4},
				La:    []int{1, 2, 3, 4},
				W:     0,
				Z:     &zval,
				Mmm:   map[string]string{"foo": "bar", "baz": "qux"},
			},
		},
		{
			name: "empty",
			data: mustruct{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s, err := SchemaForType(&test.data)
			if err != nil {
				t.Fatal(err)
			}

			c, err := s.Codec(&test.data)
			if err != nil {
				t.Fatal(err)
			}

			buf := NewWriteBuf(nil)

			c.Write(buf, unsafe.Pointer(&test.data))

			var actual mustruct
			r := NewReadBuf(buf.Bytes())
			if err := c.Read(r, unsafe.Pointer(&actual)); err != nil {
				t.Fatal(err)
			}

			if diff := cmp.Diff(test.data, actual, cmpopts.EquateEmpty()); diff != "" {
				t.Fatalf("record differs. %s", diff)
			}
		})
	}
}
