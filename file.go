package avro

import (
	"fmt"
	"reflect"
	"unsafe"

	jsoniter "github.com/json-iterator/go"
)

// FileHeader represents an AVRO file header
type FileHeader struct {
	Magic [4]byte           `json:"magic,omitempty"`
	Meta  map[string][]byte `json:"meta,omitempty"`
	Sync  [16]byte          `json:"sync,omitempty"`
}

//
var avroFileSchemaString = `{"type": "record", "name": "org.apache.avro.file.Header",
 "fields" : [
   {"name": "magic", "type": {"type": "fixed", "name": "Magic", "size": 4}},
   {"name": "meta", "type": {"type": "map", "values": "bytes"}},
   {"name": "sync", "type": {"type": "fixed", "name": "Sync", "size": 16}},
  ]
}`

var avroFileSchema = Schema{
	Type: "record",
	Object: &SchemaObject{
		Name: "org.apache.avro.file.Header",
		Fields: []SchemaRecordField{
			{
				Name: "magic",
				Type: Schema{
					Type: "fixed",
					Object: &SchemaObject{
						Name: "Magic",
						Size: 4,
					},
				},
			},
			{
				Name: "meta",
				Type: Schema{
					Type: "map",
					Object: &SchemaObject{
						Values: Schema{
							Type: "bytes",
						},
					},
				},
			},
			{
				Name: "sync",
				Type: Schema{
					Type: "fixed",
					Object: &SchemaObject{
						Name: "Magic",
						Size: 16,
					},
				},
			},
		},
	},
}

func ReadFile(r Reader, out interface{}) error {
	schema, err := readFileHeader(r)
	if err != nil {
		return err
	}

	typ := reflect.TypeOf(out)
	if typ.Kind() != reflect.Ptr {
		return fmt.Errorf("out must be a pointer")
	}

	codec, err := buildCodec(schema, typ.Elem())
	if err != nil {
		return fmt.Errorf("failed to build codec. %w", err)
	}

	// This isn't right because it doesn't deal with the array nature of the data.
	return codec.Read(r, unpackEFace(out).data)
}

func readFileHeader(r Reader) (Schema, error) {
	var fh FileHeader
	c, err := buildCodec(avroFileSchema, reflect.TypeOf(fh))
	if err != nil {
		return Schema{}, fmt.Errorf("could not build file header codec. %w", err)
	}

	if err := c.Read(r, unsafe.Pointer(&fh)); err != nil {
		return Schema{}, fmt.Errorf("failed to read file header. %w", err)
	}

	fmt.Println(fh)

	if fh.Magic != [4]byte{'O', 'b', 'j', 1} {
		return Schema{}, fmt.Errorf("file header Magic is not correct")
	}

	schemaJSON, ok := fh.Meta["avro.schema"]
	if !ok {
		return Schema{}, fmt.Errorf("no schema found in file header")
	}

	var schema Schema
	if err := jsoniter.Unmarshal(schemaJSON, &schema); err != nil {
		return Schema{}, fmt.Errorf("could not decode schema JSON from file header. %w", err)
	}

	return schema, nil
}
