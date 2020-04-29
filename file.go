package avro

import (
	"encoding/binary"
	"fmt"
	"reflect"
	"unsafe"

	jsoniter "github.com/json-iterator/go"
)

// FileHeader represents an AVRO file header
type FileHeader struct {
	Magic [4]byte           `json:"magic"`
	Meta  map[string][]byte `json:"meta"`
	Sync  [16]byte          `json:"sync"`
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

// ReadFile reads from an AVRO file. The records in the file are decoded into
// structs of the type indicated by out. These are fed back to the application
// via the cb callback. ReadFile calls cb with a pointer to the struct. The
// pointer is converted to an unsafe.Pointer. The pointer should not be retained
// by the application past the return of cb.
//
//  var records []myrecord
//  if err := ReadFile(f, myrecord{}, func(val unsafe.Pointer) error {
//      records = append(records, *(*record)(val))
//      return nil
//  }); err != nil {
//	    return err
//  }
func ReadFile(r Reader, out interface{}, cb func(val unsafe.Pointer) error) error {
	fh, err := readFileHeader(r)
	if err != nil {
		return err
	}

	schema, err := fh.schema()
	if err != nil {
		return err
	}

	typ := reflect.TypeOf(out)
	if typ.Kind() != reflect.Struct {
		return fmt.Errorf("out must be a struct")
	}

	codec, err := buildCodec(schema, typ)
	if err != nil {
		return fmt.Errorf("failed to build codec. %w", err)
	}

	count, err := binary.ReadVarint(r)
	if err != nil {
		return fmt.Errorf("failed to read item count. %w", err)
	}
	dataLength, err := binary.ReadVarint(r)
	if err != nil {
		return fmt.Errorf("failed to read data block length. %w", err)
	}
	_ = dataLength

	rtyp := unpackEFace(out).rtype
	for i := int64(0); i < count; i++ {
		// TODO: might be better to allocate vals in blocks
		val := unsafe_New(rtyp)
		if err := codec.Read(r, val); err != nil {
			return fmt.Errorf("failed to read item %d in file. %w", i, err)
		}

		if err := cb(val); err != nil {
			return err
		}
	}

	// Check the signature.
	var sig [16]byte
	fc := fixedCodec{Size: 16}
	if err := fc.Read(r, unsafe.Pointer(&sig)); err != nil {
		return fmt.Errorf("failed reading block signature. %w", err)
	}
	if sig != fh.Sync {
		return fmt.Errorf("sync block does not match")
	}

	return nil
}

func readFileHeader(r Reader) (fh FileHeader, err error) {
	c, err := buildCodec(avroFileSchema, reflect.TypeOf(fh))
	if err != nil {
		return fh, fmt.Errorf("could not build file header codec. %w", err)
	}

	if err := c.Read(r, unsafe.Pointer(&fh)); err != nil {
		return fh, fmt.Errorf("failed to read file header. %w", err)
	}

	if fh.Magic != [4]byte{'O', 'b', 'j', 1} {
		return fh, fmt.Errorf("file header Magic is not correct")
	}
	return fh, nil
}

func (fh FileHeader) schema() (schema Schema, err error) {
	schemaJSON, ok := fh.Meta["avro.schema"]
	if !ok {
		return schema, fmt.Errorf("no schema found in file header")
	}

	if err := jsoniter.Unmarshal(schemaJSON, &schema); err != nil {
		return schema, fmt.Errorf("could not decode schema JSON from file header. %w", err)
	}

	return schema, nil
}

/*
000070a0  22 5d 7d 5d 7d 5d 7d 5d  7d 5d 7d 5d 7d 00
                                                     7a 92  |"]}]}]}]}]}]}.z.|
000070b0  f7 35 e8 98 91 bf 96 2a  20 8b 0a b3 b1 fc
													 24
													    d6  |.5.....* .....$.|
000070c0  8e 02 04 02 1e 72 65 73  69 64 65 6e 74 61 64 76  |.....residentadv|
000070d0  69 73 6f 72 02 0e 33 39  32 33 31 32 39 02 30 32  |isor..3923129.02|

*/
