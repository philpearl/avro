package avro

import (
	"bytes"
	"compress/flate"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"reflect"
	"unsafe"

	"github.com/golang/snappy"
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

	var decoder compressionCodec
	if compress, ok := fh.Meta["avro.codec"]; ok {
		switch string(compress) {
		case "null":
			decoder = nullCompression{}
		case "deflate":
			decoder = &deflate{}
		case "snappy":
			decoder = &snappyCodec{}
		default:
			return fmt.Errorf("compression codec %s not supported", string(compress))
		}
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

	rtyp := unpackEFace(out).rtype
	p := unpackEFace(out).data

	var compressed []byte
	var br bytes.Reader
	for {
		count, err := readVarint(r)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("failed to read item count. %w", err)
		}
		dataLength, err := readVarint(r)
		if err != nil {
			return fmt.Errorf("failed to read data block length. %w", err)
		}
		if dataLength < 5 {
			return fmt.Errorf("dataLength too small for snappy checksum")
		}
		if cap(compressed) < int(dataLength) {
			compressed = make([]byte, dataLength)
		} else {
			compressed = compressed[:dataLength]
		}
		if _, err := io.ReadFull(r, compressed); err != nil {
			return err
		}
		uncompressed, err := decoder.decompress(compressed)
		if err != nil {
			return fmt.Errorf("decompress failed: %w", err)
		}

		br.Reset(uncompressed)

		for i := int64(0); i < count; i++ {
			// TODO: might be better to allocate vals in blocks
			// Zero the data
			typedmemclr(rtyp, p)
			if err := codec.Read(&br, p); err != nil {
				return fmt.Errorf("failed to read item %d in file. %w", i, err)
			}

			if err := cb(p); err != nil {
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
			return fmt.Errorf("sync block does not match. Have %X, want %X", sig, fh.Sync)
		}
	}
	/*
	   000070a0  6e 67 22 5d 7d 5d 7d 5d  7d 5d 7d 5d 7d 5d 7d 00  |ng"]}]}]}]}]}]}.|
	   000070b0  78 9b 8c 58 58 51 3d 39  d9 0b cb 6e c6 cc aa e6  |x..XXQ=9...n....|
	   000070c0  02 ce 62 ca ef 01 b8 4a  02 0e 68 72 6b 67 61 6d  |..b....J..hrkgam|
	   000070d0  65 02 10 31 30 32 33 31  31 35 38 02 2e 32 30 31  |e..10231158..201|
	   000070e0  39 2d 30 34 2d 31 37 54  31 37 3a 35 38 3a 30 33  |9-04-17T17:58:03|
	   000070f0  2e 36 33 5a 02 30 42 19  00 1c 32 35 2e 33 39 33  |.63Z.0B...25.393|
	   0
	*/
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

type compressionCodec interface {
	decompress(compressed []byte) ([]byte, error)
}

type nullCompression struct{}

func (nullCompression) decompress(compressed []byte) ([]byte, error) {
	return compressed, nil
}

type deflate struct {
	reader io.Reader
	buf    bytes.Reader
	out    bytes.Buffer
}

func (d *deflate) decompress(compressed []byte) ([]byte, error) {
	d.buf.Reset(compressed)
	if d.reader == nil {
		d.reader = flate.NewReader(nil)
	}
	d.reader.(flate.Resetter).Reset(&d.buf, nil)

	d.out.Reset()
	d.out.ReadFrom(d.reader)

	return d.out.Bytes(), nil
}

type snappyCodec struct {
	buf []byte
}

func (s *snappyCodec) decompress(compressed []byte) ([]byte, error) {
	var err error
	s.buf, err = snappy.Decode(s.buf[:cap(s.buf)], compressed[:len(compressed)-4])
	if err != nil {
		return nil, fmt.Errorf("snappy decode failed: %w", err)
	}

	crc := binary.BigEndian.Uint32(compressed[len(compressed)-4:])
	if crc32.ChecksumIEEE(s.buf) != crc {
		return nil, errors.New("snappy checksum mismatch")
	}

	return s.buf, nil
}
