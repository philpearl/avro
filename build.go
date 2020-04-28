package avro

import (
	"fmt"
	"math"
	"reflect"
	"strings"
)

// buildCodec builds a codec for use with a schema and type. Note that typ can
// be nil, in which case we still need a codec to know how to skip over the
// field
func buildCodec(schema Schema, typ reflect.Type) (Codec, error) {
	switch schema.Type {
	case "null":
	case "boolean":
	case "int":
	case "long":
	case "float":
	case "double":
	case "bytes":
		return buildBytesCodec(schema, typ)
	case "string":
		return buildStringCodec(schema, typ)
	case "record":
		return buildRecordCodec(schema, typ)
	case "enum":
	case "array":
		return buildArrayCodec(schema, typ)
	case "map":
		return buildMapCodec(schema, typ)
	case "union":
	case "fixed":
		return buildFixedCodec(schema, typ)
	}

	return nil, nil
}

func buildFixedCodec(schema Schema, typ reflect.Type) (Codec, error) {
	if typ != nil {
		if typ.Kind() != reflect.Array || typ.Elem().Kind() != reflect.Uint8 {
			return nil, fmt.Errorf("type for fixed must be a byte array")
		}
		if typ.Len() != schema.Object.Size {
			return nil, fmt.Errorf("array for fixed of size %d is %d", schema.Object.Size, typ.Len())
		}
	}
	return fixedCodec{Size: schema.Object.Size}, nil
}

func buildBytesCodec(schema Schema, typ reflect.Type) (Codec, error) {
	if typ != nil {
		if typ.Kind() != reflect.Slice || typ.Elem().Kind() != reflect.Uint8 {
			return nil, fmt.Errorf("type for bytes must be a byte slice, not %s", typ)
		}
	}
	return bytesCodec{}, nil
}

func buildStringCodec(schema Schema, typ reflect.Type) (Codec, error) {
	if typ != nil && typ.Kind() != reflect.String {
		return nil, fmt.Errorf("type for string must be a string")
	}
	return stringCodec{}, nil
}

func buildArrayCodec(schema Schema, typ reflect.Type) (Codec, error) {
	var itemType reflect.Type
	if typ != nil {
		if typ.Kind() != reflect.Slice {
			return nil, fmt.Errorf("type for an array must be a slice")
		}
		itemType = typ.Elem()
	}

	itemCodec, err := buildCodec(schema.Object.Items, itemType)
	if err != nil {
		return nil, fmt.Errorf("could not build array item codec. %w", err)
	}

	return &arrayCodec{itemCodec: itemCodec, itemType: typ}, nil
}

func buildMapCodec(schema Schema, typ reflect.Type) (Codec, error) {
	var valueType reflect.Type
	if typ != nil {
		if typ.Kind() != reflect.Map || typ.Key().Kind() != reflect.String {
			return nil, fmt.Errorf("type for a map must be a map with string keys")
		}
		valueType = typ.Elem()
	}

	valueCodec, err := buildCodec(schema.Object.Values, valueType)
	if err != nil {
		return nil, fmt.Errorf("could not build map value codec. %w", err)
	}

	return &mapCodec{valueCodec: valueCodec, rtype: typ}, nil
}

func buildRecordCodec(schema Schema, typ reflect.Type) (Codec, error) {
	if schema.Object == nil {
		return nil, fmt.Errorf("record schema does not have object")
	}

	var ntf map[string]reflect.StructField
	if typ != nil {
		if typ.Kind() != reflect.Struct {
			return nil, fmt.Errorf("type for a record must be struct, not %s", typ.Kind())
		}

		// Build a name to field map
		ntf = make(map[string]reflect.StructField, typ.NumField())
		for i := 0; i < typ.NumField(); i++ {
			sf := typ.Field(i)
			bqTag := sf.Tag.Get("bq")
			if bqTag == "-" {
				continue
			}
			jsonTag := sf.Tag.Get("json")
			name := strings.Split(jsonTag, ",")[0]
			if name == "-" {
				continue
			}
			if name == "" {
				name = sf.Name
			}

			ntf[name] = sf
		}
	}

	var rc recordCodec
	rc.rtype = typ

	// The schema is in the driving-seat here
	for _, schemaf := range schema.Object.Fields {
		fmt.Printf("look for field with name %s\n", schemaf.Name)
		var offset = uintptr(math.MaxUint64)
		var fieldType reflect.Type
		sf, ok := ntf[schemaf.Name]
		if ok {
			fmt.Printf(" name is present in struct\n")
			offset = sf.Offset
			fieldType = sf.Type
		}

		codec, err := buildCodec(schemaf.Type, fieldType)
		if err != nil {
			return nil, fmt.Errorf("failed to get codec for field %s. %w", schemaf.Name, err)
		}

		rc.fields = append(rc.fields, recordCodecField{
			codec:  codec,
			offset: offset,
		})
	}

	return &rc, nil
}
