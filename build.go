package avro

import (
	"fmt"
	"math"
	"reflect"
	"strings"
	"sync"
)

// CodecBuildFunc is the function signature for a codec builder. If you want to
// customise AVRO decoding for a type register a CodecBuildFunc via the Register
// call. Schema is the AVRO schema for the type to build. typ should match the
// type the function was registered under.
type CodecBuildFunc func(schema Schema, typ reflect.Type) (Codec, error)

var (
	registryMutex sync.RWMutex
	registry      = make(map[reflect.Type]CodecBuildFunc)
)

// Register is used to set a custom codec builder for a type
func Register(typ reflect.Type, f CodecBuildFunc) {
	registryMutex.Lock()
	defer registryMutex.Unlock()
	registry[typ] = f
}

// buildCodec builds a codec for use with a schema and type. Note that typ can
// be nil, in which case we still need a codec to know how to skip over the
// field
func buildCodec(schema Schema, typ reflect.Type) (Codec, error) {

	if schema.Type != "union" && schema.Type != "null" && typ != nil {
		if typ.Kind() == reflect.Ptr {
			return buildPointerCodec(schema, typ)
		}

		registryMutex.RLock()
		cf, ok := registry[typ]
		registryMutex.RUnlock()
		if ok {
			return cf(schema, typ)
		}
	}

	switch schema.Type {
	case "null":
		return buildNullCodec(schema, typ)
	case "boolean":
		return buildBoolCodec(schema, typ)
	case "int":
		return buildIntCodec(schema, typ)
	case "long":
		return buildLongCodec(schema, typ)
	case "float":
		return buildFloatCodec(schema, typ)
	case "double":
		return buildDoubleCodec(schema, typ)
	case "bytes":
		return buildBytesCodec(schema, typ)
	case "string":
		return buildStringCodec(schema, typ)
	case "record":
		return buildRecordCodec(schema, typ)
	case "enum":
		return nil, fmt.Errorf("enum not currently supported")
	case "array":
		return buildArrayCodec(schema, typ)
	case "map":
		return buildMapCodec(schema, typ)
	case "union":
		return buildUnionCodec(schema, typ)
	case "fixed":
		return buildFixedCodec(schema, typ)
	}

	return nil, fmt.Errorf("%s not currently supported", schema.Type)
}

func buildPointerCodec(schema Schema, typ reflect.Type) (Codec, error) {
	c, err := buildCodec(schema, typ.Elem())
	if err != nil {
		return nil, err
	}
	return pointerCodec{Codec: c}, nil
}

func buildBoolCodec(schema Schema, typ reflect.Type) (Codec, error) {
	if typ != nil && typ.Kind() != reflect.Bool {
		return nil, fmt.Errorf("type for boolean must be a bool, not %s", typ)
	}

	return BoolCodec{}, nil
}

func buildIntCodec(schema Schema, typ reflect.Type) (Codec, error) {
	// We can actually use the same codecs as long ints. We might want to
	// separate them if we do encoding.
	return buildLongCodec(schema, typ)
}

func buildLongCodec(schema Schema, typ reflect.Type) (Codec, error) {
	// TODO: unsigned types?
	// It's likely BQ will specify this type even for smaller integer types.
	if typ == nil {
		return Int64Codec{}, nil
	}

	switch typ.Kind() {
	case reflect.Int64, reflect.Int:
		return Int64Codec{}, nil
	case reflect.Int32:
		return Int32Codec{}, nil
	case reflect.Int16:
		return Int32Codec{}, nil
	}

	return nil, fmt.Errorf("type %s (kind %s) not supported for long codec", typ, typ.Kind())
}

func buildFloatCodec(schema Schema, typ reflect.Type) (Codec, error) {
	if typ != nil && typ.Kind() != reflect.Float32 {
		return nil, fmt.Errorf("type for float codec must be a 32 bit float, not %s", typ)
	}

	return FloatCodec{}, nil
}

func buildDoubleCodec(schema Schema, typ reflect.Type) (Codec, error) {
	if typ == nil {
		return DoubleCodec{}, nil
	}

	switch typ.Kind() {
	case reflect.Float32:
		return Float32DoubleCodec{}, nil
	case reflect.Float64:
		return DoubleCodec{}, nil
	}

	return nil, fmt.Errorf("type %s not supported for double codec", typ)

}

func buildNullCodec(schema Schema, typ reflect.Type) (Codec, error) {
	return nullCodec{}, nil
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
	return BytesCodec{}, nil
}

func buildStringCodec(schema Schema, typ reflect.Type) (Codec, error) {
	if typ != nil && typ.Kind() != reflect.String {
		return nil, fmt.Errorf("type for string must be a string, not %s", typ)
	}
	return StringCodec{}, nil
}

func buildArrayCodec(schema Schema, typ reflect.Type) (Codec, error) {
	var itemType reflect.Type
	if typ != nil {
		if typ.Kind() != reflect.Slice {
			return nil, fmt.Errorf("type for an array must be a slice, not %s", typ)
		}
		itemType = typ.Elem()
	}

	itemCodec, err := buildCodec(schema.Object.Items, itemType)
	if err != nil {
		return nil, fmt.Errorf("could not build array item codec: %w", err)
	}

	return arrayCodec{itemCodec: itemCodec, itemType: itemType}, nil
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
		return nil, fmt.Errorf("could not build map value codec: %w", err)
	}

	return MapCodec{valueCodec: valueCodec, rtype: typ}, nil
}

func buildUnionCodec(schema Schema, typ reflect.Type) (Codec, error) {
	var c unionCodec
	c.codecs = make([]Codec, len(schema.Union))

	// We're only really expecting unions that are unions of a thing and null,
	// so we can only cope with pointers for now
	for i, u := range schema.Union {
		sc, err := buildCodec(u, typ)
		if err != nil {
			return nil, fmt.Errorf("failed to build union sub-codec %q: %w", u.Type, err)
		}
		c.codecs[i] = sc
	}
	return c, nil
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
		var offset = uintptr(math.MaxUint64)
		var fieldType reflect.Type
		sf, ok := ntf[schemaf.Name]
		if ok {
			offset = sf.Offset
			fieldType = sf.Type
		}

		codec, err := buildCodec(schemaf.Type, fieldType)
		if err != nil {
			return nil, fmt.Errorf("failed to get codec for field %q: %w", schemaf.Name, err)
		}

		rc.fields = append(rc.fields, recordCodecField{
			codec:  codec,
			offset: offset,
			name:   schemaf.Name,
		})
	}

	return rc, nil
}
