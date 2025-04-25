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
type CodecBuildFunc func(schema Schema, typ reflect.Type, omit bool) (Codec, error)

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
func buildCodec(schema Schema, typ reflect.Type, omit bool) (Codec, error) {
	if schema.Type != "union" && schema.Type != "null" && typ != nil {
		if typ.Kind() == reflect.Ptr {
			return buildPointerCodec(schema, typ)
		}

		registryMutex.RLock()
		cf, ok := registry[typ]
		registryMutex.RUnlock()
		if ok {
			return cf(schema, typ, omit)
		}
	}

	switch schema.Type {
	case "null":
		return buildNullCodec()
	case "boolean":
		return buildBoolCodec(typ, omit)
	case "int":
		return buildIntCodec(typ, omit)
	case "long":
		return buildLongCodec(typ, omit)
	case "float":
		return buildFloatCodec(typ, omit)
	case "double":
		return buildDoubleCodec(typ, omit)
	case "bytes":
		return buildBytesCodec(typ, omit)
	case "string":
		return buildStringCodec(typ, omit)
	case "record":
		return buildRecordCodec(schema, typ)
	case "enum":
		return nil, fmt.Errorf("enum not currently supported")
	case "array":
		return buildArrayCodec(schema, typ, omit)
	case "map":
		return BuildMapCodec(schema, typ, omit)
	case "union":
		return buildUnionCodec(schema, typ, omit)
	case "fixed":
		return buildFixedCodec(schema, typ)
	}

	return nil, fmt.Errorf("%s not currently supported", schema.Type)
}

func buildPointerCodec(schema Schema, typ reflect.Type) (Codec, error) {
	c, err := buildCodec(schema, typ.Elem(), false)
	if err != nil {
		return nil, err
	}
	return &PointerCodec{Codec: c}, nil
}

func buildBoolCodec(typ reflect.Type, omit bool) (Codec, error) {
	if typ != nil && typ.Kind() != reflect.Bool {
		return nil, fmt.Errorf("type for boolean must be a bool, not %s", typ)
	}

	return BoolCodec{omitEmpty: omit}, nil
}

func buildIntCodec(typ reflect.Type, omit bool) (Codec, error) {
	// We can actually use the same codecs as long ints. We might want to
	// separate them if we do encoding.
	return buildLongCodec(typ, omit)
}

func buildLongCodec(typ reflect.Type, omit bool) (Codec, error) {
	// TODO: unsigned types?
	// It's likely BQ will specify this type even for smaller integer types.
	if typ == nil {
		return Int64Codec{omitEmpty: omit}, nil
	}

	switch typ.Kind() {
	case reflect.Int64, reflect.Int:
		return Int64Codec{omitEmpty: omit}, nil
	case reflect.Int32:
		return Int32Codec{omitEmpty: omit}, nil
	case reflect.Int16:
		return Int32Codec{omitEmpty: omit}, nil
	}

	return nil, fmt.Errorf("type %s (kind %s) not supported for long codec", typ, typ.Kind())
}

func buildFloatCodec(typ reflect.Type, omit bool) (Codec, error) {
	if typ != nil && typ.Kind() != reflect.Float32 {
		return nil, fmt.Errorf("type for float codec must be a 32 bit float, not %s", typ)
	}

	return FloatCodec{omitEmpty: omit}, nil
}

func buildDoubleCodec(typ reflect.Type, omit bool) (Codec, error) {
	if typ == nil {
		return DoubleCodec{omitEmpty: omit}, nil
	}

	switch typ.Kind() {
	case reflect.Float32:
		return Float32DoubleCodec{DoubleCodec: DoubleCodec{omitEmpty: omit}}, nil
	case reflect.Float64:
		return DoubleCodec{omitEmpty: omit}, nil
	}

	return nil, fmt.Errorf("type %s not supported for double codec", typ)
}

func buildNullCodec() (Codec, error) {
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
	return &fixedCodec{Size: schema.Object.Size}, nil
}

func buildBytesCodec(typ reflect.Type, omit bool) (Codec, error) {
	if typ != nil {
		if typ.Kind() != reflect.Slice || typ.Elem().Kind() != reflect.Uint8 {
			return nil, fmt.Errorf("type for bytes must be a byte slice, not %s", typ)
		}
	}
	return BytesCodec{omitEmpty: omit}, nil
}

func buildStringCodec(typ reflect.Type, omit bool) (Codec, error) {
	if typ != nil && typ.Kind() != reflect.String {
		return nil, fmt.Errorf("type for string must be a string, not %s", typ)
	}
	return StringCodec{omitEmpty: omit}, nil
}

func buildArrayCodec(schema Schema, typ reflect.Type, omit bool) (Codec, error) {
	var itemType reflect.Type
	if typ != nil {
		if typ.Kind() != reflect.Slice {
			return nil, fmt.Errorf("type for an array must be a slice, not %s", typ)
		}
		itemType = typ.Elem()
	}

	itemCodec, err := buildCodec(schema.Object.Items, itemType, false)
	if err != nil {
		return nil, fmt.Errorf("could not build array item codec: %w", err)
	}

	return &arrayCodec{itemCodec: itemCodec, itemType: itemType, omitEmpty: omit}, nil
}

func BuildMapCodec(schema Schema, typ reflect.Type, omit bool) (Codec, error) {
	var valueType reflect.Type
	if typ != nil {
		if typ.Kind() != reflect.Map || typ.Key().Kind() != reflect.String {
			return nil, fmt.Errorf("type for a map must be a map with string keys")
		}
		valueType = typ.Elem()
	}

	valueCodec, err := buildCodec(schema.Object.Values, valueType, false)
	if err != nil {
		return nil, fmt.Errorf("could not build map value codec: %w", err)
	}

	return &MapCodec{valueCodec: valueCodec, rtype: typ, omitEmpty: omit}, nil
}

func buildUnionCodec(schema Schema, typ reflect.Type, omit bool) (Codec, error) {
	if len(schema.Union) == 2 {
		if schema.Union[0].Type == "null" || schema.Union[1].Type == "null" {
			var c unionOneAndNullCodec
			if schema.Union[0].Type == "null" {
				c.nonNull = 1
			}
			u := schema.Union[c.nonNull]
			sc, err := buildCodec(u, typ, omit)
			if err != nil {
				return nil, fmt.Errorf("failed to build union sub-codec %q: %w", u.Type, err)
			}
			if _, ok := sc.(StringCodec); ok {
				return &unionNullString{codec: StringCodec{}, nonNull: c.nonNull}, nil
			}
			c.codec = sc
			return &c, nil
		}
	}

	var c unionCodec
	c.codecs = make([]Codec, len(schema.Union))

	// We're only really expecting unions that are unions of a thing and null,
	// so we can only cope with pointers for now
	for i, u := range schema.Union {
		sc, err := buildCodec(u, typ, omit)
		if err != nil {
			return nil, fmt.Errorf("failed to build union sub-codec %q: %w", u.Type, err)
		}
		c.codecs[i] = sc
	}
	return &c, nil
}

func nameForField(sf reflect.StructField) string {
	if !sf.IsExported() {
		return "-"
	}

	bqTag := sf.Tag.Get("bq")
	if bqTag == "-" {
		return "-"
	}
	jsonTag := sf.Tag.Get("json")
	name, _, _ := strings.Cut(jsonTag, ",")
	if name == "-" {
		return "-"
	}
	if name == "" {
		return sf.Name
	}

	return name
}

func omitEmpty(sf reflect.StructField) bool {
	jsonTag := sf.Tag.Get("json")
	_, opts, _ := strings.Cut(jsonTag, ",")
	for len(opts) > 0 {
		var opt string
		opt, opts, _ = strings.Cut(opts, ",")
		if opt == "omitempty" {
			return true
		}
	}
	return false
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
		for i := range typ.NumField() {
			sf := typ.Field(i)
			name := nameForField(sf)
			if name == "-" {
				continue
			}

			ntf[name] = sf
		}
	}

	var rc recordCodec
	rc.rtype = typ

	// The schema is in the driving-seat here
	for _, schemaf := range schema.Object.Fields {
		offset := uintptr(math.MaxUint64)
		var fieldType reflect.Type
		sf, ok := ntf[schemaf.Name]
		if ok {
			offset = sf.Offset
			fieldType = sf.Type
		}

		codec, err := buildCodec(schemaf.Type, fieldType, omitEmpty(sf))
		if err != nil {
			return nil, fmt.Errorf("failed to get codec for field %q: %w", schemaf.Name, err)
		}

		rc.fields = append(rc.fields, recordCodecField{
			codec:  codec,
			offset: offset,
			name:   schemaf.Name,
		})
	}

	return &rc, nil
}
