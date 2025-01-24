package avro

import (
	"fmt"
	"reflect"
	"unsafe"

	jsoniter "github.com/json-iterator/go"
	"github.com/modern-go/reflect2"
)

var json = jsoniter.Config{
	EscapeHTML: true,
}.Froze()

func init() {
	json.RegisterExtension(&schemaExtension{})
}

// Schema is a representation of AVRO schema JSON. Primitive types populate Type
// only. UnionTypes populate Type and Union fields. All other types populate
// Type and a subset of Object fields.
type Schema struct {
	Type   string
	Object *SchemaObject
	Union  []Schema
}

// Codec creates a codec for the given schema and output type
func (s Schema) Codec(out interface{}) (Codec, error) {
	typ := reflect.TypeOf(out)
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}
	if typ.Kind() != reflect.Struct {
		return nil, fmt.Errorf("out must be a struct or pointer to a struct")
	}

	return buildCodec(s, typ, false)
}

func (s *Schema) Marshal() ([]byte, error) {
	return json.Marshal(s)
}

// SchemaFromString decodes a JSON string into a Schema
func SchemaFromString(in string) (Schema, error) {
	var schema Schema
	if err := json.UnmarshalFromString(in, &schema); err != nil {
		return schema, fmt.Errorf("could not decode schema JSON. %w", err)
	}
	return schema, nil
}

// SchemaObject contains all the fields of more complex schema types
type SchemaObject struct {
	Type        string `json:"type"`
	LogicalType string `json:"logicalType,omitempty"`
	Name        string `json:"name,omitempty"`
	Namespace   string `json:"namespace,omitempty"`
	// Fields in a record
	Fields []SchemaRecordField `json:"fields,omitempty"`
	// The type of each item in an array
	Items Schema `json:"items,omitempty"`
	// The value types of a map (keys are strings)
	Values Schema `json:"values,omitempty"`
	// The size of a fixed type
	Size int `json:"size,omitempty"`
	// The values of an enum
	Symbols []string `json:"symbols,omitempty"`
}

// SchemaRecordField represents one field of a Record schema
type SchemaRecordField struct {
	Name string `json:"name,omitempty"`
	Type Schema `json:"type,omitempty"`
}

type schemaCodec struct{}

func (schemaCodec) Decode(ptr unsafe.Pointer, iter *jsoniter.Iterator) {
	s := (*Schema)(ptr)

	switch iter.WhatIsNext() {
	case jsoniter.StringValue:
		// Primitive type
		s.Type = iter.ReadString()
	case jsoniter.ArrayValue:
		s.Type = "union"
		iter.ReadVal(&s.Union)
	case jsoniter.ObjectValue:
		s.Object = &SchemaObject{}
		iter.ReadVal(s.Object)
		s.Type = s.Object.Type
		s.Object.Type = ""
	default:
		iter.ReportError("Decode schema", "must be string, array or object")
	}
}

func (schemaCodec) Encode(ptr unsafe.Pointer, stream *jsoniter.Stream) {
	s := (*Schema)(ptr)
	switch {
	case s.Object != nil:
		stream.WriteObjectStart()
		stream.WriteObjectField("type")
		stream.WriteString(s.Type)
		if s.Object.LogicalType != "" {
			stream.WriteMore()
			stream.WriteObjectField("logicalType")
			stream.WriteString(s.Object.LogicalType)
		}
		if s.Object.Name != "" {
			stream.WriteMore()
			stream.WriteObjectField("name")
			stream.WriteString(s.Object.Name)
		}
		if s.Object.Namespace != "" {
			stream.WriteMore()
			stream.WriteObjectField("namespace")
			stream.WriteString(s.Object.Namespace)
		}
		switch s.Type {
		case "record":
			stream.WriteMore()
			stream.WriteObjectField("fields")
			stream.WriteArrayStart()
			for i, f := range s.Object.Fields {
				if i != 0 {
					stream.WriteMore()
				}
				stream.WriteVal(f)
			}
			stream.WriteArrayEnd()
		case "enum":
			stream.WriteMore()
			stream.WriteObjectField("symbols")
			stream.WriteArrayStart()
			for i, v := range s.Object.Symbols {
				if i != 0 {
					stream.WriteMore()
				}
				stream.WriteString(v)
			}
			stream.WriteArrayEnd()
		case "array":
			stream.WriteMore()
			stream.WriteObjectField("items")
			stream.WriteVal(s.Object.Items)
		case "map":
			stream.WriteMore()
			stream.WriteObjectField("values")
			stream.WriteVal(s.Object.Values)
		case "fixed":
			stream.WriteMore()
			stream.WriteObjectField("size")
			stream.WriteInt(s.Object.Size)
		}
		stream.WriteObjectEnd()
	case len(s.Union) != 0:
		stream.WriteArrayStart()
		stream.WriteVal(s.Union[0])
		for _, s := range s.Union[1:] {
			stream.WriteMore()
			stream.WriteVal(s)
		}
		stream.WriteArrayEnd()
	default:
		stream.WriteString(s.Type)
	}
}

func (schemaCodec) IsEmpty(ptr unsafe.Pointer) bool {
	return ptr == nil
}

type schemaExtension struct {
	jsoniter.DummyExtension
}

var schemaType = reflect2.TypeOf(Schema{})

func (ext *schemaExtension) CreateDecoder(typ reflect2.Type) jsoniter.ValDecoder {
	switch typ {
	case schemaType:
		return schemaCodec{}
	}
	return nil
}

func (ext *schemaExtension) CreateEncoder(typ reflect2.Type) jsoniter.ValEncoder {
	switch typ {
	case schemaType:
		return schemaCodec{}
	}
	return nil
}
