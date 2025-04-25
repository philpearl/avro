package avro

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
)

var (
	schemaRegistryMutex sync.RWMutex
	schemaRegistry      = make(map[reflect.Type]Schema)
)

// Call RegisterSchema to indicate what schema should be used for a given type.
// Use this to register the schema to use for a type for which you write a
// custom codec.
func RegisterSchema(typ reflect.Type, s Schema) {
	schemaRegistryMutex.Lock()
	defer schemaRegistryMutex.Unlock()
	schemaRegistry[typ] = s
}

// SchemaForType returns a Schema for the given type. It aims to produce a
// Schema that's compatible with BigQuery.
func SchemaForType(item any) (Schema, error) {
	typ := reflect.TypeOf(item)
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}
	if typ.Kind() != reflect.Struct {
		return Schema{}, fmt.Errorf("item must be a struct or pointer to a struct")
	}

	return schemaForType(typ)
}

func isInSchemaRegistry(typ reflect.Type) (Schema, bool) {
	schemaRegistryMutex.RLock()
	defer schemaRegistryMutex.RUnlock()
	s, ok := schemaRegistry[typ]
	return s, ok
}

func schemaForType(typ reflect.Type) (Schema, error) {
	if s, ok := isInSchemaRegistry(typ); ok {
		return s, nil
	}

	// BigQuery makes every basic type nullable. We'll send null for the zero
	// value if there's an "omitempty" tag.
	switch typ.Kind() {
	case reflect.Bool:
		return Schema{Type: "boolean"}, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return Schema{Type: "long"}, nil
	case reflect.Float32, reflect.Float64:
		return Schema{Type: "double"}, nil
	case reflect.String:
		return Schema{Type: "string"}, nil
	case reflect.Struct:
		return schemaForStruct(typ)
	case reflect.Array, reflect.Slice:
		return schemaForArray(typ)
	case reflect.Map:
		return schemaForMap(typ)
	case reflect.Pointer:
		// If this is a pointer to a basic type then we don't need to wrap in a union as all the basic types are nullable.
		underlying, err := schemaForType(typ.Elem())
		if err != nil {
			return Schema{}, fmt.Errorf("getting underlying schema for pointer: %w", err)
		}
		if underlying.Type == "union" || underlying.Type == "array" || underlying.Type == "map" {
			return underlying, nil
		}
		return nullableSchema(underlying), nil
	default:
		return Schema{}, fmt.Errorf("type %s not supported", typ)
	}
}

func nullableSchema(s Schema) Schema {
	return Schema{
		Type: "union",
		Union: []Schema{
			{Type: "null"},
			s,
		},
	}
}

func schemaForStruct(typ reflect.Type) (Schema, error) {
	fields := make([]SchemaRecordField, 0, typ.NumField())
	for i := range typ.NumField() {
		field := typ.Field(i)
		name := nameForField(field)
		if name == "-" {
			continue
		}

		s, err := schemaForType(field.Type)
		if err != nil {
			return Schema{}, fmt.Errorf("getting schema for field %s: %w", name, err)
		}

		if omitEmpty(field) && s.Type != "union" {
			s = nullableSchema(s)
		}

		fields = append(fields, SchemaRecordField{
			Name: name,
			Type: s,
		})
	}

	return Schema{
		Type: "record",
		Object: &SchemaObject{
			Name: typ.Name(),
			// namespace must be a valid Avro namespace, which is a
			// dot-separated alphanumeric string.
			Namespace: namespaceReplacer.Replace(typ.PkgPath()),
			Fields:    fields,
		},
	}, nil
}

var namespaceReplacer = strings.NewReplacer("/", ".", "-", "_")

func schemaForArray(typ reflect.Type) (Schema, error) {
	elem := typ.Elem()
	if elem.Kind() == reflect.Uint8 {
		return Schema{
			Type: "bytes",
		}, nil
	}

	s, err := schemaForType(elem)
	if err != nil {
		return Schema{}, fmt.Errorf("building array schema: %w", err)
	}

	return Schema{
		Type: "array",
		Object: &SchemaObject{
			Items: s,
		},
	}, nil
}

func schemaForMap(typ reflect.Type) (Schema, error) {
	s, err := schemaForType(typ.Elem())
	if err != nil {
		return Schema{}, err
	}

	return Schema{
		Type: "map",
		Object: &SchemaObject{
			Values: s,
		},
	}, nil
}
