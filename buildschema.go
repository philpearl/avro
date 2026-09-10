package avro

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
)

var (
	schemaRegistryMutex sync.RWMutex
	schemaRegistry      = make(map[reflect.Type]Schema)

	// DoNotRefineSchemas when true stops avro from generating a full schema
	// definition for a record that's already been defined in the schema.
	// Setting this to true makes this library more compliant with the AVRO
	// spec, but will break interoperation with older versions of the library.
	//
	// The intention is that I will remove this once sufficient time has passed
	// since I deliver a version of the library that works with Schemas
	// generated in this way.
	DoNotRedefineSchemas atomic.Bool
)

// Call RegisterSchema to indicate what schema should be used for a given type.
// Use this to register the schema to use for a type for which you write a
// custom codec.
func RegisterSchema(typ reflect.Type, s Schema) {
	schemaRegistryMutex.Lock()
	defer schemaRegistryMutex.Unlock()
	schemaRegistry[typ] = s
}

type schemaKey struct {
	name      string
	namespace string
}

// SchemaForType returns a Schema for the given type. It aims to produce a
// Schema that's compatible with BigQuery.
func SchemaForType(item any) (Schema, error) {
	typ := reflect.TypeOf(item)
	if typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	if typ.Kind() != reflect.Struct {
		return Schema{}, fmt.Errorf("item must be a struct or pointer to a struct")
	}

	// definedSchemas is a mechanism for us to spot when we've already defined
	// the schema for a record type earlier in the schema.
	//
	// At the moment using this is optional and off by default. But using it
	// gives the correct schema according to the AVRO spec. We're leaving it off
	// by default until the code to cope with a schema generated like this is
	// sufficiently out in the world. At that point we can remove the variable.
	// That will also break people's builds, but frankly that's just Ravelin and
	// we can cope.
	var definedSchemas map[schemaKey]struct{}
	if DoNotRedefineSchemas.Load() {
		definedSchemas = make(map[schemaKey]struct{})
	}

	return schemaForType(typ, definedSchemas)
}

func isInSchemaRegistry(typ reflect.Type) (Schema, bool) {
	schemaRegistryMutex.RLock()
	defer schemaRegistryMutex.RUnlock()
	s, ok := schemaRegistry[typ]
	return s, ok
}

func schemaForType(typ reflect.Type, definedSchemas map[schemaKey]struct{}) (schema Schema, err error) {
	if s, ok := isInSchemaRegistry(typ); ok {
		return s, nil
	}

	// BigQuery makes every basic type nullable. We'll send null for the zero
	// value if there's an "omitempty" tag.
	switch typ.Kind() {
	case reflect.Bool:
		return Schema{Type: "boolean"}, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint64:
		return Schema{Type: "long"}, nil
	case reflect.Float32, reflect.Float64:
		return Schema{Type: "double"}, nil
	case reflect.String:
		return Schema{Type: "string"}, nil
	case reflect.Struct:
		return schemaForStruct(typ, definedSchemas)
	case reflect.Array, reflect.Slice:
		return schemaForArray(typ, definedSchemas)
	case reflect.Map:
		return schemaForMap(typ, definedSchemas)
	case reflect.Pointer:
		// If this is a pointer to a basic type then we don't need to wrap in a union as all the basic types are nullable.
		underlying, err := schemaForType(typ.Elem(), definedSchemas)
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

func schemaForStruct(typ reflect.Type, definedSchemas map[schemaKey]struct{}) (Schema, error) {
	name := typ.Name()
	namespace := namespaceReplacer.Replace(typ.PkgPath())
	if definedSchemas != nil {
		if _, alreadySeen := definedSchemas[schemaKey{name: name, namespace: namespace}]; alreadySeen {
			return Schema{
				Type: namespace + "." + name,
			}, nil
		}
		definedSchemas[schemaKey{name: name, namespace: namespace}] = struct{}{}
	}

	fields := make([]SchemaRecordField, 0, typ.NumField())
	for field := range typ.Fields() {
		name := nameForField(field)
		if name == "-" {
			continue
		}

		s, err := schemaForType(field.Type, definedSchemas)
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
			Name: name,
			// namespace must be a valid Avro namespace, which is a
			// dot-separated alphanumeric string.
			Namespace: namespace,
			Fields:    fields,
		},
	}, nil
}

var namespaceReplacer = strings.NewReplacer("/", ".", "-", "_")

func schemaForArray(typ reflect.Type, definedSchemas map[schemaKey]struct{}) (Schema, error) {
	elem := typ.Elem()
	if elem.Kind() == reflect.Uint8 {
		return Schema{
			Type: "bytes",
		}, nil
	}

	s, err := schemaForType(elem, definedSchemas)
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

func schemaForMap(typ reflect.Type, definedSchemas map[schemaKey]struct{}) (Schema, error) {
	s, err := schemaForType(typ.Elem(), definedSchemas)
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
