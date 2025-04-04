package avro

import (
	"fmt"
	"reflect"

	"github.com/go-json-experiment/json"
	"github.com/go-json-experiment/json/jsontext"
)

// Schema is a representation of AVRO schema JSON. Primitive types populate Type
// only. UnionTypes populate Type and Union fields. All other types populate
// Type and a subset of Object fields.
type Schema struct {
	Type   string
	Object *SchemaObject
	Union  []Schema
}

// Codec creates a codec for the given schema and output type
func (s Schema) Codec(out any) (Codec, error) {
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
	if err := json.Unmarshal([]byte(in), &schema); err != nil {
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

func (s *Schema) UnmarshalJSONFrom(dec *jsontext.Decoder) error {
	switch dec.PeekKind() {
	case '"':
		token, err := dec.ReadToken()
		if err != nil {
			return fmt.Errorf("reading string: %w", err)
		}
		s.Type = token.String()
	case '[':
		// This is an array of Schemas
		s.Type = "union"
		if err := json.UnmarshalDecode(dec, &s.Union); err != nil {
			return fmt.Errorf("decoding union: %w", err)
		}
	case '{':
		s.Object = &SchemaObject{}
		// do we need to isolate these decoders?
		if err := json.UnmarshalDecode(dec, s.Object); err != nil {
			return fmt.Errorf("decoding union: %w", err)
		}

		s.Type = s.Object.Type
		s.Object.Type = ""

	default:
		return fmt.Errorf("unexpected token unmarshalling schema: %s", dec.PeekKind())
	}
	return nil
}

func (s *Schema) MarshalJSONTo(enc *jsontext.Encoder) error {
	switch {
	case s.Object != nil:
		if err := enc.WriteToken(jsontext.BeginObject); err != nil {
			return fmt.Errorf("writing begin object: %w", err)
		}
		if err := enc.WriteToken(jsontext.String("type")); err != nil {
			return fmt.Errorf("writing type key: %w", err)
		}
		if err := enc.WriteToken(jsontext.String(s.Type)); err != nil {
			return fmt.Errorf("writing type value: %w", err)
		}
		if s.Object.LogicalType != "" {
			if err := enc.WriteToken(jsontext.String("logicalType")); err != nil {
				return fmt.Errorf("writing logicalType key: %w", err)
			}
			if err := enc.WriteToken(jsontext.String(s.Object.LogicalType)); err != nil {
				return fmt.Errorf("writing logicalType value: %w", err)
			}
		}
		if s.Object.Name != "" {
			if err := enc.WriteToken(jsontext.String("name")); err != nil {
				return fmt.Errorf("writing name key: %w", err)
			}
			if err := enc.WriteToken(jsontext.String(s.Object.Name)); err != nil {
				return fmt.Errorf("writing name value: %w", err)
			}
		}
		if s.Object.Namespace != "" {
			if err := enc.WriteToken(jsontext.String("namespace")); err != nil {
				return fmt.Errorf("writing namespace key: %w", err)
			}
			if err := enc.WriteToken(jsontext.String(s.Object.Namespace)); err != nil {
				return fmt.Errorf("writing namespace value: %w", err)
			}
		}
		switch s.Type {
		case "record":
			if err := enc.WriteToken(jsontext.String("fields")); err != nil {
				return fmt.Errorf("writing fields key: %w", err)
			}
			if err := json.MarshalEncode(enc, s.Object.Fields); err != nil {
				return fmt.Errorf("encoding record fields: %w", err)
			}
		case "enum":
			if err := enc.WriteToken(jsontext.String("symbols")); err != nil {
				return fmt.Errorf("writing symbols key: %w", err)
			}
			if err := json.MarshalEncode(enc, s.Object.Symbols); err != nil {
				return fmt.Errorf("encoding enum symbols: %w", err)
			}
		case "array":
			if err := enc.WriteToken(jsontext.String("items")); err != nil {
				return fmt.Errorf("writing items key: %w", err)
			}
			if err := json.MarshalEncode(enc, s.Object.Items); err != nil {
				return fmt.Errorf("encoding items: %w", err)
			}
		case "map":
			if err := enc.WriteToken(jsontext.String("values")); err != nil {
				return fmt.Errorf("writing values key: %w", err)
			}
			if err := json.MarshalEncode(enc, s.Object.Values); err != nil {
				return fmt.Errorf("encoding values: %w", err)
			}
		case "fixed":
			if err := enc.WriteToken(jsontext.String("size")); err != nil {
				return fmt.Errorf("writing size key: %w", err)
			}
			if err := enc.WriteToken(jsontext.Int(int64(s.Object.Size))); err != nil {
				return fmt.Errorf("writing size value: %w", err)
			}
		}
		if err := enc.WriteToken(jsontext.EndObject); err != nil {
			return fmt.Errorf("writing end object: %w", err)
		}

	case len(s.Union) != 0:
		if err := json.MarshalEncode(enc, s.Union); err != nil {
			return fmt.Errorf("encoding union: %w", err)
		}
	default:
		enc.WriteToken(jsontext.String(s.Type))
	}
	return nil
}
