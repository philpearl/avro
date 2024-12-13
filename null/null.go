// Package null contains avro decoders for the types in github.com/unravelin/null.
// Call RegisterCodecs to make these codecs available to avro
package null

import (
	"fmt"
	"reflect"
	"unsafe"

	"github.com/philpearl/avro"
	avrotime "github.com/philpearl/avro/time"
	"github.com/unravelin/null"
)

// RegisterCodecs registers the codecs from this package and makes them
// available to avro.
func RegisterCodecs() {
	avro.Register(reflect.TypeOf(null.Int{}), buildNullIntCodec)
	avro.Register(reflect.TypeOf(null.Bool{}), buildNullBoolCodec)
	avro.Register(reflect.TypeOf(null.Float{}), buildNullFloatCodec)
	avro.Register(reflect.TypeOf(null.String{}), buildNullStringCodec)
	avro.Register(reflect.TypeOf(null.Time{}), buildNullTimeCodec)

	avro.RegisterSchema(reflect.TypeOf(null.Int{}), nullableSchema(avro.Schema{Type: "long"}))
	avro.RegisterSchema(reflect.TypeOf(null.Bool{}), nullableSchema(avro.Schema{Type: "boolean"}))
	avro.RegisterSchema(reflect.TypeOf(null.Float{}), nullableSchema(avro.Schema{Type: "double"}))
	avro.RegisterSchema(reflect.TypeOf(null.String{}), nullableSchema(avro.Schema{Type: "string"}))

	// This reflects the common use of null.Time within Ravelin, the owner of the null package.
	avro.RegisterSchema(reflect.TypeOf(null.Time{}), nullableSchema(avro.Schema{Type: "string"}))
}

func nullableSchema(s avro.Schema) avro.Schema {
	return avro.Schema{
		Type: "union",
		Union: []avro.Schema{
			{Type: "null"},
			s,
		},
	}
}

func buildNullIntCodec(schema avro.Schema, typ reflect.Type, omit bool) (avro.Codec, error) {
	if schema.Type != "long" && schema.Type != "int" {
		return nil, fmt.Errorf("null.Int can only be used with long and int schema types")
	}
	return nullIntCodec{}, nil
}

type nullIntCodec struct {
	avro.Int64Codec
}

func (c nullIntCodec) Read(data *avro.Buffer, p unsafe.Pointer) error {
	ni := (*null.Int)(p)
	ni.Valid = true

	return c.Int64Codec.Read(data, unsafe.Pointer(&ni.Int64))
}

var intType = reflect.TypeOf(null.Int{})

func (c nullIntCodec) New(r *avro.Buffer) unsafe.Pointer {
	return r.Alloc(intType)
}

func (c nullIntCodec) Omit(p unsafe.Pointer) bool {
	ni := (*null.Int)(p)
	return !ni.Valid
}

func (c nullIntCodec) Write(w *avro.Writer, p unsafe.Pointer) error {
	// I think we'll expect this codec to always be wrapped by a null union
	// codec, so checking for empty would be done elsewhere.
	ni := *(*null.Int)(p)
	return c.Int64Codec.Write(w, unsafe.Pointer(&ni.Int64))
}

func buildNullBoolCodec(schema avro.Schema, typ reflect.Type, omit bool) (avro.Codec, error) {
	if schema.Type != "boolean" {
		return nil, fmt.Errorf("null.Bool can only be used with boolean schema types")
	}
	return nullBoolCodec{}, nil
}

type nullBoolCodec struct {
	avro.BoolCodec
}

func (c nullBoolCodec) Read(data *avro.Buffer, ptr unsafe.Pointer) error {
	nb := (*null.Bool)(ptr)
	nb.Valid = true
	return c.BoolCodec.Read(data, unsafe.Pointer(&nb.Bool))
}

var boolType = reflect.TypeOf(null.Bool{})

func (c nullBoolCodec) New(r *avro.Buffer) unsafe.Pointer {
	return r.Alloc(boolType)
}

func (c nullBoolCodec) Omit(p unsafe.Pointer) bool {
	ni := (*null.Bool)(p)
	return !ni.Valid
}

func (c nullBoolCodec) Write(w *avro.Writer, p unsafe.Pointer) error {
	// I think we'll expect this codec to always be wrapped by a null union
	// codec, so checking for empty would be done elsewhere.
	ni := *(*null.Bool)(p)
	return c.BoolCodec.Write(w, unsafe.Pointer(&ni.Bool))
}

func buildNullFloatCodec(schema avro.Schema, typ reflect.Type, omit bool) (avro.Codec, error) {
	if schema.Type == "double" {
		return nullDoubleCodec{}, nil
	}

	if schema.Type == "float" {
		return nullFloatCodec{}, nil
	}

	return nil, fmt.Errorf("null.Float can only be used with double & float schema types")
}

type nullDoubleCodec struct {
	avro.DoubleCodec
}

func (c nullDoubleCodec) Read(data *avro.Buffer, ptr unsafe.Pointer) error {
	nf := (*null.Float)(ptr)
	nf.Valid = true
	return c.DoubleCodec.Read(data, unsafe.Pointer(&nf.Float64))
}

func (c nullDoubleCodec) Omit(p unsafe.Pointer) bool {
	ni := (*null.Float)(p)
	return !ni.Valid
}

var floatType = reflect.TypeOf(null.Float{})

func (c nullDoubleCodec) New(r *avro.Buffer) unsafe.Pointer {
	return r.Alloc(floatType)
}

func (c nullDoubleCodec) Write(w *avro.Writer, p unsafe.Pointer) error {
	// I think we'll expect this codec to always be wrapped by a null union
	// codec, so checking for empty would be done elsewhere.
	ni := *(*null.Float)(p)
	return c.DoubleCodec.Write(w, unsafe.Pointer(&ni.Float64))
}

type nullFloatCodec struct {
	avro.FloatCodec
}

func (c nullFloatCodec) Read(data *avro.Buffer, ptr unsafe.Pointer) error {
	var f float32
	if err := c.FloatCodec.Read(data, unsafe.Pointer(&f)); err != nil {
		return err
	}
	nf := (*null.Float)(ptr)
	nf.Valid = true
	nf.Float64 = float64(f)
	return nil
}

func (c nullFloatCodec) New(r *avro.Buffer) unsafe.Pointer {
	return r.Alloc(floatType)
}

func (c nullFloatCodec) Omit(p unsafe.Pointer) bool {
	ni := (*null.Float)(p)
	return !ni.Valid
}

func (c nullFloatCodec) Write(w *avro.Writer, p unsafe.Pointer) error {
	// I think we'll expect this codec to always be wrapped by a null union
	// codec, so checking for empty would be done elsewhere.
	ni := *(*null.Float)(p)
	return c.FloatCodec.Write(w, unsafe.Pointer(&ni.Float64))
}

func buildNullStringCodec(schema avro.Schema, typ reflect.Type, omit bool) (avro.Codec, error) {
	if schema.Type != "string" {
		return nil, fmt.Errorf("null.String can only be used with string schema type, not %s", schema.Type)
	}
	return nullStringCodec{}, nil
}

type nullStringCodec struct {
	avro.StringCodec
}

func (c nullStringCodec) Read(data *avro.Buffer, ptr unsafe.Pointer) error {
	ns := (*null.String)(ptr)
	ns.Valid = true
	return c.StringCodec.Read(data, unsafe.Pointer(&ns.String))
}

var stringType = reflect.TypeOf(null.String{})

func (c nullStringCodec) New(r *avro.Buffer) unsafe.Pointer {
	return r.Alloc(stringType)
}

func (c nullStringCodec) Omit(p unsafe.Pointer) bool {
	ni := (*null.String)(p)
	return !ni.Valid
}

func (c nullStringCodec) Write(w *avro.Writer, p unsafe.Pointer) error {
	// I think we'll expect this codec to always be wrapped by a null union
	// codec, so checking for empty would be done elsewhere.
	ni := *(*null.String)(p)
	return c.StringCodec.Write(w, unsafe.Pointer(&ni.String))
}

func buildNullTimeCodec(schema avro.Schema, typ reflect.Type, omit bool) (avro.Codec, error) {
	if schema.Type != "string" {
		return nil, fmt.Errorf("null.Time is only supported for string, not for %s", schema.Type)
	}
	return nullTimeCodec{}, nil
}

type nullTimeCodec struct {
	avrotime.StringCodec
}

func (c nullTimeCodec) Read(data *avro.Buffer, ptr unsafe.Pointer) error {
	nt := (*null.Time)(ptr)
	nt.Valid = true
	return c.StringCodec.Read(data, unsafe.Pointer(&nt.Time))
}

var timeType = reflect.TypeOf(null.Time{})

func (c nullTimeCodec) New(r *avro.Buffer) unsafe.Pointer {
	return r.Alloc(timeType)
}

func (c nullTimeCodec) Omit(p unsafe.Pointer) bool {
	ni := (*null.Time)(p)
	return !ni.Valid
}

func (c nullTimeCodec) Write(w *avro.Writer, p unsafe.Pointer) error {
	// I think we'll expect this codec to always be wrapped by a null union
	// codec, so checking for empty would be done elsewhere.
	ni := *(*null.Time)(p)
	return c.StringCodec.Write(w, unsafe.Pointer(&ni.Time))
}
