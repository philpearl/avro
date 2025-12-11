// avroplenc is a package for encoding and decoding AVRO data that uses the
// plenccodec.Optional[T] type.
package avroplenc

import (
	"fmt"
	"reflect"
	"unsafe"

	"github.com/philpearl/avro"
	"github.com/philpearl/plenc/plenccodec"
)

// RegisterOptionalCodecFor allows you to register a codec for
// plenccodec.Optional[T] where T is a struct for which an AVRO codec can be
// built.
//
// Ideally we'd register a generic codec automatically, but I think go's reflect
// package needs some better support for generics before that will be easy to
// do.
func RegisterOptionalCodecFor[T any]() error {
	var v T
	schema, err := avro.SchemaForType(v)
	if err != nil {
		return fmt.Errorf("obtaining schema: %w", err)
	}
	typ := reflect.TypeFor[plenccodec.Optional[T]]()
	avro.Register(typ, buildOptionalCodec[T])
	avro.RegisterSchema(typ, nullableSchema(schema))

	return nil
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

func buildOptionalCodec[T any](schema avro.Schema, typ reflect.Type, omit bool) (avro.Codec, error) {
	var v T
	codec, err := schema.Codec(&v)
	if err != nil {
		return nil, fmt.Errorf("building underlying codec for plenccodec.Optional: %w", err)
	}

	return &optionalCodec[T]{
		Codec: codec,
		typ:   typ,
	}, nil
}

type optionalCodec[T any] struct {
	avro.Codec
	typ reflect.Type
}

func (c *optionalCodec[T]) Read(data *avro.ReadBuf, p unsafe.Pointer) error {
	opt := (*plenccodec.Optional[T])(p)
	opt.Set = true

	return c.Codec.Read(data, unsafe.Pointer(&opt.Value))
}

func (c *optionalCodec[T]) New(r *avro.ReadBuf) unsafe.Pointer {
	return r.Alloc(c.typ)
}

func (c optionalCodec[T]) Omit(p unsafe.Pointer) bool {
	opt := (*plenccodec.Optional[T])(p)
	return !opt.Set
}

func (c *optionalCodec[T]) Write(w *avro.WriteBuf, p unsafe.Pointer) {
	// I think we'll expect this codec to always be wrapped by a null union
	// codec, so checking for empty would be done elsewhere.
	opt := *(*plenccodec.Optional[T])(p)
	c.Codec.Write(w, unsafe.Pointer(&opt.Value))
}
