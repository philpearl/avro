package avro

import (
	"reflect"
	"unsafe"
)

type PointerCodec struct {
	Codec
}

func (c *PointerCodec) Read(r *Buffer, p unsafe.Pointer) error {
	pp := (*unsafe.Pointer)(p)
	if *pp == nil {
		*pp = c.Codec.New(r)
	}
	return c.Codec.Read(r, *pp)
}

var pointerType = reflect.TypeOf(unsafe.Pointer(nil))

func (c *PointerCodec) New(r *Buffer) unsafe.Pointer {
	return r.Alloc(pointerType)
}

func (c *PointerCodec) Schema() Schema {
	return Schema{
		Type: "union",
		Union: []Schema{
			{Type: "null"},
			c.Schema(),
		},
	}
}

func (c *PointerCodec) Write(w *Writer, p unsafe.Pointer) error {
	// TODO: do we encode the union here? or in the union type?
	pp := *(*unsafe.Pointer)(p)
	if pp == nil {
		return nil
	}
	return c.Codec.Write(w, pp)
}
