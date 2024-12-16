package avro

import (
	"reflect"
	"unsafe"
)

type PointerCodec struct {
	Codec
}

func (c *PointerCodec) Read(r *ReadBuf, p unsafe.Pointer) error {
	pp := (*unsafe.Pointer)(p)
	if *pp == nil {
		*pp = c.Codec.New(r)
	}
	return c.Codec.Read(r, *pp)
}

var pointerType = reflect.TypeOf(unsafe.Pointer(nil))

func (c *PointerCodec) New(r *ReadBuf) unsafe.Pointer {
	return r.Alloc(pointerType)
}

func (c *PointerCodec) Schema() Schema {
	return Schema{
		Type: "union",
		Union: []Schema{
			{Type: "null"},
			c.Codec.Schema(),
		},
	}
}

func (c *PointerCodec) Omit(p unsafe.Pointer) bool {
	return *(*unsafe.Pointer)(p) == nil
}

func (c *PointerCodec) Write(w *WriteBuf, p unsafe.Pointer) error {
	// Note this codec will normally be wrapped by a union codec, so we don't
	// need to worry about writing the union selector.
	pp := *(*unsafe.Pointer)(p)
	if pp == nil {
		return nil
	}
	return c.Codec.Write(w, pp)
}
