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
