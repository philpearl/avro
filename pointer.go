package avro

import "unsafe"

type pointerCodec struct {
	Codec
}

func (c *pointerCodec) Read(r Reader, p unsafe.Pointer) error {
	pp := (*unsafe.Pointer)(p)
	if *pp == nil {
		*pp = c.Codec.New()
	}
	return c.Codec.Read(r, *pp)
}

func (c *pointerCodec) New() unsafe.Pointer {
	return unsafe.Pointer(new(unsafe.Pointer))
}
