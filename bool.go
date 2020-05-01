package avro

import (
	"unsafe"
)

type BoolCodec struct{}

func (BoolCodec) Read(r *Buffer, p unsafe.Pointer) error {
	b, err := r.ReadByte()
	if err != nil {
		return err
	}

	*(*bool)(p) = b != 0
	return nil
}

func (BoolCodec) Skip(r *Buffer) error {
	return skip(r, 1)
}

func (BoolCodec) New() unsafe.Pointer {
	return unsafe.Pointer(new(bool))
}
