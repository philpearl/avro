package avro

import (
	"reflect"
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

var boolType = reflect.TypeOf(false)

func (BoolCodec) New(r *Buffer) unsafe.Pointer {
	return r.Alloc(boolType)
}
