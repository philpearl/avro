package avro

import (
	"unsafe"
)

type nullCodec struct{}

func (nullCodec) Read(r *Buffer, p unsafe.Pointer) error {
	// TODO: could consider nil-ing the pointer
	return nil
}

func (nullCodec) Skip(r *Buffer) error {
	return nil
}

func (nullCodec) New(r *Buffer) unsafe.Pointer {
	return nil
}
