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

func (rc nullCodec) Schema() Schema {
	return Schema{
		Type: "null",
	}
}

func (rc nullCodec) Omit(p unsafe.Pointer) bool {
	return true
}

func (rc nullCodec) Write(w *Writer, p unsafe.Pointer) error {
	return nil
}
