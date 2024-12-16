package avro

import (
	"unsafe"
)

type nullCodec struct{}

func (nullCodec) Read(r *ReadBuf, p unsafe.Pointer) error {
	// TODO: could consider nil-ing the pointer
	return nil
}

func (nullCodec) Skip(r *ReadBuf) error {
	return nil
}

func (nullCodec) New(r *ReadBuf) unsafe.Pointer {
	return nil
}

func (rc nullCodec) Omit(p unsafe.Pointer) bool {
	return true
}

func (rc nullCodec) Write(w *WriteBuf, p unsafe.Pointer) {
}
