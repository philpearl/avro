package avro

import (
	"reflect"
	"unsafe"
)

type fixedCodec struct {
	Size int
}

type sliceHeader struct {
	Data unsafe.Pointer
	Len  int
	Cap  int
}

func (f fixedCodec) Read(r *ReadBuf, p unsafe.Pointer) error {
	// p points to an array of size f.Size
	sh := unsafe.Slice((*byte)(p), f.Size)
	data, err := r.Next(f.Size)
	copy(sh, data)
	return err
}

func (f fixedCodec) Skip(r *ReadBuf) error {
	return skip(r, int64(f.Size))
}

func (f fixedCodec) New(r *ReadBuf) unsafe.Pointer {
	return r.AllocArray(reflect.TypeFor[byte](), f.Size)
}

func (rc fixedCodec) Omit(p unsafe.Pointer) bool {
	return false
}

func (rc fixedCodec) Write(w *WriteBuf, p unsafe.Pointer) {
	sh := unsafe.Slice((*byte)(p), rc.Size)
	w.Write(sh)
}
