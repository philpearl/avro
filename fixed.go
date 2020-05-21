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

func (f fixedCodec) Read(r *Buffer, p unsafe.Pointer) error {
	// p points to an array of size f.Size
	sh := sliceHeader{
		Data: p,
		Len:  f.Size,
		Cap:  f.Size,
	}
	data, err := r.Next(f.Size)
	copy(*(*[]byte)(unsafe.Pointer(&sh)), data)
	return err
}

func (f fixedCodec) Skip(r *Buffer) error {
	return skip(r, int64(f.Size))
}

func (f fixedCodec) New(r *Buffer) unsafe.Pointer {
	return unsafe_NewArray(unpackEFace(reflect.TypeOf(byte(0))).data, f.Size)
}
