package avro

import (
	"io"
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

func (f fixedCodec) Read(r Reader, p unsafe.Pointer) error {
	// p points to an array of size f.Size
	sh := sliceHeader{
		Data: p,
		Len:  f.Size,
		Cap:  f.Size,
	}
	_, err := io.ReadFull(r, *(*[]byte)(unsafe.Pointer(&sh)))
	return err
}

func (f fixedCodec) Skip(r Reader) error {
	return skip(r, int64(f.Size))
}

func (f fixedCodec) New() unsafe.Pointer {
	b := make([]byte, f.Size)
	return (*sliceHeader)(unsafe.Pointer(&b)).Data
}
