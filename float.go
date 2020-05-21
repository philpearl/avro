package avro

import (
	"reflect"
	"unsafe"
)

type FloatCodec struct{}

func (FloatCodec) Read(r *Buffer, p unsafe.Pointer) error {
	// This works for little-endian only (or is it bigendian?)
	return fixedCodec{Size: 4}.Read(r, p)
}

func (FloatCodec) Skip(r *Buffer) error {
	return skip(r, 4)
}

var floatType = reflect.TypeOf(float32(0))

func (FloatCodec) New(r *Buffer) unsafe.Pointer {
	return r.Alloc(floatType)
}

type DoubleCodec struct{}

func (DoubleCodec) Read(r *Buffer, p unsafe.Pointer) error {
	// This works for little-endian only (or is it bigendian?)
	return fixedCodec{Size: 8}.Read(r, p)
}

func (DoubleCodec) Skip(r *Buffer) error {
	return skip(r, 8)
}

var doubleType = reflect.TypeOf(float64(0))

func (DoubleCodec) New(r *Buffer) unsafe.Pointer {
	return r.Alloc(doubleType)
}

type Float32DoubleCodec struct {
	DoubleCodec
}

func (c Float32DoubleCodec) Read(r *Buffer, p unsafe.Pointer) error {
	var f float64
	if err := c.DoubleCodec.Read(r, unsafe.Pointer(&f)); err != nil {
		return err
	}
	*(*float32)(p) = float32(f)
	return nil
}

func (Float32DoubleCodec) New(r *Buffer) unsafe.Pointer {
	return r.Alloc(floatType)
}
