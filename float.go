package avro

import (
	"fmt"
	"reflect"
	"unsafe"
)

type floatCodec[t float32 | float64] struct{}

func (floatCodec[T]) Read(r *Buffer, p unsafe.Pointer) error {
	// This works for little-endian only (or is it bigendian?)
	return fixedCodec{Size: int(unsafe.Sizeof(T(0)))}.Read(r, p)
}

func (floatCodec[T]) Skip(r *Buffer) error {
	return skip(r, int64(unsafe.Sizeof(T(0))))
}

var (
	floatType  = reflect.TypeOf(float32(0))
	doubleType = reflect.TypeOf(float64(0))
)

func (floatCodec[T]) New(r *Buffer) unsafe.Pointer {
	switch unsafe.Sizeof(T(0)) {
	case 4:
		return r.Alloc(floatType)
	case 8:
		return r.Alloc(doubleType)
	}
	panic(fmt.Sprintf("unexpected float size %d", unsafe.Sizeof(T(0))))
}

type (
	FloatCodec  = floatCodec[float32]
	DoubleCodec = floatCodec[float64]
)

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
