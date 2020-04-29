package avro

import (
	"io"
	"unsafe"
)

type floatCodec struct{}

func (floatCodec) Read(r Reader, p unsafe.Pointer) error {
	// This works for little-endian only (or is it bigendian?)
	buf := (*[4]byte)(p)
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return err
	}
	return nil
}

func (floatCodec) Skip(r Reader) error {
	return skip(r, 4)
}

func (floatCodec) New() unsafe.Pointer {
	return unsafe.Pointer(new(float32))
}

type doubleCodec struct{}

func (doubleCodec) Read(r Reader, p unsafe.Pointer) error {
	// This works for little-endian only (or is it bigendian?)
	buf := (*[8]byte)(p)
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return err
	}
	return nil
}

func (doubleCodec) Skip(r Reader) error {
	return skip(r, 8)
}

func (doubleCodec) New() unsafe.Pointer {
	return unsafe.Pointer(new(float64))
}

type float32DoubleCodec struct {
	doubleCodec
}

func (c float32DoubleCodec) Read(r Reader, p unsafe.Pointer) error {
	var f float64
	if err := c.doubleCodec.Read(r, unsafe.Pointer(&f)); err != nil {
		return err
	}
	*(*float32)(p) = float32(f)
	return nil
}

func (float32DoubleCodec) New() unsafe.Pointer {
	return unsafe.Pointer(new(float32))
}
