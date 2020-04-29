package avro

import (
	"io"
	"unsafe"
)

type FloatCodec struct{}

func (FloatCodec) Read(r Reader, p unsafe.Pointer) error {
	// This works for little-endian only (or is it bigendian?)
	buf := (*[4]byte)(p)
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return err
	}
	return nil
}

func (FloatCodec) Skip(r Reader) error {
	return skip(r, 4)
}

func (FloatCodec) New() unsafe.Pointer {
	return unsafe.Pointer(new(float32))
}

type DoubleCodec struct{}

func (DoubleCodec) Read(r Reader, p unsafe.Pointer) error {
	// This works for little-endian only (or is it bigendian?)
	buf := (*[8]byte)(p)
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return err
	}
	return nil
}

func (DoubleCodec) Skip(r Reader) error {
	return skip(r, 8)
}

func (DoubleCodec) New() unsafe.Pointer {
	return unsafe.Pointer(new(float64))
}

type Float32DoubleCodec struct {
	DoubleCodec
}

func (c Float32DoubleCodec) Read(r Reader, p unsafe.Pointer) error {
	var f float64
	if err := c.DoubleCodec.Read(r, unsafe.Pointer(&f)); err != nil {
		return err
	}
	*(*float32)(p) = float32(f)
	return nil
}

func (Float32DoubleCodec) New() unsafe.Pointer {
	return unsafe.Pointer(new(float32))
}
