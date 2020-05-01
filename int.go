package avro

import (
	"fmt"
	"math"
	"unsafe"
)

// Int64Codec is an avro codec for int64
type Int64Codec struct{}

func (Int64Codec) Read(r *Buffer, p unsafe.Pointer) error {
	i, err := readVarint(r)
	*(*int64)(p) = i
	return err
}

// Skip skips over an int
func (Int64Codec) Skip(r *Buffer) error {
	_, err := readVarint(r)
	return err
}

// New creates a pointer to a new int64
func (Int64Codec) New() unsafe.Pointer {
	return unsafe.Pointer(new(int64))
}

type Int32Codec struct{}

func (Int32Codec) Read(r *Buffer, p unsafe.Pointer) error {
	i, err := readVarint(r)
	if i > math.MaxInt32 || i < math.MinInt32 {
		return fmt.Errorf("value %d will not fit in int32", i)
	}
	*(*int32)(p) = int32(i)
	return err
}

func (Int32Codec) Skip(r *Buffer) error {
	_, err := readVarint(r)
	return err
}

func (Int32Codec) New() unsafe.Pointer {
	return unsafe.Pointer(new(int32))
}

type Int16Codec struct{}

func (Int16Codec) Read(r *Buffer, p unsafe.Pointer) error {
	i, err := readVarint(r)
	if i > math.MaxInt16 || i < math.MinInt16 {
		return fmt.Errorf("value %d will not fit in int16", i)
	}
	*(*int16)(p) = int16(i)
	return err
}

func (Int16Codec) Skip(r *Buffer) error {
	_, err := readVarint(r)
	return err
}

func (Int16Codec) New() unsafe.Pointer {
	return unsafe.Pointer(new(int16))
}
