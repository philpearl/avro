package avro

import (
	"fmt"
	"math"
	"reflect"
	"unsafe"
)

// Int64Codec is an avro codec for int64
type Int64Codec struct{}

func (Int64Codec) Read(r *Buffer, p unsafe.Pointer) error {
	i, err := r.Varint()
	*(*int64)(p) = i
	return err
}

// Skip skips over an int
func (Int64Codec) Skip(r *Buffer) error {
	_, err := r.Varint()
	return err
}

var int64Type = reflect.TypeOf(int64(0))

// New creates a pointer to a new int64
func (Int64Codec) New(r *Buffer) unsafe.Pointer {
	return r.Alloc(int64Type)
}

type Int32Codec struct{}

func (Int32Codec) Read(r *Buffer, p unsafe.Pointer) error {
	i, err := r.Varint()
	if i > math.MaxInt32 || i < math.MinInt32 {
		return fmt.Errorf("value %d will not fit in int32", i)
	}
	*(*int32)(p) = int32(i)
	return err
}

func (Int32Codec) Skip(r *Buffer) error {
	_, err := r.Varint()
	return err
}

var int32Type = reflect.TypeOf(int32(0))

func (Int32Codec) New(r *Buffer) unsafe.Pointer {
	return r.Alloc(int32Type)
}

type Int16Codec struct{}

func (Int16Codec) Read(r *Buffer, p unsafe.Pointer) error {
	i, err := r.Varint()
	if i > math.MaxInt16 || i < math.MinInt16 {
		return fmt.Errorf("value %d will not fit in int16", i)
	}
	*(*int16)(p) = int16(i)
	return err
}

func (Int16Codec) Skip(r *Buffer) error {
	_, err := r.Varint()
	return err
}

var int16Type = reflect.TypeOf(int16(0))

func (Int16Codec) New(r *Buffer) unsafe.Pointer {
	return r.Alloc(int16Type)
}
