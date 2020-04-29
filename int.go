package avro

import (
	"encoding/binary"
	"fmt"
	"math"
	"unsafe"
)

type int64Codec struct{}

func (int64Codec) Read(r Reader, p unsafe.Pointer) error {
	i, err := binary.ReadVarint(r)
	*(*int64)(p) = i
	return err
}

func (int64Codec) Skip(r Reader) error {
	_, err := binary.ReadVarint(r)
	return err
}

func (int64Codec) New() unsafe.Pointer {
	return unsafe.Pointer(new(int64))
}

type int32Codec struct{}

func (int32Codec) Read(r Reader, p unsafe.Pointer) error {
	i, err := binary.ReadVarint(r)
	if i > math.MaxInt32 || i < math.MinInt32 {
		return fmt.Errorf("value %d will not fit in int32", i)
	}
	*(*int32)(p) = int32(i)
	return err
}

func (int32Codec) Skip(r Reader) error {
	_, err := binary.ReadVarint(r)
	return err
}

func (int32Codec) New() unsafe.Pointer {
	return unsafe.Pointer(new(int32))
}

type int16Codec struct{}

func (int16Codec) Read(r Reader, p unsafe.Pointer) error {
	i, err := binary.ReadVarint(r)
	if i > math.MaxInt16 || i < math.MinInt16 {
		return fmt.Errorf("value %d will not fit in int16", i)
	}
	*(*int16)(p) = int16(i)
	return err
}

func (int16Codec) Skip(r Reader) error {
	_, err := binary.ReadVarint(r)
	return err
}

func (int16Codec) New() unsafe.Pointer {
	return unsafe.Pointer(new(int16))
}
