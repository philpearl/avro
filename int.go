package avro

import (
	"fmt"
	"reflect"
	"unsafe"
)

// Int64Codec is an avro codec for int64
type IntCodec[T int64 | int32 | int16] struct{ omitEmpty bool }

func (IntCodec[T]) Read(r *ReadBuf, p unsafe.Pointer) error {
	i, err := r.Varint()

	if i > int64(uint64(1)<<(unsafe.Sizeof(T(0))*8-1)-1) ||
		i < -1<<(unsafe.Sizeof(T(0))*8-1) {
		return fmt.Errorf("value %d will not fit in %T", i, T(0))
	}

	*(*T)(p) = T(i)
	return err
}

// Skip skips over an int
func (IntCodec[T]) Skip(r *ReadBuf) error {
	_, err := r.Varint()
	return err
}

var (
	int64Type = reflect.TypeOf(int64(0))
	int32Type = reflect.TypeOf(int32(0))
	int16Type = reflect.TypeOf(int16(0))
)

// New creates a pointer to a new int64
func (IntCodec[T]) New(r *ReadBuf) unsafe.Pointer {
	switch unsafe.Sizeof(T(0)) {
	case 8:
		return r.Alloc(int64Type)
	case 4:
		return r.Alloc(int32Type)
	case 2:
		return r.Alloc(int16Type)
	}
	panic(fmt.Sprintf("unexpected int size %d", unsafe.Sizeof(T(0))))
}

func (rc IntCodec[T]) Omit(p unsafe.Pointer) bool {
	return rc.omitEmpty && *(*T)(p) == 0
}

func (rc IntCodec[T]) Write(w *WriteBuf, p unsafe.Pointer) {
	w.Varint(int64(*(*T)(p)))
}

type (
	Int64Codec = IntCodec[int64]
	Int32Codec = IntCodec[int32]
	Int16Codec = IntCodec[int16]
)
