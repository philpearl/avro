package avro

import (
	"encoding/binary"
	"io"
	"unsafe"
)

type reader interface {
	io.ByteReader
	io.Reader
}

func readInt64(r io.ByteReader, ptr unsafe.Pointer) error {
	i, err := binary.ReadVarint(r)
	if err != nil {
		return err
	}

	*(*int64)(ptr) = i
	return nil
}

func readBytes(r reader, ptr unsafe.Pointer) error {
	var l int64
	if err := readInt64(r, unsafe.Pointer(&l)); err != nil {
		return err
	}
	b := make([]byte, l)
	if _, err := io.ReadFull(r, b); err != nil {
		return err
	}
	*(*[]byte)(ptr) = b
	return nil
}

type boolCodec struct{}

func (boolCodec) Read(r Reader, p unsafe.Pointer) error {
	b, err := r.ReadByte()
	if err != nil {
		return err
	}

	*(*bool)(p) = b != 0
	return nil
}

func (boolCodec) Skip(r Reader) error {
	return skip(r, 1)
}

func (boolCodec) New() unsafe.Pointer {
	return unsafe.Pointer(new(bool))
}
