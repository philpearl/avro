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

func readBool(r io.ByteReader, ptr unsafe.Pointer) error {
	b, err := r.ReadByte()
	if err != nil {
		return err
	}

	*(*bool)(ptr) = b != 0
	return nil
}

func readInt32(r io.ByteReader, ptr unsafe.Pointer) error {
	i, err := binary.ReadVarint(r)
	if err != nil {
		return err
	}

	*(*int32)(ptr) = int32(i)
	return nil
}

func readInt64(r io.ByteReader, ptr unsafe.Pointer) error {
	i, err := binary.ReadVarint(r)
	if err != nil {
		return err
	}

	*(*int64)(ptr) = i
	return nil
}

func readFloat32(r io.Reader, ptr unsafe.Pointer) error {
	// This works for little-endian only (or is it bigendian?)
	buf := (*[4]byte)(ptr)
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return err
	}
	return nil
}

func readFloat64(r io.Reader, ptr unsafe.Pointer) error {
	// This works for little-endian only (or is it bigendian?)
	buf := (*[8]byte)(ptr)
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return err
	}
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
