package avro

import (
	"fmt"
	"io"
	"unsafe"
)

type bytesCodec struct{}

func (bytesCodec) Read(r Reader, ptr unsafe.Pointer) error {
	var l int64
	if err := readInt64(r, unsafe.Pointer(&l)); err != nil {
		return fmt.Errorf("failed to read length of bytes. %w", err)
	}
	if l == 0 {
		return nil
	}
	b := make([]byte, l)
	if _, err := io.ReadFull(r, b); err != nil {
		return fmt.Errorf("failed to read %d bytes of bytes body. %w", l, err)
	}
	*(*[]byte)(ptr) = b
	return nil
}

func (bytesCodec) Skip(r Reader) error {
	var l int64
	if err := readInt64(r, unsafe.Pointer(&l)); err != nil {
		return fmt.Errorf("failed to read length of bytes. %w", err)
	}
	return skip(r, l)
}

func (bytesCodec) New() unsafe.Pointer {
	return unsafe.Pointer(&[]byte{})
}
