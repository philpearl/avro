package avro

import (
	"fmt"
	"io"
	"unsafe"
)

type StringCodec struct{}

func (StringCodec) Read(r Reader, ptr unsafe.Pointer) error {
	// ptr is a *string
	var l int64
	if err := readInt64(r, unsafe.Pointer(&l)); err != nil {
		return fmt.Errorf("failed to read length of string. %w", err)
	}
	b := make([]byte, l)
	if _, err := io.ReadFull(r, b); err != nil {
		return fmt.Errorf("failed to read %d bytes of string body. %w", l, err)
	}
	*(*string)(ptr) = *(*string)(unsafe.Pointer(&b))
	return nil
}

func (StringCodec) Skip(r Reader) error {
	var l int64
	if err := readInt64(r, unsafe.Pointer(&l)); err != nil {
		return fmt.Errorf("failed to read length of string. %w", err)
	}
	return skip(r, l)
}

func (StringCodec) New() unsafe.Pointer {
	var v string
	return unsafe.Pointer(&v)
}
