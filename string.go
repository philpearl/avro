package avro

import (
	"fmt"
	"io"
	"unsafe"
)

// StringCodec is a decoder for strings
type StringCodec struct{}

func (StringCodec) Read(r Reader, ptr unsafe.Pointer) error {
	// ptr is a *string
	l, err := readVarint(r)
	if err != nil {
		return fmt.Errorf("failed to read length of string. %w", err)
	}
	if l < 0 {
		return fmt.Errorf("cannot make string with length %d", l)
	}
	b := make([]byte, l)
	if _, err := io.ReadFull(r, b); err != nil {
		return fmt.Errorf("failed to read %d bytes of string body. %w", l, err)
	}
	*(*string)(ptr) = *(*string)(unsafe.Pointer(&b))
	return nil
}

func (StringCodec) Skip(r Reader) error {
	l, err := readVarint(r)
	if err != nil {
		return fmt.Errorf("failed to read length of string. %w", err)
	}
	return skip(r, l)
}

func (StringCodec) New() unsafe.Pointer {
	var v string
	return unsafe.Pointer(&v)
}
