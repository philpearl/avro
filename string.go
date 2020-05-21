package avro

import (
	"fmt"
	"reflect"
	"unsafe"
)

// StringCodec is a decoder for strings
type StringCodec struct{}

func (StringCodec) Read(r *Buffer, ptr unsafe.Pointer) error {
	// ptr is a *string
	l, err := r.Varint()
	if err != nil {
		return fmt.Errorf("failed to read length of string. %w", err)
	}
	if l < 0 {
		return fmt.Errorf("cannot make string with length %d", l)
	}
	data, err := r.NextAsString(int(l))
	if err != nil {
		return fmt.Errorf("failed to read %d bytes of string body. %w", l, err)
	}
	*(*string)(ptr) = data
	return nil
}

func (StringCodec) Skip(r *Buffer) error {
	l, err := r.Varint()
	if err != nil {
		return fmt.Errorf("failed to read length of string. %w", err)
	}
	return skip(r, l)
}

var stringType = reflect.TypeOf("")

func (StringCodec) New(r *Buffer) unsafe.Pointer {
	return r.Alloc(stringType)
}
