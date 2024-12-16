package avro

import (
	"fmt"
	"reflect"
	"unsafe"
)

// StringCodec is a decoder for strings
type StringCodec struct{ omitEmpty bool }

func (StringCodec) Read(r *ReadBuf, ptr unsafe.Pointer) error {
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

func (StringCodec) Skip(r *ReadBuf) error {
	l, err := r.Varint()
	if err != nil {
		return fmt.Errorf("failed to read length of string. %w", err)
	}
	return skip(r, l)
}

var stringType = reflect.TypeOf("")

func (StringCodec) New(r *ReadBuf) unsafe.Pointer {
	return r.Alloc(stringType)
}

func (sc StringCodec) Omit(p unsafe.Pointer) bool {
	return sc.omitEmpty && len(*(*string)(p)) == 0
}

func (StringCodec) Write(w *WriteBuf, p unsafe.Pointer) error {
	s := *(*string)(p)
	w.Varint(int64(len(s)))
	w.Write([]byte(s))
	return nil
}
