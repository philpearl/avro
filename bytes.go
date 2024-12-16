package avro

import (
	"fmt"
	"reflect"
	"unsafe"
)

type BytesCodec struct{ omitEmpty bool }

func (BytesCodec) Read(r *ReadBuf, ptr unsafe.Pointer) error {
	l, err := r.Varint()
	if err != nil {
		return fmt.Errorf("failed to read length of bytes. %w", err)
	}
	if l == 0 {
		return nil
	}
	data, err := r.Next(int(l))
	if err != nil {
		return fmt.Errorf("failed to read %d bytes of bytes body. %w", l, err)
	}
	// We need to copy the data to avoid data issues
	b := make([]byte, l)
	copy(b, data)
	*(*[]byte)(ptr) = b
	return nil
}

func (BytesCodec) Skip(r *ReadBuf) error {
	l, err := r.Varint()
	if err != nil {
		return fmt.Errorf("failed to read length of bytes. %w", err)
	}
	return skip(r, l)
}

var bytesType = reflect.TypeOf([]byte{})

func (BytesCodec) New(r *ReadBuf) unsafe.Pointer {
	return r.Alloc(bytesType)
}

func (rc BytesCodec) Omit(p unsafe.Pointer) bool {
	return rc.omitEmpty && len(*(*[]byte)(p)) == 0
}

func (rc BytesCodec) Write(w *WriteBuf, p unsafe.Pointer) {
	sh := *(*[]byte)(p)

	w.Varint(int64(len(sh)))
	w.Write(sh)
}
