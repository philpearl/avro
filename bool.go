package avro

import (
	"reflect"
	"unsafe"
)

type BoolCodec struct{ omitEmpty bool }

func (BoolCodec) Read(r *Buffer, p unsafe.Pointer) error {
	b, err := r.ReadByte()
	if err != nil {
		return err
	}

	*(*bool)(p) = b != 0
	return nil
}

func (BoolCodec) Skip(r *Buffer) error {
	return skip(r, 1)
}

var boolType = reflect.TypeOf(false)

func (BoolCodec) New(r *Buffer) unsafe.Pointer {
	return r.Alloc(boolType)
}

func (rc BoolCodec) Schema() Schema {
	return Schema{
		Type: "boolean",
	}
}

func (rc BoolCodec) Omit(p unsafe.Pointer) bool {
	return rc.omitEmpty && !*(*bool)(p)
}

func (rc BoolCodec) Write(w *Writer, p unsafe.Pointer) error {
	if *(*bool)(p) {
		w.Byte(1)
	} else {
		w.Byte(0)
	}

	return nil
}
