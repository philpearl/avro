package avro

import (
	"fmt"
	"math"
	"reflect"
	"unsafe"
)

type recordCodecField struct {
	// Codec for this field
	codec Codec
	// offset of this field within the struct representing the record. -1 if this
	// field is not in the struct and therefore should be skipped
	offset uintptr
	name   string
}

type recordCodec struct {
	rtype  reflect.Type
	fields []recordCodecField
}

func (rc *recordCodec) Read(r *ReadBuf, p unsafe.Pointer) error {
	for i, f := range rc.fields {
		if f.offset == math.MaxUint64 {
			if err := f.codec.Skip(r); err != nil {
				return fmt.Errorf("failed to skip field %d %q of record. %w", i, f.name, err)
			}
		} else {
			if err := f.codec.Read(r, unsafe.Add(p, f.offset)); err != nil {
				return fmt.Errorf("failed reading field %d %q of record. %w", i, f.name, err)
			}
		}
	}
	return nil
}

func (rc *recordCodec) Skip(r *ReadBuf) error {
	for i, f := range rc.fields {
		if err := f.codec.Skip(r); err != nil {
			return fmt.Errorf("failed to skip field %d %q of record. %w", i, f.name, err)
		}
	}
	return nil
}

func (rc *recordCodec) New(r *ReadBuf) unsafe.Pointer {
	return r.Alloc(rc.rtype)
}

func (rc *recordCodec) Omit(p unsafe.Pointer) bool {
	return false
}

func (rc *recordCodec) Write(w *WriteBuf, p unsafe.Pointer) error {
	for i, rf := range rc.fields {
		fp := unsafe.Add(p, rf.offset)
		if err := rf.codec.Write(w, fp); err != nil {
			return fmt.Errorf("failed writing field %d %s: %w", i, rf.name, err)
		}
	}
	return nil
}
