package avro

import (
	"fmt"
	"unsafe"
)

type unionCodec struct {
	codecs []Codec
}

func (u *unionCodec) Read(r *Buffer, p unsafe.Pointer) error {
	index, err := r.Varint()
	if err != nil {
		return fmt.Errorf("failed reading union selector. %w", err)
	}
	if index < 0 || index >= int64(len(u.codecs)) {
		return fmt.Errorf("union selector %d out of range (%d types)", index, len(u.codecs))
	}

	c := u.codecs[index]
	return c.Read(r, p)
}

func (u *unionCodec) Skip(r *Buffer) error {
	index, err := r.Varint()
	if err != nil {
		return fmt.Errorf("failed reading union selector. %w", err)
	}
	if index < 0 || index >= int64(len(u.codecs)) {
		return fmt.Errorf("union selector %d out of range (%d types)", index, len(u.codecs))
	}

	c := u.codecs[index]
	return c.Skip(r)
}

func (u *unionCodec) New(r *Buffer) unsafe.Pointer {
	return nil
}

func (u *unionCodec) Schema() Schema {
	us := make([]Schema, len(u.codecs))
	for i, c := range u.codecs {
		us[i] = c.Schema()
	}
	return Schema{
		Type:  "union",
		Union: us,
	}
}

func (u *unionCodec) Write(w *Writer, p unsafe.Pointer) error {
	// TODO: Need a way to determine which type!
	return fmt.Errorf("union codec not implemented!")
}

type unionOneAndNullCodec struct {
	codec   Codec
	nonNull uint8
}

func (u *unionOneAndNullCodec) Read(r *Buffer, p unsafe.Pointer) error {
	// index must be less than 1 byte in this case.
	// The result should be 2 or 4
	index, err := r.ReadByte()
	if err != nil {
		return fmt.Errorf("failed reading union selector. %w", err)
	}
	index /= 2
	if (index)&0xFE != 0 {
		return fmt.Errorf("union selector %d out of range (2 types)", index)
	}

	if index == u.nonNull {
		return u.codec.Read(r, p)
	}
	return nil
}

func (u *unionOneAndNullCodec) Skip(r *Buffer) error {
	// index must be less than 1 byte in this case
	index, err := r.ReadByte()
	if err != nil {
		return fmt.Errorf("failed reading union selector. %w", err)
	}
	index /= 2
	if (index)&0xFE != 0 {
		return fmt.Errorf("union selector %d out of range (2 types)", index)
	}

	if index == u.nonNull {
		return u.codec.Skip(r)
	}
	return nil
}

func (u *unionOneAndNullCodec) New(r *Buffer) unsafe.Pointer {
	return nil
}

func (u *unionOneAndNullCodec) Schema() Schema {
	return Schema{
		Type: "union",
		Union: []Schema{
			{Type: "null"},
			u.codec.Schema(),
		},
	}
}

func (u *unionOneAndNullCodec) Write(w *Writer, p unsafe.Pointer) error {
	// TODO: Need a way to determine if p is null
	return fmt.Errorf("union codec not implemented!")
}

type unionNullString struct {
	codec   StringCodec
	nonNull byte
}

func (u *unionNullString) Read(r *Buffer, p unsafe.Pointer) error {
	// index must be less than 1 byte in this case
	index, err := r.ReadByte()
	if err != nil {
		return fmt.Errorf("failed reading union selector. %w", err)
	}
	index /= 2
	if (index)&0xFE != 0 {
		return fmt.Errorf("union selector %d out of range (2 types)", index)
	}

	if index == u.nonNull {
		return u.codec.Read(r, p)
	}
	return nil
}

func (u *unionNullString) Skip(r *Buffer) error {
	// index must be less than 1 byte in this case
	index, err := r.ReadByte()
	if err != nil {
		return fmt.Errorf("failed reading union selector. %w", err)
	}
	index /= 2
	if (index)&0xFE != 0 {
		return fmt.Errorf("union selector %d out of range (2 types)", index)
	}

	if index == u.nonNull {
		return u.codec.Skip(r)
	}
	return nil
}

func (u *unionNullString) New(r *Buffer) unsafe.Pointer {
	return nil
}

func (u *unionNullString) Schema() Schema {
	return Schema{
		Type: "union",
		Union: []Schema{
			{Type: "null"},
			{Type: "string"},
		},
	}
}

func (u *unionNullString) Write(w *Writer, p unsafe.Pointer) error {
	s := *(*string)(p)
	if s == "" {
		w.Varint(0)
		return nil
	}

	w.Varint(1)
	u.codec.Write(w, p)

	return nil
}
