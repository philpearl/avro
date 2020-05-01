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

func (u *unionCodec) New() unsafe.Pointer {
	return nil
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

func (u *unionOneAndNullCodec) New() unsafe.Pointer {
	return nil
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

func (u *unionNullString) New() unsafe.Pointer {
	return nil
}
