package avro

import (
	"fmt"
	"unsafe"
)

type unionCodec struct {
	codecs []Codec
}

func (u *unionCodec) Read(r Reader, p unsafe.Pointer) error {
	index, err := readVarint(r)
	if err != nil {
		return fmt.Errorf("failed reading union selector. %w", err)
	}
	if index < 0 || index >= int64(len(u.codecs)) {
		return fmt.Errorf("union selector %d out of range (%d types)", index, len(u.codecs))
	}

	c := u.codecs[index]
	return c.Read(r, p)
}

func (u *unionCodec) Skip(r Reader) error {
	index, err := readVarint(r)
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
	nonNull int64
}

func (u *unionOneAndNullCodec) Read(r Reader, p unsafe.Pointer) error {
	index, err := readVarint(r)
	if err != nil {
		return fmt.Errorf("failed reading union selector. %w", err)
	}
	if index < 0 || index > 1 {
		return fmt.Errorf("union selector %d out of range (2 types)", index)
	}

	if index == u.nonNull {
		return u.codec.Read(r, p)
	}
	return nil
}

func (u *unionOneAndNullCodec) Skip(r Reader) error {
	index, err := readVarint(r)
	if err != nil {
		return fmt.Errorf("failed reading union selector. %w", err)
	}
	if index < 0 || index > 1 {
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
	nonNull int64
}

func (u *unionNullString) Read(r Reader, p unsafe.Pointer) error {
	index, err := readVarint(r)
	if err != nil {
		return fmt.Errorf("failed reading union selector. %w", err)
	}
	if index < 0 || index > 1 {
		return fmt.Errorf("union selector %d out of range (2 types)", index)
	}

	if index == u.nonNull {
		return u.codec.Read(r, p)
	}
	return nil
}

func (u *unionNullString) Skip(r Reader) error {
	index, err := readVarint(r)
	if err != nil {
		return fmt.Errorf("failed reading union selector. %w", err)
	}
	if index < 0 || index > 1 {
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
