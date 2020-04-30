package avro

import (
	"fmt"
	"unsafe"
)

type unionCodec struct {
	codecs []Codec
}

func (u unionCodec) Read(r Reader, p unsafe.Pointer) error {
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

func (u unionCodec) Skip(r Reader) error {
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

func (u unionCodec) New() unsafe.Pointer {
	return nil
}
