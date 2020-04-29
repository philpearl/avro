package avro

import "unsafe"

type nullCodec struct{}

func (nullCodec) Read(r Reader, p unsafe.Pointer) error {
	// TODO: could consider nil-ing the pointer
	return nil
}

func (nullCodec) Skip(r Reader) error {
	return nil
}

func (nullCodec) New() unsafe.Pointer {
	return nil
}
