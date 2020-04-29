package avro

import (
	"io"
	"unsafe"
)

// Reader combines io.ByteReader and io.Reader. It's what we need to read
type Reader interface {
	io.ByteReader
	io.Reader
}

// Codec defines a decoder for a type. It may eventually define an encoder too.
// You can write custom Codecs for types. See Register and CodecBuildFunc
type Codec interface {
	// Read reads the wire format bytes for the current field from r and sets up
	// the value that p points to. The codec can assume that the memory for an
	// instance of the type for which the codec is registered is present behind
	// p
	Read(r Reader, p unsafe.Pointer) error
	// Skip advances the reader over the bytes for the current field.
	Skip(r Reader) error
	// New creates a pointer to the type for which the codec is registered. It is
	// used if the enclosing record has a field that is a pointer to this type
	New() unsafe.Pointer
}
