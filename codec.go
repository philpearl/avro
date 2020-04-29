// Package avro is an AVRO decoder aimed principly at decoding AVRO output from
// Google's Big Query. It decodes directly into Go structs, and uses json tags
// as naming hints.
//
// The primary interface to the package is ReadFile. This reads an AVRO file,
// combining the schema in the file with type information from the struct passed
// via the out parameter to decode the records. It then passes an instance of a
// struct of type out to the callback cb for each record in the file.
//
// You can implement custom decoders for your own types and register them via
// the Register function. github.com/phil/avro/null is an example of custom
// decoders for the types defined in github.com/unravelin/null
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
