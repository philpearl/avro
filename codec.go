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

// Codec defines a decoder for a type. It may eventually define an encoder too
type Codec interface {
	Read(r Reader, p unsafe.Pointer) error
	Skip(r Reader) error
	New() unsafe.Pointer
}

type avroBinaryTypes int

const (
	avroTypeNull avroBinaryTypes = iota
	avroTypeBool
	avroTypeInt
	avroTypeLong
	avroTypeFloat
	avroTypeBytes
	avroTypeString
	avroTypeRecord
	avroTypeEnum
	avroTypeArray
	avroTypeMap
	avroTypeUnion
	avroTypeFixed
)
