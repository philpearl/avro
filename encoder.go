package avro

import (
	"fmt"
	"io"
	"reflect"
	"unsafe"

	jsoniter "github.com/json-iterator/go"
)

type Encoder[T any] struct {
	schema Schema
	codec  Codec
	fw     *FileWriter
	w      io.Writer

	approxBlockSize int
	wb              *WriteBuf
	count           int
}

// NewEncoder returns a new Encoder. Data will be written to w in Avro format,
// including a schema header. The data will be compressed using the specified
// compression algorithm. Data is written in blocks of at least approxBlockSize
// bytes. A block is written when it reaches that size, or when Flush is called.
func NewEncoderFor[T any](w io.Writer, compression Compression, approxBlockSize int) (*Encoder[T], error) {
	var t T

	typ := reflect.TypeFor[T]()
	if typ.Kind() != reflect.Struct {
		return nil, fmt.Errorf("only structs are supported, got %v", typ)
	}

	s, err := schemaForType(typ)
	if err != nil {
		return nil, fmt.Errorf("generating schema: %w", err)
	}

	c, err := s.Codec(t)
	if err != nil {
		return nil, fmt.Errorf("generating codec: %w", err)
	}

	schemaBytes, err := jsoniter.Marshal(s)
	if err != nil {
		return nil, fmt.Errorf("marshaling schema: %w", err)
	}

	fw, err := NewFileWriter(schemaBytes, compression)
	if err != nil {
		return nil, fmt.Errorf("creating file writer: %w", err)
	}

	if err := fw.WriteHeader(w); err != nil {
		return nil, fmt.Errorf("writing file header: %w", err)
	}

	return &Encoder[T]{
		schema: s,
		codec:  c,
		fw:     fw,
		w:      w,

		approxBlockSize: approxBlockSize,
		wb:              NewWriteBuf(make([]byte, 0, approxBlockSize)),
	}, nil
}

// Encode writes a new row to the Avro file.
func (e *Encoder[T]) Encode(v *T) error {
	e.codec.Write(e.wb, unsafe.Pointer(v))
	e.count++

	if e.wb.Len() >= e.approxBlockSize {
		if err := e.Flush(); err != nil {
			return fmt.Errorf("flushing: %w", err)
		}
	}

	return nil
}

// Flush writes any buffered data to the underlying writer. It completes the
// current block. It must be called before closing the underlying file.
func (e *Encoder[T]) Flush() error {
	if e.count > 0 {
		if err := e.fw.WriteBlock(e.w, e.count, e.wb.Bytes()); err != nil {
			return fmt.Errorf("writing block: %w", err)
		}
		e.count = 0
		e.wb.Reset()
	}
	return nil
}
