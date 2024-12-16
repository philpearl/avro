package avro

import (
	"fmt"
	"io"
	"unsafe"

	jsoniter "github.com/json-iterator/go"
)

type Encoder[T any] struct {
	schema Schema
	codec  Codec
	fw     *FileWriter
	w      io.Writer

	approxBlockSize int
	wb              *Writer
	count           int
}

// NewEncoder returns a new Encoder.
func NewEncoderFor[T any](w io.Writer, compression Compression, approxBlockSize int) (*Encoder[T], error) {
	var t T

	s, err := SchemaForType(t)
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
		wb:              NewWriter(make([]byte, 0, approxBlockSize)),
	}, nil
}

func (e *Encoder[T]) Encode(v T) error {
	if err := e.codec.Write(e.wb, unsafe.Pointer(&v)); err != nil {
		return fmt.Errorf("writing value: %w", err)
	}
	e.count++

	if e.wb.Len() > e.approxBlockSize {
		if err := e.Flush(); err != nil {
			return fmt.Errorf("flushing: %w", err)
		}
	}

	return nil
}

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
