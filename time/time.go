// Package time contains avro decoders for time.Time.
package time

import (
	"encoding/binary"
	"fmt"
	"reflect"
	"time"
	"unsafe"

	"github.com/philpearl/avro"
)

// RegisterCodecs makes the codecs in this package available to avro
func RegisterCodecs() {
	avro.Register(reflect.TypeOf(time.Time{}), buildTimeCodec)
}

func buildTimeCodec(schema avro.Schema, typ reflect.Type) (avro.Codec, error) {
	// If in future we want to decode an integer unix epoc time we can add a
	// switch here
	if schema.Type != "string" {
		return nil, fmt.Errorf("time.Time codec works only with string schema, not %q", schema.Type)
	}
	return StringCodec{}, nil
}

// StringCodec is a decoder from an AVRO string with RFC3339 encoding to a time.Time
type StringCodec struct{ avro.StringCodec }

func (c StringCodec) Read(r *avro.Buffer, p unsafe.Pointer) error {
	// Can we do better than using the underlying string codec?
	l, err := binary.ReadVarint(r)
	if err != nil {
		return fmt.Errorf("failed to read length of time: %w", err)
	}

	data, err := r.Next(int(l))
	if err != nil {
		return fmt.Errorf("failed to read %d bytes of time string body: %w", l, err)
	}

	s := *(*string)(unsafe.Pointer(&data))
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return fmt.Errorf("failed to parse time: %w", err)
	}
	*(*time.Time)(p) = t
	return nil
}

// New create a pointer to a new time.Time
func (c StringCodec) New() unsafe.Pointer {
	return unsafe.Pointer(&time.Time{})
}
