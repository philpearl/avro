// Package time contains avro decoders for time.Time.
package time

import (
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
	switch schema.Type {
	case "string":
		return StringCodec{}, nil
	case "long":
		var c LongCodec
		c.mult = 1
		if schema.Object != nil {
			switch schema.Object.LogicalType {
			case "timestamp-micros":
				c.mult = 1000
			case "timestamp-millis":
				c.mult = 1e6
			}
		}
		return c, nil
	case "int":
		if schema.Object != nil {
			switch schema.Object.LogicalType {
			// BigQuery claims to use this for it's DATE type but doesn't. We've
			// seen DATEs as strings with no logical type. Format is 2006-01-02
			case "date":
				return DateCodec{}, nil
			}
		}
	}

	return nil, fmt.Errorf("time.Time codec works only with string and long schema, not %q", schema.Type)
}

// DateCodec is a decoder from an AVRO date logical type, which is a number of
// days since 1 Jan 1970
type DateCodec struct{ avro.Int32Codec }

func (c DateCodec) Read(r *avro.Buffer, p unsafe.Pointer) error {
	var l int64
	if err := c.Int32Codec.Read(r, unsafe.Pointer(&l)); err != nil {
		return err
	}

	*(*time.Time)(p) = time.Date(1970, 1, int(l), 0, 0, 0, 0, time.UTC)
	return nil
}

// New create a pointer to a new time.Time
func (c DateCodec) New(r *avro.Buffer) unsafe.Pointer {
	return r.Alloc(timeType)
}

// StringCodec is a decoder from an AVRO string with RFC3339 encoding to a time.Time
type StringCodec struct{ avro.StringCodec }

func (c StringCodec) Read(r *avro.Buffer, p unsafe.Pointer) error {
	// Can we do better than using the underlying string codec?
	l, err := r.Varint()
	if err != nil {
		return fmt.Errorf("failed to read length of time: %w", err)
	}

	if l == 0 {
		// pragmatically better to just leave the time alone if there's no
		// content to parse.
		return nil
	}

	data, err := r.Next(int(l))
	if err != nil {
		return fmt.Errorf("failed to read %d bytes of time string body: %w", l, err)
	}

	s := *(*string)(unsafe.Pointer(&data))
	t, err := parseTime(s)
	if err != nil {
		return fmt.Errorf("failed to parse time: %w", err)
	}
	*(*time.Time)(p) = t
	return nil
}

var timeType = reflect.TypeOf(time.Time{})

// New create a pointer to a new time.Time
func (c StringCodec) New(r *avro.Buffer) unsafe.Pointer {
	return r.Alloc(timeType)
}

// LongCodec is a decoder from an AVRO long where the time is encoded as
// nanoseconds since the UNIX epoch
type LongCodec struct {
	avro.Int64Codec
	mult int64
}

func (c LongCodec) Read(r *avro.Buffer, p unsafe.Pointer) error {
	var l int64
	if err := c.Int64Codec.Read(r, unsafe.Pointer(&l)); err != nil {
		return err
	}

	*(*time.Time)(p) = time.Unix(0, l*c.mult).UTC()
	return nil
}

// New create a pointer to a new time.Time
func (c LongCodec) New(r *avro.Buffer) unsafe.Pointer {
	return r.Alloc(timeType)
}
