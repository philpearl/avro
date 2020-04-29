package main

import (
	"bufio"
	"fmt"
	"os"
	"reflect"
	"time"
	"unsafe"

	"github.com/philpearl/avro"
	"github.com/philpearl/avro/null"
	"github.com/unravelin/core/feature-extraction-service/profile"
)

func main() {
	fmt.Println(run())
}

func run() error {
	f, err := os.Open("./profiles")
	if err != nil {
		return err
	}
	defer f.Close()

	null.RegisterCodecs()
	avro.Register(reflect.TypeOf(time.Time{}), buildTimeCodec)

	var count int
	start := time.Now()
	defer func() {
		println(count, time.Since(start).String())
	}()
	return avro.ReadFile(bufio.NewReader(f), profile.Profile{}, func(val unsafe.Pointer) error {
		count++
		return nil
	})
}

func buildTimeCodec(schema avro.Schema, typ reflect.Type) (avro.Codec, error) {
	if schema.Type != "string" {
		return nil, fmt.Errorf("time.Time codec works only with string schema, not %q", schema.Type)
	}
	return TimeCodec{}, nil
}

type TimeCodec struct{ avro.StringCodec }

func (c TimeCodec) Read(r avro.Reader, p unsafe.Pointer) error {
	var s string
	if err := c.StringCodec.Read(r, unsafe.Pointer(&s)); err != nil {
		return err
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return fmt.Errorf("failed to parse time: %w", err)
	}
	*(*time.Time)(p) = t
	return nil
}

func (c TimeCodec) New() unsafe.Pointer {
	return unsafe.Pointer(&time.Time{})
}
