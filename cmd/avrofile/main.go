package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"reflect"
	"runtime/pprof"
	"time"
	"unsafe"

	"github.com/philpearl/avro"
	"github.com/philpearl/avro/null"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
}

func run() error {
	var memprofile string
	var cpuprofile string
	flag.StringVar(&memprofile, "memprofile", "", "turn on memory profiling and write to this file")
	flag.StringVar(&cpuprofile, "cpuprofile", "", "turn on cpu profiling and write to this file")
	flag.Parse()

	if memprofile != "" {
		f, err := os.Create(memprofile)
		if err != nil {
			return fmt.Errorf("failed to create profiling file: %w", err)
		}
		defer f.Close()
		defer pprof.WriteHeapProfile(f)
	}
	if cpuprofile != "" {
		f, err := os.Create(cpuprofile)
		if err != nil {
			return fmt.Errorf("failed to create profiling file: %w", err)
		}
		defer f.Close()
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

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
	return avro.ReadFile(bufio.NewReaderSize(f, 1024*1024), struct{}{}, func(val unsafe.Pointer) error {
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
