package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"runtime/pprof"
	"time"
	"unsafe"

	"github.com/philpearl/avro"
	"github.com/philpearl/avro/null"
	avrotime "github.com/philpearl/avro/time"
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
	avrotime.RegisterCodecs()

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
