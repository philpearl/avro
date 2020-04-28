package avro

import (
	"fmt"
	"reflect"
	"unsafe"
)

// TODO: should we just do some specific map codecs?
type mapCodec struct {
	valueCodec Codec
	rtype      reflect.Type
}

func (m *mapCodec) Read(r Reader, p unsafe.Pointer) error {
	// p is a pointer to a map pointer
	if *(*unsafe.Pointer)(p) == nil {
		*(*unsafe.Pointer)(p) = m.New()
	}
	mp := *(*unsafe.Pointer)(p)

	// Blocks are repeated until there's a zero count block
	for {
		var count int64
		if err := readInt64(r, unsafe.Pointer(&count)); err != nil {
			return fmt.Errorf("failed to read count of map block. %w", err)
		}
		if count == 0 {
			break
		}

		if count < 0 {
			count = -count
			// Block size is more useful if we're skipping over the map
			var bs int64
			if err := readInt64(r, unsafe.Pointer(&bs)); err != nil {
				return fmt.Errorf("failed to read block size of map block. %w", err)
			}
		}

		var sc stringCodec
		for ; count > 0; count-- {
			var key string
			if err := sc.Read(r, unsafe.Pointer(&key)); err != nil {
				return fmt.Errorf("failed to read key for map. %w", err)
			}

			// TODO: can we just reuse one val?
			val := m.valueCodec.New()
			if err := m.valueCodec.Read(r, val); err != nil {
				return fmt.Errorf("failed to read value for map key %s. %w", key, err)
			}
			// Put the thing in the thing
			mapassign(unpackEFace(m.rtype).data, mp, unsafe.Pointer(&key), val)
		}
	}

	return nil
}

func (m *mapCodec) Skip(r Reader) error {
	for {
		var count int64
		if err := readInt64(r, unsafe.Pointer(&count)); err != nil {
			return fmt.Errorf("failed to read count of map block. %w", err)
		}

		if count == 0 {
			break
		}

		if count < 0 {
			var bs int64
			if err := readInt64(r, unsafe.Pointer(&bs)); err != nil {
				return fmt.Errorf("failed to read block size of map block. %w", err)
			}
			if err := skip(r, bs); err != nil {
				return fmt.Errorf("failed skipping block of map. %w", err)
			}
			continue
		}

		var sc stringCodec
		for ; count > 0; count-- {
			if err := sc.Skip(r); err != nil {
				return fmt.Errorf("failed to skip key for map. %w", err)
			}

			if err := m.valueCodec.Skip(r); err != nil {
				return fmt.Errorf("failed to skip value for map. %w", err)
			}
		}
	}

	return nil
}

func (m *mapCodec) New() unsafe.Pointer {
	return unsafe.Pointer(reflect.MakeMap(m.rtype).Pointer())
}

//go:linkname mapassign reflect.mapassign
//go:noescape
func mapassign(typ unsafe.Pointer, hmap unsafe.Pointer, key, val unsafe.Pointer)
