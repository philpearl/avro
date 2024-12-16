package avro

import (
	"fmt"
	"reflect"
	"unsafe"
)

// MapCodec is a decoder for map types. The key must always be string
type MapCodec struct {
	valueCodec Codec
	rtype      reflect.Type
	omitEmpty  bool
}

func (m *MapCodec) Read(r *ReadBuf, p unsafe.Pointer) error {
	// p is a pointer to a map pointer
	if *(*unsafe.Pointer)(p) == nil {
		*(*unsafe.Pointer)(p) = m.New(r)
	}
	mp := *(*unsafe.Pointer)(p)

	// Blocks are repeated until there's a zero count block
	for {
		count, err := r.Varint()
		if err != nil {
			return fmt.Errorf("failed to read count of map block. %w", err)
		}
		if count == 0 {
			break
		}

		if count < 0 {
			count = -count
			// Block size is more useful if we're skipping over the map
			if _, err := r.Varint(); err != nil {
				return fmt.Errorf("failed to read block size of map block. %w", err)
			}
		}

		var sc StringCodec
		for ; count > 0; count-- {
			var key string
			if err := sc.Read(r, unsafe.Pointer(&key)); err != nil {
				return fmt.Errorf("failed to read key for map. %w", err)
			}

			// TODO: can we just reuse one val?
			val := m.valueCodec.New(r)
			if err := m.valueCodec.Read(r, val); err != nil {
				return fmt.Errorf("failed to read value for map key %s. %w", key, err)
			}
			// Put the thing in the thing
			mapassign(unpackEFace(m.rtype).data, mp, unsafe.Pointer(&key), val)
		}
	}

	return nil
}

func (m *MapCodec) Skip(r *ReadBuf) error {
	for {
		count, err := r.Varint()
		if err != nil {
			return fmt.Errorf("failed to read count of map block. %w", err)
		}

		if count == 0 {
			break
		}

		if count < 0 {
			bs, err := r.Varint()
			if err != nil {
				return fmt.Errorf("failed to read block size of map block. %w", err)
			}
			if err := skip(r, bs); err != nil {
				return fmt.Errorf("failed skipping block of map. %w", err)
			}
			continue
		}

		var sc StringCodec
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

func (m *MapCodec) New(r *ReadBuf) unsafe.Pointer {
	return unsafe.Pointer(reflect.MakeMap(m.rtype).Pointer())
}

func (m *MapCodec) Schema() Schema {
	return Schema{
		Type: "map",
		Object: &SchemaObject{
			Values: m.valueCodec.Schema(),
		},
	}
}

func (m *MapCodec) Omit(p unsafe.Pointer) bool {
	return m.omitEmpty && maplen(p) == 0
}

func (m *MapCodec) Write(w *WriteBuf, p unsafe.Pointer) error {
	// p is a pointer to a map pointer, but maps are already pointery
	p = *(*unsafe.Pointer)(p)

	// Start with the count. Note the same ability to use a negative count to
	// record a block size exists here too.
	w.Varint(int64(maplen(p)))

	var iterM mapiter
	iter := (unsafe.Pointer)(&iterM)
	mapiterinit(unpackEFace(m.rtype).data, p, iter)

	var sc StringCodec

	for {
		k := mapiterkey(iter)
		if k == nil {
			break
		}
		v := mapiterelem(iter)

		if err := sc.Write(w, k); err != nil {
			return fmt.Errorf("writing key: %w", err)
		}

		if err := m.valueCodec.Write(w, v); err != nil {
			return fmt.Errorf("writing value: %w", err)
		}

		mapiternext(iter)
	}

	// like arrays, theoretically there can be multiple blocks so we need to write a zero count to say there's no more.
	w.Varint(0)
	return nil
}
