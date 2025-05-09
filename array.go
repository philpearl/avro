package avro

import (
	"fmt"
	"reflect"
	"unsafe"
)

type arrayCodec struct {
	itemCodec Codec
	itemType  reflect.Type
	omitEmpty bool
}

func (rc *arrayCodec) Read(r *ReadBuf, p unsafe.Pointer) error {
	sh := (*sliceHeader)(p)

	// Blocks can be repeated
	for {
		count, err := r.Varint()
		if err != nil {
			return fmt.Errorf("failed to read count for array. %w", err)
		}
		if count == 0 {
			break
		}
		if count < 0 {
			// negative length means there's a block size, which is only really
			// useful for skipping.
			count = -count
			if _, err := r.Varint(); err != nil {
				return fmt.Errorf("failed to read block size for array. %w", err)
			}
		}

		// If our array is nil or undersized then we can fix it up here.
		*sh = rc.resizeSlice(r, *sh, int(count))

		itemSize := rc.itemType.Size()
		for i := range count {
			cursor := unsafe.Add(sh.Data, uintptr(sh.Len)*itemSize)
			if err := rc.itemCodec.Read(r, cursor); err != nil {
				return fmt.Errorf("failed to decode array entry %d. %w", i, err)
			}
			sh.Len++
		}
	}

	return nil
}

func (rc *arrayCodec) Skip(r *ReadBuf) error {
	for {
		count, err := r.Varint()
		if err != nil {
			return fmt.Errorf("failed to read count for array. %w", err)
		}
		if count == 0 {
			break
		}
		if count < 0 {
			// negative count means there's a block size we can use to skip the
			// rest of this block
			bs, err := r.Varint()
			if err != nil {
				return fmt.Errorf("failed to read block size for array. %w", err)
			}
			if err := skip(r, bs); err != nil {
				return err
			}
			continue
		}

		for ; count > 0; count-- {
			if err := rc.itemCodec.Skip(r); err != nil {
				return fmt.Errorf("failed to skip array entry. %w", err)
			}
		}
	}

	return nil
}

var sliceType = reflect.TypeFor[sliceHeader]()

func (rc *arrayCodec) New(r *ReadBuf) unsafe.Pointer {
	return r.Alloc(sliceType)
}

// resizeSlice increases the length of the slice by len entries
func (rc *arrayCodec) resizeSlice(r *ReadBuf, in sliceHeader, len int) sliceHeader {
	if in.Len+len <= in.Cap {
		return in
	}
	// Will assume for now that blocks are sensible sizes
	out := sliceHeader{
		Cap: in.Len + len,
		Len: in.Len,
	}
	out.Data = r.AllocArray(rc.itemType, out.Cap)

	if in.Data != nil {
		elemType := unpackEFace(rc.itemType).data
		typedslicecopy(elemType, out, in)
	}
	return out
}

func (rc *arrayCodec) Omit(p unsafe.Pointer) bool {
	return rc.omitEmpty && len(*(*[]byte)(p)) == 0
}

func (rc *arrayCodec) Write(w *WriteBuf, p unsafe.Pointer) {
	sh := (*sliceHeader)(p)
	if sh.Len == 0 {
		w.Varint(0)
		return
	}

	// TODO: you can write negative counts, which are then followed by the size
	// of the block, then the data. That makes it easier to skip over data. TBD if we want to do that
	w.Varint(int64(sh.Len))
	for i := range sh.Len {
		cursor := unsafe.Add(sh.Data, uintptr(i)*rc.itemType.Size())
		rc.itemCodec.Write(w, cursor)
	}

	// Write a zero count to indicate the end of the array. This does appear to
	// be necessary as you can write multiple blocks.
	w.Varint(0)
}
