package avro

import (
	"fmt"
	"reflect"
	"unsafe"
)

type arrayCodec struct {
	itemCodec Codec
	itemType  reflect.Type
}

func (rc *arrayCodec) Read(r Reader, p unsafe.Pointer) error {
	if p == nil {
		return fmt.Errorf("nil pointer reading an array")
	}
	sh := (*sliceHeader)(p)

	// Blocks can be repeated
	for {
		var count int64
		if err := readInt64(r, unsafe.Pointer(&count)); err != nil {
			return fmt.Errorf("failed to read count for array. %w", err)
		}
		if count == 0 {
			break
		}
		if count < 0 {
			// negative length means there's a block size, which is only really
			// useful for skipping.
			count = -count
			var bs int64
			if err := readInt64(r, unsafe.Pointer(&bs)); err != nil {
				return fmt.Errorf("failed to read block size for array. %w", err)
			}
		}

		// If our array is nil or undersized then we can fix it up here.
		*sh = rc.resizeSlice(*sh, int(count))

		itemSize := rc.itemType.Size()
		for ; count > 0; count-- {
			cursor := unsafe.Pointer(uintptr(sh.Data) + uintptr(sh.Len)*itemSize)
			if err := rc.itemCodec.Read(r, cursor); err != nil {
				return fmt.Errorf("failed to decode array entry. %w", err)
			}
			sh.Len++
		}
	}

	return nil
}

func (rc *arrayCodec) Skip(r Reader) error {
	for {
		var count int64
		if err := readInt64(r, unsafe.Pointer(&count)); err != nil {
			return fmt.Errorf("failed to read count for array. %w", err)
		}
		if count == 0 {
			break
		}
		if count < 0 {
			// negative count means there's a block size we can use to skip the
			// rest of this block
			count = -count
			var bs int64
			if err := readInt64(r, unsafe.Pointer(&bs)); err != nil {
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

func (rc *arrayCodec) New() unsafe.Pointer {
	return unsafe.Pointer(&sliceHeader{})
}

// resizeSlice increases the length of the slice by len entries
func (rc *arrayCodec) resizeSlice(in sliceHeader, len int) sliceHeader {
	if in.Len+len <= in.Cap {
		return in
	}
	// Will assume for now that blocks are sensible sizes
	out := sliceHeader{
		Cap: in.Len + len,
		Len: in.Len,
	}
	elemType := unpackEFace(rc.itemType).data
	out.Data = unsafe_NewArray(elemType, out.Cap)

	if in.Data != nil {
		typedslicecopy(elemType, out, in)
	}
	return out
}

//go:linkname unsafe_NewArray reflect.unsafe_NewArray
func unsafe_NewArray(rtype unsafe.Pointer, length int) unsafe.Pointer

// typedslicecopy copies a slice of elemType values from src to dst,
// returning the number of elements copied.
//go:linkname typedslicecopy reflect.typedslicecopy
//go:noescape
func typedslicecopy(elemType unsafe.Pointer, dst, src sliceHeader) int
