package avro

import (
	"encoding/binary"
	"errors"
	"io"
	"reflect"
	"sync"
	"unsafe"
)

// WriteBuf is a simple, append only, replacement for bytes.Buffer. It is used
// by AVRO encoders. It is not safe for concurrent use.
type WriteBuf struct {
	buf []byte
}

// NewWriteBuf returns a new WriteBuf.
func NewWriteBuf(buf []byte) *WriteBuf {
	return &WriteBuf{buf: buf}
}

func (w *WriteBuf) Varint(v int64) {
	w.buf = binary.AppendVarint(w.buf, v)
}

func (w *WriteBuf) Byte(val byte) {
	w.buf = append(w.buf, val)
}

func (w *WriteBuf) Write(val []byte) {
	w.buf = append(w.buf, val...)
}

func (w *WriteBuf) Bytes() []byte {
	return w.buf
}

func (w *WriteBuf) Reset() {
	w.buf = w.buf[:0]
}

func (w *WriteBuf) Len() int {
	return len(w.buf)
}

// ReadBuf is a very simple replacement for bytes.Reader that avoids data copies
type ReadBuf struct {
	i   int
	buf []byte
	rb  *ResourceBank
}

// NewReadBuf returns a new Buffer.
func NewReadBuf(data []byte) *ReadBuf {
	return &ReadBuf{buf: data, rb: newResourceBank()}
}

// Reset allows you to reuse a buffer with a new set of data
func (d *ReadBuf) Reset(data []byte) {
	d.i = 0
	d.buf = data
	if d.rb == nil {
		d.rb = newResourceBank()
	}
}

// ExtractResourceBank extracts the current ResourceBank from the buffer, and replaces
// it with a fresh one.
func (d *ReadBuf) ExtractResourceBank() *ResourceBank {
	rb := d.rb
	d.rb = newResourceBank()
	return rb
}

// Next returns the next l bytes from the buffer. It does so without copying, so
// if you hold onto the data you risk holding onto a lot of data. If l exceeds
// the remaining space Next returns io.EOF
func (d *ReadBuf) Next(l int) ([]byte, error) {
	if l+d.i > len(d.buf) {
		return nil, io.EOF
	}
	d.i += l
	return d.buf[d.i-l : d.i], nil
}

// NextAsString returns the next l bytes from the buffer as a string. The string
// data is held in a StringBank and will be valid only until someone calls Close
// on that bank. If l exceeds the remaining space NextAsString returns io.EOF
func (d *ReadBuf) NextAsString(l int) (string, error) {
	if l+d.i > len(d.buf) {
		return "", io.EOF
	}
	d.i += l
	return d.rb.ToString(d.buf[d.i-l : d.i]), nil
}

func (d *ReadBuf) NextAsBytes(l int) ([]byte, error) {
	if l+d.i > len(d.buf) {
		return nil, io.EOF
	}
	d.i += l
	return d.rb.ToBytes(d.buf[d.i-l : d.i]), nil
}

// Alloc allocates a pointer to the type rtyp. The data is allocated in a ResourceBank
func (d *ReadBuf) Alloc(rtyp reflect.Type) unsafe.Pointer {
	return d.rb.Alloc(rtyp)
}

func (d *ReadBuf) AllocArray(rtyp reflect.Type, len int) unsafe.Pointer {
	return d.rb.AllocArray(rtyp, len)
}

// ReadByte returns the next byte from the buffer. If no bytes are left it
// returns io.EOF
func (d *ReadBuf) ReadByte() (byte, error) {
	if d.i >= len(d.buf) {
		return 0, io.EOF
	}
	d.i++
	return d.buf[d.i-1], nil
}

// Len returns the length of unread data in the buffer
func (d *ReadBuf) Len() int {
	return len(d.buf) - d.i
}

// Varint reads a varint from the buffer
func (d *ReadBuf) Varint() (int64, error) {
	v, err := d.uvarint() // ok to continue in presence of error
	return int64(v>>1) ^ -int64(v&1), err
}

var errOverflow = errors.New("varint overflows a 64-bit integer")

func (d *ReadBuf) uvarint() (uint64, error) {
	var x uint64
	var s uint
	for i := 0; ; i++ {
		b, err := d.ReadByte()
		if err != nil {
			return x, err
		}
		if b < 0x80 {
			if i > 9 || i == 9 && b > 1 {
				return x, errOverflow
			}
			return x | uint64(b)<<s, nil
		}
		x |= uint64(b&0x7f) << s
		s += 7
	}
}

var resourceBankPool = sync.Pool{
	New: func() any {
		return &ResourceBank{}
	},
}

type resourceType struct {
	// Type information for this type.
	ptyp unsafe.Pointer
	// Where the bits of memory for this type is
	array unsafe.Pointer
	// How much memory we currently have
	cap int
	// How much of the memory is currently in-use
	len int
	// The size of this type
	size int
}

// ResourceBank is used to allocate memory used to create structs to decode AVRO
// into. The primary reason for having it is to allow the user to flag the
// memory can be re-used, so reducing the strain on the GC
//
// We allocate using the required type of thing so the GC can still inspect
// within the memory.
type ResourceBank struct {
	types []resourceType

	// We also have a special store for string data
	sData []byte
}

func newResourceBank() *ResourceBank {
	return resourceBankPool.Get().(*ResourceBank)
}

// Alloc reserves some memory in the ResourceBank. Note that this memory may be
// re-used after Close is called.
func (rb *ResourceBank) Alloc(rtyp reflect.Type) unsafe.Pointer {
	rt := rb.findTyp(rtyp)

	rt.ensureSpace(1)

	start := rt.len
	rt.len++
	ptr := unsafe.Add(rt.array, start*rt.size)
	// Because we're re-using we need to clear the memory ourselves. Should perhaps
	// do this on Close
	typedmemclr(rt.ptyp, ptr)
	return ptr
}

// AllocArray reserves some memory in the ResourceBank for an array of the given
// type and length. Note that this memory may be re-used after Close is called.
func (rb *ResourceBank) AllocArray(rtyp reflect.Type, len int) unsafe.Pointer {
	rt := rb.findTyp(rtyp)
	rt.ensureSpace(len)
	start := rt.len
	rt.len += len
	ptr := unsafe.Add(rt.array, start*rt.size)
	typedarrayclear(rt.ptyp, ptr, len)

	return ptr
}

func (rt *resourceType) ensureSpace(len int) {
	if rt.len+len <= rt.cap {
		return
	}
	newCap := max(rt.cap*2, len, 16)

	rt.array = unsafe_NewArray(rt.ptyp, newCap)
	rt.cap = newCap
	rt.len = 0
}

func (rb *ResourceBank) findTyp(rtyp reflect.Type) *resourceType {
	ptyp := unpackEFace(rtyp).data
	// We don't expect many types, so we just do a linear search
	for i := range rb.types {
		rt := &rb.types[i]
		if rt.ptyp == ptyp {
			return rt
		}
	}

	rb.types = append(rb.types, resourceType{
		ptyp: ptyp,
		size: int(rtyp.Size()),
	})

	return &rb.types[len(rb.types)-1]
}

// Close marks the resources in the ResourceBank as available for re-use
func (rb *ResourceBank) Close() {
	// We don't free the memory here. We keep our arrays at the maximum size we've
	// needed, but we set the length used to zero so we can re-use it all.
	for i := range rb.types {
		t := &rb.types[i]
		t.len = 0
	}

	// We also need to clear the string data
	rb.sData = rb.sData[:0]

	resourceBankPool.Put(rb)
}

// ToString saves string data in the bank and returns a string. The string is
// valid until someone calls Close
func (rb *ResourceBank) ToString(in []byte) string {
	start := len(rb.sData)
	// append will do some unnecessary copying. But we should get to the right
	// size and stop growing pretty quickly
	rb.sData = append(rb.sData, in...)
	out := rb.sData[start:]

	return unsafe.String(unsafe.SliceData(out), len(out))
}

// ToBytes saves byte data in the bank and returns a byte slice. The slice is
// valid until someone calls Close
func (rb *ResourceBank) ToBytes(in []byte) []byte {
	start := len(rb.sData)
	// append will do some unnecessary copying. But we should get to the right
	// size and stop growing pretty quickly
	rb.sData = append(rb.sData, in...)
	return rb.sData[start : start+len(in) : start+len(in)]
}
