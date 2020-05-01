package avro

import (
	"errors"
	"io"
)

// Buffer is a very simple replacement for bytes.Reader that avoids data copies
type Buffer struct {
	i   int
	buf []byte
}

// NewBuffer returns a new Buffer.
func NewBuffer(data []byte) *Buffer {
	return &Buffer{buf: data}
}

// Reset allows you to reuse a buffer with a new set of data
func (d *Buffer) Reset(data []byte) {
	d.i = 0
	d.buf = data
}

// Next returns the next l bytes from the buffer. It does so without copying, so
// if you hold onto the data you risk holding onto a lot of data. If l exceeds
// the remaining space Next returns io.EOF
func (d *Buffer) Next(l int) ([]byte, error) {
	if l+d.i > len(d.buf) {
		return nil, io.EOF
	}
	d.i += l
	return d.buf[d.i-l : d.i], nil
}

// ReadByte returns the next byte from the buffer. If no bytes are left it
// returns io.EOF
func (d *Buffer) ReadByte() (byte, error) {
	if d.i >= len(d.buf) {
		return 0, io.EOF
	}
	d.i++
	return d.buf[d.i-1], nil
}

// Len returns the length of unread data in the buffer
func (d *Buffer) Len() int {
	return len(d.buf) - d.i
}

// Varint reads a varint from the buffer
func (d *Buffer) Varint() (int64, error) {
	v, err := d.uvarint() // ok to continue in presence of error
	return int64(v>>1) ^ -int64(v&1), err
}

var errOverflow = errors.New("varint overflows a 64-bit integer")

func (d *Buffer) uvarint() (uint64, error) {
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
