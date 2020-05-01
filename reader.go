package avro

import "io"

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
