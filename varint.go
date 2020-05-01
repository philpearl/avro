package avro

import "errors"

var errOverflow = errors.New("varint overflows a 64-bit integer")

// We copy these from encoding/binary so we can use our own interface type and
// avoid interface conversion overheads!

// readUvarint reads an encoded unsigned integer from r and returns it as a uint64.
func readUvarint(r Reader) (uint64, error) {
	var x uint64
	var s uint
	for i := 0; ; i++ {
		b, err := r.ReadByte()
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

// readVarint reads an encoded signed integer from r and returns it as an int64.
func readVarint(r Reader) (int64, error) {
	v, err := readUvarint(r) // ok to continue in presence of error
	return int64(v>>1) ^ -int64(v&1), err
}
