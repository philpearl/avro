package avro

import (
	"bufio"
	"io"
	"io/ioutil"
)

func skip(r Reader, l int64) error {
	if r, ok := r.(io.Seeker); ok {
		_, err := r.Seek(l, io.SeekCurrent)
		return err
	}

	if r, ok := r.(*bufio.Reader); ok {
		_, err := r.Discard(int(l))
		return err
	}

	_, err := io.CopyN(ioutil.Discard, r, l)
	return err
}
