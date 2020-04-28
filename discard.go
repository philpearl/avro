package avro

import (
	"io"
	"io/ioutil"
)

func skip(r Reader, l int64) error {
	if r, ok := r.(io.Seeker); ok {
		_, err := r.Seek(l, io.SeekCurrent)
		return err
	}

	_, err := io.CopyN(ioutil.Discard, r, l)
	return err
}
