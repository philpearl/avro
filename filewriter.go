package avro

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
)

type Compression string

const (
	CompressionNull    Compression = "null"
	CompressionDeflate Compression = "deflate"
	CompressionSnappy  Compression = "snappy"
)

// FileWriter provides limited support for writing AVRO files. It allows you to
// write blocks of already encoded data. Actually encoding data as AVRO is supported
// by the Encoder type.
type FileWriter struct {
	sync [16]byte
	// It may make sense for the schema to be a Schema object. But we won't use
	// that until we have encoding support.
	schema      []byte
	compression Compression
	varintBuf   [binary.MaxVarintLen64]byte
	compressor  compressionCodec
}

// NewFileWriter creates a new FileWriter. The schema is the JSON encoded
// schema. The compression parameter indicates the compression codec to use.
func NewFileWriter(schema []byte, compression Compression) (*FileWriter, error) {
	// Generate a random sync value
	f := &FileWriter{
		schema:      schema,
		compression: compression,
	}
	_, err := rand.Read(f.sync[:])
	if err != nil {
		return nil, fmt.Errorf("creating sync value: %w", err)
	}

	switch compression {
	case CompressionNull:
		f.compressor = nullCompression{}
	case CompressionDeflate:
		f.compressor = &deflate{}
	case CompressionSnappy:
		f.compressor = &snappyCodec{}
	default:
		return nil, fmt.Errorf("compression codec %s not supported", compression)
	}

	return f, nil
}

// WriteHeader writes the AVRO file header to the writer.
func (f *FileWriter) WriteHeader(w io.Writer) error {
	buf := make([]byte, 0, 1024)
	buf = f.AppendHeader(buf)
	_, err := w.Write(buf)
	return err
}

// AppendHeader appends the AVRO file header to the provided buffer.
func (f *FileWriter) AppendHeader(buf []byte) []byte {
	// Write the magic bytes
	buf = append(buf, FileMagic[:]...)

	// Count of how many metadata blocks there are.
	buf = binary.AppendVarint(buf, 2)

	// Write the metadata block. There will be an entry for the compression type
	// and an entry for the schema. Each entry is a string key followed by a
	// string value. Strings are written as a varint encoded length and then the
	// bytes of the string.
	buf = appendString(buf, "avro.schema")
	buf = appendString(buf, f.schema)
	buf = appendString(buf, "avro.codec")
	buf = appendString(buf, f.compression)

	// Append a zero count to indicate no more header blocks.
	buf = binary.AppendVarint(buf, 0)

	// Write the sync bytes. This is just the 16 bytes of the sync field.
	buf = append(buf, f.sync[:]...)
	return buf
}

type appendable interface {
	~string | ~[]byte
}

func appendString[T appendable](buf []byte, s T) []byte {
	buf = binary.AppendVarint(buf, int64(len(s)))
	buf = append(buf, s...)
	return buf
}

func (f *FileWriter) writeVarInt(w io.Writer, v int) error {
	n := binary.PutVarint(f.varintBuf[:], int64(v))
	_, err := w.Write(f.varintBuf[:n])
	return err
}

// WriteBlock writes a block of data to the writer. The block must be rowCount
// rows of AVRO encoded data.
func (f *FileWriter) WriteBlock(w io.Writer, rowCount int, block []byte) error {
	// Write the count of rows in the block
	if err := f.writeVarInt(w, rowCount); err != nil {
		return fmt.Errorf("writing row count: %w", err)
	}

	compressed, err := f.compressor.compress(block)
	if err != nil {
		return fmt.Errorf("compressing block: %w", err)
	}

	// Write the (compressed) block size
	if err := f.writeVarInt(w, len(compressed)); err != nil {
		return fmt.Errorf("writing block len: %w", err)
	}

	// Write the block data.
	if _, err := w.Write(compressed); err != nil {
		return fmt.Errorf("writing block: %w", err)
	}

	// Write the sync block
	if _, err := w.Write(f.sync[:]); err != nil {
		return fmt.Errorf("writing sync: %w", err)
	}
	return nil
}
