package avro_test

import (
	"bufio"
	"os"
	"path/filepath"
	"testing"
	"unsafe"

	"github.com/google/go-cmp/cmp"
	jsoniter "github.com/json-iterator/go"
	"github.com/philpearl/avro"
)

func TestWritingFile(t *testing.T) {
	type record struct {
		Name string `json:"name"`
		Hat  string `json:"hat"`
	}

	schema := avro.Schema{
		Type: "record",
		Object: &avro.SchemaObject{
			Name: "Record",
			Fields: []avro.SchemaRecordField{
				{
					Name: "name",
					Type: avro.Schema{
						Type: "string",
					},
				},
				{
					Name: "hat",
					Type: avro.Schema{
						Type: "string",
					},
				},
			},
		},
	}

	schemaJSON, err := jsoniter.Marshal(schema)
	if err != nil {
		t.Fatal(err)
	}

	data := []byte{
		6, 'j', 'i', 'm',
		6, 'c', 'a', 't',

		6, 's', 'i', 'm',
		6, 'h', 'a', 't',
	}

	for _, compression := range []avro.Compression{avro.CompressionDeflate, avro.CompressionSnappy} {
		t.Run(string(compression), func(t *testing.T) {
			dir := t.TempDir()
			filename := filepath.Join(dir, "test.avro")

			f, err := os.Create(filename)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()

			fw, err := avro.NewFileWriter(schemaJSON, compression)
			if err != nil {
				t.Fatal(err)
			}

			if err := fw.WriteHeader(f); err != nil {
				t.Fatal(err)
			}

			if err := fw.WriteBlock(f, 2, data); err != nil {
				t.Fatal(err)
			}

			if err := f.Close(); err != nil {
				t.Fatal(err)
			}

			r, err := os.Open(filename)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()

			var records []record

			if err := avro.ReadFile(bufio.NewReader(r), record{}, func(val unsafe.Pointer, rb *avro.ResourceBank) error {
				r := (*record)(val)
				t.Logf("read record: %+v", r)
				records = append(records, *r)
				return nil
			}); err != nil {
				t.Fatal(err)
			}

			if diff := cmp.Diff([]record{
				{Name: "jim", Hat: "cat"},
				{Name: "sim", Hat: "hat"},
			}, records); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}
