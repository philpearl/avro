package avro

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestSchemaEncoding(t *testing.T) {
	data, err := avroFileSchema.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	var out Schema
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(avroFileSchema, out); diff != "" {
		t.Fatalf("results differ. %s", diff)
	}

	out2, err := SchemaFromString(string(data))
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(avroFileSchema, out2); diff != "" {
		t.Fatalf("results differ. %s", diff)
	}
}
