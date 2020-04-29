package avro

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	jsoniter "github.com/json-iterator/go"
)

func TestSchemaEncoding(t *testing.T) {
	data, err := jsoniter.Marshal(avroFileSchema)
	if err != nil {
		t.Fatal(err)
	}
	var out Schema
	if err := jsoniter.Unmarshal(data, &out); err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(avroFileSchema, out); diff != "" {
		t.Fatalf("results differ. %s", diff)
	}
}
