package main

import (
	"context"
	"fmt"
	"os"

	bqstorage "cloud.google.com/go/bigquery/storage/apiv1"
	"github.com/googleapis/gax-go"
	jsoniter "github.com/json-iterator/go"
	"github.com/philpearl/avro"
	bqstoragepb "google.golang.org/genproto/googleapis/cloud/bigquery/storage/v1"
	"google.golang.org/grpc"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

var (
	projectID = "ravelin-1043"
	dataSet   = "auto_temp_eu"
	tableName = "a20ed210_59ee_4888_46f2_3d021fe808b5"
)

func run() error {

	ctx := context.Background()
	client, err := bqstorage.NewBigQueryReadClient(ctx)
	if err != nil {
		return fmt.Errorf("couldn't get BQ client. %w", err)
	}
	defer client.Close()

	table := fmt.Sprintf("projects/%s/datasets/%s/tables/%s",
		projectID,
		dataSet,
		tableName,
	)

	req := bqstoragepb.CreateReadSessionRequest{
		Parent: "projects/" + projectID,
		ReadSession: &bqstoragepb.ReadSession{
			Table:      table,
			DataFormat: bqstoragepb.DataFormat_AVRO,
		},
		MaxStreamCount: 1,
	}

	session, err := client.CreateReadSession(ctx, &req, rpcOpts)
	if err != nil {
		return fmt.Errorf("could not create BQ read session. %w", err)
	}

	schema := session.GetAvroSchema().GetSchema()

	var s avro.Schema
	if err := jsoniter.UnmarshalFromString(schema, &s); err != nil {
		return fmt.Errorf("failed to unmarshal schema. %w", err)
	}

	out, err := jsoniter.MarshalToString(&s)
	if err != nil {
		return fmt.Errorf("failed to marshal schema")
	}
	fmt.Println(out)

	return nil
}

func printSchema(s avro.Schema) {
	fmt.Println(s.Type)
	if s.Object != nil {
		fmt.Println(s.Object.Name, s.Object.Namespace)
		switch s.Type {
		case "record":
			for _, field := range s.Object.Fields {
				fmt.Println(field.Name)
				printSchema(field.Type)
			}
		case "fixed":
			fmt.Println(s.Object.Size)
		case "array":
			printSchema(s.Object.Items)
		default:
			fmt.Println("OOPS")
		}
	}
	for _, s := range s.Union {
		printSchema(s)
	}
}

var rpcOpts = gax.WithGRPCOptions(
	grpc.MaxCallRecvMsgSize(1024 * 1024 * 11),
)
