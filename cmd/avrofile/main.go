package main

import (
	"bufio"
	"fmt"
	"os"

	"github.com/philpearl/avro"
)

func main() {
	fmt.Println(run())
}

func run() error {
	f, err := os.Open("./profiles")
	if err != nil {
		return err
	}
	defer f.Close()

	type hat struct {
		Cheese string `json:"cheese"`
	}

	var h hat

	return avro.ReadFile(bufio.NewReader(f), &h)

}
