// Package null contains avro decoders for the types in github.com/unravelin/null.
// Call RegisterCodecs to make these codecs available to avro
package null

import "github.com/philpearl/avro/avronull"

// RegisterCodecs registers the codecs from this package and makes them
// available to avro.
//
//go:fix inline
func RegisterCodecs() {
	avronull.RegisterCodecs()
}
