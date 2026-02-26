package dque

// Codec handles serialization and deserialization of queue items to and from bytes.
// The bytes produced by Encode must be fully self-contained: Decode receives only
// those bytes plus a builder func and must reconstruct the original value exactly.
//
// Encode uses an append-style signature so callers can provide a pre-allocated
// buffer via dst (e.g. from a sync.Pool) and avoid a heap allocation per call.
// Passing nil or an empty dst is always safe; the codec allocates internally.
type Codec interface {
	// Encode appends the encoded form of v to dst and returns the extended slice.
	// On error it returns dst unchanged.
	Encode(dst []byte, v interface{}) ([]byte, error)

	// Decode decodes data into a new object constructed by builder.
	// builder must return a non-nil pointer of the correct concrete type.
	Decode(data []byte, builder func() interface{}) (interface{}, error)
}
