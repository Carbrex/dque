package dque

import (
	"bytes"
	"encoding/gob"
)

// GobCodec is the default Codec using encoding/gob.
// It is wire-compatible with the original joncrlsn/dque segment file format,
// so existing queue data on disk can be read without migration.
type GobCodec struct{}

// Encode gob-encodes v and appends the result to dst.
func (GobCodec) Encode(dst []byte, v interface{}) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(v); err != nil {
		return dst, err
	}
	return append(dst, buf.Bytes()...), nil
}

// Decode gob-decodes data into a new object returned by builder.
func (GobCodec) Decode(data []byte, builder func() interface{}) (interface{}, error) {
	obj := builder()
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(obj); err != nil {
		return nil, err
	}
	return obj, nil
}
