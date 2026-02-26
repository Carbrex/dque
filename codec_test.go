// codec_test.go — white-box unit tests for Codec implementations in the dque package.
package dque

import (
	"strings"
	"testing"
)

// testPayload is the struct used in codec roundtrip tests.
type testPayload struct {
	Name   string
	Value  int
	Tags   []string
	Counts map[string]int
}

func testPayloadBuilder() interface{} {
	return &testPayload{}
}

// TestGobCodec_EncodeDecodeRoundtrip verifies that GobCodec can encode and decode
// the same value without data loss, including nested maps and slices.
func TestGobCodec_EncodeDecodeRoundtrip(t *testing.T) {
	codec := GobCodec{}

	cases := []struct {
		name string
		val  *testPayload
	}{
		{
			name: "basic fields",
			val:  &testPayload{Name: "hello", Value: 42},
		},
		{
			name: "with slice",
			val:  &testPayload{Name: "tagged", Tags: []string{"a", "b", "c"}},
		},
		{
			name: "with map",
			val:  &testPayload{Name: "counted", Counts: map[string]int{"x": 1, "y": 2}},
		},
		{
			name: "zero value",
			val:  &testPayload{},
		},
		{
			name: "large string",
			val:  &testPayload{Name: strings.Repeat("x", 4096)},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			// Encode: provide a 4-byte placeholder just like segment.add() does.
			dst := []byte{0, 0, 0, 0}
			encoded, err := codec.Encode(dst, tc.val)
			if err != nil {
				t.Fatalf("Encode failed: %v", err)
			}
			if len(encoded) <= 4 {
				t.Fatalf("Encode produced no payload bytes (len=%d)", len(encoded))
			}

			// The payload starts at offset 4 (after the placeholder).
			payload := encoded[4:]

			decoded, err := codec.Decode(payload, testPayloadBuilder)
			if err != nil {
				t.Fatalf("Decode failed: %v", err)
			}
			got, ok := decoded.(*testPayload)
			if !ok {
				t.Fatalf("decoded type is %T, want *testPayload", decoded)
			}

			if got.Name != tc.val.Name {
				t.Errorf("Name: got %q want %q", got.Name, tc.val.Name)
			}
			if got.Value != tc.val.Value {
				t.Errorf("Value: got %d want %d", got.Value, tc.val.Value)
			}
			if len(got.Tags) != len(tc.val.Tags) {
				t.Errorf("Tags len: got %d want %d", len(got.Tags), len(tc.val.Tags))
			}
			for i := range tc.val.Tags {
				if got.Tags[i] != tc.val.Tags[i] {
					t.Errorf("Tags[%d]: got %q want %q", i, got.Tags[i], tc.val.Tags[i])
				}
			}
			for k, v := range tc.val.Counts {
				if got.Counts[k] != v {
					t.Errorf("Counts[%q]: got %d want %d", k, got.Counts[k], v)
				}
			}
		})
	}
}

// TestGobCodec_EncodeAppendsToExistingDst verifies the append-style contract:
// bytes before the encode point must be preserved.
func TestGobCodec_EncodeAppendsToExistingDst(t *testing.T) {
	codec := GobCodec{}
	prefix := []byte{0, 0, 0, 0} // 4-byte placeholder, as used by segment.add

	val := &testPayload{Name: "append-test", Value: 7}
	result, err := codec.Encode(prefix, val)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	// First 4 bytes must still be the placeholder.
	for i := 0; i < 4; i++ {
		if result[i] != 0 {
			t.Errorf("byte[%d] was overwritten: got %d want 0", i, result[i])
		}
	}
	// Payload must follow.
	if len(result) <= 4 {
		t.Fatal("no payload bytes after prefix")
	}
}

// TestGobCodec_DecodeReturnsErrorOnGarbage ensures corrupt data causes a decode error.
func TestGobCodec_DecodeReturnsErrorOnGarbage(t *testing.T) {
	codec := GobCodec{}
	_, err := codec.Decode([]byte{0xDE, 0xAD, 0xBE, 0xEF}, testPayloadBuilder)
	if err == nil {
		t.Fatal("expected decode error on garbage data, got nil")
	}
}

// TestGobCodec_EncodeReturnsDstOnError verifies that on an encode error,
// dst is returned unchanged (no partial data appended).
func TestGobCodec_EncodeReturnsDstOnError(t *testing.T) {
	codec := GobCodec{}
	// gob can't encode a channel type.
	ch := make(chan int)
	dst := []byte{1, 2, 3}
	result, err := codec.Encode(dst, ch)
	if err == nil {
		t.Fatal("expected encode error for channel type, got nil")
	}
	// result must be exactly the original dst slice
	if len(result) != len(dst) {
		t.Errorf("result len changed on error: got %d want %d", len(result), len(dst))
	}
}

// BenchmarkGobCodec_Encode measures the per-encode allocation and throughput.
func BenchmarkGobCodec_Encode(b *testing.B) {
	codec := GobCodec{}
	val := &testPayload{
		Name:   "benchmark-item",
		Value:  42,
		Tags:   []string{"tag1", "tag2"},
		Counts: map[string]int{"a": 1, "b": 2},
	}
	buf := make([]byte, 0, 1024)
	b.ResetTimer()
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		buf = buf[:0]
		buf = append(buf, 0, 0, 0, 0)
		result, err := codec.Encode(buf, val)
		if err != nil {
			b.Fatal(err)
		}
		_ = result
	}
}

// BenchmarkGobCodec_Decode measures the per-decode allocation and throughput.
func BenchmarkGobCodec_Decode(b *testing.B) {
	codec := GobCodec{}
	val := &testPayload{
		Name:   "benchmark-item",
		Value:  42,
		Tags:   []string{"tag1", "tag2"},
		Counts: map[string]int{"a": 1, "b": 2},
	}
	encoded, err := codec.Encode([]byte{0, 0, 0, 0}, val)
	if err != nil {
		b.Fatal(err)
	}
	payload := encoded[4:]

	b.ResetTimer()
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		_, err := codec.Decode(payload, testPayloadBuilder)
		if err != nil {
			b.Fatal(err)
		}
	}
}
