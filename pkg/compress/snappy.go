package compress

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/golang/snappy"
)

var (
	// MagicCompressed is the 4-byte header prefixed to Snappy compressed payloads.
	MagicCompressed = []byte{0x53, 0x4E, 0x50, 0x01} // "SNP\x01"

	// MagicUncompressed is the 4-byte header prefixed to uncompressed payloads (skipped due to size/ratio).
	MagicUncompressed = []byte{0x53, 0x4E, 0x50, 0x00} // "SNP\x00"

	// ErrEmptyPayload is returned when input data is empty.
	ErrEmptyPayload = errors.New("compress: payload cannot be empty")

	// ErrPayloadTooShort is returned when decompressing data shorter than the magic header.
	ErrPayloadTooShort = errors.New("compress: payload too short for magic header")

	// ErrInvalidMagicHeader is returned when the payload does not contain a recognized magic header.
	ErrInvalidMagicHeader = errors.New("compress: unrecognized payload magic header")
)

const (
	// DefaultMinSizeThreshold is the default minimum byte size before compression is applied.
	DefaultMinSizeThreshold = 256

	// DefaultMaxCompressionRatio is the default ratio threshold (compressed / original).
	// If compression does not achieve at least this ratio (e.g., 1.0 = smaller), raw storage is preferred.
	DefaultMaxCompressionRatio = 1.0

	// HeaderLength is the size in bytes of the magic prefix marker.
	HeaderLength = 4
)

// Compressor defines the interface for compressing and decompressing stream event payloads.
type Compressor interface {
	Compress(data []byte) ([]byte, error)
	Decompress(data []byte) ([]byte, error)
}

// Option represents a functional configuration option for SnappyCompressor.
type Option func(*SnappyCompressor)

// WithMinSizeThreshold sets the minimum data size (in bytes) required to attempt compression.
func WithMinSizeThreshold(threshold int) Option {
	return func(s *SnappyCompressor) {
		if threshold >= 0 {
			s.minSizeThreshold = threshold
		}
	}
}

// WithCompressionRatioThreshold sets the maximum allowable compression ratio (compressed / original).
// If compressedSize / originalSize >= ratioThreshold, the payload remains uncompressed.
func WithCompressionRatioThreshold(ratio float64) Option {
	return func(s *SnappyCompressor) {
		if ratio > 0.0 && ratio <= 1.0 {
			s.ratioThreshold = ratio
		}
	}
}

// WithLegacyRawFallback enables or disables treating payloads without a recognized magic header as raw data.
func WithLegacyRawFallback(allow bool) Option {
	return func(s *SnappyCompressor) {
		s.allowRawFallback = allow
	}
}

// SnappyCompressor implements Compressor using Google's Snappy algorithm with magic byte markers.
type SnappyCompressor struct {
	minSizeThreshold int
	ratioThreshold   float64
	allowRawFallback bool
}

// NewSnappyCompressor creates a new SnappyCompressor configured with the provided options.
func NewSnappyCompressor(opts ...Option) *SnappyCompressor {
	sc := &SnappyCompressor{
		minSizeThreshold: DefaultMinSizeThreshold,
		ratioThreshold:   DefaultMaxCompressionRatio,
		allowRawFallback: true,
	}
	for _, opt := range opts {
		opt(sc)
	}
	return sc
}

// Compress encodes the provided data.
// If the payload is smaller than minSizeThreshold or if compression yields no space savings,
// it prepends MagicUncompressed to avoid negative compression overhead.
// Otherwise, it compresses via Snappy and prepends MagicCompressed.
func (s *SnappyCompressor) Compress(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return nil, ErrEmptyPayload
	}

	// Avoid compressing small payloads below threshold
	if len(data) < s.minSizeThreshold {
		out := make([]byte, HeaderLength+len(data))
		copy(out[:HeaderLength], MagicUncompressed)
		copy(out[HeaderLength:], data)
		return out, nil
	}

	compressed := snappy.Encode(nil, data)

	// Check if compression ratio threshold was satisfied
	ratio := float64(len(compressed)) / float64(len(data))
	if ratio >= s.ratioThreshold {
		out := make([]byte, HeaderLength+len(data))
		copy(out[:HeaderLength], MagicUncompressed)
		copy(out[HeaderLength:], data)
		return out, nil
	}

	out := make([]byte, HeaderLength+len(compressed))
	copy(out[:HeaderLength], MagicCompressed)
	copy(out[HeaderLength:], compressed)
	return out, nil
}

// Decompress decodes the provided payload based on its magic header.
// It transparently handles compressed payloads, marked uncompressed payloads, and optional raw fallback.
func (s *SnappyCompressor) Decompress(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return nil, ErrEmptyPayload
	}

	if len(data) < HeaderLength {
		if s.allowRawFallback {
			result := make([]byte, len(data))
			copy(result, data)
			return result, nil
		}
		return nil, ErrPayloadTooShort
	}

	header := data[:HeaderLength]
	payload := data[HeaderLength:]

	if bytes.Equal(header, MagicCompressed) {
		decompressed, err := snappy.Decode(nil, payload)
		if err != nil {
			return nil, fmt.Errorf("compress: snappy decompression failed: %w", err)
		}
		return decompressed, nil
	}

	if bytes.Equal(header, MagicUncompressed) {
		result := make([]byte, len(payload))
		copy(result, payload)
		return result, nil
	}

	// Payload does not have a recognized magic header
	if s.allowRawFallback {
		result := make([]byte, len(data))
		copy(result, data)
		return result, nil
	}

	return nil, ErrInvalidMagicHeader
}

// IsCompressed inspects whether data begins with the Snappy compressed magic header marker.
func IsCompressed(data []byte) bool {
	return len(data) >= HeaderLength && bytes.Equal(data[:HeaderLength], MagicCompressed)
}
