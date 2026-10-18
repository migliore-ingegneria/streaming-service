package compress_test

import (
	"bytes"
	"crypto/rand"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/streaming-service/pkg/compress"
)

func TestCompress_EmptyPayload(t *testing.T) {
	c := compress.NewSnappyCompressor()

	_, err := c.Compress(nil)
	assert.ErrorIs(t, err, compress.ErrEmptyPayload)

	_, err = c.Compress([]byte{})
	assert.ErrorIs(t, err, compress.ErrEmptyPayload)

	_, err = c.Decompress(nil)
	assert.ErrorIs(t, err, compress.ErrEmptyPayload)

	_, err = c.Decompress([]byte{})
	assert.ErrorIs(t, err, compress.ErrEmptyPayload)
}

func TestCompress_SmallPayloadBelowThreshold(t *testing.T) {
	c := compress.NewSnappyCompressor(compress.WithMinSizeThreshold(256))

	smallData := []byte("hello world small payload under 256 bytes")
	compressed, err := c.Compress(smallData)
	require.NoError(t, err)

	// Verify header is MagicUncompressed
	require.True(t, len(compressed) >= compress.HeaderLength)
	assert.Equal(t, compress.MagicUncompressed, compressed[:compress.HeaderLength])
	assert.False(t, compress.IsCompressed(compressed))

	// Decompress and verify round-trip
	decompressed, err := c.Decompress(compressed)
	require.NoError(t, err)
	assert.Equal(t, smallData, decompressed)
}

func TestCompress_LargePayloadAboveThreshold(t *testing.T) {
	c := compress.NewSnappyCompressor(compress.WithMinSizeThreshold(256))

	// Create a compressible 4KB repetitive string
	rawText := strings.Repeat("{\"event_type\": \"user_click\", \"user_id\": \"12345\", \"timestamp\": 1672531199}\n", 50)
	rawData := []byte(rawText)
	require.True(t, len(rawData) > 256)

	compressed, err := c.Compress(rawData)
	require.NoError(t, err)

	// Verify header is MagicCompressed
	require.True(t, len(compressed) >= compress.HeaderLength)
	assert.Equal(t, compress.MagicCompressed, compressed[:compress.HeaderLength])
	assert.True(t, compress.IsCompressed(compressed))

	// Confirm actual compression space savings
	assert.True(t, len(compressed) < len(rawData))

	// Decompress and verify integrity
	decompressed, err := c.Decompress(compressed)
	require.NoError(t, err)
	assert.Equal(t, rawData, decompressed)
}

func TestCompress_IncompressibleData(t *testing.T) {
	c := compress.NewSnappyCompressor(
		compress.WithMinSizeThreshold(256),
		compress.WithCompressionRatioThreshold(0.95),
	)

	// High-entropy random bytes cannot be compressed effectively
	randomData := make([]byte, 512)
	_, err := rand.Read(randomData)
	require.NoError(t, err)

	compressed, err := c.Compress(randomData)
	require.NoError(t, err)

	// Since random data fails the 0.95 ratio threshold, it should be marked as uncompressed
	assert.Equal(t, compress.MagicUncompressed, compressed[:compress.HeaderLength])
	assert.False(t, compress.IsCompressed(compressed))

	decompressed, err := c.Decompress(compressed)
	require.NoError(t, err)
	assert.Equal(t, randomData, decompressed)
}

func TestDecompress_LegacyRawFallback(t *testing.T) {
	t.Run("fallback allowed", func(t *testing.T) {
		c := compress.NewSnappyCompressor(compress.WithLegacyRawFallback(true))

		rawEvent := []byte("plain JSON event without any magic header bytes")
		decompressed, err := c.Decompress(rawEvent)
		require.NoError(t, err)
		assert.Equal(t, rawEvent, decompressed)

		// Short raw event (< 4 bytes)
		shortRaw := []byte("abc")
		decompressedShort, err := c.Decompress(shortRaw)
		require.NoError(t, err)
		assert.Equal(t, shortRaw, decompressedShort)
	})

	t.Run("fallback disabled", func(t *testing.T) {
		c := compress.NewSnappyCompressor(compress.WithLegacyRawFallback(false))

		rawEvent := []byte("plain JSON event without any magic header bytes")
		_, err := c.Decompress(rawEvent)
		assert.ErrorIs(t, err, compress.ErrInvalidMagicHeader)

		shortRaw := []byte("abc")
		_, err = c.Decompress(shortRaw)
		assert.ErrorIs(t, err, compress.ErrPayloadTooShort)
	})
}

func TestDecompress_CorruptCompressedPayload(t *testing.T) {
	c := compress.NewSnappyCompressor()

	corrupt := append(bytes.Clone(compress.MagicCompressed), []byte("invalid corrupted snappy stream")...)
	_, err := c.Decompress(corrupt)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "snappy decompression failed")
}

func TestCustomOptions(t *testing.T) {
	c := compress.NewSnappyCompressor(
		compress.WithMinSizeThreshold(10),
		compress.WithCompressionRatioThreshold(0.8),
		compress.WithLegacyRawFallback(false),
	)

	payload := []byte("1234567890 repeating repeating repeating")
	compressed, err := c.Compress(payload)
	require.NoError(t, err)

	decompressed, err := c.Decompress(compressed)
	require.NoError(t, err)
	assert.Equal(t, payload, decompressed)
}
