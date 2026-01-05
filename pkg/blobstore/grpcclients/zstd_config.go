package grpcclients

import (
	"github.com/klauspost/compress/zstd"
)

const (
	// DefaultMaxConcurrentEncoders is the default number of concurrent encoding operations.
	// Each encoder uses approximately 4MB of memory with default window size.
	DefaultMaxConcurrentEncoders = 16

	// DefaultMaxConcurrentDecoders is the default number of concurrent decoding operations.
	// Each decoder uses approximately 8MB of memory with default max window.
	DefaultMaxConcurrentDecoders = 32

	// DefaultCompressionThreshold is the minimum blob size in bytes to apply compression.
	// Matches Bazel's default value for --experimental_remote_cache_compression_threshold.
	DefaultCompressionThreshold = 100

	// DefaultEncoderWindowSize is the default encoder window size (4MB).
	// Larger windows improve compression ratio but use more memory.
	DefaultEncoderWindowSize = 4 << 20

	// DefaultDecoderMaxWindow is the default maximum decoder window size (8MB).
	// Must be at least as large as the encoder window size used by remote peers.
	DefaultDecoderMaxWindow = 8 << 20

	// DefaultEncoderMemoryBytes is the estimated memory per encoder.
	DefaultEncoderMemoryBytes = 4 * 1024 * 1024

	// DefaultDecoderMemoryBytes is the estimated memory per decoder.
	DefaultDecoderMemoryBytes = 8 * 1024 * 1024
)

// ZstdPoolConfig holds validated configuration for a BoundedZstdPool.
type ZstdPoolConfig struct {
	// MaxConcurrentEncoders limits concurrent compression operations.
	MaxConcurrentEncoders int64

	// MaxConcurrentDecoders limits concurrent decompression operations.
	MaxConcurrentDecoders int64

	// CompressionThreshold is the minimum blob size to compress (in bytes).
	// Blobs smaller than this are sent uncompressed.
	CompressionThreshold int64

	// EncoderLevel is the compression level for encoders.
	EncoderLevel zstd.EncoderLevel

	// EncoderWindowSize is the window size for encoders in bytes.
	EncoderWindowSize int

	// DecoderMaxWindow is the maximum window size for decoders in bytes.
	DecoderMaxWindow int
}

// DefaultZstdPoolConfig returns a configuration with sensible defaults.
// Peak memory usage with defaults: ~320MB
//   - 16 encoders × 4MB = 64MB
//   - 32 decoders × 8MB = 256MB
func DefaultZstdPoolConfig() ZstdPoolConfig {
	return ZstdPoolConfig{
		MaxConcurrentEncoders: DefaultMaxConcurrentEncoders,
		MaxConcurrentDecoders: DefaultMaxConcurrentDecoders,
		CompressionThreshold:  DefaultCompressionThreshold,
		EncoderLevel:          zstd.SpeedDefault,
		EncoderWindowSize:     DefaultEncoderWindowSize,
		DecoderMaxWindow:      DefaultDecoderMaxWindow,
	}
}

// EncoderOptions returns zstd.EOption slice for creating encoders.
func (c ZstdPoolConfig) EncoderOptions() []zstd.EOption {
	return []zstd.EOption{
		zstd.WithEncoderConcurrency(1), // No internal goroutines
		zstd.WithEncoderLevel(c.EncoderLevel),
		zstd.WithWindowSize(c.EncoderWindowSize),
	}
}

// DecoderOptions returns zstd.DOption slice for creating decoders.
func (c ZstdPoolConfig) DecoderOptions() []zstd.DOption {
	return []zstd.DOption{
		zstd.WithDecoderConcurrency(1), // No internal goroutines
		zstd.WithDecoderMaxWindow(uint64(c.DecoderMaxWindow)),
	}
}

// EstimateMaxMemoryUsage returns the estimated peak memory usage in bytes.
func (c ZstdPoolConfig) EstimateMaxMemoryUsage() int64 {
	encoderMem := c.MaxConcurrentEncoders * int64(c.EncoderWindowSize)
	decoderMem := c.MaxConcurrentDecoders * int64(c.DecoderMaxWindow)
	return encoderMem + decoderMem
}

// NewPool creates a BoundedZstdPool from this configuration.
func (c ZstdPoolConfig) NewPool() *BoundedZstdPool {
	return NewBoundedZstdPool(
		c.MaxConcurrentEncoders,
		c.MaxConcurrentDecoders,
		c.EncoderOptions(),
		c.DecoderOptions(),
	)
}

// ConfigFromMemoryBudget calculates pool limits based on a memory budget.
// This is useful for auto-configuration based on available system memory.
//
// The function allocates 30% of the budget to encoders and 70% to decoders,
// since reads (cache hits) typically outnumber writes in cache scenarios.
//
// Example:
//
//	// For a 512MB memory budget
//	config := ConfigFromMemoryBudget(512 * 1024 * 1024)
//	pool := config.NewPool()
func ConfigFromMemoryBudget(memoryBudgetBytes int64) ZstdPoolConfig {
	config := DefaultZstdPoolConfig()

	// Allocate 30% to encoders, 70% to decoders
	// (reads typically dominate in cache scenarios)
	encoderBudget := memoryBudgetBytes * 30 / 100
	decoderBudget := memoryBudgetBytes * 70 / 100

	config.MaxConcurrentEncoders = max64(1, encoderBudget/DefaultEncoderMemoryBytes)
	config.MaxConcurrentDecoders = max64(1, decoderBudget/DefaultDecoderMemoryBytes)

	return config
}

// ParseEncoderLevel converts a string level name to zstd.EncoderLevel.
// Valid values: "fastest", "default", "better", "best"
// Returns zstd.SpeedDefault for unrecognized values.
func ParseEncoderLevel(level string) zstd.EncoderLevel {
	switch level {
	case "fastest":
		return zstd.SpeedFastest
	case "default", "":
		return zstd.SpeedDefault
	case "better":
		return zstd.SpeedBetterCompression
	case "best":
		return zstd.SpeedBestCompression
	default:
		return zstd.SpeedDefault
	}
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
