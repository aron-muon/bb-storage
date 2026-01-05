package grpcclients

import (
	"context"
	"io"
	"runtime"
	"sync"

	"github.com/klauspost/compress/zstd"
	"golang.org/x/sync/semaphore"
)

// EncoderWrapper wraps a *zstd.Encoder for safe use in a sync.Pool.
// A finalizer ensures Close() is called even if the wrapper is not returned to the pool,
// preventing goroutine and memory leaks as documented in:
// https://github.com/klauspost/compress/issues/264
type EncoderWrapper struct {
	*zstd.Encoder
}

// DecoderWrapper wraps a *zstd.Decoder for safe use in a sync.Pool.
// A finalizer ensures Close() is called even if the wrapper is not returned to the pool.
type DecoderWrapper struct {
	*zstd.Decoder
}

// BoundedZstdPool provides pooled ZSTD encoders/decoders with concurrency limits.
// This prevents OOM conditions under high load by:
// 1. Reusing encoders/decoders via sync.Pool (avoiding allocation per request)
// 2. Limiting concurrent operations via semaphores (capping peak memory)
// 3. Using finalizers to clean up leaked encoders/decoders
type BoundedZstdPool struct {
	encoderPool sync.Pool
	decoderPool sync.Pool
	encoderSem  *semaphore.Weighted
	decoderSem  *semaphore.Weighted

	encoderOptions []zstd.EOption
	decoderOptions []zstd.DOption
}

// DefaultEncoderOptions returns encoder options optimized for streaming with bounded memory.
func DefaultEncoderOptions() []zstd.EOption {
	return []zstd.EOption{
		zstd.WithEncoderConcurrency(1), // Single-threaded per encoder, no goroutine spawning
		zstd.WithEncoderLevel(zstd.SpeedDefault),
		zstd.WithWindowSize(4 << 20), // 4MB window
	}
}

// DefaultDecoderOptions returns decoder options optimized for streaming with bounded memory.
func DefaultDecoderOptions() []zstd.DOption {
	return []zstd.DOption{
		zstd.WithDecoderConcurrency(1),     // Single-threaded per decoder
		zstd.WithDecoderMaxWindow(8 << 20), // 8MB max window
	}
}

// NewBoundedZstdPool creates a pool with memory-bounded concurrency.
//
// Parameters:
//   - maxEncoders: Maximum concurrent encoding operations (each uses ~4MB with default options)
//   - maxDecoders: Maximum concurrent decoding operations (each uses ~8MB with default options)
//   - encoderOptions: Options passed to zstd.NewWriter (use DefaultEncoderOptions() if nil)
//   - decoderOptions: Options passed to zstd.NewReader (use DefaultDecoderOptions() if nil)
//
// Memory budget estimation:
//
//	peakMemory ≈ (maxEncoders * 4MB) + (maxDecoders * 8MB)
//
// With defaults of 16 encoders and 32 decoders: ~320MB peak
func NewBoundedZstdPool(
	maxEncoders int64,
	maxDecoders int64,
	encoderOptions []zstd.EOption,
	decoderOptions []zstd.DOption,
) *BoundedZstdPool {
	if encoderOptions == nil {
		encoderOptions = DefaultEncoderOptions()
	}
	if decoderOptions == nil {
		decoderOptions = DefaultDecoderOptions()
	}

	p := &BoundedZstdPool{
		encoderSem:     semaphore.NewWeighted(maxEncoders),
		decoderSem:     semaphore.NewWeighted(maxDecoders),
		encoderOptions: encoderOptions,
		decoderOptions: decoderOptions,
	}

	// Factory for new encoders - called when pool is empty
	p.encoderPool.New = func() interface{} {
		enc, err := zstd.NewWriter(nil, p.encoderOptions...)
		if err != nil {
			// This should only fail with invalid options, which is a programming error
			panic("failed to create ZSTD encoder: " + err.Error())
		}
		ew := &EncoderWrapper{Encoder: enc}

		// CRITICAL: Finalizer prevents goroutine/memory leaks if caller
		// forgets to return encoder to pool. The zstd.Encoder spawns
		// internal goroutines that are only cleaned up by Close().
		runtime.SetFinalizer(ew, func(ew *EncoderWrapper) {
			ew.Encoder.Close()
		})

		return ew
	}

	// Factory for new decoders - called when pool is empty
	p.decoderPool.New = func() interface{} {
		dec, err := zstd.NewReader(nil, p.decoderOptions...)
		if err != nil {
			panic("failed to create ZSTD decoder: " + err.Error())
		}
		dw := &DecoderWrapper{Decoder: dec}

		// CRITICAL: Finalizer prevents goroutine/memory leaks
		runtime.SetFinalizer(dw, func(dw *DecoderWrapper) {
			dw.Decoder.Close()
		})

		return dw
	}

	return p
}

// AcquireEncoder gets an encoder from the pool, blocking if at capacity.
// Returns error if context is cancelled while waiting for capacity.
//
// The caller MUST call ReleaseEncoder when done, typically via defer:
//
//	enc, err := pool.AcquireEncoder(ctx, writer)
//	if err != nil {
//	    return err
//	}
//	defer pool.ReleaseEncoder(enc)
func (p *BoundedZstdPool) AcquireEncoder(ctx context.Context, w io.Writer) (*EncoderWrapper, error) {
	// Block until we have capacity (respects context cancellation)
	if err := p.encoderSem.Acquire(ctx, 1); err != nil {
		return nil, err
	}

	ew := p.encoderPool.Get().(*EncoderWrapper)
	ew.Reset(w)
	return ew, nil
}

// ReleaseEncoder returns an encoder to the pool.
// This MUST be called after AcquireEncoder, even if encoding failed.
func (p *BoundedZstdPool) ReleaseEncoder(ew *EncoderWrapper) {
	if ew == nil {
		return
	}
	// Reset to nil to release reference to writer, allowing GC
	ew.Reset(nil)
	p.encoderPool.Put(ew)
	p.encoderSem.Release(1)
}

// AcquireDecoder gets a decoder from the pool, blocking if at capacity.
// Returns error if context is cancelled while waiting for capacity.
//
// The caller MUST call ReleaseDecoder when done, typically via defer:
//
//	dec, err := pool.AcquireDecoder(ctx, reader)
//	if err != nil {
//	    return err
//	}
//	defer pool.ReleaseDecoder(dec)
func (p *BoundedZstdPool) AcquireDecoder(ctx context.Context, r io.Reader) (*DecoderWrapper, error) {
	if err := p.decoderSem.Acquire(ctx, 1); err != nil {
		return nil, err
	}

	dw := p.decoderPool.Get().(*DecoderWrapper)
	if err := dw.Reset(r); err != nil {
		// Failed to reset - release semaphore but don't return broken decoder to pool
		p.decoderSem.Release(1)
		return nil, err
	}
	return dw, nil
}

// ReleaseDecoder returns a decoder to the pool.
// This MUST be called after AcquireDecoder, even if decoding failed.
func (p *BoundedZstdPool) ReleaseDecoder(dw *DecoderWrapper) {
	if dw == nil {
		return
	}
	// Reset to nil to release reference to reader, allowing GC
	_ = dw.Reset(nil)
	p.decoderPool.Put(dw)
	p.decoderSem.Release(1)
}

// PooledReadCloser wraps a DecoderWrapper to auto-return to pool on Close().
// This is useful when the decoder is used via io.ReadCloser interface,
// ensuring proper cleanup even when passed to code that only knows about ReadCloser.
type PooledReadCloser struct {
	pool    *BoundedZstdPool
	decoder *DecoderWrapper
	reader  io.Reader
}

// Read implements io.Reader.
func (r *PooledReadCloser) Read(p []byte) (n int, err error) {
	return r.reader.Read(p)
}

// Close returns the decoder to the pool and implements io.Closer.
func (r *PooledReadCloser) Close() error {
	if r.decoder != nil {
		r.pool.ReleaseDecoder(r.decoder)
		r.decoder = nil
	}
	return nil
}

// AcquireDecoderAsReadCloser gets a decoder wrapped as io.ReadCloser.
// Calling Close() on the returned ReadCloser automatically returns the decoder to the pool.
// This is the preferred method when the decoder will be passed to code expecting io.ReadCloser.
func (p *BoundedZstdPool) AcquireDecoderAsReadCloser(ctx context.Context, r io.Reader) (io.ReadCloser, error) {
	dw, err := p.AcquireDecoder(ctx, r)
	if err != nil {
		return nil, err
	}
	return &PooledReadCloser{
		pool:    p,
		decoder: dw,
		reader:  dw.IOReadCloser(),
	}, nil
}

// TryAcquireEncoder attempts to get an encoder without blocking.
// Returns (encoder, true) if successful, (nil, false) if pool is at capacity.
// Useful for implementing backpressure or fallback to non-compressed writes.
func (p *BoundedZstdPool) TryAcquireEncoder(w io.Writer) (*EncoderWrapper, bool) {
	if !p.encoderSem.TryAcquire(1) {
		return nil, false
	}

	ew := p.encoderPool.Get().(*EncoderWrapper)
	ew.Reset(w)
	return ew, true
}

// TryAcquireDecoder attempts to get a decoder without blocking.
// Returns (decoder, true) if successful, (nil, false) if pool is at capacity.
func (p *BoundedZstdPool) TryAcquireDecoder(r io.Reader) (*DecoderWrapper, bool) {
	if !p.decoderSem.TryAcquire(1) {
		return nil, false
	}

	dw := p.decoderPool.Get().(*DecoderWrapper)
	if err := dw.Reset(r); err != nil {
		p.decoderSem.Release(1)
		return nil, false
	}
	return dw, true
}
