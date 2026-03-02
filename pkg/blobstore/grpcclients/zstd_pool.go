package grpcclients

import (
	"context"
	"io"
	"sync"

	"github.com/klauspost/compress/zstd"
	"golang.org/x/sync/semaphore"
)

// EncoderWrapper wraps a *zstd.Encoder for safe use in a sync.Pool.
type EncoderWrapper struct {
	*zstd.Encoder
}

// DecoderWrapper wraps a *zstd.Decoder for safe use in a sync.Pool.
type DecoderWrapper struct {
	*zstd.Decoder
}

// BoundedZstdPool provides pooled ZSTD encoders/decoders with concurrency limits.
// This prevents OOM conditions under high load by:
// 1. Reusing encoders/decoders via sync.Pool (avoiding allocation per request)
// 2. Limiting concurrent operations via semaphores (capping peak memory)
type BoundedZstdPool struct {
	encoderPool sync.Pool
	decoderPool sync.Pool
	encoderSem  *semaphore.Weighted
	decoderSem  *semaphore.Weighted
}

// NewBoundedZstdPool creates a pool with memory-bounded concurrency.
//
// Parameters:
//   - maxEncoders: Maximum concurrent encoding operations
//   - maxDecoders: Maximum concurrent decoding operations
//   - encoderOptions: Options passed to zstd.NewWriter
//   - decoderOptions: Options passed to zstd.NewReader
func NewBoundedZstdPool(
	maxEncoders int64,
	maxDecoders int64,
	encoderOptions []zstd.EOption,
	decoderOptions []zstd.DOption,
) *BoundedZstdPool {
	p := &BoundedZstdPool{
		encoderSem: semaphore.NewWeighted(maxEncoders),
		decoderSem: semaphore.NewWeighted(maxDecoders),
	}

	p.encoderPool.New = func() interface{} {
		enc, err := zstd.NewWriter(nil, encoderOptions...)
		if err != nil {
			panic("failed to create ZSTD encoder: " + err.Error())
		}
		return &EncoderWrapper{Encoder: enc}
	}

	p.decoderPool.New = func() interface{} {
		dec, err := zstd.NewReader(nil, decoderOptions...)
		if err != nil {
			panic("failed to create ZSTD decoder: " + err.Error())
		}
		return &DecoderWrapper{Decoder: dec}
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
	_ = dw.Reset(nil)
	p.decoderPool.Put(dw)
	p.decoderSem.Release(1)
}
