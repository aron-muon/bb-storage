package grpcclients_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/buildbarn/bb-storage/pkg/blobstore/grpcclients"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
)

func newTestPool(maxEncoders, maxDecoders int64) *grpcclients.BoundedZstdPool {
	return grpcclients.NewBoundedZstdPool(
		maxEncoders, maxDecoders,
		[]zstd.EOption{zstd.WithEncoderConcurrency(1)},
		[]zstd.DOption{zstd.WithDecoderConcurrency(1)},
	)
}

func TestBoundedZstdPool_EncoderAcquireRelease(t *testing.T) {
	pool := newTestPool(2, 2)

	var buf bytes.Buffer
	enc, err := pool.AcquireEncoder(context.Background(), &buf)
	require.NoError(t, err)
	require.NotNil(t, enc)

	_, err = enc.Write([]byte("hello world"))
	require.NoError(t, err)
	err = enc.Close()
	require.NoError(t, err)

	pool.ReleaseEncoder(enc)

	// Verify we can acquire again (reuses the same encoder).
	var buf2 bytes.Buffer
	enc2, err := pool.AcquireEncoder(context.Background(), &buf2)
	require.NoError(t, err)
	require.NotNil(t, enc2)
	pool.ReleaseEncoder(enc2)
}

func TestBoundedZstdPool_DecoderAcquireRelease(t *testing.T) {
	pool := newTestPool(2, 2)

	var compressed bytes.Buffer
	enc, err := pool.AcquireEncoder(context.Background(), &compressed)
	require.NoError(t, err)

	testData := []byte("hello world, this is test data for compression")
	_, err = enc.Write(testData)
	require.NoError(t, err)
	err = enc.Close()
	require.NoError(t, err)
	pool.ReleaseEncoder(enc)

	dec, err := pool.AcquireDecoder(context.Background(), bytes.NewReader(compressed.Bytes()))
	require.NoError(t, err)
	require.NotNil(t, dec)

	decompressed, err := io.ReadAll(dec)
	require.NoError(t, err)
	require.Equal(t, testData, decompressed)

	pool.ReleaseDecoder(dec)
}

func TestBoundedZstdPool_ConcurrencyLimit(t *testing.T) {
	pool := newTestPool(2, 2)

	var buf1, buf2 bytes.Buffer
	enc1, err := pool.AcquireEncoder(context.Background(), &buf1)
	require.NoError(t, err)
	enc2, err := pool.AcquireEncoder(context.Background(), &buf2)
	require.NoError(t, err)

	// Third acquire should block and time out.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	var buf3 bytes.Buffer
	_, err = pool.AcquireEncoder(ctx, &buf3)
	require.Error(t, err)
	require.Equal(t, context.DeadlineExceeded, err)

	// Release one encoder, then acquire should succeed.
	pool.ReleaseEncoder(enc1)

	enc3, err := pool.AcquireEncoder(context.Background(), &buf3)
	require.NoError(t, err)
	require.NotNil(t, enc3)

	pool.ReleaseEncoder(enc2)
	pool.ReleaseEncoder(enc3)
}

func TestBoundedZstdPool_ConcurrentAccess(t *testing.T) {
	pool := newTestPool(4, 4)

	testData := []byte("concurrent test data that needs to be compressed and decompressed")

	var compressed bytes.Buffer
	enc, _ := pool.AcquireEncoder(context.Background(), &compressed)
	enc.Write(testData)
	enc.Close()
	pool.ReleaseEncoder(enc)
	compressedBytes := compressed.Bytes()

	var wg sync.WaitGroup
	errs := make(chan error, 100)

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			var buf bytes.Buffer
			enc, err := pool.AcquireEncoder(context.Background(), &buf)
			if err != nil {
				errs <- err
				return
			}
			enc.Write(testData)
			enc.Close()
			pool.ReleaseEncoder(enc)

			dec, err := pool.AcquireDecoder(context.Background(), bytes.NewReader(compressedBytes))
			if err != nil {
				errs <- err
				return
			}
			result, err := io.ReadAll(dec)
			pool.ReleaseDecoder(dec)
			if err != nil {
				errs <- err
				return
			}
			if !bytes.Equal(result, testData) {
				errs <- fmt.Errorf("decompressed data mismatch: got %d bytes, want %d bytes", len(result), len(testData))
				return
			}
		}()
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Errorf("concurrent operation failed: %v", err)
	}
}

func TestBoundedZstdPool_ContextCancellation(t *testing.T) {
	pool := newTestPool(1, 1)

	var buf bytes.Buffer
	enc, _ := pool.AcquireEncoder(context.Background(), &buf)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var buf2 bytes.Buffer
	_, err := pool.AcquireEncoder(ctx, &buf2)
	require.Error(t, err)
	require.Equal(t, context.Canceled, err)

	pool.ReleaseEncoder(enc)
}

func TestBoundedZstdPool_NilRelease(t *testing.T) {
	pool := newTestPool(2, 2)

	// Should not panic on nil release.
	pool.ReleaseEncoder(nil)
	pool.ReleaseDecoder(nil)
}

func BenchmarkBoundedZstdPool_AcquireRelease(b *testing.B) {
	pool := newTestPool(16, 16)
	testData := bytes.Repeat([]byte("benchmark data "), 1000)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			var buf bytes.Buffer
			enc, _ := pool.AcquireEncoder(context.Background(), &buf)
			enc.Write(testData)
			enc.Close()
			pool.ReleaseEncoder(enc)
		}
	})
}

func BenchmarkZstdNoPool(b *testing.B) {
	testData := bytes.Repeat([]byte("benchmark data "), 1000)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			var buf bytes.Buffer
			enc, _ := zstd.NewWriter(&buf, zstd.WithEncoderConcurrency(1))
			enc.Write(testData)
			enc.Close()
		}
	})
}
