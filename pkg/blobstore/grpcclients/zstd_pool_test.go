package grpcclients
package grpcclients_test

import (
	"bytes"
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/buildbarn/bb-storage/pkg/blobstore/grpcclients"
	"github.com/stretchr/testify/require"
)

func TestBoundedZstdPool_EncoderAcquireRelease(t *testing.T) {
	pool := grpcclients.NewBoundedZstdPool(2, 2, nil, nil)

	var buf bytes.Buffer
	enc, err := pool.AcquireEncoder(context.Background(), &buf)
	require.NoError(t, err)
	require.NotNil(t, enc)

	// Write some data
	_, err = enc.Write([]byte("hello world"))
	require.NoError(t, err)
	err = enc.Close()
	require.NoError(t, err)

	// Release back to pool
	pool.ReleaseEncoder(enc)

	// Verify we can acquire again (reuses the same encoder)
	var buf2 bytes.Buffer
	enc2, err := pool.AcquireEncoder(context.Background(), &buf2)
	require.NoError(t, err)
	require.NotNil(t, enc2)
	pool.ReleaseEncoder(enc2)
}

func TestBoundedZstdPool_DecoderAcquireRelease(t *testing.T) {
	// First compress some data
	pool := grpcclients.NewBoundedZstdPool(2, 2, nil, nil)

	var compressed bytes.Buffer
	enc, err := pool.AcquireEncoder(context.Background(), &compressed)
	require.NoError(t, err)

	testData := []byte("hello world, this is test data for compression")
	_, err = enc.Write(testData)
	require.NoError(t, err)
	err = enc.Close()
	require.NoError(t, err)
	pool.ReleaseEncoder(enc)

	// Now decompress
	dec, err := pool.AcquireDecoder(context.Background(), bytes.NewReader(compressed.Bytes()))
	require.NoError(t, err)
	require.NotNil(t, dec)

	decompressed, err := io.ReadAll(dec)
	require.NoError(t, err)
	require.Equal(t, testData, decompressed)

	pool.ReleaseDecoder(dec)
}

func TestBoundedZstdPool_ConcurrencyLimit(t *testing.T) {
	// Pool with only 2 concurrent encoders
	pool := grpcclients.NewBoundedZstdPool(2, 2, nil, nil)

	// Acquire 2 encoders (should succeed immediately)
	var buf1, buf2 bytes.Buffer
	enc1, err := pool.AcquireEncoder(context.Background(), &buf1)
	require.NoError(t, err)
	enc2, err := pool.AcquireEncoder(context.Background(), &buf2)
	require.NoError(t, err)

	// Third acquire should block
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	var buf3 bytes.Buffer
	_, err = pool.AcquireEncoder(ctx, &buf3)
	require.Error(t, err) // Should timeout
	require.Equal(t, context.DeadlineExceeded, err)

	// Release one encoder
	pool.ReleaseEncoder(enc1)

	// Now acquire should succeed
	enc3, err := pool.AcquireEncoder(context.Background(), &buf3)
	require.NoError(t, err)
	require.NotNil(t, enc3)

	// Cleanup
	pool.ReleaseEncoder(enc2)
	pool.ReleaseEncoder(enc3)
}

func TestBoundedZstdPool_TryAcquire(t *testing.T) {
	pool := grpcclients.NewBoundedZstdPool(1, 1, nil, nil)

	// First try should succeed
	var buf bytes.Buffer
	enc, ok := pool.TryAcquireEncoder(&buf)
	require.True(t, ok)
	require.NotNil(t, enc)

	// Second try should fail (pool at capacity)
	var buf2 bytes.Buffer
	enc2, ok := pool.TryAcquireEncoder(&buf2)
	require.False(t, ok)
	require.Nil(t, enc2)

	// Release and try again
	pool.ReleaseEncoder(enc)
	enc3, ok := pool.TryAcquireEncoder(&buf2)
	require.True(t, ok)
	require.NotNil(t, enc3)
	pool.ReleaseEncoder(enc3)
}

func TestBoundedZstdPool_PooledReadCloser(t *testing.T) {
	pool := grpcclients.NewBoundedZstdPool(2, 1, nil, nil)

	// Compress test data
	var compressed bytes.Buffer
	enc, _ := pool.AcquireEncoder(context.Background(), &compressed)
	testData := []byte("test data for read closer")
	enc.Write(testData)
	enc.Close()
	pool.ReleaseEncoder(enc)

	// Get decoder as ReadCloser
	rc, err := pool.AcquireDecoderAsReadCloser(context.Background(), bytes.NewReader(compressed.Bytes()))
	require.NoError(t, err)

	// Pool should be at capacity (1 decoder)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err = pool.AcquireDecoder(ctx, bytes.NewReader(compressed.Bytes()))
	require.Error(t, err) // Should timeout

	// Read and close
	data, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, testData, data)

	err = rc.Close()
	require.NoError(t, err)

	// Now pool should have capacity again
	dec, err := pool.AcquireDecoder(context.Background(), bytes.NewReader(compressed.Bytes()))
	require.NoError(t, err)
	require.NotNil(t, dec)
	pool.ReleaseDecoder(dec)
}

func TestBoundedZstdPool_ConcurrentAccess(t *testing.T) {
	pool := grpcclients.NewBoundedZstdPool(4, 4, nil, nil)

	testData := []byte("concurrent test data that needs to be compressed and decompressed")

	// First, create compressed data
	var compressed bytes.Buffer
	enc, _ := pool.AcquireEncoder(context.Background(), &compressed)
	enc.Write(testData)
	enc.Close()
	pool.ReleaseEncoder(enc)
	compressedBytes := compressed.Bytes()

	// Run many concurrent operations
	var wg sync.WaitGroup
	errors := make(chan error, 100)

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			// Compress
			var buf bytes.Buffer
			enc, err := pool.AcquireEncoder(context.Background(), &buf)
			if err != nil {
				errors <- err
				return
			}
			enc.Write(testData)
			enc.Close()
			pool.ReleaseEncoder(enc)

			// Decompress
			dec, err := pool.AcquireDecoder(context.Background(), bytes.NewReader(compressedBytes))
			if err != nil {
				errors <- err
				return
			}
			result, err := io.ReadAll(dec)
			pool.ReleaseDecoder(dec)
			if err != nil {
				errors <- err
				return
			}
			if !bytes.Equal(result, testData) {
				errors <- err
				return
			}
		}()
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("concurrent operation failed: %v", err)
	}
}

func TestBoundedZstdPool_ContextCancellation(t *testing.T) {
	pool := grpcclients.NewBoundedZstdPool(1, 1, nil, nil)

	// Acquire the only encoder
	var buf bytes.Buffer
	enc, _ := pool.AcquireEncoder(context.Background(), &buf)

	// Try to acquire with already-cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	var buf2 bytes.Buffer
	_, err := pool.AcquireEncoder(ctx, &buf2)
	require.Error(t, err)
	require.Equal(t, context.Canceled, err)

	pool.ReleaseEncoder(enc)
}

func TestBoundedZstdPool_NilRelease(t *testing.T) {
	pool := grpcclients.NewBoundedZstdPool(2, 2, nil, nil)

	// Should not panic on nil release
	pool.ReleaseEncoder(nil)
	pool.ReleaseDecoder(nil)
}

func BenchmarkBoundedZstdPool_AcquireRelease(b *testing.B) {
	pool := grpcclients.NewBoundedZstdPool(16, 16, nil, nil)
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
			// This is what Tyler's current code does - new encoder per operation
			enc, _ := grpcclients.NewBoundedZstdPool(1, 1, nil, nil).AcquireEncoder(context.Background(), &buf)
			enc.Write(testData)
			enc.Close()
		}
	})
}
