// Package grpcservers_test contains characterisation tests for the
// ByteStream Read path under conditions that match production:
// concurrency, mid-stream cancellation, backpressure, and a pooled
// ZSTD encoder shared across reads.
//
// Purpose. We are investigating a Bazel-side
// OutputDigestMismatchException where every observed "received - expected"
// byte diff is an exact multiple of 65 536 (the ByteStream chunk
// constant in cmd/bb_storage/main.go: readChunkSize := 1<<16). The
// "received" digests are NotFound in production CAS, so the wrong bytes
// are a transient product of the read pipeline rather than a stored
// blob. These tests probe the server-side suspects (pool reuse after
// cancellation, cross-read contamination, slow-consumer backpressure)
// against a real Unix-socket gRPC server and a real BoundedPool with
// production-matching configuration.
//
// Failure shape we are watching for: a Read whose concatenated
// ReadResponse.data bytes exceed the expected (single-shot) compressed
// size by a multiple of 65 536, OR a Read whose zstd-decompressed
// output does not match the original blob bytes (size and sha256).
//
// Pass result. All tests pass on a single run is *not* sufficient — the
// production failure is non-deterministic. Each parallel test runs many
// iterations to surface intermittent state leaks. A single divergence
// is enough to fail the test.
package grpcservers_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/grpcservers"
	"github.com/buildbarn/bb-storage/pkg/blobstore/slicing"
	"github.com/buildbarn/bb-storage/pkg/digest"
	bb_zstd "github.com/buildbarn/bb-storage/pkg/zstd"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"

	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// Production-matching ZSTD configuration. From the staging configmap:
// encoder_window_size_bytes = 4 MiB, max_encoders = 1056. Tests below
// vary maxEncoders to stress different pool-reuse rates while keeping
// the encoder shape identical.
const prodEncoderWindow = 4 << 20 // 4 MiB

// readChunkSize matches the production default from cmd/bb_storage/main.go.
// This is the constant the production failure's byte diff aligns to.
const readChunkSize = 1 << 16 // 64 KiB

// mapBlobAccess is a minimal real BlobAccess backed by an in-memory
// map. Returns real CASBufferFromByteSlice values, so Get → IntoWriter
// exercises the validated-reader path the production ZSTD Read uses.
// Safe for concurrent Get calls (read-only after Put).
type mapBlobAccess struct {
	mu    sync.RWMutex
	blobs map[string][]byte // hash -> raw bytes
}

func newMapBlobAccess() *mapBlobAccess {
	return &mapBlobAccess{blobs: map[string][]byte{}}
}

func (m *mapBlobAccess) store(d digest.Digest, data []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.blobs[d.GetHashString()] = data
}

func (m *mapBlobAccess) Get(ctx context.Context, d digest.Digest) buffer.Buffer {
	m.mu.RLock()
	data, ok := m.blobs[d.GetHashString()]
	m.mu.RUnlock()
	if !ok {
		return buffer.NewBufferFromError(status.Errorf(5, "blob %s not found", d.GetHashString()))
	}
	// Return a fresh byte-slice-backed buffer each call. The slice is
	// shared but read-only; the buffer wraps it without taking a
	// reference, so concurrent reads can't conflict.
	return buffer.NewCASBufferFromByteSlice(d, data, buffer.UserProvided)
}

func (m *mapBlobAccess) GetFromComposite(ctx context.Context, parent, child digest.Digest, slicer slicing.BlobSlicer) buffer.Buffer {
	return buffer.NewBufferFromError(status.Errorf(12, "GetFromComposite not implemented"))
}

func (m *mapBlobAccess) Put(ctx context.Context, d digest.Digest, b buffer.Buffer) error {
	data, err := b.ToByteSlice(1 << 30)
	if err != nil {
		return err
	}
	m.store(d, data)
	return nil
}

func (m *mapBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	return digest.EmptySet, nil
}

func (m *mapBlobAccess) GetCapabilities(ctx context.Context, instanceName digest.InstanceName) (*remoteexecution.ServerCapabilities, error) {
	return &remoteexecution.ServerCapabilities{}, nil
}

var _ blobstore.BlobAccess = (*mapBlobAccess)(nil)

// fixture holds a blob's content plus its digest for a given
// digest.Function (SHA256 throughout these tests).
type fixture struct {
	name   string
	data   []byte
	digest digest.Digest
}

// makeFixture creates a deterministic blob of the given size. The
// content is a repeating byte pattern so zstd compresses well —
// matters because we want compressed-output sizes that are non-trivial
// (multiple chunks on the wire) but the tests still finish quickly.
// A purely random fill would be incompressible and balloon the test
// runtime.
func makeFixture(t *testing.T, name string, size int) fixture {
	t.Helper()
	data := make([]byte, size)
	// Mix repeating + per-position content so zstd can compress but
	// the bytes aren't pathologically uniform.
	for i := range data {
		data[i] = byte(i*31 + (i / 4096))
	}
	df := digest.MustNewFunction("", remoteexecution.DigestFunction_SHA256)
	gen := df.NewGenerator(int64(size))
	gen.Write(data)
	return fixture{name: name, data: data, digest: gen.Sum()}
}

// startTestServer spins up a real gRPC server bound to a Unix socket
// (not bufconn) so chunking and HTTP/2 framing are real. Returns a
// connected client and a cleanup func.
func startTestServer(t *testing.T, ba blobstore.BlobAccess, pool bb_zstd.Pool) (bytestream.ByteStreamClient, func()) {
	t.Helper()

	dir, err := os.MkdirTemp("", "bytestream-conc-*")
	require.NoError(t, err)
	sockPath := filepath.Join(dir, "test.sock")

	lis, err := net.Listen("unix", sockPath)
	require.NoError(t, err)

	server := grpc.NewServer(
		grpc.InitialWindowSize(4<<20),
		grpc.InitialConnWindowSize(8<<20),
		grpc.MaxRecvMsgSize(16<<20),
		grpc.MaxSendMsgSize(16<<20),
	)
	bytestream.RegisterByteStreamServer(server, grpcservers.NewByteStreamServer(ba, readChunkSize, pool))

	go func() {
		_ = server.Serve(lis)
	}()

	conn, err := grpc.NewClient(
		"unix://"+sockPath,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(64<<20)),
	)
	require.NoError(t, err)

	cleanup := func() {
		_ = conn.Close()
		server.Stop()
		_ = lis.Close()
		_ = os.RemoveAll(dir)
	}
	return bytestream.NewByteStreamClient(conn), cleanup
}

// readBlob reads the named resource and returns the concatenated
// ReadResponse.data bytes and the number of chunks observed. It does
// NOT decompress — the caller decides.
func readBlob(ctx context.Context, client bytestream.ByteStreamClient, resource string) ([]byte, int, error) {
	stream, err := client.Read(ctx, &bytestream.ReadRequest{ResourceName: resource})
	if err != nil {
		return nil, 0, err
	}
	var buf []byte
	chunks := 0
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			return buf, chunks, nil
		}
		if err != nil {
			return buf, chunks, err
		}
		buf = append(buf, resp.Data...)
		chunks++
	}
}

// readBlobChunked reads up to maxChunks chunks (or to EOF, whichever
// first) and returns the bytes, chunk count, and EOF flag. Used by
// the cancellation tests to interrupt mid-stream.
func readBlobChunked(ctx context.Context, client bytestream.ByteStreamClient, resource string, maxChunks int) ([]byte, int, bool, error) {
	stream, err := client.Read(ctx, &bytestream.ReadRequest{ResourceName: resource})
	if err != nil {
		return nil, 0, false, err
	}
	var buf []byte
	chunks := 0
	for {
		if chunks >= maxChunks {
			return buf, chunks, false, nil
		}
		resp, err := stream.Recv()
		if err == io.EOF {
			return buf, chunks, true, nil
		}
		if err != nil {
			return buf, chunks, false, err
		}
		buf = append(buf, resp.Data...)
		chunks++
	}
}

// verifyZstdRead decompresses the concatenated compressed bytes and
// asserts the result matches the original fixture. Returns
// (compressedBytes, decompressedSha, ok). On mismatch, logs the
// production signal: delta_mod_65536, decompressed_sha_ok, decompressed_size_ok.
func verifyZstdRead(t *testing.T, label string, f fixture, compressed []byte, chunks int) (string, bool) {
	t.Helper()
	dec, err := zstd.NewReader(nil)
	require.NoError(t, err)
	defer dec.Close()
	plain, err := dec.DecodeAll(compressed, nil)
	if err != nil {
		t.Logf("DECODE_FAIL label=%s blob=%s compressed_recv=%d chunks=%d err=%v",
			label, f.name, len(compressed), chunks, err)
		return "", false
	}
	sum := sha256.Sum256(plain)
	sumHex := hex.EncodeToString(sum[:])
	sizeOK := int64(len(plain)) == f.digest.GetSizeBytes()
	shaOK := sumHex == f.digest.GetHashString()
	ok := sizeOK && shaOK
	t.Logf("READ label=%s blob=%s compressed_recv=%d chunks=%d decompressed_size=%d decompressed_sha_ok=%v decompressed_size_ok=%v",
		label, f.name, len(compressed), chunks, len(plain), shaOK, sizeOK)
	if !ok {
		t.Logf("MISMATCH expected_sha=%s got_sha=%s expected_size=%d got_size=%d",
			f.digest.GetHashString(), sumHex, f.digest.GetSizeBytes(), len(plain))
	}
	return sumHex, ok
}

// prodZstdPool returns a BoundedPool with production-shaped encoders.
func prodZstdPool(maxEncoders int64) bb_zstd.Pool {
	return bb_zstd.NewBoundedPool(
		maxEncoders, maxEncoders,
		[]zstd.EOption{zstd.WithEncoderConcurrency(1), zstd.WithWindowSize(prodEncoderWindow)},
		[]zstd.DOption{zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxWindow(prodEncoderWindow)},
	)
}

func compressedResource(d digest.Digest) string {
	return fmt.Sprintf("compressed-blobs/zstd/%s/%d", d.GetHashString(), d.GetSizeBytes())
}

func identityResource(d digest.Digest) string {
	return fmt.Sprintf("blobs/%s/%d", d.GetHashString(), d.GetSizeBytes())
}

// fixtureSet builds the three blob sizes used by every test below.
func fixtureSet(t *testing.T) (small, medium, large fixture) {
	t.Helper()
	small = makeFixture(t, "small_32KiB", 32<<10)
	medium = makeFixture(t, "medium_10MiB", 10<<20)
	if testing.Short() {
		// Large blob is 16 MiB in -short mode; matches multiple chunks
		// without slowing the suite to a crawl.
		large = makeFixture(t, "large_16MiB", 16<<20)
	} else {
		// Full size matches the smaller failing fixtures from the
		// production report (cargo, grpcio). 64 MiB is enough to
		// produce ~1000 wire chunks while finishing in seconds.
		large = makeFixture(t, "large_64MiB", 64<<20)
	}
	return
}

// 2.0 ─────────────────────────────────────────────────────────────────
// Raw-bytes determinism. The production failure shows the *wire*
// (post-server, pre-client-decode) byte count exceeding the expected
// compressed size by a multiple of 65 536. Bazel's only client-side
// transformation on the compressed-blobs/zstd path is zstd decoding
// (GrpcCacheClient.java in 8.6.0); anything else would have to come
// from the server. So the highest-signal test is: hash the raw bytes
// of ReadResponse.data concatenated across many reads of the same
// digest, including under mid-stream cancellation pressure, and assert
// they are bit-identical. If they vary, the server is the source.
//
// Caveat: zstd encoders are deterministic for fixed configuration +
// fixed input, but the pooled encoder's state must be fully cleared by
// Reset() between acquires for this property to hold across reads.
// That property is exactly what H-S1 questions. If pool-reuse can
// leak state, this test catches it directly.
func TestRawZstdBytesDeterminism(t *testing.T) {
	ba := newMapBlobAccess()
	_, medium, large := fixtureSet(t)
	ba.store(medium.digest, medium.data)
	ba.store(large.digest, large.data)

	iterations := 200
	if testing.Short() {
		iterations = 50
	}

	checkDeterminism := func(t *testing.T, label string, pool bb_zstd.Pool, f fixture, withCancelPressure bool) {
		client, cleanup := startTestServer(t, ba, pool)
		defer cleanup()

		var (
			first     string
			firstSize int
			mu        sync.Mutex
			distinct  = map[string]int{} // sha -> count
		)

		for i := 0; i < iterations; i++ {
			// Optionally interleave a cancelled read against the SAME
			// digest to maximise pool-reuse contention. Each cancel
			// sends the encoder back into sync.Pool mid-flush.
			if withCancelPressure {
				cctx, ccancel := context.WithCancel(context.Background())
				_, _, _, _ = readBlobChunked(cctx, client, compressedResource(f.digest), 1+i%5)
				ccancel()
			}

			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			compressed, _, err := readBlob(ctx, client, compressedResource(f.digest))
			cancel()
			require.NoError(t, err)

			sum := sha256.Sum256(compressed)
			sumHex := hex.EncodeToString(sum[:])
			mu.Lock()
			distinct[sumHex]++
			if first == "" {
				first = sumHex
				firstSize = len(compressed)
			}
			mu.Unlock()

			// Sanity: every read must still decompress to the original.
			_, ok := verifyZstdRead(t, fmt.Sprintf("det_%s_i%d", label, i), f, compressed, 0)
			require.True(t, ok, "raw bytes decompressed to wrong content on iter %d (size=%d)", i, len(compressed))
		}

		t.Logf("DETERMINISM label=%s blob=%s iterations=%d distinct_raw_byte_hashes=%d first_size=%d",
			label, f.name, iterations, len(distinct), firstSize)
		for h, n := range distinct {
			t.Logf("  sha=%s count=%d", h, n)
		}

		// PRIMARY ASSERTION: every read of the same blob must produce
		// bit-identical wire bytes. If this fails, the server is
		// non-deterministically emitting different streams — direct
		// match for the production signal.
		require.Lenf(t, distinct, 1,
			"server emitted %d distinct raw byte streams for the same blob (label=%s, blob=%s, iterations=%d). "+
				"Production signal: deltas should be exactly divisible by 65536. "+
				"Inspect the t.Log table above for per-stream sizes.",
			len(distinct), label, f.name, iterations)
	}

	t.Run("Pool1_Medium_NoCancel", func(t *testing.T) {
		checkDeterminism(t, "p1_med_clean", prodZstdPool(1), medium, false)
	})
	t.Run("Pool1_Medium_WithCancelPressure", func(t *testing.T) {
		checkDeterminism(t, "p1_med_cancel", prodZstdPool(1), medium, true)
	})
	t.Run("Pool64_Medium_WithCancelPressure", func(t *testing.T) {
		checkDeterminism(t, "p64_med_cancel", prodZstdPool(64), medium, true)
	})
	t.Run("Pool1_Large_WithCancelPressure", func(t *testing.T) {
		if testing.Short() {
			t.Skip("large+pressure+pool=1 too slow for -short")
		}
		checkDeterminism(t, "p1_large_cancel", prodZstdPool(1), large, true)
	})
}

// 2.1 ─────────────────────────────────────────────────────────────────
// Baseline: serial reads of each fixture on the compressed path. Must
// pass cleanly. If this fails, the bug is fully isolable server-side
// and the rest of the tests are unnecessary.
func TestBaselineSequentialZstd(t *testing.T) {
	ba := newMapBlobAccess()
	small, medium, large := fixtureSet(t)
	for _, f := range []fixture{small, medium, large} {
		ba.store(f.digest, f.data)
	}
	client, cleanup := startTestServer(t, ba, prodZstdPool(64))
	defer cleanup()

	for _, f := range []fixture{small, medium, large} {
		t.Run(f.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			compressed, chunks, err := readBlob(ctx, client, compressedResource(f.digest))
			require.NoError(t, err)
			_, ok := verifyZstdRead(t, "baseline_seq", f, compressed, chunks)
			require.True(t, ok, "baseline serial read must verify")
		})
	}
}

// 2.2 ─────────────────────────────────────────────────────────────────
// High-concurrency reads of the SAME digest. Hits H-S2: shared
// reader/buffer state, sync.Pool returning the same object to two
// callers, etc.
func TestConcurrentReadsSameBlob(t *testing.T) {
	ba := newMapBlobAccess()
	_, _, large := fixtureSet(t)
	ba.store(large.digest, large.data)

	for _, N := range []int{2, 8, 32, 128} {
		if testing.Short() && N > 32 {
			continue
		}
		t.Run(fmt.Sprintf("N=%d", N), func(t *testing.T) {
			// Pool size = N forces each reader to acquire its own
			// encoder slot. Smaller pool would queue, hiding any
			// per-encoder state leak.
			client, cleanup := startTestServer(t, ba, prodZstdPool(int64(N)))
			defer cleanup()

			ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
			defer cancel()

			var wg sync.WaitGroup
			var failures atomic.Int32
			for i := 0; i < N; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					compressed, chunks, err := readBlob(ctx, client, compressedResource(large.digest))
					if err != nil {
						t.Errorf("reader %d: read failed: %v", i, err)
						failures.Add(1)
						return
					}
					_, ok := verifyZstdRead(t, fmt.Sprintf("conc_same_N%d_r%d", N, i), large, compressed, chunks)
					if !ok {
						failures.Add(1)
					}
				}(i)
			}
			wg.Wait()
			require.Zero(t, failures.Load(), "%d / %d concurrent readers failed", failures.Load(), N)
		})
	}
}

// 2.3 ─────────────────────────────────────────────────────────────────
// High-concurrency reads of DISTINCT digests. Watches for cross-blob
// contamination: a reader of blob X observing bytes that decode to
// blob Y. The fixture set spans three sizes so cross-contamination
// would produce a size mismatch as well as a sha mismatch.
func TestConcurrentReadsDistinctBlobs(t *testing.T) {
	ba := newMapBlobAccess()
	small, medium, large := fixtureSet(t)
	fixtures := []fixture{small, medium, large}
	for _, f := range fixtures {
		ba.store(f.digest, f.data)
	}

	for _, N := range []int{8, 32, 64} {
		if testing.Short() && N > 32 {
			continue
		}
		t.Run(fmt.Sprintf("N=%d", N), func(t *testing.T) {
			client, cleanup := startTestServer(t, ba, prodZstdPool(int64(N)))
			defer cleanup()

			ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
			defer cancel()

			var wg sync.WaitGroup
			var failures atomic.Int32
			for i := 0; i < N; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					f := fixtures[i%len(fixtures)]
					compressed, chunks, err := readBlob(ctx, client, compressedResource(f.digest))
					if err != nil {
						t.Errorf("reader %d (%s): read failed: %v", i, f.name, err)
						failures.Add(1)
						return
					}
					_, ok := verifyZstdRead(t, fmt.Sprintf("conc_distinct_N%d_r%d", N, i), f, compressed, chunks)
					if !ok {
						failures.Add(1)
					}
				}(i)
			}
			wg.Wait()
			require.Zero(t, failures.Load(), "%d / %d concurrent cross-blob readers failed", failures.Load(), N)
		})
	}
}

// 2.4 ─────────────────────────────────────────────────────────────────
// Mid-stream cancel + immediate retry. Targets H-S1 directly: if the
// zstd encoder pool returns a partially-flushed encoder to its sync.Pool
// after a cancel, a fresh read picking up the same encoder will observe
// stale state. Run with maxEncoders=1 (every retry MUST reuse the same
// encoder) and maxEncoders=64 (production-shaped reuse).
func TestMidStreamCancelAndRetry(t *testing.T) {
	ba := newMapBlobAccess()
	_, medium, _ := fixtureSet(t)
	ba.store(medium.digest, medium.data)

	iterations := 200
	if testing.Short() {
		iterations = 30
	}
	cancelChunkCounts := []int{1, 2, 5, 50, 500}

	for _, maxEnc := range []int64{1, 64} {
		t.Run(fmt.Sprintf("pool=%d", maxEnc), func(t *testing.T) {
			client, cleanup := startTestServer(t, ba, prodZstdPool(maxEnc))
			defer cleanup()

			for _, k := range cancelChunkCounts {
				t.Run(fmt.Sprintf("cancelAt=%d", k), func(t *testing.T) {
					var failures atomic.Int32
					for i := 0; i < iterations; i++ {
						// 1. Open and cancel after k chunks.
						cctx, ccancel := context.WithCancel(context.Background())
						_, _, _, _ = readBlobChunked(cctx, client, compressedResource(medium.digest), k)
						ccancel()

						// 2. Immediately re-read clean.
						ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
						compressed, chunks, err := readBlob(ctx, client, compressedResource(medium.digest))
						cancel()
						if err != nil {
							t.Errorf("iter %d: re-read failed: %v", i, err)
							failures.Add(1)
							continue
						}
						_, ok := verifyZstdRead(t, fmt.Sprintf("cancel_pool%d_k%d_i%d", maxEnc, k, i), medium, compressed, chunks)
						if !ok {
							failures.Add(1)
						}
					}
					require.Zero(t, failures.Load(), "%d / %d retries returned corrupt data after mid-stream cancel(k=%d)", failures.Load(), iterations, k)
				})
			}
		})
	}
}

// 2.5 ─────────────────────────────────────────────────────────────────
// Concurrent cancellation + clean reads. H-S1 + H-S2 combined. At
// concurrency 32, randomly cancel ~20% of streams mid-read; the rest
// must still verify clean.
func TestConcurrentCancellationAndReads(t *testing.T) {
	ba := newMapBlobAccess()
	_, medium, large := fixtureSet(t)
	ba.store(medium.digest, medium.data)
	ba.store(large.digest, large.data)

	const N = 32
	const rounds = 5
	if testing.Short() {
		t.Skip("skipping concurrent cancel storm in -short")
	}

	client, cleanup := startTestServer(t, ba, prodZstdPool(N))
	defer cleanup()

	rng := rand.New(rand.NewSource(42))
	var cleanFailures, cancelledOK atomic.Int32

	for round := 0; round < rounds; round++ {
		var wg sync.WaitGroup
		for i := 0; i < N; i++ {
			wg.Add(1)
			// Compute all randomized decisions on the main goroutine —
			// math/rand.Rand is not safe for concurrent use, and the
			// test goroutines doing reads must not share an rng. The
			// "cancel storm" being tested is the server-side state, not
			// the rng.
			willCancel := rng.Intn(5) == 0 // ~20%
			cancelAfter := 1 + rng.Intn(10)
			f := medium
			if i%2 == 0 {
				f = large
			}
			go func(i int, cancel bool, cancelAfter int, f fixture) {
				defer wg.Done()
				if cancel {
					cctx, ccancel := context.WithCancel(context.Background())
					_, _, _, _ = readBlobChunked(cctx, client, compressedResource(f.digest), cancelAfter)
					ccancel()
					cancelledOK.Add(1)
					return
				}
				ctx, c := context.WithTimeout(context.Background(), 60*time.Second)
				defer c()
				compressed, chunks, err := readBlob(ctx, client, compressedResource(f.digest))
				if err != nil {
					t.Errorf("round %d reader %d: %v", round, i, err)
					cleanFailures.Add(1)
					return
				}
				_, ok := verifyZstdRead(t, fmt.Sprintf("storm_r%d_i%d", round, i), f, compressed, chunks)
				if !ok {
					cleanFailures.Add(1)
				}
			}(i, willCancel, cancelAfter, f)
		}
		wg.Wait()
	}
	t.Logf("concurrent_cancel_storm: clean_failures=%d cancelled_total=%d", cleanFailures.Load(), cancelledOK.Load())
	require.Zero(t, cleanFailures.Load(), "%d clean reads returned corrupt data during cancellation storm", cleanFailures.Load())
}

// 2.6 ─────────────────────────────────────────────────────────────────
// Slow consumer. Wraps the client Recv loop with a 5 ms sleep per
// chunk, forcing the gRPC 4 MiB flow-control window to backpressure
// the server. If a server-side write buffer is shared across reads,
// backpressure timing can expose it.
func TestSlowConsumerBackpressure(t *testing.T) {
	ba := newMapBlobAccess()
	_, _, large := fixtureSet(t)
	ba.store(large.digest, large.data)

	if testing.Short() {
		t.Skip("skipping slow-consumer backpressure in -short")
	}

	for _, N := range []int{1, 8} {
		t.Run(fmt.Sprintf("N=%d", N), func(t *testing.T) {
			client, cleanup := startTestServer(t, ba, prodZstdPool(int64(N)))
			defer cleanup()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()

			var wg sync.WaitGroup
			var failures atomic.Int32
			for i := 0; i < N; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					stream, err := client.Read(ctx, &bytestream.ReadRequest{ResourceName: compressedResource(large.digest)})
					if err != nil {
						t.Errorf("reader %d open: %v", i, err)
						failures.Add(1)
						return
					}
					var buf []byte
					chunks := 0
					for {
						resp, err := stream.Recv()
						if err == io.EOF {
							break
						}
						if err != nil {
							t.Errorf("reader %d recv: %v", i, err)
							failures.Add(1)
							return
						}
						buf = append(buf, resp.Data...)
						chunks++
						time.Sleep(5 * time.Millisecond)
					}
					_, ok := verifyZstdRead(t, fmt.Sprintf("slow_N%d_r%d", N, i), large, buf, chunks)
					if !ok {
						failures.Add(1)
					}
				}(i)
			}
			wg.Wait()
			require.Zero(t, failures.Load())
		})
	}
}

// 2.7 ─────────────────────────────────────────────────────────────────
// Raw (identity) path parity. The reported bug is specific to
// compressed-blobs/zstd. The raw path must always pass; any failure
// here shifts suspicion off the zstd wrapper and onto a more general
// ByteStream bug. Mirrors 2.1, 2.2, 2.4 against the identity resource.
func TestRawReadParity(t *testing.T) {
	ba := newMapBlobAccess()
	_, medium, large := fixtureSet(t)
	ba.store(medium.digest, medium.data)
	ba.store(large.digest, large.data)

	client, cleanup := startTestServer(t, ba, prodZstdPool(64))
	defer cleanup()

	verifyRaw := func(label string, f fixture, raw []byte) bool {
		sum := sha256.Sum256(raw)
		ok := hex.EncodeToString(sum[:]) == f.digest.GetHashString() && int64(len(raw)) == f.digest.GetSizeBytes()
		t.Logf("RAW label=%s blob=%s size=%d ok=%v", label, f.name, len(raw), ok)
		return ok
	}

	t.Run("Sequential", func(t *testing.T) {
		for _, f := range []fixture{medium, large} {
			ctx, c := context.WithTimeout(context.Background(), 60*time.Second)
			raw, _, err := readBlob(ctx, client, identityResource(f.digest))
			c()
			require.NoError(t, err)
			require.True(t, verifyRaw("raw_seq", f, raw))
		}
	})

	t.Run("Concurrent", func(t *testing.T) {
		const N = 32
		if testing.Short() {
			t.Skip("-short")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		var wg sync.WaitGroup
		var failures atomic.Int32
		for i := 0; i < N; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				raw, _, err := readBlob(ctx, client, identityResource(large.digest))
				if err != nil {
					t.Errorf("reader %d: %v", i, err)
					failures.Add(1)
					return
				}
				if !verifyRaw(fmt.Sprintf("raw_conc_r%d", i), large, raw) {
					failures.Add(1)
				}
			}(i)
		}
		wg.Wait()
		require.Zero(t, failures.Load())
	})

	t.Run("CancelAndRetry", func(t *testing.T) {
		iterations := 50
		if testing.Short() {
			iterations = 10
		}
		for i := 0; i < iterations; i++ {
			cctx, ccancel := context.WithCancel(context.Background())
			_, _, _, _ = readBlobChunked(cctx, client, identityResource(large.digest), 3)
			ccancel()
			ctx, c := context.WithTimeout(context.Background(), 30*time.Second)
			raw, _, err := readBlob(ctx, client, identityResource(large.digest))
			c()
			require.NoError(t, err)
			require.True(t, verifyRaw(fmt.Sprintf("raw_retry_i%d", i), large, raw))
		}
	})
}
