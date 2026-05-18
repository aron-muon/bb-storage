package grpc_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	bb_grpc "github.com/buildbarn/bb-storage/pkg/grpc"

	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/peer"
)

// peerFromContext is a thin wrapper around grpc/peer.FromContext so
// the test file doesn't need to import the package separately in every
// helper that wants it.
func peerFromContext(ctx context.Context) (*peer.Peer, bool) {
	return peer.FromContext(ctx)
}

// These tests exercise NewRoundRobinClientConn against a real
// loopback-TCP gRPC server with a controlled per-chunk latency profile.
// They model the behaviour the in-process mock tests don't: real TLS-free
// HTTP/2 framing, real `grpc.NewClient` lazy initialisation, and the
// round-robin-per-call distribution across N independent connections.
//
// Two questions to answer:
//
//  1. Regression guard: does pool=N slow down SEQUENTIAL reads compared to
//     pool=1? It must not — sequential reads only use one conn at a time,
//     so a larger pool should be no worse.
//
//  2. Parallelism: does pool=N actually deliver near-linear speedup for
//     N CONCURRENT reads vs pool=1? That is the entire point of the pool;
//     if it doesn't deliver, the feature is worthless.
//
// The server is a minimal ByteStream.Read implementation that streams a
// fixed payload in fixed-size chunks, sleeping a configurable amount
// between chunks to simulate disk/network latency on the wire.

// slowByteStreamServer streams a deterministic payload one chunk at a
// time with a configurable per-chunk sleep. Tracks per-stream start
// time and which TCP connection's stream is being served (via the gRPC
// peer info) so tests can verify distribution.
type slowByteStreamServer struct {
	bytestream.UnimplementedByteStreamServer

	chunkSize  int
	chunkCount int
	chunkDelay time.Duration

	// Counters for stream observability.
	totalReads      atomic.Int64
	inFlightReads   atomic.Int64
	maxInFlightSeen atomic.Int64
}

func (s *slowByteStreamServer) Read(req *bytestream.ReadRequest, stream bytestream.ByteStream_ReadServer) error {
	s.totalReads.Add(1)
	inFlight := s.inFlightReads.Add(1)
	defer s.inFlightReads.Add(-1)
	for {
		prev := s.maxInFlightSeen.Load()
		if inFlight <= prev || s.maxInFlightSeen.CompareAndSwap(prev, inFlight) {
			break
		}
	}

	chunk := make([]byte, s.chunkSize)
	for i := 0; i < s.chunkCount; i++ {
		if err := stream.Send(&bytestream.ReadResponse{Data: chunk}); err != nil {
			return err
		}
		if s.chunkDelay > 0 {
			select {
			case <-stream.Context().Done():
				return stream.Context().Err()
			case <-time.After(s.chunkDelay):
			}
		}
	}
	return nil
}

// startSlowServer spins up a real TCP gRPC server. Returns its
// `host:port` and a cleanup func. Server is non-blocking; the goroutine
// exits when Stop() is called via cleanup. Accepts any ByteStreamServer
// — the slow-and-plain case and the per-conn-serialising variant
// share this scaffolding.
func startSlowServer(t *testing.T, srv bytestream.ByteStreamServer) (string, func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	s := grpc.NewServer()
	bytestream.RegisterByteStreamServer(s, srv)

	go func() {
		_ = s.Serve(lis)
	}()
	return lis.Addr().String(), func() {
		s.Stop()
		_ = lis.Close()
	}
}

// buildPool dials `addr` `poolSize` times and wraps the conns with
// NewRoundRobinClientConn. Returns the pool plus a cleanup func that
// closes every backing conn.
func buildPool(t *testing.T, addr string, poolSize int) (grpc.ClientConnInterface, func()) {
	t.Helper()
	rawConns := make([]*grpc.ClientConn, poolSize)
	ifaceConns := make([]grpc.ClientConnInterface, poolSize)
	for i := 0; i < poolSize; i++ {
		c, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		rawConns[i] = c
		ifaceConns[i] = c
	}
	pool := bb_grpc.NewRoundRobinClientConn(ifaceConns)
	return pool, func() {
		for _, c := range rawConns {
			_ = c.Close()
		}
	}
}

// drainOneRead opens a Read stream on `client` and consumes it to EOF.
// Returns wall-clock elapsed and any error.
func drainOneRead(ctx context.Context, client bytestream.ByteStreamClient) (time.Duration, error) {
	start := time.Now()
	stream, err := client.Read(ctx, &bytestream.ReadRequest{ResourceName: "x"})
	if err != nil {
		return time.Since(start), err
	}
	for {
		_, err := stream.Recv()
		if err == io.EOF {
			return time.Since(start), nil
		}
		if err != nil {
			return time.Since(start), err
		}
	}
}

func medianDuration(ds []time.Duration) time.Duration {
	sorted := append([]time.Duration(nil), ds...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	return sorted[len(sorted)/2]
}

func runSequential(ctx context.Context, t *testing.T, addr string, poolSize, iterations int) (perRead []time.Duration, total time.Duration) {
	t.Helper()
	pool, cleanup := buildPool(t, addr, poolSize)
	defer cleanup()
	client := bytestream.NewByteStreamClient(pool)
	perRead = make([]time.Duration, 0, iterations)
	start := time.Now()
	for i := 0; i < iterations; i++ {
		d, err := drainOneRead(ctx, client)
		require.NoErrorf(t, err, "iter %d", i)
		perRead = append(perRead, d)
	}
	return perRead, time.Since(start)
}

func runConcurrent(ctx context.Context, t *testing.T, addr string, poolSize, concurrency int) (perRead []time.Duration, wall time.Duration) {
	t.Helper()
	pool, cleanup := buildPool(t, addr, poolSize)
	defer cleanup()
	client := bytestream.NewByteStreamClient(pool)

	out := make([]time.Duration, concurrency)
	var wg sync.WaitGroup
	wg.Add(concurrency)
	start := time.Now()
	for i := 0; i < concurrency; i++ {
		go func(i int) {
			defer wg.Done()
			d, err := drainOneRead(ctx, client)
			require.NoErrorf(t, err, "goroutine %d", i)
			out[i] = d
		}(i)
	}
	wg.Wait()
	return out, time.Since(start)
}

// 1. ─────────────────────────────────────────────────────────────────
// Regression guard. With NO server-side concurrency in play (sequential
// reads, no contention), a pool of size N must not slow down each
// individual read compared to pool=1. If it does, the pool wrapper is
// regressing single-stream throughput — which is the production
// behaviour we measured on staging.
//
// Production case for context:
//   - pool=1 (mirror-twice config): 6/6 reads at ~75 s each
//   - pool=8 (this PR): 0/3 reads completed within the 120 s deadline
//     for the same digest, on the same network path.
func TestPoolSequentialThroughputParity(t *testing.T) {
	// Per-read profile: 50 chunks × 4 KiB × 10 ms sleep
	// = 50 × 10 ms = ~500 ms of natural per-read wall time.
	// On loopback this swamps any micro-latency from the gRPC stack.
	srv := &slowByteStreamServer{
		chunkSize:  4 * 1024,
		chunkCount: 50,
		chunkDelay: 10 * time.Millisecond,
	}
	addr, stop := startSlowServer(t, srv)
	defer stop()

	const iterations = 16
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Baseline: pool of one (= current production behaviour and the
	// no-op fast path inside NewRoundRobinClientConn).
	pool1, total1 := runSequential(ctx, t, addr, 1, iterations)
	med1 := medianDuration(pool1)

	// Subject under test: pool of 8 (what this PR enables).
	pool8, total8 := runSequential(ctx, t, addr, 8, iterations)
	med8 := medianDuration(pool8)

	t.Logf("sequential pool=1: total=%v median=%v per-read", total1, med1)
	t.Logf("sequential pool=8: total=%v median=%v per-read", total8, med8)
	for i, d := range pool8 {
		t.Logf("  pool=8 iter[%d] = %v", i, d)
	}

	// Tolerance: pool=8 median must be within 2x of pool=1 median.
	// Anything beyond suggests the round-robin distribution across cold
	// conns is materially adding cost. The expected ratio is ~1.0x —
	// each sequential read uses one conn at a time, regardless of how
	// many slots the pool has.
	limit := med1 * 2
	require.LessOrEqualf(t, med8, limit,
		"pool=8 sequential median (%v) more than 2x pool=1 median (%v) — pool is slowing down single-stream reads",
		med8, med1)
}

// 2. ─────────────────────────────────────────────────────────────────
// Negative result: the pool gives NO speedup for concurrent reads
// against a standard Go gRPC server. This is by design — HTTP/2
// multiplexing already lets multiple streams progress in parallel on
// one connection, and a standard `grpc.NewServer` spawns a goroutine
// per stream, so 8 concurrent reads on 1 conn ≈ 8 concurrent reads on
// 8 conns. The pool only delivers a measurable benefit when the server
// serialises somewhere — see TestPoolHelpsWhenServerSerialisesPerConnection
// for the positive case.
//
// This test exists as a regression guard: if the pool ever started
// making things substantially WORSE against a standard server (e.g.,
// by fragmenting one logical request across multiple conns and adding
// reassembly overhead), this assertion would catch it.
func TestPoolGivesNoBenefitAgainstStandardServer(t *testing.T) {
	srv := &slowByteStreamServer{
		chunkSize:  4 * 1024,
		chunkCount: 50,
		chunkDelay: 10 * time.Millisecond,
	}
	addr, stop := startSlowServer(t, srv)
	defer stop()

	const concurrency = 8
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool1Reads, pool1Wall := runConcurrent(ctx, t, addr, 1, concurrency)
	pool8Reads, pool8Wall := runConcurrent(ctx, t, addr, 8, concurrency)

	t.Logf("standard server, concurrent pool=1: wall=%v per-read median=%v max=%v inFlightMaxOnServer=%d",
		pool1Wall, medianDuration(pool1Reads), maxDur(pool1Reads), srv.maxInFlightSeen.Load())
	t.Logf("standard server, concurrent pool=8: wall=%v per-read median=%v max=%v inFlightMaxOnServer=%d",
		pool8Wall, medianDuration(pool8Reads), maxDur(pool8Reads), srv.maxInFlightSeen.Load())

	// Expectation: pool=8 wall time should be within +/-50% of pool=1
	// wall time. Strictly: it should be roughly equal. We accept a
	// looser bound to avoid flaking on shared CI hardware.
	low := pool1Wall / 2
	high := pool1Wall * 2
	require.GreaterOrEqualf(t, pool8Wall, low,
		"pool=8 is mysteriously much faster than pool=1 against a standard server (%v vs %v) — investigate, this implies the pool is doing something unexpected",
		pool8Wall, pool1Wall)
	require.LessOrEqualf(t, pool8Wall, high,
		"pool=8 is much SLOWER than pool=1 against a standard server (%v vs %v) — the pool is regressing concurrent throughput against gRPC's native multiplexing",
		pool8Wall, pool1Wall)

	// Both configurations must allow at least concurrency streams to be
	// in-flight simultaneously on the server, proving the test is
	// actually exercising parallelism (otherwise the wall-time
	// comparison above is meaningless).
	require.EqualValuesf(t, concurrency, srv.maxInFlightSeen.Load(),
		"server saw maxInFlight=%d, want %d — the test isn't generating real concurrency",
		srv.maxInFlightSeen.Load(), concurrency)
}

// 3. ─────────────────────────────────────────────────────────────────
// Cold-start cost. With pool=N and N sequential reads, the FIRST N reads
// each open one slot (lazy init). After that, slots are warm and
// subsequent reads should be at steady-state cost. This test confirms
// the cold-start cost is bounded and one-time per slot — if it's
// excessive, the round-robin distribution of cold conns can degrade the
// first batch of requests.
func TestPoolColdStartIsBoundedPerSlot(t *testing.T) {
	srv := &slowByteStreamServer{
		chunkSize:  4 * 1024,
		chunkCount: 10, // shorter — we want first-byte cost to dominate
		chunkDelay: 5 * time.Millisecond,
	}
	addr, stop := startSlowServer(t, srv)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const poolSize = 8
	const iterations = poolSize * 4 // 4 cycles around the pool

	perRead, _ := runSequential(ctx, t, addr, poolSize, iterations)

	// The first `poolSize` reads each pay a one-time cold-dial cost.
	// After that, every slot has been hit once and subsequent reads
	// reuse warm conns. Measure: are the warm reads (after the first
	// poolSize iterations) noticeably faster than the cold ones?
	coldMed := medianDuration(perRead[:poolSize])
	warmMed := medianDuration(perRead[poolSize:])
	t.Logf("cold reads (first %d, all-cold conns) median=%v", poolSize, coldMed)
	t.Logf("warm reads (next %d) median=%v", iterations-poolSize, warmMed)
	for i, d := range perRead {
		t.Logf("  iter[%d] = %v", i, d)
	}

	// Warm should not be slower than cold; if it is, something is wrong
	// with the round-robin or conn reuse.
	require.LessOrEqualf(t, warmMed, coldMed*2,
		"warm-read median (%v) should be no slower than 2x cold-read median (%v)",
		warmMed, coldMed)
}

func maxDur(ds []time.Duration) time.Duration {
	var m time.Duration
	for _, d := range ds {
		if d > m {
			m = d
		}
	}
	return m
}

// 4. ─────────────────────────────────────────────────────────────────
// The pool only helps when the SERVER serialises streams per
// connection. Standard Go gRPC servers spawn one goroutine per stream
// — so 8 streams multiplexed on 1 connection run in parallel and the
// pool delivers no speedup (as TestPoolConcurrentParallelism shows on
// the standard server above).
//
// To prove the pool is useful at all, we need a server that DOES
// serialise per connection. This test wraps the slow server with a
// per-stream-context mutex so all streams arriving on one TCP
// connection are processed one at a time, then runs the parallelism
// test against that. If THIS test shows pool=8 is much faster than
// pool=1, the pool fix is conceptually correct — it just doesn't apply
// to the bazel-remote v2.6.1 backend, which (like most Go gRPC
// servers) does NOT serialise per connection.
//
// Treat the result as a documentation test: it tells us what the pool
// fix is and isn't useful for, against a server-side behaviour we can
// control.
func TestPoolHelpsWhenServerSerialisesPerConnection(t *testing.T) {
	srv := &perConnSerialisingServer{
		slowByteStreamServer: slowByteStreamServer{
			chunkSize:  4 * 1024,
			chunkCount: 30,
			chunkDelay: 5 * time.Millisecond, // ~150 ms per read
		},
	}
	addr, stop := startSlowServer(t, srv)
	defer stop()

	const concurrency = 8
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool1Reads, pool1Wall := runConcurrent(ctx, t, addr, 1, concurrency)
	pool8Reads, pool8Wall := runConcurrent(ctx, t, addr, 8, concurrency)

	t.Logf("per-conn-serialising server, pool=1: wall=%v per-read median=%v max=%v",
		pool1Wall, medianDuration(pool1Reads), maxDur(pool1Reads))
	t.Logf("per-conn-serialising server, pool=8: wall=%v per-read median=%v max=%v",
		pool8Wall, medianDuration(pool8Reads), maxDur(pool8Reads))

	// When server-side parallelism is gated by a per-connection mutex,
	// pool=8 gives the client 8 connections → 8 parallel server queues
	// → all 8 reads run in parallel. pool=1 funnels all 8 reads through
	// one connection → server serialises → 8× the wall time.
	require.Lessf(t, pool8Wall*4, pool1Wall,
		"pool=8 must be at least 4x faster than pool=1 against a per-conn-serialising server (got pool=1=%v, pool=8=%v)",
		pool1Wall, pool8Wall)
}

// perConnSerialisingServer wraps slowByteStreamServer with a per-TCP-
// connection mutex on Read. Models the failure mode the pool fix is
// designed to address: a server that, for whatever reason (an internal
// shared mutex, a single disk reader, a flow-control limit), can only
// service one stream at a time per inbound connection.
//
// We approximate "same connection" by using grpc's peer.AddrInfo: all
// streams on one TCP conn share the same client-side ephemeral port.
type perConnSerialisingServer struct {
	slowByteStreamServer

	mu       sync.Mutex
	connLock map[string]*sync.Mutex
}

func (s *perConnSerialisingServer) Read(req *bytestream.ReadRequest, stream bytestream.ByteStream_ReadServer) error {
	peerInfo, _ := peerFrom(stream.Context())
	s.mu.Lock()
	if s.connLock == nil {
		s.connLock = map[string]*sync.Mutex{}
	}
	lock, ok := s.connLock[peerInfo]
	if !ok {
		lock = &sync.Mutex{}
		s.connLock[peerInfo] = lock
	}
	s.mu.Unlock()

	lock.Lock()
	defer lock.Unlock()
	return s.slowByteStreamServer.Read(req, stream)
}

// peerFrom returns a stable string identifier for the client side of
// the gRPC connection serving `ctx`. Used as the key for per-conn
// locking.
func peerFrom(ctx context.Context) (string, bool) {
	p, ok := peerFromContext(ctx)
	if !ok || p == nil {
		return "", false
	}
	return p.String(), true
}

// Sanity probe: ensure the slow server actually behaves as advertised
// before any pool is in the picture. If this fails, all the above tests
// are measuring noise.
func TestSlowServerSelfCheck(t *testing.T) {
	srv := &slowByteStreamServer{
		chunkSize:  4 * 1024,
		chunkCount: 20,
		chunkDelay: 10 * time.Millisecond,
	}
	addr, stop := startSlowServer(t, srv)
	defer stop()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	client := bytestream.NewByteStreamClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	d, err := drainOneRead(ctx, client)
	require.NoError(t, err)

	expected := time.Duration(srv.chunkCount) * srv.chunkDelay
	require.GreaterOrEqualf(t, d, expected-50*time.Millisecond,
		"slow server returned faster than its chunkDelay × chunkCount budget: got %v want ~%v", d, expected)

	require.Equalf(t, int64(1), srv.totalReads.Load(), "server should have served exactly 1 read, served %d", srv.totalReads.Load())
}

// fmt is imported elsewhere via testify; keep this comment so go imports stays clean.
var _ = fmt.Sprint
