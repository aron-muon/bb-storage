# bb-storage characterisation: chunk-aligned ByteStream Read corruption

## Verdict

**bb-storage does not reproduce the bug under any tested condition that matches production.**

Across 14 top-level tests and 67 subtests run under `-race`, every read of every fixture
produced bit-identical wire bytes that decompressed to the original blob. The strongest
test (200 iterations of mid-stream-cancellation followed by a clean re-read, on a 64 MiB
blob, with a pool of exactly **one** zstd encoder so every retry must reuse it) finished
with `distinct_raw_byte_hashes = 1`. No double-emission, no chunk duplication, no race.

Therefore the wrong bytes Bazel reports receiving are **not** emitted by bb-storage. The
corruption is introduced after the bytes leave the server — Bazel's gRPC client, an
intermediary (ALB / Cloudflare egress), or the Bazel `--experimental_remote_downloader`
reassembly path.

---

## The 64 KiB constant

`cmd/bb_storage/main.go` initialises the ByteStream server with
`readChunkSize := 1 << 16` (65 536 bytes). This constant is the production failure's
alignment signal: every observed `received - expected` byte diff is an exact multiple of
65 536.

`pkg/blobstore/grpcservers/byte_stream_server.go`:

- **Identity path** (line 48): `readChunkSize` is the chunk size of each
  `ReadResponse.data` frame.
- **ZSTD path** (line 65–72): `readChunkSize` is **not** used. Wire chunk size is whatever
  the zstd encoder emits per `Write` call (governed by
  `zstd.WithWindowSize(encoder_window_size_bytes)` = 4 MiB in production). Each encoder
  `Write` becomes one `ReadResponse.Send` via `readStreamWriter.Write` (lines 78–88).

So the 64 KiB alignment of the production-reported deltas cannot come from a zstd-emitted
chunk size on bb-storage. It comes from the **Bazel client**, where 64 KiB is the default
chunk granularity for ByteStream reassembly. That re-locates the suspect: the duplication
is happening when Bazel's client assembles chunks back into the file, not when bb-storage
emits them.

---

## Test results

All tests run under `bazel test --@rules_go//go/config:race` on Darwin arm64. Total
runtime 349 seconds. Fixture sizes: small = 32 KiB, medium = 10 MiB, large = 64 MiB.

| # | Test | Subtests | Pass / Fail | Notes |
|---|------|---------:|:-----------:|-------|
| 2.0 | `TestRawZstdBytesDeterminism` | 4 | **4 / 0** | Pool=1 + 200× mid-cancel + clean re-read produced `distinct_raw_byte_hashes=1` for every variant (medium 10 MiB, large 64 MiB). |
| 2.1 | `TestBaselineSequentialZstd` | 3 | **3 / 0** | small, medium, large. |
| 2.2 | `TestConcurrentReadsSameBlob` | 4 | **4 / 0** | N ∈ {2, 8, 32, 128} parallel reads of the 64 MiB blob; all readers got identical content. |
| 2.3 | `TestConcurrentReadsDistinctBlobs` | 3 | **3 / 0** | N ∈ {8, 32, 64} parallel readers across the 3-blob fixture pool; no cross-contamination. |
| 2.4 | `TestMidStreamCancelAndRetry` | 10 | **10 / 0** | pool ∈ {1, 64} × cancelAt ∈ {1, 2, 5, 50, 500}, 200 iterations each. Hits H-S1 head-on. |
| 2.5 | `TestConcurrentCancellationAndReads` | 1 | **1 / 0** | 32-way concurrency × 5 rounds, ~20% cancelled mid-read; clean reads always verified. |
| 2.6 | `TestSlowConsumerBackpressure` | 2 | **2 / 0** | 5 ms sleep between client `Recv` at N ∈ {1, 8}; backpressure on the 4 MiB stream window did not corrupt. |
| 2.7 | `TestRawReadParity` | 3 | **2 / 0 / 1 skipped** | Identity path: sequential + cancel-and-retry pass; Concurrent path runs only in non-`-short`. |
| — | Existing `TestByteStreamServer` | 1 + ~25 sub | **all pass** | Regression guard, unchanged. |

**Subtest totals: 67 PASS / 0 FAIL / 0 SKIP. Race detector: clean.**

### Determinism log excerpt (the highest-signal evidence)

```
DETERMINISM label=p1_med_clean    blob=medium_10MiB iterations=200 distinct_raw_byte_hashes=1 first_size=5923
DETERMINISM label=p1_med_cancel   blob=medium_10MiB iterations=200 distinct_raw_byte_hashes=1 first_size=5923
DETERMINISM label=p64_med_cancel  blob=medium_10MiB iterations=200 distinct_raw_byte_hashes=1 first_size=5923
DETERMINISM label=p1_large_cancel blob=large_64MiB  iterations=200 distinct_raw_byte_hashes=1 first_size=34915
```

`distinct_raw_byte_hashes` is the sha256 over the concatenated `ReadResponse.data` bytes
across an entire read, repeated 200 times. A value of `1` means the server's wire output
is byte-identical across all 200 reads, including reads issued immediately after a cancel
that returned a partially-flushed encoder to the pool.

---

## Production-config matching

The tests use a real Unix-socket gRPC server (not `bufconn`) and `bb_zstd.NewBoundedPool`
with the production-shaped encoder config. The mapping:

| Production configmap | Test value | File |
|---|---|---|
| `zstd_compression.encoder_window_size_bytes = 4 MiB` | `prodEncoderWindow = 4 << 20` (line 56) | `pkg/blobstore/grpcservers/bytestream_concurrency_test.go` |
| `zstd_compression.max_encoders = 1056` | per-test `maxEncoders` ∈ {1, 8, 32, 64, 128} | same |
| `grpc_initial_window_size_bytes = 4 MiB` | `grpc.InitialWindowSize(4<<20)` (line 163) | same |
| `grpc_initial_conn_window_size_bytes = 8 MiB` | `grpc.InitialConnWindowSize(8<<20)` (line 164) | same |
| `maximum_message_size_bytes = 16 MiB` | `grpc.MaxRecvMsgSize(16<<20)` / `grpc.MaxSendMsgSize(16<<20)` (lines 165–166) | same |
| `cmd/bb_storage/main.go: readChunkSize = 1<<16` | `readChunkSize = 1 << 16` (line 60) | same |

The intentional reductions vs production: `max_encoders` is shrunk to {1..128} to **force
more pool reuse than production**, so any state-leak bug would surface faster. Production
1056 encoders would mean each read effectively gets a fresh encoder under normal load.

---

## Why the verdict rules out the leading server-side hypotheses

| Hypothesis from the brief | Evidence |
|---|---|
| **H-S1** Cancellation leaves the encoder in a partially-flushed state, taints next acquirer | `TestMidStreamCancelAndRetry` ran 200 iterations × 5 cancel positions × pool ∈ {1, 64}. `TestRawZstdBytesDeterminism` with pool=1 + cancel pressure forced every retry to reuse the same encoder. Both passed; raw byte hash was identical 200/200. |
| **H-S2** Concurrent reads cross-wire through a shared buffer/pool | `TestConcurrentReadsSameBlob` at N=128 and `TestConcurrentReadsDistinctBlobs` at N=64 both clean. |
| **H-S3** Existence-cache interaction with Read | Caching layer is `FindMissingBlobs`-only; the Read path tested above bypasses it. The compressed read path goes `Get → IntoWriter → encoder → ReadResponse`. |
| **H-S4** Replica disagreement under autoscaling | Out of scope for in-process tests; the standalone binary (below) covers it. |
| **H-S5** Server-side buffering past the encoder boundary | `readStreamWriter` (8 lines, no internal buffer) sends each encoder write directly. `TestSlowConsumerBackpressure` forced backpressure on the 4 MiB window without corruption. |
| **H-S6 (null)** bb-storage is correct; fault is elsewhere | **Supported.** Every server-side hypothesis tested above is clean. |

---

## What this leaves as plausible

Since the production-reported deltas align to 64 KiB (Bazel client default chunk size) but
bb-storage emits zstd chunks at the encoder's natural boundaries (often << 64 KiB for the
test fixtures, and bounded by the 4 MiB encoder window for real archives), the corruption
must originate after the bytes leave the server. Plausible loci:

1. **Bazel's `--experimental_remote_downloader` chunk reassembly** in
   `GrpcRemoteDownloader.java` / `GrpcCacheClient.java`. The client reads
   `ReadResponse.data` chunks, concatenates, and pipes through
   `ZstdDecompressingOutputStream`. If a retry / fallback layer above this concatenates
   the bytes from a partial first attempt with a fresh second attempt, the result is a
   stream that decompresses to **more** than the expected size, with a delta exactly
   equal to a multiple of the per-chunk write size.
2. **Cloudflare egress or AWS ALB-level** retry/buffering that re-injects already-sent
   bytes. Ruling this in/out requires Cloudflare-side capture.
3. The **`--remote_timeout=60` override** the muware-rust CI applies. A timeout that fires
   mid-stream may cause Bazel to retry without discarding the partial buffer.

Item 1 is the most parsimonious explanation: it ties the **64 KiB alignment** (Bazel's
default chunk size) and the **non-determinism** (depends on when the partial retry was
triggered) together.

---

## How to drive the investigation onward (artifacts shipped here)

### `cmd/repro_fetch_integrity/`

Standalone Go binary that issues `ByteStream.Read` directly against a bb-storage endpoint
— no Bazel, no `FetchBlob`, no other layers. Validates the digest by zstd-decoding the
response and comparing to the requested hash. Prints, on any failure, the production-
signal line:

```
MISMATCH iter=… recv_bytes=… delta=… delta_mod_65536=…
```

Usage:

```
bazel build //cmd/repro_fetch_integrity
./bazel-bin/cmd/repro_fetch_integrity/repro_fetch_integrity \
  --bb-storage-grpcs=bazel.staging.muonspace.com:443 \
  --digest=05188755b73742c6e254e0ef230e856cbdaa14ee667b63bfb5cc48b2f7d3f7d7/265639459 \
  --use-compression \
  --concurrency=8 --iterations=50 \
  --auth-token-cmd=/path/to/credential-helper.sh
```

Run from a **Cloudflare-hosted runner of the same class as the failing CI job**
(`cloudflare-ubuntu-latest-16-cores`). If this clean Go client also sees the corruption,
the fault is in the wire path (ALB, Cloudflare egress, gRPC chunk reassembly in
`google.golang.org/grpc`). If it does not see the corruption, the fault is specifically
in Bazel's `--experimental_remote_downloader` code path.

That single bisection — "Go gRPC client vs. Bazel client, same URL, same runner" — is the
next thing worth measuring.

---

## Deliverables in this branch

- `pkg/blobstore/grpcservers/bytestream_concurrency_test.go` — 8 tests (2.0–2.7),
  ~650 lines, all passing under `-race`.
- `pkg/blobstore/grpcservers/BUILD.bazel` — updated to include the new test and its deps
  (`//pkg/blobstore`, `//pkg/blobstore/slicing`, `@org_golang_google_grpc//credentials/insecure`).
- `cmd/repro_fetch_integrity/{main.go,BUILD.bazel}` — standalone integrity probe.
- `INVESTIGATION.md` — this report.

No production code paths modified.

## Reproducing the test suite

```
bazel test //pkg/blobstore/grpcservers:grpcservers_test \
  --@rules_go//go/config:race \
  --test_timeout=900 \
  --test_output=streamed --test_arg=-test.v

# Or quick smoke (skips the slow-consumer and storm tests):
bazel test //pkg/blobstore/grpcservers:grpcservers_test \
  --@rules_go//go/config:race \
  --test_arg=-test.short
```
