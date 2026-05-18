// repro_fetch_integrity is a standalone client for reproducing the
// "OutputDigestMismatchException" report from Bazel by issuing
// concurrent ByteStream.Read calls directly to a bb-storage instance.
//
// It deliberately does NOT use bb-remote-asset — the production report
// proves FetchBlob returns the correct digest, so this tool isolates
// the read path.
//
// Usage:
//
//	repro_fetch_integrity \
//	  --bb-storage-grpcs=bazel.staging.muonspace.com:443 \
//	  --digest=05188755...d3f7d7/265639459 \
//	  --use-compression \
//	  --concurrency=8 --iterations=20 \
//	  --auth-token-cmd='/path/to/credential-helper.sh'
//
// On any digest mismatch the tool prints the production-signal line
//
//	MISMATCH iter=... recv_bytes=... delta=... delta_mod_65536=...
//
// and exits with a non-zero status. A clean run prints the success
// count and exits 0.
package main

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/zstd"
	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

func main() {
	endpoint := flag.String("bb-storage-grpcs", "", "host:port of bb-storage gRPC endpoint (use grpcs:// scheme implicitly)")
	insecureFlag := flag.Bool("insecure", false, "disable TLS — for local testing only")
	digestFlag := flag.String("digest", "", "sha256/size, e.g. 05188755…d3f7d7/265639459")
	useCompression := flag.Bool("use-compression", true, "read compressed-blobs/zstd/D/S (default) or blobs/D/S")
	concurrency := flag.Int("concurrency", 1, "parallel ByteStream.Read calls per iteration")
	iterations := flag.Int("iterations", 1, "total iterations (across all concurrency)")
	authTokenCmd := flag.String("auth-token-cmd", "", "shell command that prints a bearer token on stdout; sent as 'authorization: Bearer <token>' metadata")
	timeoutSec := flag.Int("timeout-seconds", 600, "per-read timeout")
	instanceName := flag.String("instance-name", "", "REAPI instance name (default empty)")
	flag.Parse()

	if *endpoint == "" || *digestFlag == "" {
		log.Fatal("--bb-storage-grpcs and --digest are required")
	}
	parts := strings.SplitN(*digestFlag, "/", 2)
	if len(parts) != 2 {
		log.Fatalf("--digest must be sha256/size, got %q", *digestFlag)
	}
	wantSha := parts[0]
	wantSize, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		log.Fatalf("--digest size: %v", err)
	}

	var dialOpts []grpc.DialOption
	if *insecureFlag {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{})))
	}
	dialOpts = append(dialOpts, grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(64<<20)))

	conn, err := grpc.NewClient(*endpoint, dialOpts...)
	if err != nil {
		log.Fatalf("dial %s: %v", *endpoint, err)
	}
	defer conn.Close()
	client := bytestream.NewByteStreamClient(conn)

	resource := buildResource(*instanceName, wantSha, wantSize, *useCompression)
	log.Printf("resource=%s concurrency=%d iterations=%d", resource, *concurrency, *iterations)

	var success, failure atomic.Int64
	var wg sync.WaitGroup
	work := make(chan int, *iterations)
	for i := 0; i < *iterations; i++ {
		work <- i
	}
	close(work)

	for w := 0; w < *concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range work {
				ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*timeoutSec)*time.Second)
				if *authTokenCmd != "" {
					tok, err := runAuthCmd(*authTokenCmd)
					if err != nil {
						log.Printf("iter=%d auth: %v", i, err)
						failure.Add(1)
						cancel()
						continue
					}
					ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+tok)
				}
				ok := doOne(ctx, client, resource, wantSha, wantSize, *useCompression, i)
				cancel()
				if ok {
					success.Add(1)
				} else {
					failure.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	log.Printf("DONE success=%d failure=%d", success.Load(), failure.Load())
	if failure.Load() > 0 {
		os.Exit(1)
	}
}

func buildResource(instance, sha string, size int64, compressed bool) string {
	prefix := ""
	if instance != "" {
		prefix = instance + "/"
	}
	if compressed {
		return fmt.Sprintf("%scompressed-blobs/zstd/%s/%d", prefix, sha, size)
	}
	return fmt.Sprintf("%sblobs/%s/%d", prefix, sha, size)
}

func runAuthCmd(cmd string) (string, error) {
	out, err := exec.Command("sh", "-c", cmd).Output()
	if err != nil {
		return "", fmt.Errorf("auth-token-cmd: %w", err)
	}
	return strings.TrimSpace(string(out)), nil
}

// doOne returns true on a clean digest match, false on any failure
// (gRPC error, decompression error, or digest mismatch). On mismatch
// it prints the production-signal line with the byte diff modulo
// 65 536 so a single grep can confirm the report.
func doOne(ctx context.Context, client bytestream.ByteStreamClient, resource, wantSha string, wantSize int64, compressed bool, iter int) bool {
	stream, err := client.Read(ctx, &bytestream.ReadRequest{ResourceName: resource})
	if err != nil {
		log.Printf("iter=%d open: %v", iter, err)
		return false
	}
	var raw []byte
	chunks := 0
	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			log.Printf("iter=%d recv (after %d chunks, %d bytes): %v", iter, chunks, len(raw), err)
			return false
		}
		raw = append(raw, resp.Data...)
		chunks++
	}
	plain := raw
	if compressed {
		dec, err := zstd.NewReader(nil)
		if err != nil {
			log.Printf("iter=%d new decoder: %v", iter, err)
			return false
		}
		defer dec.Close()
		plain, err = dec.DecodeAll(raw, nil)
		if err != nil {
			delta := int64(len(raw))
			log.Printf("MISMATCH iter=%d kind=decode_error recv_bytes=%d delta=%d delta_mod_65536=%d err=%v",
				iter, len(raw), delta, delta%65536, err)
			return false
		}
	}
	sum := sha256.Sum256(plain)
	gotSha := hex.EncodeToString(sum[:])
	if gotSha == wantSha && int64(len(plain)) == wantSize {
		log.Printf("OK iter=%d chunks=%d compressed_bytes=%d decompressed_bytes=%d", iter, chunks, len(raw), len(plain))
		return true
	}
	delta := int64(len(plain)) - wantSize
	log.Printf("MISMATCH iter=%d kind=digest chunks=%d compressed_bytes=%d decompressed_bytes=%d want_sha=%s got_sha=%s want_size=%d got_size=%d delta=%d delta_mod_65536=%d",
		iter, chunks, len(raw), len(plain), wantSha, gotSha, wantSize, len(plain), delta, delta%65536)
	return false
}
