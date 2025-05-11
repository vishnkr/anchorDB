package bench

import (
	crand "crypto/rand"
	"fmt"
	"math/rand"
	"time"
)

type KV struct {
	Key   []byte
	Value []byte
}

type DB interface {
	Put(key, value []byte) error
	Get(key []byte) ([]byte, error)
}

// BenchmarkOptions allows you to configure test parameters.
type BenchmarkOptions struct {
	NumKeys     int
	KeySize     int
	ValueSize   int
	ReadPercent int // Percentage of keys to read after writing
}

// Result captures benchmark performance numbers.
type Result struct {
	WriteTime     time.Duration
	ReadTime      time.Duration
	WriteOpsPerSec float64
	ReadOpsPerSec  float64
}

func randomBytes(n int) []byte {
	b := make([]byte, n)
	crand.Read(b)
	return b
}

// RunBenchmark runs the full benchmark and returns performance stats.
func RunBenchmark(db DB, opts BenchmarkOptions) (*Result, error) {
	keys := make([][]byte, opts.NumKeys)
	val := randomBytes(opts.ValueSize)

	fmt.Printf("📦 Benchmarking %d writes (%dB keys, %dB values)...\n", opts.NumKeys, opts.KeySize, opts.ValueSize)
	startWrite := time.Now()
	for i := 0; i < opts.NumKeys; i++ {
		k := randomBytes(opts.KeySize)
		keys[i] = k
		if err := db.Put(k, val); err != nil {
			return nil, fmt.Errorf("put error: %w", err)
		}
	}
	writeDur := time.Since(startWrite)

	readCount := opts.NumKeys * opts.ReadPercent / 100
	fmt.Printf("🔍 Benchmarking %d reads (%d%% of keys)...\n", readCount, opts.ReadPercent)
	startRead := time.Now()
	for i := 0; i < readCount; i++ {
		k := keys[rand.Intn(len(keys))]
		if _, err := db.Get(k); err != nil {
			return nil, fmt.Errorf("get error: %w", err)
		}
	}
	readDur := time.Since(startRead)

	return &Result{
		WriteTime:      writeDur,
		ReadTime:       readDur,
		WriteOpsPerSec: float64(opts.NumKeys) / writeDur.Seconds(),
		ReadOpsPerSec:  float64(readCount) / readDur.Seconds(),
	}, nil
}
