package bench_test

import (
	"anchordb"
	"anchordb/bench"
	"fmt"
	"os"
	"runtime/pprof"
	"testing"
)

type BenchmarkCase struct {
	Name        string
	StorageOpts *anchordb.StorageOptions
}


func TestBenchmarkLSMDB(t *testing.T) {
	if err := os.MkdirAll("pprofs", os.ModePerm); err != nil {
		t.Fatal("could not create pprofs directory: ", err)
	}
	cases := []BenchmarkCase{
		{
			Name: "Default_4KB_NoBloom",
			StorageOpts: &anchordb.StorageOptions{
				EnableWal:        false,
				MaxMemTableCount: 2,
				BlockSize:        4096,
				TargetSstSize:    4 * 1024 * 1024,
				EnableBloomFilter: false,
			},
		},
		{
			Name: "HighWrite_16KB_NoBloom",
			StorageOpts: &anchordb.StorageOptions{
				EnableWal:        false,
				MaxMemTableCount: 16,
				BlockSize:        4 * 4096,
				TargetSstSize:    16 * 1024 * 1024,
				EnableBloomFilter: false,
			},
		},
		{
			Name: "Default_4KB",
			StorageOpts: &anchordb.StorageOptions{
				EnableWal:        false,
				MaxMemTableCount: 2,
				BlockSize:        4096,
				TargetSstSize:    4 * 1024 * 1024,
				EnableBloomFilter: true,
			},
		},
		{
			Name: "HighWrite_16KB",
			StorageOpts: &anchordb.StorageOptions{
				EnableWal:        false,
				MaxMemTableCount: 16,
				BlockSize:        4 * 4096,
				TargetSstSize:    16 * 1024 * 1024,
				EnableBloomFilter: true,
			},
		},

	}

	benchOpts := bench.BenchmarkOptions{
		NumKeys:     1_000_000,
		KeySize:     16,
		ValueSize:   64,
		ReadPercent: 50,
	}

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			f, err := os.Create(fmt.Sprintf("pprofs/%s.cpu.pprof", c.Name))
			if err != nil {
				t.Fatal("could not create CPU profile: ", err)
			}
			defer f.Close()

			if err := pprof.StartCPUProfile(f); err != nil {
				t.Fatal("could not start CPU profile: ", err)
			}
			defer pprof.StopCPUProfile()
			path := fmt.Sprintf("bench-data-%s", c.Name)
			db, err := anchordb.Open(path, c.StorageOpts)
			if err != nil {
				t.Fatalf("failed to open DB: %v", err)
			}
			defer os.RemoveAll(path)

			result, err := bench.RunBenchmark(db, benchOpts)
			if err != nil {
				t.Fatalf("benchmark failed: %v", err)
			}

			fmt.Printf("\n[%s] Benchmark Results:\n", c.Name)
			fmt.Printf("  Write: %.2fs (%.2f ops/sec)\n", result.WriteTime.Seconds(), result.WriteOpsPerSec)
			fmt.Printf("  Read:  %.2fs (%.2f ops/sec)\n", result.ReadTime.Seconds(), result.ReadOpsPerSec)
		})
	}
}
