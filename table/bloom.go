package table

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"
)

type BloomFilter struct {
    filter []byte
    k      uint8
}

const (
    // CRC32 over (filter||k)
    bloomChecksumSize = 4
    // one byte to store k
    bloomKSize = 1
)

// BuildFromKeyHashes builds a Bloom filter from 32-bit key hashes.
func BuildFromKeyHashes(keys []uint32, falsePositiveRate float64) *BloomFilter {
    n := len(keys)
    // bits per key: m = -n ln(p) / (ln2)^2, then m/n
    m := -float64(n) * math.Log(falsePositiveRate) / (math.Ln2 * math.Ln2)
    bitsPerKey := int(math.Ceil(m / float64(n)))
    kf := int(math.Ceil(float64(bitsPerKey) * math.Ln2)) // optimal k
    if kf < 1 {
        kf = 1
    }
    if kf > 30 {
        kf = 30
    }
    k := uint8(kf)

    // ensure at least 64 bits
    totalBits := bitsPerKey * n
    if totalBits < 64 {
        totalBits = 64
    }
    totalBits = ((totalBits + 7) / 8) * 8 // round up to full bytes
    bytesLen := totalBits / 8

    filter := make([]byte, bytesLen)
    for _, h32 := range keys {
        // expand to 64-bit so delta arithmetic matches Rust
        h := uint64(h32)
        delta := (h >> 17) | (h << 15)
        for i := 0; i < int(k); i++ {
            pos := int(h % uint64(totalBits))
            filter[pos/8] |= 1 << (pos % 8)
            h += delta
        }
    }
    return &BloomFilter{filter: filter, k: k}
}

// Encode appends (filter||k||crc32(filter||k)) to dst and returns the new slice.
func (bf *BloomFilter) Encode(dst []byte) []byte {
    // append filter
    dst = append(dst, bf.filter...)
    // append k
    dst = append(dst, bf.k)
    // compute crc32 over (filter||k)
    cs := crc32.ChecksumIEEE(dst[len(dst)-len(bf.filter)-1:])
    // append 4-byte big-endian crc32
    tmp := make([]byte, 4)
    binary.BigEndian.PutUint32(tmp, cs)
    dst = append(dst, tmp...)
    return dst
}

// Decode verifies the CRC32 and returns a new BloomFilter or an error.
func DecodeBloom(buf []byte) (*BloomFilter, error) {
    if len(buf) < bloomChecksumSize+bloomKSize {
        return nil, fmt.Errorf("buffer too small for bloom")
    }
    // split out crc
    crcOff := len(buf) - bloomChecksumSize
    stored := binary.BigEndian.Uint32(buf[crcOff:])
    data := buf[:crcOff]
    // verify
    if crc32.ChecksumIEEE(data) != stored {
        return nil, fmt.Errorf("bloom checksum mismatch")
    }
    // k is the last byte of data
    k := data[len(data)-1]
    filter := make([]byte, len(data)-1)
    copy(filter, data[:len(data)-1])
    return &BloomFilter{filter: filter, k: k}, nil
}

// MayContain returns true if key (32-bit hash) may be in the set.
func (bf *BloomFilter) MayContain(hash32 uint32) bool {
    totalBits := len(bf.filter) * 8
    h := uint64(hash32)
    delta := (h >> 17) | (h << 15)
    for i := 0; i < int(bf.k); i++ {
        pos := int(h % uint64(totalBits))
        if (bf.filter[pos/8] & (1 << (pos % 8))) == 0 {
            return false
        }
        h += delta
    }
    return true
}
