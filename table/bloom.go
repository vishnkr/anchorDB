package table

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"
)

const (
	BLOOM_FILTER_CHECKSUM_SIZE = 4
	HASH_FUNCTIONS_SIZE	 = 1
)
type BloomFilter struct{
	filter []byte
	k uint8
}

func setBit(data []byte, idx int, isSet bool) {
    pos := idx / 8
    offset := uint(idx % 8)
    if isSet {
        data[pos] |= 1 << offset
    } else {
        data[pos] &^= 1 << offset
    }
}

func getBit(data []byte, idx int) bool{
	pos := idx/8
	offset := uint(idx%8)
	return data[pos] & (1 << offset) !=0
}

func bitsPerKey(entries int, falsePositiveRate float64) int {
	// Calculate the size of the Bloom filter in bits
	size := -1.0 * float64(entries) * math.Log(falsePositiveRate) / math.Pow(math.Ln2, 2)
	// Calculate the number of bits per key, rounding up
	bitsPerKey := math.Ceil(size / float64(entries))
	return int(bitsPerKey)
}

// Calculate the optimal number of hash functions.
func calculateHashFunctions(bitsPerKey, numKeys int) int {
    // k = (m / n) * ln(2)
    return int(math.Ceil(float64(bitsPerKey) * math.Ln2))
}

func generateFromKeyHashes(keys []uint32,bitsPerKey int) *BloomFilter{
	k := uint8(math.Max(1,math.Min(30,float64(bitsPerKey)*0.69)))
	nbits := int(math.Max(64,float64(len(keys)*bitsPerKey)))
	bytes := (nbits + 7)/ 8
	bloom := make([]byte,bytes)
	for _,h := range keys{
		d := (h >> 17) | (h >> 15)
		for i:= 0; i< int(k); i++{
			pos := int(h) % nbits
			setBit(bloom,pos,true)
			h += d
		}
	}
	return &BloomFilter{
		filter: bloom,
		k: k,
	}
}

func Decode(buf []byte) (*BloomFilter,error){
	if len(buf) < BLOOM_FILTER_CHECKSUM_SIZE + 1 {
        return nil, fmt.Errorf("buffer too small")
    }
    
	bloomCheckSum := binary.BigEndian.Uint32(buf[len(buf)-BLOOM_FILTER_CHECKSUM_SIZE:])
	data := buf[:len(buf)-BLOOM_FILTER_CHECKSUM_SIZE]
	if crc32.ChecksumIEEE(data)!=bloomCheckSum{
		return nil, fmt.Errorf("checksum mismatch")
	}
	bloom := data[:len(data)-HASH_FUNCTIONS_SIZE]
	k := data[len(data)-HASH_FUNCTIONS_SIZE]
	return &BloomFilter{
		filter: bloom,
		k: k,
	},nil
}

func (b *BloomFilter) Encode(buf *bytes.Buffer){
	buf.Write(b.filter)
	buf.WriteByte(b.k)
	checkSum := crc32.ChecksumIEEE(buf.Bytes())
	binary.Write(buf,binary.BigEndian,checkSum)
}