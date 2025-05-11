package block

import "encoding/binary"

type BlockBuilder struct{
	offsets []uint16
	data []byte
	blockSize int
	firstKey []byte
	lastKey []byte
}

func (b *BlockBuilder) FirstKey() []byte{
	return b.firstKey
}

func (b *BlockBuilder) LastKey() []byte {
	return b.lastKey
}


func NewBlockBuilder(size int) *BlockBuilder{
	return &BlockBuilder{
		offsets: make([]uint16, 0),
		data: make([]byte, 0),
		blockSize: size,
		firstKey: make([]byte, 0),
		lastKey: make([]byte,0),
	}
}


func (b *BlockBuilder) estimatedSize() int{
	// size of offsets start pos u16
	return OFFSET_SIZE +
	// offsets for each entry
	len(b.offsets) * OFFSET_SIZE +
	len(b.data)
}

func (b *BlockBuilder) isEmpty() bool{
	return len(b.offsets)==0
}

func computeOverlap(firstKey, key []byte) int{
	i := 0
    for i < len(firstKey) && i < len(key) && firstKey[i] == key[i] {
        i++
    }
    return i
}

func appendU16(data []byte, value uint16) []byte {
    buf := make([]byte, 2)
    binary.BigEndian.PutUint16(buf, value)
    return append(data, buf...)
}


func (b *BlockBuilder) oldAdd(key []byte,value []byte) bool{
	if(len(key)==0){ return false}
	if b.estimatedSize() + len(key) + len(value) + 3*OFFSET_SIZE > b.blockSize && !b.isEmpty(){
		return false
	}

	b.offsets = append(b.offsets, uint16(len(b.data)))
	overlap:= computeOverlap(b.firstKey,key)
	b.data = appendU16(b.data, uint16(overlap))
	b.data = appendU16(b.data,uint16(len(key)-overlap))
	b.data = append(b.data,key[overlap:]...)
	b.data = appendU16(b.data,uint16(len(value)))
	b.data = append(b.data, value...)

	if len(b.firstKey) == 0{
		b.firstKey = make([]byte, len(key))
		copy(b.firstKey,key)
	}
	b.lastKey = b.lastKey[:0] // reuse memory
	b.lastKey = append(b.lastKey, key...) 
	return true
}

func (b *BlockBuilder) Build() Block{
	if b.isEmpty() {
        panic("block should not be empty")
    }
	return Block{
		data: b.data,
		offsets: b.offsets,
	}
}

func encodeVarint(x uint64) []byte{
	var buf []byte
    for x >= 0x80 {
        buf = append(buf, byte(x|0x80))
        x >>= 7
    }
    buf = append(buf, byte(x))
    return buf
}

func decodeVarint(data []byte) (uint64, int) {
    var x uint64
    var shift uint
    for i, b := range data {
        x |= uint64(b&0x7F) << shift
        if b&0x80 == 0 {
            return x, i + 1
        }
        shift += 7
    }
    panic("varint decoding failed")
}

func (b *BlockBuilder) Add(key []byte,value []byte) bool{
	if len(key)==0{
		return false
	}

	keyLenBytes := encodeVarint(uint64(len(key)))
	valueLenBytes := encodeVarint(uint64(len(value)))
	estimatedSize := b.estimatedSize() + len(key) + len(keyLenBytes) + len(value) + len(valueLenBytes) + OFFSET_SIZE
	if  estimatedSize > b.blockSize && !b.isEmpty(){
		return false
	}
	// max 64KB Block size for now
	b.offsets = append(b.offsets, uint16(len(b.data)))
	
	b.data = append(b.data, keyLenBytes...)
	b.data = append(b.data, key...)

	b.data = append(b.data, valueLenBytes...)
	b.data = append(b.data, value...)

	if len(b.firstKey) == 0 {
        b.firstKey = make([]byte, len(key))
        copy(b.firstKey, key)
    }
	b.lastKey = b.lastKey[:0]
	b.lastKey = append(b.lastKey, key...) 
	return true
}