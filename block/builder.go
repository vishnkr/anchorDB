package block

import "encoding/binary"

type BlockBuilder struct{
	offsets []uint16
	data []byte
	blockSize int
	firstKey []byte
}

func NewBlockBuilder(size int) *BlockBuilder{
	return &BlockBuilder{
		offsets: make([]uint16, 0),
		data: make([]byte, 0),
		blockSize: size,
		firstKey: make([]byte, 0),
	}
}


func (b *BlockBuilder) estimatedSize() int{
	// key value pair count in this block + offsets + data
	return OFFSET_SIZE +
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


func (b *BlockBuilder) Add(key []byte,value []byte) bool{
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