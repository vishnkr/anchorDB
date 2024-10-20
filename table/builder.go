package table

import "anchor-db/block"


type SSTBuilder struct{
	blockBuilder *block.BlockBuilder
	blockSize int
	blockMeta []BlockMeta
	firstKey []byte
	lastKey []byte
	data []byte
}

func NewSSTBuilder(blockSize int) *SSTBuilder{
	return &SSTBuilder{
		blockBuilder: block.NewBlockBuilder(blockSize),
		blockSize: blockSize,
		blockMeta: make([]BlockMeta, 0),
		firstKey: make([]byte, 0),
		lastKey: make([]byte, 0),
		data: make([]byte, 0),
	}
}

func (b *SSTBuilder) Add(key []byte,value []byte) {
	if len(b.firstKey)==0{
		b.firstKey = b.firstKey[:0]
		b.firstKey = append(b.firstKey, key...)
	}
	if b.blockBuilder.Add(key,value){
		b.lastKey = b.lastKey[:0]
		b.lastKey = append(b.lastKey, key...)
		return
	}
}