package table

import (
	"anchor-db/block"
	"hash/crc32"
)


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
	// add failed - no space left in block
	b.addBlock()
	b.firstKey = append([]byte{}, key...)
	b.lastKey = append([]byte{}, key...)
	if !b.blockBuilder.Add(key, value) {
		panic("failed to add key-value to new block after resetting")
	}
}

func (b *SSTBuilder) Build(tableId int,path string) *SSTable{
	b.addBlock()
	buf := b.data
	metaOff := uint32(len(buf))
	encoded := encodeBlockMetaData(b.blockMeta)
	buf = append(buf, encoded...)
	buf = append(buf, byte(metaOff))
	fileWrap,_ := CreateFileWrapper(path,buf)
	firstKey := b.blockMeta[0].firstKey
	lastKey := b.blockMeta[len(b.blockMeta)-1].lastKey
	return &SSTable{
		id: tableId,
		fileWrap: fileWrap,
		firstKey: firstKey,
		lastKey: lastKey,
		blockMeta: b.blockMeta,
		blockMetaOffset: metaOff,
	}
}

func (b *SSTBuilder) addBlock(){
	blk := b.blockBuilder.Build()
	encoded := blk.Encode()
	b.blockMeta = append(b.blockMeta, BlockMeta{
		offset: uint32(len(b.data)),
		firstKey: b.firstKey,
		lastKey: b.lastKey,
	})
	checksum := crc32.ChecksumIEEE(encoded)
	b.data = append(b.data, encoded...)
	b.data = append(b.data, byte(checksum))
	b.blockBuilder = block.NewBlockBuilder(b.blockSize)
}