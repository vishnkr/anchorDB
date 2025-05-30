package table

import (
	"anchordb/block"
	"encoding/binary"
	"fmt"
	"hash/crc32"
)


type SSTBuilder struct{
	blockBuilder *block.BlockBuilder
	blockSize int
	blockMeta []BlockMeta
	firstKey []byte
	lastKey []byte
	data []byte
	keyHashes []uint32
}

func NewSSTBuilder(blockSize int) *SSTBuilder{
	return &SSTBuilder{
		blockBuilder: block.NewBlockBuilder(blockSize),
		blockSize: blockSize,
		blockMeta: make([]BlockMeta, 0),
		firstKey: make([]byte, 0),
		//lastKey: make([]byte, 0),
		data: make([]byte, 0),
	}
}

func hashKey(key []byte)uint32{
	return crc32.ChecksumIEEE(key)
}

func (b *SSTBuilder) Add(key []byte,value []byte) {
	b.keyHashes = append(b.keyHashes, hashKey(key))
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
	b.addBlockToSST()
	b.firstKey = append([]byte{}, key...)
	b.lastKey = append([]byte{}, key...)
	if !b.blockBuilder.Add(key, value) {
		panic("failed to add key-value to new block after resetting")
	}
}

func (b *SSTBuilder) Build(tableId int,path string) *SSTable{
	b.addBlockToSST()
	buf := b.data
	metaOffset := uint32(len(buf))
	encodedMetaData := encodeBlockMetaData(b.blockMeta)
	buf = append(buf, encodedMetaData...)
	buf = append(buf, byte(metaOffset))
	bf := BuildFromKeyHashes(b.keyHashes,0.01)
	//start := len(buf)
	buf = bf.Encode(buf)
	/*fmt.Printf(" Bloom written: crc32=%08x, len=%d\n",
    binary.BigEndian.Uint32(buf[start+len(bf.filter):start+len(bf.filter)+4]),
    len(buf)-start,
	)*/

	fileWrap,err := CreateFileWrapper(path,buf)
	if err!=nil{
		fmt.Printf("err : %s",err.Error())
	}
	firstKey := b.blockMeta[0].firstKey
	lastKey := b.blockMeta[len(b.blockMeta)-1].lastKey
	return &SSTable{
		Id: tableId,
		fileWrap: fileWrap,
		firstKey: firstKey,
		lastKey: lastKey,
		blockMeta: b.blockMeta,
		blockMetaOffset: metaOffset,
		BloomFilter: *bf,
	}
}

func (b *SSTBuilder) addBlockToSST(){
	blk := b.blockBuilder.Build()
	encoded := blk.Encode()
	b.blockMeta = append(b.blockMeta, BlockMeta{
		offset: uint32(len(b.data)),
		firstKey: b.blockBuilder.FirstKey(),
		lastKey: b.blockBuilder.LastKey(),
	})
	checksum := crc32.ChecksumIEEE(encoded)
	var checksumBuf [4]byte
	binary.BigEndian.PutUint32(checksumBuf[:], checksum)
	b.data = append(b.data, encoded...)
	b.data = append(b.data, checksumBuf[:]...)
	b.blockBuilder = block.NewBlockBuilder(b.blockSize)
}