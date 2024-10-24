package table

import (
	"anchor-db/block"
	"bytes"
	"encoding/binary"
	"io"
	"os"
)

const(
	BLOCK_OFFSET_SIZE = block.OFFSET_SIZE
	META_BLOCK_COUNT_SIZE = 4
	META_OFFSET_SIZE = 4
)

type BlockMeta struct{
	offset uint32
	firstKey []byte
	lastKey []byte
}


func calculateEstimatedBlockMetaSize(blockMeta []*BlockMeta) int{
	estSize := META_BLOCK_COUNT_SIZE
	for _,meta := range blockMeta{
		estSize += META_OFFSET_SIZE
		estSize += 2 + len(meta.firstKey)
		estSize += 2 + len(meta.lastKey)
	}
	return estSize
}
func encodeBlockMetaData(blockMeta []*BlockMeta)([]byte){
	estimatedSize := calculateEstimatedBlockMetaSize(blockMeta)
	buf := bytes.NewBuffer(make([]byte, estimatedSize))

	//TODO: handle errors
	binary.Write(buf,binary.BigEndian,uint32(len(blockMeta)))
	for _,meta := range blockMeta{
		binary.Write(buf,binary.BigEndian,meta.offset)
		binary.Write(buf,binary.BigEndian,uint16(len(meta.firstKey)))
		binary.Write(buf,binary.BigEndian,meta.firstKey)
		binary.Write(buf,binary.BigEndian,uint16(len(meta.lastKey)))
		binary.Write(buf,binary.BigEndian,meta.lastKey)
	}
	return buf.Bytes()
}

func decodeBlockMetaData(data []byte) []BlockMeta{
	buf := bytes.NewReader(data)
	var numEntries uint32
	_ = binary.Read(buf,binary.BigEndian,&numEntries)
	blockMeta := make([]BlockMeta,0,numEntries)
	//TODO: handle errors
	for i:=uint32(0);i<numEntries;i++ {
		var meta BlockMeta
		var firstKeyLen, lastKeyLen uint16
		binary.Read(buf,binary.BigEndian,&meta.offset)
		binary.Read(buf,binary.BigEndian,&firstKeyLen)
		meta.firstKey = make([]byte, firstKeyLen)
		binary.Read(buf,binary.BigEndian,&lastKeyLen)
		meta.lastKey = make([]byte, lastKeyLen)
		blockMeta = append(blockMeta, meta)
	}
	return blockMeta
}

/*
Sorted String Table Encoding
--------------------------------------------------------------------------------------------
|           Blocks          |              Meta                   |      Extra             |
-------------------------------------------------------------------------------------------
| Block #1 | ... | Block #N | Meta block #1 | ... | Meta block #N | meta block offset(u32) |
--------------------------------------------------------------------------------------------

*/

type SSTable struct{
	blockMeta []BlockMeta
	fileWrap FileWrapper
	firstKey []byte
	lastKey []byte
}

type FileWrapper struct{
	file *os.File
}

func (f *FileWrapper) size() int64 {
	stat,err := f.file.Stat()
	if err!=nil{
		//handle err
	}
	return stat.Size()
}


func OpenSSTable(id int,f FileWrapper) *SSTable{
	fSize := f.size()
	f.file.Seek(META_OFFSET_SIZE,io.SeekEnd)
	blockMetaOffsetBytes := make([]byte, META_OFFSET_SIZE)
	f.file.Read(blockMetaOffsetBytes)
	blockMetaOffsetValue := binary.BigEndian.Uint32(blockMetaOffsetBytes)
	f.file.Seek(int64(blockMetaOffsetValue),io.SeekStart)
	blockMetaOffsets := make([]byte, fSize-META_OFFSET_SIZE-int64(blockMetaOffsetValue))
	f.file.Read(blockMetaOffsets)
	
	blockMeta := decodeBlockMetaData(blockMetaOffsets)
	firstKey := blockMeta[0].firstKey
	lastKey := blockMeta[len(blockMeta)-1].lastKey
	//if err!=nil{ /*handle err*/}


	return &SSTable{
		blockMeta: blockMeta,
		fileWrap: f,
		firstKey: firstKey,
		lastKey: lastKey,
	}
}

func (s *SSTable) readBlock(idx uint)*block.Block{
	
	blockMeta := s.blockMeta[idx]
	var blockEndOffset uint = 0
	if idx+1<uint(len(s.blockMeta)){
		blockEndOffset = uint(s.blockMeta[idx+1].offset)
	} else { blockEndOffset = uint(blockMeta.offset)}
	
	blockLen := blockEndOffset - uint(blockMeta.offset) - META_OFFSET_SIZE
	blockData := make([]byte,blockLen)
	s.fileWrap.file.ReadAt(blockData,int64(blockLen))
	block, _ := block.Decode(blockData)
	return block
}