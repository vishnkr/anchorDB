package table

import (
	"anchordb/block"
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"sort"
)

const(
	BLOCK_OFFSET_SIZE = block.OFFSET_SIZE
	META_BLOCK_COUNT_SIZE = 4
	META_OFFSET_SIZE = 4
	KEY_LENGTH_SIZE = 2
)

type Key []byte

/*
Sorted String Table Encoding
--------------------------------------------------------------------------------------------
|           Blocks          |              Meta                   |      Extra             |
-------------------------------------------------------------------------------------------
| Block #1 | ... | Block #N | Meta block #1 | ... | Meta block #N | meta block offset(u32) |
--------------------------------------------------------------------------------------------
*/

type BlockMeta struct{
	offset uint32
	firstKey []byte
	lastKey []byte
}

type SSTable struct{
	id int
	blockMeta []BlockMeta
	blockMetaOffset uint32
	fileWrap *FileWrapper
	firstKey []byte
	lastKey []byte
}

type SSTIterator struct{
	sst *SSTable
	blockIdx int
	blockIter *block.BlockIterator
}

type LevelIterator struct{
	levelSSTs []*SSTable
	sstIter *SSTIterator
	curIdx int
}

func (s *SSTable) GetFirstKey()[]byte{
	return s.firstKey
}

func (s *SSTable) GetLastKey()[]byte{
	return s.lastKey
}

func calculateEstimatedBlockMetaSize(blockMeta []BlockMeta) int{
	estSize := META_BLOCK_COUNT_SIZE
	for _,meta := range blockMeta{
		estSize += META_OFFSET_SIZE
		estSize += KEY_LENGTH_SIZE + len(meta.firstKey)
		//estSize += 2 + len(meta.lastKey)
	}
	return estSize
}
func encodeBlockMetaData(blockMeta []BlockMeta)([]byte){
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
		var firstKeyLen uint16
		binary.Read(buf,binary.BigEndian,&meta.offset)
		binary.Read(buf,binary.BigEndian,&firstKeyLen)
		meta.firstKey = make([]byte, firstKeyLen)
		//binary.Read(buf,binary.BigEndian,&lastKeyLen)
		//meta.lastKey = make([]byte, lastKeyLen)
		blockMeta = append(blockMeta, meta)
	}
	return blockMeta
}

func OpenSSTable(id int,f *FileWrapper) *SSTable{
	
	blockMetaOffsetBytes := f.ReadAt(f.size-META_OFFSET_SIZE,META_OFFSET_SIZE)
	blockMetaOffsetValue := binary.BigEndian.Uint32(blockMetaOffsetBytes)
	metaSize := int(f.size - META_OFFSET_SIZE - int64(blockMetaOffsetValue))
	blockMetaOffsets := f.ReadAt(int64(blockMetaOffsetValue),metaSize)
	blockMeta := decodeBlockMetaData(blockMetaOffsets)
	firstKey := blockMeta[0].firstKey
	lastKey := blockMeta[len(blockMeta)-1].lastKey

	return &SSTable{
		id: id,
		blockMeta: blockMeta,
		fileWrap: f,
		firstKey: firstKey,
		lastKey: lastKey,
	}
}

func (s *SSTable) getBlockCount() int{
	return len(s.blockMeta)
}

func (s *SSTable) readBlock(blockIdx int)*block.Block{
	blockMeta := s.blockMeta[blockIdx]
	var blockEndOffset uint32 = 0
	if blockIdx+1 < len(s.blockMeta) {
		blockEndOffset = s.blockMeta[blockIdx+1].offset
	} else {
		blockEndOffset = s.blockMetaOffset // Last block ends at block meta offset
	}
	//fmt.Printf("BLCK ENDOFF IS %d, METAOFF IS %d,\n",blockEndOffset,blockMeta.offset)
	
	blockLen := int(blockEndOffset - blockMeta.offset - META_OFFSET_SIZE)

	if blockLen <= 0 {
		fmt.Printf("invalid block length")
		return nil
	}
	blockDataWithChecksum := make([]byte, blockEndOffset-blockMeta.offset)
	_, err := s.fileWrap.file.ReadAt(blockDataWithChecksum, int64(blockMeta.offset))
	if err != nil {
		fmt.Printf(err.Error())
		return nil
	}
	blockData := blockDataWithChecksum[:blockLen]
	checksum := binary.BigEndian.Uint32(blockDataWithChecksum[blockLen:])
	if checksum != crc32.ChecksumIEEE(blockData) {
		fmt.Printf("block checksum mismatched")
		return nil
	}

	block, _ := block.Decode(blockData)
	return block
}

func (s *SSTable) getBlockIdx(key []byte) int{
	idx:= sort.Search(len(s.blockMeta),func (i int) bool{
		return bytes.Compare(s.blockMeta[i].firstKey,key) > 0
	})
	if idx == 0 {
		return 0
	}
	return idx - 1
}

func SeekToKeyBlock(sst *SSTable,key []byte) (*block.BlockIterator,int){
	blockIdx := sst.getBlockIdx(key)
	blk := sst.readBlock(blockIdx)
	blockIter := block.CreateBlockIterAndSeekToKey(blk,key)
	if !blockIter.IsValid(){
		blockIdx+=1
		if blockIdx<= sst.getBlockCount(){
			blk = sst.readBlock(blockIdx)
			blockIter = block.CreateBlockIterAndSeekToFirst(blk)
		}
	}
	return blockIter,blockIdx
}

func SeekToFirstBlock(sst *SSTable) *block.BlockIterator{
	blk := sst.readBlock(0)
	return block.CreateBlockIterAndSeekToFirst(blk)
}

func (si *SSTIterator) SeekToFirst(){
	iter := SeekToFirstBlock(si.sst)
	si.blockIdx = 0
	si.blockIter = iter
}

func (si *SSTIterator) SeekToKey(key []byte){
	iter, idx := SeekToKeyBlock(si.sst,key)
	si.blockIdx = idx
	si.blockIter = iter
}

func CreateSSTIterAndSeekToKey(sst *SSTable, key []byte) *SSTIterator{
	blockIter, blockIdx := SeekToKeyBlock(sst,key)
	return &SSTIterator{
		sst,
		blockIdx,
		blockIter,
	}
}

func CreateSSTIterAndSeekToFirst(sst *SSTable)*SSTIterator{
	iter := SeekToFirstBlock(sst)
	return &SSTIterator{
		sst:sst,
		blockIdx:0,
		blockIter: iter,
	}

}

// StorageIterator interface implementation for SSTIterator
func (si *SSTIterator) Next() error{
	si.blockIter.Next()
	if !si.blockIter.IsValid(){
		si.blockIdx += 1
		if si.blockIdx < si.sst.getBlockCount(){
			blk := si.sst.readBlock(si.blockIdx)
			si.blockIter = block.CreateBlockIterAndSeekToFirst(blk)
		}
	}
	return nil
}

func (si *SSTIterator) IsValid() bool {
	return si.blockIter.IsValid()
}

func (si *SSTIterator) Key() []byte{
	return si.blockIter.Key()
}

func (si *SSTIterator) Value() []byte{
	return si.blockIter.Value()
}

func checkLevelValidity(level []*SSTable){
	for i,sst := range level{
		if(bytes.Compare(sst.firstKey,sst.lastKey) <= 0){ 
			panic(fmt.Sprintf("invalid SST ordering in SSTable at index %d: firstKey (%v) should not be greater than lastKey (%v)", 
                i, sst.firstKey, sst.lastKey))
		}
	}
	
	for i:=0;i<len(level)-1;i++{
		if(bytes.Compare(level[i].lastKey,level[i+1].firstKey) <= 0){ 
			panic(fmt.Sprintf("invalid SST ordering between SSTable at index %d and SSTable at index %d: lastKey (%v) of first SSTable is greater than firstKey (%v) of second SSTable", 
                i, i+1, level[i].lastKey, level[i+1].firstKey))
		}
	}
} 

func CreateLevelIterAndSeekToKey(level []*SSTable,key []byte) *LevelIterator{
	checkLevelValidity(level)
	
	idx := sort.Search(len(level),func (i int) bool{
		return bytes.Compare(level[i].firstKey,key) <= 0
	})
	if(idx>=len(level)){
		return &LevelIterator{
			levelSSTs: level,
			sstIter: nil,
			curIdx: len(level)-1,
		}
	}

	l:= &LevelIterator{
		levelSSTs: level,
		sstIter: CreateSSTIterAndSeekToKey(level[idx],key),
		curIdx: idx,
	}
	l.Next()
	return l
}

func CreateLevelIterAndSeekToFirst(level []*SSTable) *LevelIterator{
	checkLevelValidity(level)
	if len(level)==0{
		return &LevelIterator{
			curIdx: -1,
			sstIter: nil,
		}
	}
	
	l:= &LevelIterator{
		levelSSTs: level,
		sstIter: CreateSSTIterAndSeekToFirst(level[0]),
		curIdx: 0,
	}
	l.Next()
	return l
}

// StorageIterator interface implementation for SSTLevelIterator
func (l *LevelIterator) Next() error{
	for l.sstIter!=nil {
		if l.IsValid(){ break }
		if l.curIdx+1 >= len(l.levelSSTs){
			l.sstIter = nil
		} else {
			l.curIdx+=1
			l.sstIter = CreateSSTIterAndSeekToFirst(l.levelSSTs[l.curIdx])
		}
	}
	return nil
}

func (l *LevelIterator) IsValid() bool{
	return l.sstIter!=nil && l.sstIter.IsValid()
}

func (l *LevelIterator) Key() []byte{
	return l.sstIter.Key()
}

func (l *LevelIterator) Value() []byte{
	return l.sstIter.Value()
}