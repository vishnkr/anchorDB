package block

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

/*
Block Encoding
------------------------------------------------------------------------------------
|             Data             |              Offset             |      Extra      |
------------------------------------------------------------------------------------
| Entry #1 |  ...   | Entry #N | Offset #1 |   ...   | Offset #N | num_of_elements |
------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------
|                           Entry #1                                        | ... |
-----------------------------------------------------------------------------------
| overlap_len(2B) | key_len (2B) | key (len) | value_len (2B) | value (len) | ... |
-----------------------------------------------------------------------------------
*/

type Block struct{
	data []byte
	offsets []uint16
}

const OFFSET_SIZE = 2

func (b *Block) Encode() []byte{
	offlen := len(b.offsets)
	buf := make([]byte, len(b.data))
	copy(buf, b.data)
	for _,offset := range b.offsets{
		offsetBytes := make([]byte,OFFSET_SIZE)
		binary.BigEndian.PutUint16(offsetBytes, offset)
		buf = append(buf, offsetBytes...)
	}

	offlenBytes := make([]byte,OFFSET_SIZE)
	binary.BigEndian.PutUint16(offlenBytes,uint16(offlen))
	buf = append(buf, offlenBytes...)
	return buf
}

func Decode(data []byte) (*Block,error){
	offsetCount := binary.BigEndian.Uint16(data[len(data) - OFFSET_SIZE:])
	offsetStart := len(data) - int(OFFSET_SIZE + (OFFSET_SIZE*offsetCount))
	offsetData := data[offsetStart:len(data)-OFFSET_SIZE]
	var offsets []uint16
	for i:=0;i<len(offsetData);i+=OFFSET_SIZE{
		offset := binary.BigEndian.Uint16(offsetData[i:i+OFFSET_SIZE])
		offsets = append(offsets, offset)
	}

	blockData := data[:offsetStart]
	return &Block{
		data: blockData,
		offsets: offsets,
	},nil
} 

func (b *Block) getFirstKey() ([]byte,error){

	keyLen, n := decodeVarint(b.data)
	if n == 0 {
        return nil, fmt.Errorf("failed to decode key length")
    }
	buf := bytes.NewReader(b.data[n:])
	key := make([]byte, keyLen)
    _, err := buf.Read(key)
    if err != nil {
        return nil, fmt.Errorf("failed to read key: %w", err)
    }

    return key, nil
}

type BlockIterator struct{
	block	*Block
	key []byte
	valueRange [2]int
	idx int
	firstKey []byte
}

func NewBlockIterator(block *Block) *BlockIterator{
	iter := &BlockIterator{
		block: block,
		idx: 0,
	}
	iter.firstKey,_ = block.getFirstKey()
	return iter
}

func CreateBlockIterAndSeekToFirst(block *Block) *BlockIterator {
    iter := NewBlockIterator(block)
    iter.SeekToFirst()
    return iter
}

func CreateBlockIterAndSeekToKey(block *Block, key []byte) *BlockIterator {
    iter := NewBlockIterator(block)
    iter.SeekToKey(key)
    return iter
}

func (bi *BlockIterator) Next() error{
	bi.idx++
	bi.SeekTo(bi.idx)
	return nil
}

func (bi *BlockIterator) SeekTo(idx int){
	if idx >= len(bi.block.offsets){
		bi.key = nil
		bi.valueRange = [2]int{0,0}
		return
	}
	offset := bi.block.offsets[idx]
	bi.SeekToOffset(int(offset))
	bi.idx = idx
}

func (bi *BlockIterator) SeekToOffset(offset int){
	buf := bytes.NewReader(bi.block.data[offset:])
	keyLen, n := decodeVarint(bi.block.data[offset:])
	if n == 0 {
		bi.key = nil
		bi.valueRange = [2]int{0, 0}
		return
	}
	offset += n

	key := make([]byte, keyLen)
	_, err := buf.ReadAt(key, int64(n))
	if err != nil {
		bi.key = nil
		bi.valueRange = [2]int{0, 0}
		return
	}
	bi.key = key
	offset += int(keyLen)
	valueLen, n := decodeVarint(bi.block.data[offset:])
	if n == 0 {
		bi.key = nil
		bi.valueRange = [2]int{0, 0}
		return
	}
	offset += n
	bi.valueRange = [2]int{offset, offset + int(valueLen)}
	offset += int(valueLen)
}

func (bi *BlockIterator) SeekToFirst() {
    bi.SeekTo(0)
}

func (bi *BlockIterator) IsValid() bool{
	return len(bi.key)!=0
}

func (bi *BlockIterator) SeekToKey(key []byte){
	low, high:= 0, len(bi.block.offsets)-1
	for low < high{
		mid := (low + (high-low))/2
		bi.SeekTo(mid)

		switch bytes.Compare(bi.Key(),key){
		case -1: // bi.key < key
			low = mid +1
		case 1: // bi.key > key
			high = mid
		case 0:
			break
		}
	}
	bi.SeekTo(low)

}

func (bi *BlockIterator) Value() []byte{
	if len(bi.key)==0{
		//return nil,fmt.Errorf("invalid iterator")
		panic("invalid iterator")
	}
	return bi.block.data[bi.valueRange[0]:bi.valueRange[1]]
}

func (bi *BlockIterator) Key() []byte{
	if len(bi.key)==0{
		//return nil,fmt.Errorf("invalid iterator")
		panic("invalid iterator")
	}
	return bi.key
}

