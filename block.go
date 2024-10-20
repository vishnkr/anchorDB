package anchordb

import (
	"encoding/binary"
	"fmt"
)

/*
Block Encoding
----------------------------------------------------------------------------------------------------
|             Data Section             |              Offset Section             |      Extra      |
----------------------------------------------------------------------------------------------------
| Entry #1 | Entry #2 | ... | Entry #N | Offset #1 | Offset #2 | ... | Offset #N | num_of_elements |
----------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------
|                           Entry #1                            | ... |
-----------------------------------------------------------------------
| key_len (2B) | key (keylen) | value_len (2B) | value (varlen) | ... |
-----------------------------------------------------------------------
*/

type Block struct{
	data []byte
	offsets []uint16
}

const OFFSET_SIZE = 2

func (b *Block) encode() []byte{
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

func decode(data []byte) (*Block,error){
	entryOffsetsLen := binary.BigEndian.Uint16(data[len(data) - OFFSET_SIZE:])
	if len(data) < int(OFFSET_SIZE + (OFFSET_SIZE*entryOffsetsLen)){
		return nil,fmt.Errorf("data is corrupted")
	}

	offsetStart := len(data) - int(OFFSET_SIZE + (OFFSET_SIZE*entryOffsetsLen))
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


