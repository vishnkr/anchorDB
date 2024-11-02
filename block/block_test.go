package block

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)


func TestBlockEncodeDecode(t *testing.T) {
	originalBlock := &Block{
        data:    []byte("some key-value data"),
        offsets: []uint16{0, 5, 10},
    }

	encoded := originalBlock.Encode()
	decodedBlock, err := Decode(encoded)
    require.NoError(t, err)
	require.Equal(t, originalBlock.data, decodedBlock.data)
    require.Equal(t, originalBlock.offsets, decodedBlock.offsets)
}

func TestBlockEncodeEmptyBlock(t *testing.T) {
    emptyBlock := &Block{
        data:    []byte{},
        offsets: []uint16{},
    }

    encoded := emptyBlock.Encode()
    decodedBlock, err := Decode(encoded)
    require.NoError(t, err)

    require.Empty(t, decodedBlock.data)
    require.Empty(t, decodedBlock.offsets)
}

func TestBlockDecodeInvalidData(t *testing.T){
	invalidData := []byte{0x45,0x36,0x02}
	_, err := Decode(invalidData)
	require.Error(t,err)
}

func commonPrefixLength(a, b []byte) int {
    length := len(a)
    if len(b) < length {
        length = len(b)
    }
    for i := 0; i < length; i++ {
        if a[i] != b[i] {
            return i
        }
    }
    return length
}

func encodeTestData() []byte {
    var data []byte
    keys := [][]byte{
        []byte("apple"),
        []byte("apricot"),
        []byte("banana"),
    }
    values := [][]byte{
        []byte("value1"),
        []byte("value2"),
        []byte("value3"),
    }

    var prevKey []byte

    for i, key := range keys {
        var overlapLen int

        if prevKey != nil {
            overlapLen = commonPrefixLength(prevKey, key)
        }
        suffixKey := key[overlapLen:]

        entry := make([]byte, OFFSET_SIZE+OFFSET_SIZE+len(suffixKey))
        binary.BigEndian.PutUint16(entry[0:], uint16(overlapLen))
        binary.BigEndian.PutUint16(entry[OFFSET_SIZE:], uint16(len(suffixKey)))
        copy(entry[OFFSET_SIZE+OFFSET_SIZE:], suffixKey)                    

        value := values[i]
        valueLen := uint16(len(value))
        valueEncoded := make([]byte, OFFSET_SIZE+len(value))
        binary.BigEndian.PutUint16(valueEncoded, valueLen)
        copy(valueEncoded[OFFSET_SIZE:], value)                            

        data = append(data, entry...)
        data = append(data, valueEncoded...)

        prevKey = key 
    }

    return data
}

func TestBlockIterator(t *testing.T){
    bb := NewBlockBuilder(50)
    valid := bb.Add([]byte("apple"),[]byte("value1"))
    require.True(t,true,valid)
    valid = bb.Add([]byte("application"),[]byte{13,14,255})
    valid = bb.Add([]byte("apricot"),[]byte("val2"))
    valid = bb.Add([]byte("banana"),[]byte("value3"))
    
    block := bb.Build()
    encoded:= block.Encode()
    resBlock,err := Decode(encoded)
    require.NoError(t,err)
    iter := CreateBlockIterAndSeekToFirst(resBlock)
    require.True(t,iter.IsValid())
    require.Equal(t,[]byte("apple"),iter.Key())
    require.Equal(t,[]byte("value1"),iter.Value())
    iter.Next()
    require.True(t,iter.IsValid())
    require.Equal(t,[]byte("application"),iter.Key())
    require.Equal(t,[]byte{13,14,255},iter.Value())
    iter.Next()
    require.False(t,iter.IsValid())

}