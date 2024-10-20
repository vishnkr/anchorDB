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

	encoded := originalBlock.encode()
	decodedBlock, err := decode(encoded)
    require.NoError(t, err)
	require.Equal(t, originalBlock.data, decodedBlock.data)
    require.Equal(t, originalBlock.offsets, decodedBlock.offsets)
}

func TestBlockEncodeEmptyBlock(t *testing.T) {
    emptyBlock := &Block{
        data:    []byte{},
        offsets: []uint16{},
    }

    encoded := emptyBlock.encode()
    decodedBlock, err := decode(encoded)
    require.NoError(t, err)

    require.Empty(t, decodedBlock.data)
    require.Empty(t, decodedBlock.offsets)
}

func TestBlockDecodeInvalidData(t *testing.T){
	invalidData := []byte{0x45,0x36,0x02}
	_, err := decode(invalidData)
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
    bb := newBlockBuilder(2000)
    bb.Add([]byte("apple"),[]byte("value1"))
    bb.Add([]byte("apricot"),[]byte("val2"))
    bb.Add([]byte("banana"),[]byte("value3"))
    block := bb.Build()
    /*testData := encodeTestData()
    block := Block{ data: testData}*/
    encoded:= block.encode()
    resBlock,err := decode(encoded)
    require.NoError(t,err)
    iter := createAndSeekToFirst(resBlock)
    require.True(t,iter.IsValid())
    require.Equal(t,iter.Key(),[]byte("apple"))
    require.Equal(t,iter.Value(),[]byte("value1"))
    iter.Next()
    require.True(t,iter.IsValid())
    require.Equal(t,iter.Key(),[]byte("apricot"))
    require.Equal(t,iter.Value(),[]byte("val2"))
    iter.Next()
    require.True(t,iter.IsValid())
    require.Equal(t,iter.Key(),[]byte("apricot"))
    require.Equal(t,iter.Value(),[]byte("val2"))

}