package anchordb

import (
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