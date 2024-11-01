package table

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

const tempDir = "sst_test/"
func TestSSTSingleKey(t *testing.T){
	dir, _ := os.MkdirTemp("", tempDir)
	defer os.RemoveAll(dir)

	builder := NewSSTBuilder(300)
	keys := []string{"key1","abcdsd","keys2"}
	vals := []string{"124444","lopolop","3f!sf#f"}
	for i,k := range keys{
		builder.Add([]byte(k),[]byte(vals[i]))
	}
	
	filePath := dir + "1.sst"
	sst := builder.Build(0,filePath)
	require.Equal(t,"key1",string(sst.firstKey))
	require.Equal(t,"keys2",string(sst.lastKey))
}