package anchordb

import (
	"fmt"
	"math/rand"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

const tempDir = "db_test"
const charBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"

func genRandString(n int) string {
    b := make([]byte, n)
    for i := range b {
        b[i] = charBytes[rand.Int63() % int64(len(charBytes))]
    }
    return string(b)
}


func TestWriteFile(t *testing.T){
	f,err := os.Create("data/testfile")
	if err != nil {
		fmt.Println("Error creating test file:", err)
	} else {
		fmt.Println("Test file written successfully")
	}
	_,err = f.Write([]byte("hello"))

	if err != nil {
		fmt.Println("Error writing test file:", err)
	} else {
		fmt.Println("Test file written successfully")
	}
}
func TestReadWriteMemtables(t *testing.T) {
	numKeys := 100000
	var k,v string
	//require.NoError(t,err)
	db,err := Open("data")
	require.NoError(t,err)
	keys := make([]string,numKeys)
	for i:=0;i<numKeys;i++{
		k = fmt.Sprintf("key-%d",i) //genRandString(5)
		keys[i] = k
		v = "Value_"+ fmt.Sprint(i)
		db.Put(k,[]byte(v))
	}
	for i:=0;i<numKeys;i++{
		k = keys[i]
		fmt.Printf("Value %d is %s\n",i,string(db.Get(k)))
	}
}


func TestReadWriteSST(t *testing.T) {
	numKeys := 200
	var k,v string
	//require.NoError(t,err)
	db,err := Open("data")
	require.NoError(t,err)
	keys := make([]string,numKeys)
	for i:=0;i<numKeys;i++{
		k = fmt.Sprintf("key-%d",i) //genRandString(5)
		keys[i] = k
		v = "Value_"+ fmt.Sprint(i)
		db.Put(k,[]byte(v))
	}
	for i:=0;i<numKeys;i++{
		k = keys[i]
		fmt.Printf("Value %d is %s\n",i,string(db.Get(k)))
	}
}