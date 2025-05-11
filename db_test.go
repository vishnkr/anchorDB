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
	numKeys := 1000000
	var k,v string
	//require.NoError(t,err)
	db,err := Open("data")
	require.NoError(t,err)
	keys := make([]string,numKeys)
	const longString = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum. " +
    "Curabitur pretium tincidunt lacus. Nulla gravida orci a odio. Nullam varius, turpis et commodo pharetra, est eros bibendum elit, nec luctus magna felis sollicitudin mauris. Integer in mauris eu nibh euismod gravida. Duis ac tellus et risus vulputate vehicula. Donec lobortis risus a elit. Etiam tempor. Ut ullamcorper, ligula eu tempor congue, eros est euismod turpis, id tincidunt sapien risus a quam. Maecenas fermentum consequat mi. Donec fermentum. Pellentesque malesuada nulla a mi. Duis sapien sem, aliquet nec, commodo eget, consequat quis, neque. Aliquam faucibus, elit ut dictum aliquet, felis nisl adipiscing sapien, sed malesuada diam lacus eget erat.";

	for i:=0;i<numKeys;i++{
		k = fmt.Sprintf("key-%d",i) //genRandString(5)
		keys[i] = k
		v = "Value_"+ fmt.Sprint(i)//+longString
		db.Put(k,[]byte(v))
	}
	fmt.Println("write done")
	for i:=0;i<numKeys;i++{
		k = keys[i]
		key,err := db.Get(k)
		if(err!=nil){//[]byte("Value_"+ fmt.Sprint(i))){
			fmt.Println(err.Error())
			fmt.Println("Value",i,"is",string(key))
		} else{
			//fmt.Println("Value",i,"is",string(key))
		}
		//db.Get(k),[]byte("Value_"+ fmt.Sprint(i)))
	}
}

func BenchmarkReadWriteMemtables(b *testing.B) {
	numKeys := 10
	var k, v string

	db, err := Open("data")
	require.NoError(b, err)

	keys := make([]string, numKeys)

	// Benchmark writes
	b.Run("Put", func(b *testing.B) {
		for i := 0; i < numKeys; i++ {
			k = fmt.Sprintf("key-%d", i)
			keys[i] = k
			v = "Value_" + fmt.Sprint(i)
			db.Put(k, []byte(v))
		}
	})

	// Benchmark reads
	b.Run("Get", func(b *testing.B) {
		for i := 0; i < numKeys; i++ {
			k = keys[i]
			_, err := db.Get(k)
			if err != nil {
				b.Logf("Failed to get key %s: %s", k, err.Error())
			}
		}
	})
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
		val,_ := db.Get(k)
		fmt.Printf("Value %d is %s\n",i,string(val))
	}
}