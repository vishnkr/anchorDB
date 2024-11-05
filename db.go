package anchordb

import "fmt"

type AnchorDB struct{
	storage *Storage
}

func Open(path string) (*AnchorDB,error){
	options := StorageOptions{ 
		enableWal: false,
		maxMemTableCount: 3,
		blockSize: 32,
		targetSstSize: 2 << 5,
	}

	
	storage,_ := setupStorage(path,options)
	return &AnchorDB{ 
		storage: storage,  
	},nil
}

func (a *AnchorDB) Put(key string,value []byte){
	err := a.storage.Put(key,value)
	if err!=nil{
		fmt.Printf("Error: %s",err.Error())
	}
}

func (a *AnchorDB) Get(key string) []byte{
	value, err := a.storage.Get(key)
	if err!=nil{
		fmt.Printf("Error: %s",err.Error())
		return nil
	}
	return value.Value()
}

func (a *AnchorDB) Delete(key string){
	a.storage.Delete(key)
}