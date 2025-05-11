package anchordb

import "fmt"

type AnchorDB struct{
	storage *Storage
}

func Open(path string) (*AnchorDB,error){
	options := StorageOptions{ 
		enableWal: false,
		maxMemTableCount: 2,
		blockSize: 4000,
		targetSstSize: 10240,//4194304,
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

func (a *AnchorDB) Get(key string) ([]byte,error){
	fmt.Println("in",key)
	value, err := a.storage.Get(key)
	fmt.Println(string(value.Key()),":------ got",string(value.Value()))
	if err!=nil{
		return nil,err
	}
	return value.Value(),nil
}

func (a *AnchorDB) Delete(key string){
	a.storage.Delete(key)
}