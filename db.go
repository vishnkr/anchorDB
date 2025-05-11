package anchordb

import "fmt"

type AnchorDB struct{
	storage *Storage
}

func Open(path string,opts *StorageOptions) (*AnchorDB,error){
	if opts==nil{
		opts = &StorageOptions{
			EnableWal:        false,
			MaxMemTableCount: 2,
			BlockSize:        4096,
			TargetSstSize:    4 * 1024 * 1024,
			EnableBloomFilter: true,
		}
	}

	
	storage,_ := setupStorage(path,opts)
	return &AnchorDB{ 
		storage: storage,  
	},nil
}

func (a *AnchorDB) Put(key []byte,value []byte) error{
	err := a.storage.Put(string(key),value)
	if err!=nil{
		fmt.Printf("Error: %s",err.Error())
		return err
	}
	return nil
}

func (a *AnchorDB) Get(key []byte) ([]byte,error){
	//fmt.Println("in",key)
	value, err := a.storage.Get(string(key))
	//fmt.Println(string(value.Key()),":------ got",string(value.Value()))
	if err!=nil{
		return nil,err
	}
	return value.Value(),nil
}

func (a *AnchorDB) Delete(key string){
	a.storage.Delete(key)
}