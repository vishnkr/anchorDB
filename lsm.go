package anchordb

import (
	"anchor-db/table"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type AnchorDB struct{
	storage *LSMStore
	options *StorageOptions
	flushNotifier chan struct{}
}

type StorageOptions struct{
	enableWal bool
	maxMemTableSize int64
	maxMemTableCount uint8
	blockSize uint
	targetSstSize uint
}

func Open(path string) (*AnchorDB,error){
	options := &StorageOptions{ 
		enableWal: false,
		maxMemTableCount: 1,
		blockSize: 4096,
		targetSstSize: 2 << 20,
	}

	storage,err := createNewLSMStore(path,options.enableWal)
	if err!=nil{
		return nil,err
	}
	return &AnchorDB{ 
		storage: storage, 
		options: options, 
		flushNotifier: make(chan struct{}),
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
	}
	return value
}

func (a *AnchorDB) Delete(key string){
	a.storage.Delete(key)
}

type LSMStore struct{
	memtable *Memtable
	immutables []*Memtable
	mu sync.RWMutex
	path string
	l0SSTables []int
	levels [][]int
	sstables map[int]table.SSTable
	wg sync.WaitGroup
	ctx context.Context
	cancel context.CancelFunc
}



func createNewLSMStore(path string, enableWal bool) (*LSMStore,error){
	var memtable *Memtable
	if(enableWal){
		memtable = CreateNewMemTableWithWal(0,path)
	} else {
		memtable = CreateNewMemTable(0)
	}
	fp := filepath.Join(path)
	err := os.MkdirAll(fp,os.ModePerm)
	if err!=nil{
		return nil,err
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &LSMStore{
		memtable:memtable,
		immutables:make([]*Memtable, 0),
		ctx: ctx,
		cancel: cancel,
	},nil
}

func (l *LSMStore) Put(key string, value []byte) error{
	l.mu.Lock()
	defer l.mu.Unlock()
	entry := buildEntry(key,value,false)
	l.memtable.Put(entry)
	return nil
} 

func (l *LSMStore) Get(key string) ([]byte,error){
	l.mu.Lock()
	defer l.mu.Unlock()
	entry, ok := l.memtable.Get(key)
	if !ok{
		return nil,fmt.Errorf("key %s does not exist",key)
	}
	return entry.value,nil
} 

func (l *LSMStore) Delete(key string) error{
	l.mu.Lock()
	defer l.mu.Unlock()
	entry := buildEntry(key,nil,true)
	return l.memtable.Put(entry)
} 

func (l *LSMStore) RangeScan(start string, end string) []*Entry{
	return l.memtable.Scan(start,end)
}

