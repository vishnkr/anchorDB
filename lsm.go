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
	storage *Storage
	flushNotifier chan struct{}
}

type LSMStore struct{
	memtable *table.Memtable
	immutable []*table.Memtable
	mu sync.RWMutex
	path string
	l0SSTables []int
	levels [][]int
	sstables map[int]*table.SSTable
	wg sync.WaitGroup
	ctx context.Context
	cancel context.CancelFunc
}

type Storage struct {
	store *LSMStore
	options StorageOptions
	nextId int
}

type StorageOptions struct{
	enableWal bool
	maxMemTableSize int64
	maxMemTableCount uint8
	blockSize uint
	targetSstSize uint
}

func Open(path string) (*AnchorDB,error){
	options := StorageOptions{ 
		enableWal: false,
		maxMemTableCount: 1,
		blockSize: 4096,
		targetSstSize: 2 << 20,
	}

	
	storage,_ := setupStorage(path,options)
	return &AnchorDB{ 
		storage: storage,  
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
	return value.Value()
}

func (a *AnchorDB) Delete(key string){
	a.storage.Delete(key)
}

func setupStorage(path string,options StorageOptions) (*Storage,error){
	dbPath := filepath.Join(path,"db_root")

	store,err := createNewLSMStore(dbPath,options.enableWal)
	if err!=nil{
		return nil,err
	}

	if err := os.MkdirAll(dbPath,os.ModePerm); err!=nil{
		return nil,err
	}
	nextId := 0
	return &Storage{
		store,
		options,
		nextId,
	},nil
}

func (s *Storage) Put(key string, value []byte) error{
	return s.store.Put(key,value)
}

func (s *Storage) Delete(key string) error{
	return s.store.Delete(key)
}

func (s *Storage) Get(key string) (*table.Entry,error){
	return s.store.Get(key)
}


func createNewLSMStore(path string, enableWal bool) (*LSMStore,error){
	var memtable *table.Memtable
	if(enableWal){
		memtable = table.CreateNewMemTableWithWal(0,path)
	} else {
		memtable = table.CreateNewMemTable(0)
	}
	fp := filepath.Join(path)
	err := os.MkdirAll(fp,os.ModePerm)
	if err!=nil{
		return nil,err
	}
	ctx, cancel := context.WithCancel(context.Background())
	sstables := make(map[int]*table.SSTable)
	
	for i:=0;i<=2;i++{
		fw,err := table.OpenFileWrapper(fmt.Sprintf("%d.sst",i))
		if err!=nil{
			fmt.Printf("error opening sstable")
		}
		sst := table.OpenSSTable(i,fw)
		sstables[i] = sst
	}
	return &LSMStore{
		sstables: sstables,
		memtable:memtable,
		immutable: make([]*table.Memtable,0),
		ctx: ctx,
		cancel: cancel,
	},nil
}

func (l *LSMStore) Put(key string, value []byte) error{
	l.mu.Lock()
	defer l.mu.Unlock()
	entry := table.BuildEntry(key,value,false)
	l.memtable.Put(entry)
	return nil
} 

func (l *LSMStore) Get(key string) (*table.Entry,error){
	l.mu.Lock()
	defer l.mu.Unlock()
	entry, ok := l.memtable.Get(key)
	if !ok{
		return nil,fmt.Errorf("key %s does not exist",key)
	}
	return entry,nil
} 

func (l *LSMStore) Delete(key string) error{
	l.mu.Lock()
	defer l.mu.Unlock()
	entry := table.BuildEntry(key,nil,true)
	return l.memtable.Put(entry)
} 

func (l *LSMStore) RangeScan(start string, end string) []*table.Entry{
	return l.memtable.Scan(start,end)
}

func (s *Storage) attemptFreeze(estSize uint){
	if (estSize >= s.options.targetSstSize){
		if s.store.memtable.GetSize() >= s.options.maxMemTableSize{

		}
	}
}