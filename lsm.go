package anchordb

import (
	"anchor-db/table"
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)


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
	storeLock sync.Mutex
	path string
}

type StorageOptions struct{
	enableWal bool
	maxMemTableSize int64
	maxMemTableCount uint8
	blockSize uint
	targetSstSize uint
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
		store:store,
		options:options,
		nextId:nextId,
		path: path,
	},nil
}

func (s *Storage) Put(key string, value []byte) error{
	return s.store.Put(key,value)
}

func (s *Storage) Delete(key string) error{
	return s.store.Delete(key)
}

func (s *Storage) Get(key string) (*table.Entry,error){
	return s.store.Get([]byte(key))
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

func (l *LSMStore) Get(key []byte) (*table.Entry,error){
	var memtable *table.Memtable
	var immutable []*table.Memtable

	l.mu.RLock()
	memtable = l.memtable
	immutable = l.immutable
	l.mu.RUnlock()

	if entry, ok := memtable.Get(string(key)); ok {
		if len(entry.Value()) == 0 {
			return nil, nil // Tombstone found, key was deleted
		}
		return entry,nil
	}
	for _, imm := range immutable{
		if entry, ok := imm.Get(string(key)); ok {
			if len(entry.Value()) == 0 {
				return nil, nil
			}
			return entry,nil
		}
	}
	var iter *table.SSTIterator
	l0Iters := make([]*table.SSTIterator,len(l.l0SSTables))
	for _,tableId := range l.l0SSTables{
		if sst,ok := l.sstables[tableId]; ok{
			iter = table.CreateSSTIterAndSeekToKey(sst,key)
			l0Iters = append(l0Iters, iter)
		}
	}

	levelIters := make([]*table.LevelIterator,len(l.levels))
	for _, level := range l.levels{
		levelSSTs := make([]*table.SSTable,len(level))
		for _,tableId := range level{
			if sst,ok := l.sstables[tableId];ok{
				levelSSTs = append(levelSSTs, sst)
			}
		}
		levelIters = append(levelIters, table.CreateLevelIterAndSeekToKey(levelSSTs,key))
	}
	twoMergeIter,_ := table.NewTwoMergeIterator(
		table.NewMergeIterator(l0Iters),
		table.NewMergeIterator(levelIters),
	)
	if twoMergeIter.IsValid() && bytes.Compare(twoMergeIter.Key(),key)==0 && len(twoMergeIter.Value())>0{
		e:= &table.Entry{}
		e.SetKey(key)
		e.SetValue(twoMergeIter.Value())
		return e,nil
	}
	return nil, fmt.Errorf("key %s does not exist", key)
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
			//TODO
		}
	}
}

func (s *Storage) flushLastImmutableMemTable() error{
	s.storeLock.Lock()
	defer s.storeLock.Unlock()

	var flushMemtable *table.Memtable
	s.store.mu.RLock()
	immCount := len(s.store.immutable)
	if(immCount==0){
		s.store.mu.RUnlock()
        return fmt.Errorf("no imm memtables to flush")
	}
	flushMemtable = s.store.immutable[immCount-1]
	s.store.mu.RUnlock()
	sstBuilder := table.NewSSTBuilder(int(s.options.blockSize))
	flushMemtable.Flush(sstBuilder)
	sstPath := filepath.Join(s.path,fmt.Sprintf("%d.sst",flushMemtable.GetID()))
	sst := sstBuilder.Build(
		flushMemtable.GetID(),
		sstPath,
	)
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	
	s.store.immutable = s.store.immutable[:immCount-1]
	s.store.l0SSTables = append([]int{flushMemtable.GetID()},s.store.l0SSTables...)
	s.store.sstables[flushMemtable.GetID()]=sst
	syncDir(s.path)
	return nil
}
