package anchordb

import (
	"anchordb/table"
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)


type LSMStore struct{
	memtable *table.Memtable
	immutable []*table.Memtable
	mu sync.RWMutex
	path string
	l0SSTables []int
	levels [][]int
	sstables map[int]*table.SSTable
	ctx context.Context
	cancel context.CancelFunc
	seqCounter uint64
	options *StorageOptions
}

type Storage struct {
	store *LSMStore
	options *StorageOptions
	nextId int
	storeLock sync.RWMutex
	mu sync.Mutex
	path string
	flushNotifier chan struct{}
	flushStop chan struct{}
}

type StorageOptions struct{
	EnableWal bool
	EnableBloomFilter bool
	MaxMemTableSize int64
	MaxMemTableCount int
	BlockSize uint
	TargetSstSize uint
	CompactionType CompactionType
}

func setupStorage(path string,options *StorageOptions) (*Storage,error){
	dbPath := filepath.Join(path)
	if err := os.MkdirAll(dbPath,os.ModePerm); err!=nil{
		return nil,err
	}
	store,err := createNewLSMStore(dbPath,options)
	if err!=nil{
		return nil,err
	}

	nextId := 1
	storage:= &Storage{
		store:store,
		options:options,
		nextId:nextId,
		path: path,
		flushNotifier: make(chan struct{}, 1), // Buffered to avoid blocking
		flushStop:    make(chan struct{}),
	}
	storage.spawnFlushTrigger()
	return storage,nil
}

func (s *Storage) Put(key string, value []byte) error{
	if key == "" {
		return errors.New("key cannot be empty")
	}
	if len(value) == 0 {
		return errors.New("value cannot be empty")
	}
	
	err := s.store.Put(key, value)
	if err != nil {
		return err
	}
	s.attemptFreeze()
	
	return nil
}

func (s *Storage) Delete(key string) error{
	return s.store.Delete(key)
}

func (s *Storage) Get(key string) (*table.Entry,error){
	result,err := s.store.Get([]byte(key))
	if err!=nil{
		return nil,err
	}
	return result,nil
}


func createNewLSMStore(path string, options *StorageOptions) (*LSMStore,error){
	var memtable *table.Memtable
	if(options.EnableWal){
		memtable = table.CreateNewMemTableWithWal(0,path)
	} else {
		memtable = table.CreateNewMemTable(0)
	}

	ctx, cancel := context.WithCancel(context.Background())
	sstables := make(map[int]*table.SSTable)
	
	for i:=0;i<=2;i++{
		fw,err := table.OpenFileWrapper(fmt.Sprintf("%d.sst",i))
		if err!=nil{
			//fmt.Printf("error opening sstable")
			continue
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
		options: options,
	},nil
}


func (l *LSMStore) nextSeq() uint64 {
	return atomic.AddUint64(&l.seqCounter, 1)
}

func (l *LSMStore) Put(key string, value []byte) error{
	l.mu.Lock()
	defer l.mu.Unlock()
	seq := l.nextSeq()
	entry := table.BuildEntryWithSeqNo([]byte(key),value,seq)
	l.memtable.Put(entry)
	return nil
} 

func (l *LSMStore) Get(key []byte) (*table.Entry,error){
	
	var memtable *table.Memtable
	var immutable []*table.Memtable

	l.mu.RLock()
	defer l.mu.RUnlock()
	memtable = l.memtable
	immutable = l.immutable
	

	if entry, ok := memtable.Get(key); ok {
		if len(entry.Value()) == 0 {
			return nil, nil // Tombstone found, key was deleted
		}
		return entry,nil
	}
	for _, imm := range immutable{
		if entry, ok := imm.Get(key); ok {
			if len(entry.Value()) == 0 {
				return nil, nil
			}
			return entry,nil
		}
	}
	var iter *table.SSTIterator
	l0Iters := make([]*table.SSTIterator, 0, len(l.l0SSTables))
	//fmt.Println("we here")
	for _, tableID := range l.l0SSTables {
		sst, ok := l.sstables[tableID]
		if !ok {
			continue
		}
	
		if !isKeyWithinRange(key, sst.GetFirstKey(), sst.GetLastKey()) {
			continue
		}
	
		// If bloom filter is enabled, use it
		if l.options.EnableBloomFilter {
			if !sst.BloomFilter.MayContain(crc32.ChecksumIEEE(key)) {
				continue
			}
		}
	
		iter = table.CreateSSTIterAndSeekToKey(sst, key)
		if iter.IsValid() && bytes.Equal(iter.Key(), key) {
			l0Iters = append(l0Iters, iter)
		}
	}

	levelIters := make([]*table.LevelIterator,0,len(l.levels))
	for _, level := range l.levels{
		levelSSTs := make([]*table.SSTable,0,len(level))
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
	if twoMergeIter.IsValid() && bytes.Equal(twoMergeIter.Key(),key) && len(twoMergeIter.Value())>0{
		e:= table.BuildEntry(key,twoMergeIter.Value()) 
		/*&table.Entry{}
		e.SetKey(key)
		e.SetValue(twoMergeIter.Value())*/
		return e,nil
	}
	return nil, fmt.Errorf("key %s does not exist", key)
} 

func isKeyWithinRange(key, firstKey, lastKey []byte) bool{
	return bytes.Compare(key, firstKey) >= 0 && bytes.Compare(key, lastKey) <= 0
}

func (l *LSMStore) Delete(key string) error{
	l.mu.Lock()
	defer l.mu.Unlock()
	seq := l.nextSeq()
	entry := table.BuildEntryWithSeqNo([]byte(key),nil,seq)
	return l.memtable.Put(entry)
} 

func (l *LSMStore) RangeScan(start string, end string) []*table.Entry{
	return l.memtable.Scan([]byte(start),[]byte(end))
}

func (s *Storage) attemptFreeze(){
	currentSize := s.store.memtable.GetSize()
	if currentSize >= int64(s.options.TargetSstSize){
		s.storeLock.Lock()
		defer s.storeLock.Unlock()
		memtableID := s.nextId
		s.nextId++  
		s.store.freezeAndReplaceMemtable(memtableID)
	}
}

func (l *LSMStore) freezeAndReplaceMemtable(id int){
	l.mu.Lock()
	defer l.mu.Unlock()
	newMemtable := table.CreateNewMemTable(id)
	oldMemtable := l.memtable
	l.immutable = append([]*table.Memtable{oldMemtable},l.immutable...)
	l.memtable = newMemtable
}

func (s *Storage) flushLastImmutableMemTable() error{
	s.storeLock.Lock()
	defer s.storeLock.Unlock()

	var flushMemtable *table.Memtable
	s.store.mu.RLock()
	immCount := len(s.store.immutable)
	fmt.Println("flushing,",immCount,"tables")
	if(immCount==0){
		s.store.mu.RUnlock()
        return nil
	}
	flushMemtable = s.store.immutable[immCount-1]
	s.store.mu.RUnlock()
	sstBuilder := table.NewSSTBuilder(int(s.options.BlockSize))
	flushMemtable.Flush(sstBuilder)
	sstPath := filepath.Join(s.path,fmt.Sprintf("%d.sst",flushMemtable.GetID()))
	sst := sstBuilder.Build(
		flushMemtable.GetID(),
		sstPath,
	)
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	fmt.Println("got sst")
	s.store.immutable = s.store.immutable[:immCount-1]
	s.store.l0SSTables = append([]int{flushMemtable.GetID()},s.store.l0SSTables...)
	s.store.sstables[flushMemtable.GetID()]=sst
	syncDir(s.path)
	return nil
}

func (s *Storage) flushAllImmutableMemTables() error {
	s.storeLock.Lock()
	defer s.storeLock.Unlock()

	for {
		s.store.mu.RLock()
		immCount := len(s.store.immutable)
		if immCount == 0 {
			s.store.mu.RUnlock()
			break
		}
		flushMemtable := s.store.immutable[immCount-1]
		s.store.mu.RUnlock()

		sstBuilder := table.NewSSTBuilder(int(s.options.BlockSize))
		flushMemtable.Flush(sstBuilder)
		sstPath := filepath.Join(s.path, fmt.Sprintf("%d.sst", flushMemtable.GetID()))
		sst := sstBuilder.Build(
			flushMemtable.GetID(),
			sstPath,
		)

		s.store.mu.Lock()
		s.store.immutable = s.store.immutable[:immCount-1]
		s.store.l0SSTables = append([]int{flushMemtable.GetID()}, s.store.l0SSTables...)
		s.store.sstables[flushMemtable.GetID()] = sst
		s.store.mu.Unlock()

		syncDir(s.path)
		//fmt.Printf("Flushed memtable %d to %s\n", flushMemtable.GetID(), sstPath)
	}
	return nil
}


func (s *Storage) spawnFlushTrigger(){
	go func ()  {
		ticker := time.NewTicker(100*time.Millisecond)
		defer ticker.Stop()
		
		for {
			select {
			case <- ticker.C:
				s.shouldTriggerFlush()
			case <-s.flushNotifier:
				//fmt.Println("got flush")
				if err:=s.flushAllImmutableMemTables();err!=nil{
					fmt.Println("Flush failed:", err)
				}	
			case <-s.flushStop:
				fmt.Println("Stopping flush trigger")
				return
			}
		}

	}()
}

func (s *Storage) shouldTriggerFlush(){
	s.store.mu.RLock()
	defer s.store.mu.RUnlock()
	if(len(s.store.immutable)+1 >= s.options.MaxMemTableCount){
		select {
		case s.flushNotifier <- struct{}{}:
		default:
		}
	}
}

func (s *Storage) stopFlushTrigger(){
	close(s.flushStop)
}

func (s *Storage) getSSTPath(id int) string{
	return fmt.Sprintf("%s/%d.sst",s.store.path,id)
}