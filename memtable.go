package anchordb

import (
	wal "anchor-db/wal"
	"time"

	"github.com/huandu/skiplist"
)
type Memtable struct{
	skiplist skiplist.SkipList
	size int64
	wal *wal.WAL
}

func CreateNewMemTable(id int) *Memtable{
	return &Memtable{
		skiplist: *skiplist.New(skiplist.String),
		size: 0,
	}
}

func CreateNewMemTableWithWal(id int,path string) *Memtable{
	
	return &Memtable{
		skiplist: *skiplist.New(skiplist.String),
		size: 0,
		wal: wal.OpenWAL(path),
	}
}


type MemTableEntry struct{
	key string
	value []byte 
	deleted bool
	timestamp int64
}

func (m *Memtable) GetSize() int64{
	return m.size
}

func (m *Memtable) Put(key string, value []byte) error{

	valueSize := int64(len(value))
	existing := m.skiplist.Get(key)
	if existing!=nil{
		m.size -= int64(len(existing.Value.(*MemTableEntry).value))
	} else {
		m.size += int64(len(key))
	}
	m.size+=valueSize
	entry := buildMemTableEntry(key,value,false)
	m.skiplist.Set(key,entry)

	return nil
}

func (m *Memtable) Get(key string) (*MemTableEntry,bool){
	value,ok := m.skiplist.GetValue(key)
	if !ok{
		return nil,false
	}
	return value.(*MemTableEntry),true
} 

func (m *Memtable) Delete(key string) error{
	existing := m.skiplist.Get(key)
	if existing!=nil{
		m.size -= int64(len(existing.Value.(*MemTableEntry).value))
	} else {
		m.size += int64(len(key))
	}
	m.skiplist.Set(key,buildMemTableEntry(key,nil,true))
	return nil
}

func buildMemTableEntry(key string, value []byte, deleted bool) *MemTableEntry{
	entry := MemTableEntry{
		key: key,
		deleted: deleted,
		timestamp: time.Now().UnixNano(),
	}
	if len(value)>0{
		entry.value = value
	}
	return &entry
}