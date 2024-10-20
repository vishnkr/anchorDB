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


type Entry struct{
	key string
	value []byte 
	deleted bool
	timestamp int64
}

func (m *Memtable) GetSize() int64{
	return m.size
}

func (m *Memtable) Put(entry *Entry) error{

	valueSize := int64(len(entry.value))
	existing := m.skiplist.Get(entry.key)
	if existing!=nil{
		m.size -= int64(len(existing.Value.(*Entry).value))
	} else {
		m.size += int64(len(entry.key))
	}
	m.size+=valueSize
	
	m.skiplist.Set(entry.key,entry)

	return nil
}

func (m *Memtable) Get(key string) (*Entry,bool){
	value,ok := m.skiplist.GetValue(key)
	if !ok{
		return nil,false
	}
	return value.(*Entry),true
} 

func (m *Memtable) Scan(start string, end string) []*Entry{
	var entries []*Entry
	i := m.skiplist.Find(start)
	for i!=nil{
		if i.Element().Key().(string) > end{
			break
		}
		entries = append(entries, i.Value.(*Entry))
		i = i.Next()
	}
	return entries
}

func buildEntry(key string, value []byte, deleted bool) *Entry{
	entry := Entry{
		key: key,
		deleted: deleted,
		timestamp: time.Now().UnixNano(),
	}
	if len(value)>0{
		entry.value = value
	}
	return &entry
}