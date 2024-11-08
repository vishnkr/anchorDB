package table

import (
	wal "anchordb/wal"

	"github.com/huandu/skiplist"
)
type Memtable struct{
	skiplist skiplist.SkipList
	size int64
	wal *wal.WAL
	id int
}

type MemtableIterator struct{
	skiplist *skiplist.SkipList
	curEntry *skiplist.Element
}

func CreateNewMemTable(id int) *Memtable{
	return &Memtable{
		skiplist: *skiplist.New(skiplist.String),
		size: 0,
		id: id,
	}
}

func CreateNewMemTableWithWal(id int,path string) *Memtable{
	
	return &Memtable{
		skiplist: *skiplist.New(skiplist.String),
		size: 0,
		wal: wal.OpenWAL(path),
		id: id,
	}
}

func (m *Memtable) GetSize() int64{
	return m.size
}

func (m *Memtable) GetID()int {
	return m.id
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

func (m *Memtable) Flush(s *SSTBuilder) {
	var k,v []byte
	elem := m.skiplist.Front()
	for elem!=nil{
		k=elem.Value.(*Entry).Key()
		v=elem.Value.(*Entry).Value()
		s.Add(k,v)
		elem= elem.Next()
	}
}

func (m *MemtableIterator) Next() error{
	element := m.skiplist.Element().Next()
	m.curEntry = element
	return nil
}

func (m *MemtableIterator) Value() []byte{
	return m.curEntry.Value.(*Entry).Value()
}

func (m *MemtableIterator) Key() []byte{
	return m.curEntry.Value.(*Entry).Key()
}

func (m *MemtableIterator) IsValid() bool{
	return m.curEntry!=nil
}