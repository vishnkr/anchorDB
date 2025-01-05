package table

import (
	wal "anchordb/wal"
	"bytes"

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
		skiplist: *skiplist.New(skiplist.Bytes),
		size: 0,
		id: id,
	}
}

func CreateNewMemTableWithWal(id int,path string) *Memtable{
	
	return &Memtable{
		skiplist: *skiplist.New(skiplist.Bytes),
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

	valueSize := int64(len(entry.internalValue.value))
	existing := m.skiplist.Get(entry.key)
	if existing!=nil{
		m.size -= int64(len(existing.Value.(*InternalValue).value))
	} else {
		m.size += int64(len(entry.key))
	}
	m.size+=valueSize
	
	m.skiplist.Set(entry.key,entry.internalValue)

	return nil
}

func (m *Memtable) Get(key []byte) (*Entry,bool){
	value,ok := m.skiplist.GetValue(key)
	if !ok{
		return nil,false
	}
	var internalValue *InternalValue = value.(*InternalValue)
	entry:= &Entry{key, internalValue}
	return entry,true
} 

func (m *Memtable) Scan(start []byte, end []byte) []*Entry{
	var entries []*Entry
	i := m.skiplist.Find(start)
	for i!=nil{
		if bytes.Compare(i.Element().Key().([]byte), end) > 0 {
            break
        }
		entry := i.Value.(*Entry)
        if !entry.internalValue.tombstone {
            entries = append(entries, entry)
        }

        i = i.Next()
	}
	return entries
}

func (m *Memtable) Flush(s *SSTBuilder) {
	var k,v []byte
	elem := m.skiplist.Front()
	for elem!=nil{
		k = elem.Key().([]byte)
		v=elem.Value.(*InternalValue).Value()
		//fmt.Printf("adding key:%s, value:%s\n",string(k),string(v))
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