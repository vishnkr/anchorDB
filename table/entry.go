package table

import "time"

type Entry struct{
	key string
	value []byte 
	deleted bool
	timestamp int64
}

func (e *Entry) Key() []byte{
	return []byte(e.key)
}

func (e *Entry) Value() []byte{
	return []byte(e.value)
}

func BuildEntry(key string, value []byte, deleted bool) *Entry{
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