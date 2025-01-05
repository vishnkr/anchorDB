package table

import (
	"time"
)

type OldEntry struct{
	key string
	value []byte 
	deleted bool
	timestamp int64
}

type InternalValue struct{
	value []byte
	seq uint64
	tombstone bool
}
type Entry struct{
	key []byte
	internalValue *InternalValue
}


/*func compareEntry(k1, k2 Entry) int {
	key1 := k1.key
	key2 := k2.key

	// Lexicographic comparison on key
	cmp := bytes.Compare(key1, key2)
	if cmp != 0 {
		return cmp
	}

	// If keys are equal, prefer higher sequence number (reverse order)
	if k1.seq > k2.seq {
		return -1
	} else if k1.seq < k2.seq {
		return 1
	}

	return 0
}*/
func (i *InternalValue) Value() []byte{return i.value}
func (e *OldEntry) SetKey(key []byte){ e.key = string(key)}
func (e *OldEntry) SetValue(value []byte){ e.value = value}

func BuildEntry(key []byte, value []byte) *Entry{
	entry := Entry{
		key: key,
		internalValue: &InternalValue{
			tombstone: value==nil,
		},
	}
	if len(value)>0{
		entry.internalValue.value = value
	}
	return &entry
}

func BuildEntryWithSeqNo(key []byte, value []byte, seqNo uint64) *Entry{
	entry := BuildEntry(key,value)
	entry.internalValue.seq = seqNo
	return entry
}

func (e *Entry) InternalValue() *InternalValue{
	return e.internalValue
}

func (e *Entry) Key() []byte{
	return e.key
}

func (e *Entry) IsTombstone() bool{
	return e.internalValue.tombstone
}

func (e *Entry) SeqNo() uint64{
	return e.internalValue.seq
}

func (e *Entry) Value() []byte{
	return []byte(e.internalValue.value)
}

func BuildOldEntry(key string, value []byte) *OldEntry{
	entry := OldEntry{
		key: key,
		deleted: value==nil,
		timestamp: time.Now().UnixNano(),
	}
	if len(value)>0{
		entry.value = value
	}
	return &entry
}