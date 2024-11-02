package anchordb

import (
	"bytes"
	"container/heap"
)

type StorageIterator interface{
	Value() []byte
	Key() []byte
	IsValid() bool
	Next() error
}

type HeapWrapper struct {
	idx int
	iterator StorageIterator
}

type IteratorHeap []*HeapWrapper

func (h IteratorHeap) Len() int{ return len(h)}

func (h IteratorHeap) Less(i,j int) bool { 
	return bytes.Compare(h[i].iterator.Key(),h[j].iterator.Key()) < 0 ||
	(bytes.Equal(h[i].iterator.Key(),h[j].iterator.Key()) && h[i].idx < h[j].idx)
}

func (h IteratorHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *IteratorHeap) Push(x interface{}){ *h = append(*h,x.(*HeapWrapper))}
func (h *IteratorHeap) Pop() interface{}{
	old := *h
	n:= len(old)
	x:= old[n-1]
	*h = old[:n-1]
	return x

}

type MergeIterator struct{
	iterators IteratorHeap
	current *HeapWrapper 
}

func NewMergeIterator(iters []StorageIterator) *MergeIterator{
	h := &IteratorHeap{}
	heap.Init(h)
	for i, iter := range iters{
		if iter.IsValid(){
			heap.Push(h,&HeapWrapper{idx:i,iterator: iter})
		}
	} 
	m := &MergeIterator{
		iterators: *h,
	}
	if h.Len() > 0{
		m.current = heap.Pop(h).(*HeapWrapper)
	}
	return m
}

func (m *MergeIterator) Key() []byte{
	return m.current.iterator.Key()
}

func (m *MergeIterator) Value() []byte{
	return m.current.iterator.Value()
}

func (m *MergeIterator) IsValid() bool{
	return m.current!=nil && m.current.iterator.IsValid()
}

func (m *MergeIterator) Next() error{
	if !m.IsValid(){return nil}
	if err:= m.current.iterator.Next(); err!=nil{
		return err
	}
	heap.Push(&m.iterators,m.current)
	if m.iterators.Len() > 0{
		m.current = heap.Pop(&m.iterators).(*HeapWrapper)
	} else {
		m.current = nil
	}
	return nil
}