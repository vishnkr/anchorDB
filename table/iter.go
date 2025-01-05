package table

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

// Iterator that merges iterators of same type (multiple memtable/sst/block iterators)
type MergeIterator struct{
	iterators IteratorHeap
	current *HeapWrapper 
}

func NewMergeIterator[T StorageIterator](iters []T) *MergeIterator{
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

//Iterator that merges two iterators of different types
type TwoMergeIterator struct{
	iFlag bool
	i0 StorageIterator
	i1 StorageIterator
}

func NewTwoMergeIterator(i0,i1 StorageIterator) (*TwoMergeIterator,error){
	t := &TwoMergeIterator{
		iFlag: false,
		i0: i0,
		i1: i1,
	}
	if err := t.skipI1();err!=nil{
		return nil,err
	}
	t.iFlag = t.shouldSelectI0()
	return t,nil
}

func (t *TwoMergeIterator) shouldSelectI0() bool{
	if !t.i0.IsValid(){
		return false
	}
	if !t.i1.IsValid(){
		return true
	}
	return bytes.Compare(t.i0.Key(),t.i1.Key()) < 0
}

func (t *TwoMergeIterator) skipI1() error{
	if t.i0.IsValid() && t.i1.IsValid() && bytes.Equal(t.i0.Key(), t.i1.Key()){
		return t.i1.Next()
	}
	return nil
}

func (t *TwoMergeIterator) Key() []byte{
	if t.iFlag{
		return t.i0.Key()
	}
	return t.i1.Key()
}

func (t *TwoMergeIterator) Value() []byte{
	if t.iFlag{
		return t.i0.Value()
	}
	return t.i0.Value()
}

func (t *TwoMergeIterator) IsValid() bool {
	if t.iFlag {
		return t.i0.IsValid()
	}
	return t.i1.IsValid()
}

func (t *TwoMergeIterator) Next() error{
	iter:= t.i0
	if t.iFlag{
		iter = t.i1
	}
	if err := iter.Next(); err != nil {
		return err
	}
	if err := t.skipI1();err!=nil{
		return err
	}
	t.iFlag = t.shouldSelectI0()
	return nil
}