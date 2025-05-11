package anchordb

import (
	"anchordb/table"
	"errors"
	"fmt"
	"os"
	"time"
)

type CompactionTask interface{}
type CompactionType interface{}
type NoCompaction struct{}
type LeveledCompaction struct{}
type TieredCompaction struct{}


type LeveledCompactionTask struct{
	IsLowerLevelBottom bool
}

type FullCompaction struct{
	L1SSTables []int
	L0SSTables []int
}

func CompactToBottomLevel(task CompactionTask) bool{
	switch t := task.(type) {
	case FullCompaction:
		return true
	case LeveledCompactionTask:
		return t.IsLowerLevelBottom
	default:
		return false
	}
}

type CompactionController interface {
	GenCompactionTask(snapshot *LSMStore) *CompactionTask
	ApplyCompactionResult(snapshot *LSMStore, task *CompactionTask, output []int, inRecovry bool) (LSMStore,[]int)
	FlushToL0() bool
}

type NoCompactionController struct{}

func (ctrl NoCompactionController) GenerateCompactionTask(snapshot *LSMStore) *CompactionTask {
	return nil
}

func (ctrl NoCompactionController) ApplyCompactionResult(snapshot *LSMStore, task *CompactionTask, output []int, inRecovery bool) (*LSMStore, []int) {
	return snapshot, output
}

func (ctrl NoCompactionController) FlushToL0() bool {
	return false
}
type LeveledController struct{}

func (ctrl LeveledController) GenCompactionTask(snapshot *LSMStore) *CompactionTask{
	task := &LeveledCompactionTask{IsLowerLevelBottom: true}
	var compactionTask CompactionTask = task
	return &compactionTask
}

func (s *Storage) spawnCompaction(rx <-chan struct{}){
	go func(){
		ticker := time.NewTicker(50*time.Millisecond)
		defer ticker.Stop()
		for {
			select{
			case <-ticker.C:
				if err:= s.triggerCompaction();err!=nil{
					println("compaction failed",err)
				}
			case <-rx:
				return
			}
		}
	}()
}

func (s *Storage) triggerCompaction() error{
	//TODO
	return nil
}

func (s *Storage) performFullCompaction() error {
	if _, ok := s.options.CompactionType.(NoCompaction); !ok {
		return errors.New("full compaction cannot be performed when compaction is enabled")
	}
	s.storeLock.Lock()
	snapshot := s.store
	s.storeLock.RUnlock()
	l0sst := append([]int{},snapshot.l0SSTables...)
	l1sst := append([]int{},snapshot.levels[0]...)
	compactTask := FullCompaction{
		L0SSTables: l0sst,
		L1SSTables: l1sst,
	}

	sstables,_ := s.compact(compactTask)
	s.storeLock.Lock()
	//snapshot = *s.store
	s.storeLock.RUnlock()
	for _, sst := range append(l0sst, l1sst...) {
		delete(snapshot.sstables, sst)
	}
	var ids []int

	for _,sst := range sstables{
		ids = append(ids, sst.Id)
		snapshot.sstables[sst.Id] = &sst
	}

	snapshot.levels[0] = ids
	l0Set := make(map[int]bool)
	for _,sst := range l0sst{
		l0Set[sst] = true
	}

	var newL0 []int
	for _,x := range snapshot.l0SSTables{
		if !l0Set[x]{
			newL0 = append(newL0, x)
		}
	}
	snapshot.l0SSTables = newL0

	s.mu.Lock()
	s.store = snapshot
	s.mu.Unlock()

	for _, sst := range append(l0sst, l1sst...) {
		if err := os.Remove(s.getSSTPath(sst)); err != nil {
			return err
		}
	}
	return nil
}

func (s *Storage) compact(compactionTask CompactionTask) ([]table.SSTable,error){
	s.storeLock.RLock()
	snapshot := s.store
	s.storeLock.RUnlock()
	
	switch t:= compactionTask.(type){
	case *FullCompaction:
		l0Iters := make([]*table.SSTIterator,0,len(t.L0SSTables))
		for _,id := range t.L0SSTables{
			sst, ok:= snapshot.sstables[id]
			if !ok{
				return nil, fmt.Errorf("sstable %d not found",id)
			}
			iter := table.CreateSSTIterAndSeekToFirst(sst)
			l0Iters = append(l0Iters, iter)
		}
		l1SSTs := make([]*table.SSTable,0,len(t.L1SSTables))
		for _,id := range t.L1SSTables{
			sst, ok:= snapshot.sstables[id]
			if !ok{
				return nil, fmt.Errorf("sstable %d not found",id)
			}
			l1SSTs = append(l1SSTs, sst)
		}
		_,err := table.NewTwoMergeIterator(
			table.NewMergeIterator(l0Iters),
			table.CreateSSTConcatIterAndSeekToFirst(l1SSTs),
		)
		if err!=nil{
			return nil,err
		}
	}
	return nil,nil
}