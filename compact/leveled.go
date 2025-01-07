package compact

import (
	"anchordb"
)
type LevelCompactionOptions struct{
	MaxLevels int
	BaseLevelSizeMB int
	// 10
	SizeMultiplier int
	L0FileCompactionTrigger int
}

type LevelTask struct {
	UpperLevel *int
	UpperLevelSSTIds []int
	LowerLevel int
	LowerLevelSSTIds []int
	IsLowerBottom bool
}

type LevelCompactionController struct {
	options LevelCompactionOptions
}

func NewLevelCompactionController(options LevelCompactionOptions) *LevelCompactionController{
	return &LevelCompactionController{options}
}

func (c *LevelCompactionController) getOverlappingSSTs(
	state *anchordb.LSMStore,
	sstIds []int,
	level int,
) []int{
	//startKey := state.
	return []int{}
}




