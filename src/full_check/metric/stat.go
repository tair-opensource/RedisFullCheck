package metric

import (
	"full_check/common"
)

type Stat struct {
	Scan          AtomicSpeedCounter
	ConflictField [common.EndKeyTypeIndex][common.EndConflict]AtomicSpeedCounter
	ConflictKey   [common.EndKeyTypeIndex][common.EndConflict]AtomicSpeedCounter
}

func (p *Stat) Rotate() {
	p.Scan.Rotate()
	for keyType := common.KeyTypeIndex(0); keyType < common.EndKeyTypeIndex; keyType++ {
		for conType := common.ConflictType(0); conType < common.EndConflict; conType++ {
			p.ConflictField[keyType][conType].Rotate()
			p.ConflictKey[keyType][conType].Rotate()
		}
	}
}

func (p *Stat) Reset() {
	p.Scan.Reset()
	for keyType := common.KeyTypeIndex(0); keyType < common.EndKeyTypeIndex; keyType++ {
		for conType := common.ConflictType(0); conType < common.EndConflict; conType++ {
			p.ConflictField[keyType][conType].Reset()
			p.ConflictKey[keyType][conType].Reset()
		}
	}
}