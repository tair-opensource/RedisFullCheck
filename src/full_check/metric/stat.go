package metric

import (
	"full_check/common"
)

type Stat struct {
	Scan          AtomicSpeedCounter
	ConflictField [common.EndKeyTypeIndex][common.EndConflict]AtomicSpeedCounter
	ConflictKey   [common.EndKeyTypeIndex][common.EndConflict]AtomicSpeedCounter

	TotalConflictFields int64
	TotalConflictKeys int64
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

func (p *Stat) Reset(clear bool) {
	p.Scan.Reset()
	if clear {
		p.TotalConflictFields = 0
		p.TotalConflictKeys = 0
		return
	}
	for keyType := common.KeyTypeIndex(0); keyType < common.EndKeyTypeIndex; keyType++ {
		for conType := common.ConflictType(0); conType < common.EndConflict; conType++ {
			if conType < common.NoneConflict {
				keyConflict := p.ConflictKey[keyType][conType].Total()
				fieldConflict := p.ConflictField[keyType][conType].Total()
				if keyConflict != 0 {
					p.TotalConflictKeys += keyConflict
					common.Logger.Debugf("key conflict: keyType[%v] conType[%v]", keyType, conType)
				}
				if fieldConflict != 0 {
					p.TotalConflictFields += fieldConflict
					common.Logger.Debugf("field conflict: keyType[%v] conType[%v]", keyType, conType)
				}
			}

			p.ConflictField[keyType][conType].Reset()
			p.ConflictKey[keyType][conType].Reset()
		}
	}
}