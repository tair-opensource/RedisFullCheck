package checker

import (
	"full_check/metric"
	"full_check/common"
	"full_check/client"
)

func NewValueOutlineVerifier(stat *metric.Stat, param *FullCheckParameter) *ValueOutlineVerifier {
	return &ValueOutlineVerifier{VerifierBase{stat, param}}
}

func (p *ValueOutlineVerifier) VerifyOneGroupKeyInfo(keyInfo []*common.Key, conflictKey chan<- *common.Key, sourceClient *client.RedisClient, targetClient *client.RedisClient) {
	p.FetchTypeAndLen(keyInfo, sourceClient, targetClient)

	// re-check ttl on the source side when key missing on the target side
	p.RecheckTTL(keyInfo, sourceClient)

	// compare, filter
	for i := 0; i < len(keyInfo); i++ {
		// 取type时，source redis上key已经被删除，认为是没有不一致
		if keyInfo[i].Tp == common.NoneKeyType {
			keyInfo[i].ConflictType = common.NoneConflict
			p.IncrKeyStat(keyInfo[i])
			continue
		}

		// 在fetch type和之后的轮次扫描之间源端类型更改，不处理这种错误
		if keyInfo[i].SourceAttr.ItemCount == common.TypeChanged {
			continue
		}

		// key lack in target redis
		if keyInfo[i].TargetAttr.ItemCount == 0 && keyInfo[i].TargetAttr.ItemCount != keyInfo[i].SourceAttr.ItemCount {
			keyInfo[i].ConflictType = common.LackTargetConflict
			p.IncrKeyStat(keyInfo[i])
			conflictKey <- keyInfo[i]
			continue
		}

		// type mismatch, ItemCount == -1，表明key在target redis上的type与source不同
		if keyInfo[i].TargetAttr.ItemCount == common.TypeChanged {
			keyInfo[i].ConflictType = common.TypeConflict
			p.IncrKeyStat(keyInfo[i])
			conflictKey <- keyInfo[i]
			continue
		}

		// string,  strlen mismatch, 先过滤一遍
		if keyInfo[i].SourceAttr.ItemCount != keyInfo[i].TargetAttr.ItemCount {
			keyInfo[i].ConflictType = common.ValueConflict
			p.IncrKeyStat(keyInfo[i])
			conflictKey <- keyInfo[i]
			continue
		}
	} // end of for i := 0; i < len(keyInfo); i++
}