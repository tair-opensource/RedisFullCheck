package checker

import (
	"full_check/common"
	"sync"
	"full_check/metric"
	"full_check/client"
)

type KeyOutlineVerifier struct {
	VerifierBase
}

func NewKeyOutlineVerifier(stat *metric.Stat, param *FullCheckParameter) *KeyOutlineVerifier {
	return &KeyOutlineVerifier{VerifierBase{stat, param}}
}

func (p *KeyOutlineVerifier) FetchKeys(keyInfo []*common.Key, sourceClient *client.RedisClient, targetClient *client.RedisClient) {
	// fetch type
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		sourceKeyTypeStr, err := sourceClient.PipeTypeCommand(keyInfo)
		if err != nil {
			panic(common.Logger.Critical(err))
		}
		for i, t := range sourceKeyTypeStr {
			keyInfo[i].Tp = common.NewKeyType(t)
		}
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		targetKeyTypeStr, err := targetClient.PipeExistsCommand(keyInfo)
		if err != nil {
			panic(common.Logger.Critical(err))
		}
		for i, t := range targetKeyTypeStr {
			keyInfo[i].TargetAttr.ItemCount = t
		}
		wg.Done()
	}()

	wg.Wait()
}

func (p *KeyOutlineVerifier) VerifyOneGroupKeyInfo(keyInfo []*common.Key, conflictKey chan<- *common.Key, sourceClient *client.RedisClient, targetClient *client.RedisClient) {
	p.FetchKeys(keyInfo, sourceClient, targetClient)

	// re-check ttl on the source side when key missing on the target side
	p.RecheckTTL(keyInfo, sourceClient)

	// compare, filter
	for i := 0; i < len(keyInfo); i++ {
		// key lack in target redis
		if keyInfo[i].TargetAttr.ItemCount == 0 &&
				keyInfo[i].TargetAttr.ItemCount != keyInfo[i].SourceAttr.ItemCount {
			keyInfo[i].ConflictType = common.LackTargetConflict
			p.IncrKeyStat(keyInfo[i])
			conflictKey <- keyInfo[i]
		}
	} // end of for i := 0; i < len(keyInfo); i++
}