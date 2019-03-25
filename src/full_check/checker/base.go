package checker

import (
	"full_check/common"
	"sync"
	"full_check/metric"
	"full_check/client"
)

type FullCheckParameter struct {
	SourceHost   client.RedisHost
	TargetHost   client.RedisHost
	ResultDBFile string
	CompareCount int
	Interval     int
	BatchCount   int
	Parallel     int
	FilterTree   *common.Trie
}

type VerifierBase struct {
	Stat         *metric.Stat
	Param        *FullCheckParameter
}

func (p *VerifierBase) IncrKeyStat(oneKeyInfo *common.Key) {
	p.Stat.ConflictKey[oneKeyInfo.Tp.Index][oneKeyInfo.ConflictType].Inc(1)
}

func (p *VerifierBase) IncrFieldStat(oneKeyInfo *common.Key, conType common.ConflictType) {
	p.Stat.ConflictField[oneKeyInfo.Tp.Index][conType].Inc(1)
}

func (p *VerifierBase) FetchTypeAndLen(keyInfo []*common.Key, sourceClient, targetClient *client.RedisClient) {
	// fetch type
	sourceKeyTypeStr, err := sourceClient.PipeTypeCommand(keyInfo)
	if err != nil {
		panic(common.Logger.Critical(err))
	}
	for i, t := range sourceKeyTypeStr {
		keyInfo[i].Tp = common.NewKeyType(t)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	// fetch len
	go func() {
		sourceKeyLen, err := sourceClient.PipeLenCommand(keyInfo)
		if err != nil {
			panic(common.Logger.Critical(err))
		}
		for i, keylen := range sourceKeyLen {
			keyInfo[i].SourceAttr.ItemCount = keylen
		}
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		targetKeyLen, err := targetClient.PipeLenCommand(keyInfo)
		if err != nil {
			panic(common.Logger.Critical(err))
		}
		for i, keylen := range targetKeyLen {
			keyInfo[i].TargetAttr.ItemCount = keylen
		}
		wg.Done()
	}()

	wg.Wait()
}

type IVerifier interface {
	VerifyOneGroupKeyInfo(keyInfo []*common.Key, conflictKey chan<- *common.Key, sourceClient *client.RedisClient,
		targetClient *client.RedisClient)
}

type ValueOutlineVerifier struct {
	VerifierBase
}