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
		// fmt.Printf("key:%v, type:%v cmd:%v\n", string(keyInfo[i].Key), t, keyInfo[i].Tp.FetchLenCommand)
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

func (p *VerifierBase) RecheckTTL(keyInfo []*common.Key, client *client.RedisClient) {
	reCheckKeys := make([]*common.Key, 0, len(keyInfo))
	for _, key := range keyInfo {
		if key.TargetAttr.ItemCount == 0 && key.SourceAttr.ItemCount > 0 {
			reCheckKeys = append(reCheckKeys, key)
		}
	}
	if len(reCheckKeys) != 0 {
		p.recheckTTL(reCheckKeys, client)
	}
}

func (p *VerifierBase) recheckTTL(keyInfo []*common.Key, client *client.RedisClient) {
	keyExpire, err := client.PipeTTLCommand(keyInfo)
	if err != nil {
		panic(common.Logger.Critical(err))
	}
	for i, expire := range keyExpire {
		if expire {
			keyInfo[i].SourceAttr.ItemCount = 0
		}
	}
}

type IVerifier interface {
	VerifyOneGroupKeyInfo(keyInfo []*common.Key, conflictKey chan<- *common.Key, sourceClient *client.RedisClient,
		targetClient *client.RedisClient)
}

type ValueOutlineVerifier struct {
	VerifierBase
}