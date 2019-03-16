package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/garyburd/redigo/redis"
	_ "github.com/mattn/go-sqlite3"
	"os"
	_ "path"
	"strconv"
	"strings"
	"sync"
	"time"
	"full_check/common"
	"math"
	"full_check/metric"
)

type CheckType int

const (
	FullValue            = 1
	ValueLengthOutline   = 2
	KeyOutline           = 3
	FullValueWithOutline = 4
)

type IVerifier interface {
	VerifyOneGroupKeyInfo(keyInfo []*common.Key, conflictKey chan<- *common.Key, sourceClient *RedisClient,
		targetClient *RedisClient)
}

type VerifierBase struct {
	stat         *metric.Stat
	param        *FullCheckParameter
}

func (p *VerifierBase) IncrKeyStat(oneKeyInfo *common.Key) {
	p.stat.ConflictKey[oneKeyInfo.Tp.Index][oneKeyInfo.ConflictType].Inc(1)
}

func (p *VerifierBase) IncrFieldStat(oneKeyInfo *common.Key, conType common.ConflictType) {
	p.stat.ConflictField[oneKeyInfo.Tp.Index][conType].Inc(1)
}

func (p *VerifierBase) FetchTypeAndLen(keyInfo []*common.Key, sourceClient *RedisClient, targetClient *RedisClient) {
	// fetch type
	sourceKeyTypeStr, err := sourceClient.PipeTypeCommand(keyInfo)
	if err != nil {
		panic(logger.Critical(err))
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
			panic(logger.Critical(err))
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
			panic(logger.Critical(err))
		}
		for i, keylen := range targetKeyLen {
			keyInfo[i].TargetAttr.ItemCount = keylen
		}
		wg.Done()
	}()

	wg.Wait()
}

type ValueOutlineVerifier struct {
	VerifierBase
}

func NewValueOutlineVerifier(stat *metric.Stat, param *FullCheckParameter) *ValueOutlineVerifier {
	return &ValueOutlineVerifier{VerifierBase{stat, param}}
}

func (p *ValueOutlineVerifier) VerifyOneGroupKeyInfo(keyInfo []*common.Key, conflictKey chan<- *common.Key, sourceClient *RedisClient, targetClient *RedisClient) {
	p.FetchTypeAndLen(keyInfo, sourceClient, targetClient)

	// compare, filter
	for i := 0; i < len(keyInfo); i++ {
		// 取type时，source redis上key已经被删除，认为是没有不一致
		if keyInfo[i].Tp == common.NoneKeyType {
			keyInfo[i].ConflictType = common.NoneConflict
			p.IncrKeyStat(keyInfo[i])
			continue
		}

		// key lack in target redis
		if keyInfo[i].TargetAttr.ItemCount == 0 {
			keyInfo[i].ConflictType = common.LackTargetConflict
			p.IncrKeyStat(keyInfo[i])
			conflictKey <- keyInfo[i]
			continue
		}

		// type mismatch, ItemCount == -1，表明key在target redis上的type与source不同
		if keyInfo[i].TargetAttr.ItemCount == -1 {
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

type KeyOutlineVerifier struct {
	VerifierBase
}

func NewKeyOutlineVerifier(stat *metric.Stat, param *FullCheckParameter) *KeyOutlineVerifier {
	return &KeyOutlineVerifier{VerifierBase{stat, param}}
}

func (p *KeyOutlineVerifier) FetchKeys(keyInfo []*common.Key, sourceClient *RedisClient, targetClient *RedisClient) {
	// fetch type
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		sourceKeyTypeStr, err := sourceClient.PipeTypeCommand(keyInfo)
		if err != nil {
			panic(logger.Critical(err))
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
			panic(logger.Critical(err))
		}
		for i, t := range targetKeyTypeStr {
			keyInfo[i].TargetAttr.ItemCount = t
		}
		wg.Done()
	}()

	wg.Wait()
}

func (p *KeyOutlineVerifier) VerifyOneGroupKeyInfo(keyInfo []*common.Key, conflictKey chan<- *common.Key, sourceClient *RedisClient, targetClient *RedisClient) {
	p.FetchKeys(keyInfo, sourceClient, targetClient)

	// compare, filter
	for i := 0; i < len(keyInfo); i++ {
		// key lack in target redis
		if keyInfo[i].TargetAttr.ItemCount == 0 {
			keyInfo[i].ConflictType = common.LackTargetConflict
			p.IncrKeyStat(keyInfo[i])
			conflictKey <- keyInfo[i]
		}
	} // end of for i := 0; i < len(keyInfo); i++
}

type FullValueVerifier struct {
	VerifierBase
	ignoreBigKey bool // only compare value length for big key when this parameter is enabled.
}

func NewFullValueVerifier(stat *metric.Stat, param *FullCheckParameter, ignoreBigKey bool) *FullValueVerifier {
	return &FullValueVerifier{
		VerifierBase: VerifierBase{stat, param},
		ignoreBigKey: ignoreBigKey,
	}
}

func (p *FullValueVerifier) VerifyOneGroupKeyInfo(keyInfo []*common.Key, conflictKey chan<- *common.Key, sourceClient *RedisClient, targetClient *RedisClient) {
	// 对于没有类型的Key, 取类型和长度
	noTypeKeyInfo := make([]*common.Key, 0, len(keyInfo))
	for i := 0; i < len(keyInfo); i++ {
		if keyInfo[i].Tp == common.EndKeyType {
			noTypeKeyInfo = append(noTypeKeyInfo, keyInfo[i])
		}
	}
	p.FetchTypeAndLen(noTypeKeyInfo, sourceClient, targetClient)

	// compare, filter
	fullCheckFetchAllKeyInfo := make([]*common.Key, 0, len(keyInfo))
	retryNewVerifyKeyInfo := make([]*common.Key, 0, len(keyInfo))
	for i := 0; i < len(keyInfo); i++ {
		/************ 所有第一次比较的key，之前未比较的 key ***********/
		if keyInfo[i].ConflictType == common.EndConflict { // 第二轮及以后比较的key，conflictType 肯定不是EndConflict
			// 取type时，source redis上key已经被删除，认为是没有不一致
			if keyInfo[i].Tp == common.NoneKeyType {
				keyInfo[i].ConflictType = common.NoneConflict
				p.IncrKeyStat(keyInfo[i])
				continue
			}

			// key lack in target redis
			if keyInfo[i].TargetAttr.ItemCount == 0 &&
					keyInfo[i].TargetAttr.ItemCount != keyInfo[i].SourceAttr.ItemCount  {
				keyInfo[i].ConflictType = common.LackTargetConflict
				p.IncrKeyStat(keyInfo[i])
				conflictKey <- keyInfo[i]
				continue
			}

			// type mismatch, ItemCount == -1，表明key在target redis上的type与source不同
			if keyInfo[i].TargetAttr.ItemCount == -1 {
				keyInfo[i].ConflictType = common.TypeConflict
				p.IncrKeyStat(keyInfo[i])
				conflictKey <- keyInfo[i]
				continue
			}

			// string,  strlen mismatch, 先过滤一遍
			if keyInfo[i].Tp == common.StringKeyType && keyInfo[i].SourceAttr.ItemCount != keyInfo[i].TargetAttr.ItemCount {
				keyInfo[i].ConflictType = common.ValueConflict
				p.IncrKeyStat(keyInfo[i])
				conflictKey <- keyInfo[i]
				continue
			}

			// 太大的 hash、list、set、zset 特殊单独处理
			if keyInfo[i].Tp != common.StringKeyType &&
					(keyInfo[i].SourceAttr.ItemCount > common.BigKeyThreshold || keyInfo[i].TargetAttr.ItemCount > common.BigKeyThreshold) {
				if p.ignoreBigKey {
					// 如果启用忽略大key开关，则进入这个分支
					if keyInfo[i].SourceAttr.ItemCount != keyInfo[i].TargetAttr.ItemCount {
						keyInfo[i].ConflictType = common.ValueConflict
						p.IncrKeyStat(keyInfo[i])
						conflictKey <- keyInfo[i]
					} else {
						keyInfo[i].ConflictType = common.NoneConflict
						p.IncrKeyStat(keyInfo[i])
					}
					continue
				}

				switch keyInfo[i].Tp {
				case common.HashKeyType:
					fallthrough
				case common.SetKeyType:
					fallthrough
				case common.ZsetKeyType:
					sourceValue, err := sourceClient.FetchValueUseScan_Hash_Set_SortedSet(keyInfo[i], p.param.batchCount)
					if err != nil {
						panic(logger.Error(err))
					}
					targetValue, err := targetClient.FetchValueUseScan_Hash_Set_SortedSet(keyInfo[i], p.param.batchCount)
					if err != nil {
						panic(logger.Error(err))
					}
					p.Compare_Hash_Set_SortedSet(keyInfo[i], conflictKey, sourceValue, targetValue)
				case common.ListKeyType:
					p.CheckFullBigValue_List(keyInfo[i], conflictKey, sourceClient, targetClient)
				}
				continue
			}
			// 剩下的都进入 fullCheckFetchAllKeyInfo(), pipeline + 一次性取全量数据的方式比较value
			fullCheckFetchAllKeyInfo = append(fullCheckFetchAllKeyInfo, keyInfo[i])

			continue
		} else {
			/************ 之前比较过的key，进入后面的多轮比较 ***********/
			// 这3种类型，重新比较
			if keyInfo[i].ConflictType == common.LackSourceConflict ||
					keyInfo[i].ConflictType == common.LackTargetConflict ||
					keyInfo[i].ConflictType == common.TypeConflict {
				keyInfo[i].Tp = common.EndKeyType            // 重新取 type、len
				keyInfo[i].ConflictType = common.EndConflict // 使用 第一轮比较用的方式
				retryNewVerifyKeyInfo = append(retryNewVerifyKeyInfo, keyInfo[i])
				continue
			}

			if keyInfo[i].ConflictType == common.ValueConflict {
				if keyInfo[i].Tp != common.StringKeyType &&
						(keyInfo[i].SourceAttr.ItemCount > common.BigKeyThreshold || keyInfo[i].TargetAttr.ItemCount > common.BigKeyThreshold) &&
						p.ignoreBigKey {
					// 如果启用忽略大key开关，则进入这个分支
					if keyInfo[i].SourceAttr.ItemCount != keyInfo[i].TargetAttr.ItemCount {
						keyInfo[i].ConflictType = common.ValueConflict
						p.IncrKeyStat(keyInfo[i])
						conflictKey <- keyInfo[i]
					} else {
						keyInfo[i].ConflictType = common.NoneConflict
						p.IncrKeyStat(keyInfo[i])
					}
					continue
				}

				switch keyInfo[i].Tp {
				// string 和 list 每次都要重新比较所有field value。
				// list有lpush、lpop，会导致field value平移，所以需要重新比较所有field value
				case common.StringKeyType:
					fullCheckFetchAllKeyInfo = append(fullCheckFetchAllKeyInfo, keyInfo[i])
				case common.ListKeyType:
					if keyInfo[i].SourceAttr.ItemCount > common.BigKeyThreshold || keyInfo[i].TargetAttr.ItemCount > common.BigKeyThreshold {
						p.CheckFullBigValue_List(keyInfo[i], conflictKey, sourceClient, targetClient)
					} else {
						fullCheckFetchAllKeyInfo = append(fullCheckFetchAllKeyInfo, keyInfo[i])
					}
					// hash、set、zset, 只比较前一轮有不一致的field
				case common.HashKeyType:
					p.CheckPartialValueHash(keyInfo[i], conflictKey, sourceClient, targetClient)
				case common.SetKeyType:
					p.CheckPartialValueSet(keyInfo[i], conflictKey, sourceClient, targetClient)
				case common.ZsetKeyType:
					p.CheckPartialValueSortedSet(keyInfo[i], conflictKey, sourceClient, targetClient)
				}
				continue
			}
		}
	} // end of for i := 0; i < len(keyInfo); i++

	if len(fullCheckFetchAllKeyInfo) != 0 {
		p.CheckFullValueFetchAll(fullCheckFetchAllKeyInfo, conflictKey, sourceClient, targetClient)
	}
	if len(retryNewVerifyKeyInfo) != 0 {
		p.VerifyOneGroupKeyInfo(retryNewVerifyKeyInfo, conflictKey, sourceClient, targetClient)
	}

}

func (p *FullValueVerifier) CheckFullValueFetchAll(keyInfo []*common.Key, conflictKey chan<- *common.Key, sourceClient *RedisClient, targetClient *RedisClient) {
	// fetch value
	sourceReply, err := sourceClient.PipeValueCommand(keyInfo)
	if err != nil {
		panic(logger.Critical(err))
	}

	targetReply, err := targetClient.PipeValueCommand(keyInfo)
	if err != nil {
		panic(logger.Critical(err))
	}

	// compare value
	for i, oneKeyInfo := range keyInfo {
		switch oneKeyInfo.Tp {
		case common.StringKeyType:
			var sourceValue, targetValue []byte
			if sourceReply[i] != nil {
				sourceValue = sourceReply[i].([]byte)
			}
			if targetReply[i] != nil {
				targetValue = targetReply[i].([]byte)
			}
			p.Compare_String(oneKeyInfo, conflictKey, sourceValue, targetValue)
			p.IncrKeyStat(oneKeyInfo)
		case common.HashKeyType:
			fallthrough
		case common.ZsetKeyType:
			sourceValue, targetValue := ValueHelper_Hash_SortedSet(sourceReply[i]), ValueHelper_Hash_SortedSet(targetReply[i])
			p.Compare_Hash_Set_SortedSet(oneKeyInfo, conflictKey, sourceValue, targetValue)
		case common.ListKeyType:
			sourceValue, targetValue := ValueHelper_List(sourceReply[i]), ValueHelper_List(targetReply[i])
			p.Compare_List(oneKeyInfo, conflictKey, sourceValue, targetValue)
		case common.SetKeyType:
			sourceValue, targetValue := ValueHelper_Set(sourceReply[i]), ValueHelper_Set(targetReply[i])
			p.Compare_Hash_Set_SortedSet(oneKeyInfo, conflictKey, sourceValue, targetValue)
		}
	}
}

func (p *FullValueVerifier) CheckPartialValueHash(oneKeyInfo *common.Key, conflictKey chan<- *common.Key, sourceClient *RedisClient, targetClient *RedisClient) {
	sourceValue, targetValue := make(map[string][]byte), make(map[string][]byte)
	for fieldIndex := 0; fieldIndex < len(oneKeyInfo.Field); {
		args := make([]interface{}, 0, p.param.batchCount)
		args = append(args, oneKeyInfo.Key)
		for count := 0; count < p.param.batchCount && fieldIndex < len(oneKeyInfo.Field); count, fieldIndex = count+1, fieldIndex+1 {
			args = append(args, oneKeyInfo.Field[fieldIndex].Field)
		}

		sourceReply, err := sourceClient.Do("hmget", args...)
		if err != nil {
			panic(logger.Error(err))
		}
		targetReply, err := targetClient.Do("hmget", args...)
		if err != nil {
			panic(logger.Error(err))
		}
		sendField := args[1:]

		tmpSourceValue, tmpTargetValue := sourceReply.([]interface{}), targetReply.([]interface{})
		for i := 0; i < len(sendField); i++ {
			fieldStr := string(sendField[i].([]byte))
			if tmpSourceValue[i] != nil {
				sourceValue[fieldStr] = tmpSourceValue[i].([]byte)
			}
			if tmpTargetValue[i] != nil {
				targetValue[fieldStr] = tmpTargetValue[i].([]byte)
			}
		}
	} // end of for fieldIndex := 0; fieldIndex < len(oneKeyInfo.Field)
	p.Compare_Hash_Set_SortedSet(oneKeyInfo, conflictKey, sourceValue, targetValue)
}

func (p *FullValueVerifier) CheckPartialValueSet(oneKeyInfo *common.Key, conflictKey chan<- *common.Key, sourceClient *RedisClient, targetClient *RedisClient) {
	sourceValue, targetValue := make(map[string][]byte), make(map[string][]byte)
	for fieldIndex := 0; fieldIndex < len(oneKeyInfo.Field); {
		sendField := make([][]byte, 0, p.param.batchCount)
		for count := 0; count < p.param.batchCount && fieldIndex < len(oneKeyInfo.Field); count, fieldIndex = count+1, fieldIndex+1 {
			sendField = append(sendField, oneKeyInfo.Field[fieldIndex].Field)
		}
		tmpSourceValue, err := sourceClient.PipeSismemberCommand(oneKeyInfo.Key, sendField)
		if err != nil {
			panic(logger.Error(err))
		}
		tmpTargetValue, err := targetClient.PipeSismemberCommand(oneKeyInfo.Key, sendField)
		if err != nil {
			panic(logger.Error(err))
		}
		for i := 0; i < len(sendField); i++ {
			fieldStr := string(sendField[i])
			sourceNum := tmpSourceValue[i].(int64)
			if sourceNum != 0 {
				sourceValue[fieldStr] = nil
			}
			targetNum := tmpTargetValue[i].(int64)
			if targetNum != 0 {
				targetValue[fieldStr] = nil
			}
		}
	} // for fieldIndex := 0; fieldIndex < len(oneKeyInfo.Field);
	p.Compare_Hash_Set_SortedSet(oneKeyInfo, conflictKey, sourceValue, targetValue)
}

func (p *FullValueVerifier) CheckPartialValueSortedSet(oneKeyInfo *common.Key, conflictKey chan<- *common.Key, sourceClient *RedisClient, targetClient *RedisClient) {
	sourceValue, targetValue := make(map[string][]byte), make(map[string][]byte)
	for fieldIndex := 0; fieldIndex < len(oneKeyInfo.Field); {
		sendField := make([][]byte, 0, p.param.batchCount)
		for count := 0; count < p.param.batchCount && fieldIndex < len(oneKeyInfo.Field); count, fieldIndex = count+1, fieldIndex+1 {
			sendField = append(sendField, oneKeyInfo.Field[fieldIndex].Field)
		}

		tmpSourceValue, err := sourceClient.PipeZscoreCommand(oneKeyInfo.Key, sendField)
		if err != nil {
			panic(logger.Error(err))
		}
		tmpTargetValue, err := targetClient.PipeZscoreCommand(oneKeyInfo.Key, sendField)
		if err != nil {
			panic(logger.Error(err))
		}

		for i := 0; i < len(sendField); i++ {
			fieldStr := string(sendField[i])
			if tmpSourceValue[i] != nil {
				sourceValue[fieldStr] = tmpSourceValue[i].([]byte)
			}
			if tmpTargetValue[i] != nil {
				targetValue[fieldStr] = tmpTargetValue[i].([]byte)
			}
		}
	}
	p.Compare_Hash_Set_SortedSet(oneKeyInfo, conflictKey, sourceValue, targetValue)
}

func (p *FullValueVerifier) CheckFullBigValue_List(oneKeyInfo *common.Key, conflictKey chan<- *common.Key, sourceClient *RedisClient, targetClient *RedisClient) {
	conflictField := make([]common.Field, 0, oneKeyInfo.SourceAttr.ItemCount/100+1)
	oneCmpCount := p.param.batchCount * 10
	if oneCmpCount > 10240 {
		oneCmpCount = 10240
	}

	startIndex := 0
	for {
		sourceReply, err := sourceClient.Do("lrange", oneKeyInfo.Key, startIndex, startIndex+oneCmpCount-1)
		if err != nil {
			panic(logger.Critical(err))
		}
		sourceValue := sourceReply.([]interface{})

		targetReply, err := targetClient.Do("lrange", oneKeyInfo.Key, startIndex, startIndex+oneCmpCount-1)
		if err != nil {
			panic(logger.Error(err))
		}
		targetValue := targetReply.([]interface{})

		minLen := common.Min(len(sourceValue), len(targetValue))
		for i := 0; i < minLen; i++ {
			if bytes.Equal(sourceValue[i].([]byte), targetValue[i].([]byte)) == false {
				field := common.Field{
					Field:        []byte(strconv.FormatInt(int64(startIndex+i), 10)),
					ConflictType: common.ValueConflict,
				}
				conflictField = append(conflictField, field)
				p.IncrFieldStat(oneKeyInfo, common.ValueConflict)
			} else {
				p.IncrFieldStat(oneKeyInfo, common.NoneConflict)
			}
		}
		// list 只返回第一个不相同的位置
		if len(conflictField) != 0 {
			break
		}
		// 说明source或者target list，已经读完了
		if minLen < oneCmpCount {
			break
		}
		startIndex += oneCmpCount
	} // end for{}

	if len(conflictField) != 0 {
		// list 只返回第一个不相同的位置
		oneKeyInfo.Field = conflictField[0:1]
		oneKeyInfo.ConflictType = common.ValueConflict
		conflictKey <- oneKeyInfo
	} else {
		oneKeyInfo.Field = nil
		oneKeyInfo.ConflictType = common.NoneConflict
	}
	p.IncrKeyStat(oneKeyInfo)
}

func (p *FullValueVerifier) Compare_String(oneKeyInfo *common.Key, conflictKey chan<- *common.Key, sourceValue, targetValue []byte) {
	if len(sourceValue) == 0 {
		if len(targetValue) == 0 {
			oneKeyInfo.ConflictType = common.NoneConflict
		} else {
			oneKeyInfo.ConflictType = common.LackSourceConflict
		}
	} else if len(targetValue) == 0 {
		if len(sourceValue) == 0 {
			oneKeyInfo.ConflictType = common.NoneConflict
		} else {
			oneKeyInfo.ConflictType = common.LackTargetConflict
		}
	} else if bytes.Equal(sourceValue, targetValue) == false {
		oneKeyInfo.ConflictType = common.ValueConflict
	} else {
		oneKeyInfo.ConflictType = common.NoneConflict
	}
	if oneKeyInfo.ConflictType != common.NoneConflict {
		conflictKey <- oneKeyInfo
	}
}

func (p *FullValueVerifier) Compare_Hash_Set_SortedSet(oneKeyInfo *common.Key, conflictKey chan<- *common.Key, sourceValue, targetValue map[string][]byte) {
	conflictField := make([]common.Field, 0, len(sourceValue)/50+1)
	for k, v := range sourceValue {
		vTarget, ok := targetValue[k]
		if ok == false {
			conflictField = append(conflictField, common.Field{
				Field: []byte(k),
				ConflictType: common.LackTargetConflict})
			p.IncrFieldStat(oneKeyInfo, common.LackTargetConflict)
		} else {
			delete(targetValue, k)
			if bytes.Equal(v, vTarget) == false {
				conflictField = append(conflictField, common.Field{
					Field: []byte(k),
					ConflictType: common.ValueConflict})
				p.IncrFieldStat(oneKeyInfo, common.ValueConflict)
			} else {
				p.IncrFieldStat(oneKeyInfo, common.NoneConflict)
			}
		}
	}

	for k, _ := range targetValue {
		conflictField = append(conflictField, common.Field{
			Field: []byte(k),
			ConflictType: common.LackSourceConflict})
		p.IncrFieldStat(oneKeyInfo, common.LackSourceConflict)
	}

	if len(conflictField) != 0 {
		oneKeyInfo.Field = conflictField
		oneKeyInfo.ConflictType = common.ValueConflict
		conflictKey <- oneKeyInfo
	} else {
		oneKeyInfo.ConflictType = common.NoneConflict
	}
	p.IncrKeyStat(oneKeyInfo)
}

func (p *FullValueVerifier) Compare_List(oneKeyInfo *common.Key, conflictKey chan<- *common.Key, sourceValue, targetValue [][]byte) {
	minLen := common.Min(len(sourceValue), len(targetValue))

	oneKeyInfo.ConflictType = common.NoneConflict
	for i := 0; i < minLen; i++ {
		if bytes.Equal(sourceValue[i], targetValue[i]) == false {
			// list 只保存第一个不一致的field
			oneKeyInfo.Field = make([]common.Field, 1)
			oneKeyInfo.Field[0] = common.Field{
				Field: []byte(strconv.FormatInt(int64(i), 10)),
				ConflictType: common.ValueConflict}
			oneKeyInfo.ConflictType = common.ValueConflict
			conflictKey <- oneKeyInfo
			break
		}
	}
	p.IncrKeyStat(oneKeyInfo)
}

type FullCheckParameter struct {
	sourceHost   RedisHost
	targetHost   RedisHost
	resultDBFile string
	compareCount int
	interval     int
	batchCount   int
	parallel     int
	filterTree   *common.Trie
}

type FullCheck struct {
	FullCheckParameter

	stat            metric.Stat
	currentDB       int32
	times           int
	db              [100]*sql.DB
	sourceIsProxy   bool
	sourceNodeCount int
	sourceDBNums    map[int32]int64

	totalConflict      int64
	totalKeyConflict   int64
	totalFieldConflict int64

	verifier IVerifier
}

func NewFullCheck(f FullCheckParameter, checktype CheckType) *FullCheck {
	var verifier IVerifier

	fullcheck := &FullCheck{
		FullCheckParameter: f,
	}

	switch checktype {
	case ValueLengthOutline:
		verifier = NewValueOutlineVerifier(&fullcheck.stat, &fullcheck.FullCheckParameter)
	case KeyOutline:
		verifier = NewKeyOutlineVerifier(&fullcheck.stat, &fullcheck.FullCheckParameter)
	case FullValue:
		verifier = NewFullValueVerifier(&fullcheck.stat, &fullcheck.FullCheckParameter, false)
	case FullValueWithOutline:
		verifier = NewFullValueVerifier(&fullcheck.stat, &fullcheck.FullCheckParameter, true)
	default:
		panic(fmt.Sprintf("no such check type : %d", checktype))
	}

	fullcheck.verifier = verifier
	return fullcheck
}

func (p *FullCheck) PrintStat(finished bool) {
	var buf bytes.Buffer

	var metricStat *metric.Metric
	finishPercent := p.stat.Scan.Total() * 100 * int64(p.times) / (p.sourceDBNums[p.currentDB] * int64(p.compareCount))
	if p.times == 1 {
		metricStat = &metric.Metric{
			CompareTimes: p.times,
			Db: p.currentDB,
			DbKeys: p.sourceDBNums[p.currentDB],
			Process: finishPercent,
			OneCompareFinished: finished,
			AllFinished: false,
			Timestamp:   time.Now().Unix(),
			DateTime: time.Now().Format("2006-01-02T15:04:05Z"),
			Id: opts.Id,
			JobId: opts.JobId,
			TaskId: opts.TaskId}
		fmt.Fprintf(&buf, "times:%d,db:%d,dbkeys:%d,finish:%d%%,finished:%v\n", p.times, p.currentDB,
			p.sourceDBNums[p.currentDB], finishPercent, finished)
	} else {
		metricStat = &metric.Metric{
			CompareTimes: p.times,
			Db: p.currentDB,
			Process: finishPercent,
			OneCompareFinished: finished,
			AllFinished: false,
			Timestamp: time.Now().Unix(),
			DateTime: time.Now().Format("2006-01-02T15:04:05Z"),
			Id: opts.Id,
			JobId: opts.JobId,
			TaskId: opts.TaskId}
		fmt.Fprintf(&buf, "times:%d,db:%d,finished:%v\n", p.times, p.currentDB, finished)
	}

	p.totalConflict = int64(0)
	p.totalKeyConflict = int64(0)
	p.totalFieldConflict = int64(0)

	// fmt.Fprintf(&buf, "--- key scan ---\n")
	fmt.Fprintf(&buf, "KeyScan:%v\n", p.stat.Scan)
	metricStat.KeyScan = p.stat.Scan.Json()
	metricStat.KeyMetric = make(map[string]map[string]*metric.CounterStat)

	// fmt.Fprintf(&buf, "--- key equal ---\n")
	for i := common.KeyTypeIndex(0); i < common.EndKeyTypeIndex; i++ {
		metricStat.KeyMetric[i.String()] = make(map[string]*metric.CounterStat)
		if p.stat.ConflictKey[i][common.NoneConflict].Total() != 0 {
			metricStat.KeyMetric[i.String()]["equal"] = p.stat.ConflictKey[i][common.NoneConflict].Json()
			if p.times == p.compareCount {
				fmt.Fprintf(&buf, "KeyEqualAtLast|%s|%s|%v\n", i, common.NoneConflict, 
					p.stat.ConflictKey[i][common.NoneConflict])
			} else {
				fmt.Fprintf(&buf, "KeyEqualInProcess|%s|%s|%v\n", i, common.NoneConflict, 
					p.stat.ConflictKey[i][common.NoneConflict])
			}
		}
	}
	// fmt.Fprintf(&buf, "--- key conflict ---\n")
	for i := common.KeyTypeIndex(0); i < common.EndKeyTypeIndex; i++ {
		for j := common.ConflictType(0); j < common.NoneConflict; j++ {
			if p.stat.ConflictKey[i][j].Total() != 0 {
				metricStat.KeyMetric[i.String()][j.String()] = p.stat.ConflictKey[i][j].Json()
				if p.times == p.compareCount {
					fmt.Fprintf(&buf, "KeyConflictAtLast|%s|%s|%v\n", i, j, p.stat.ConflictKey[i][j])
					p.totalKeyConflict += p.stat.ConflictKey[i][j].Total()
				} else {
					fmt.Fprintf(&buf, "KeyConflictInProcess|%s|%s|%v\n", i, j, p.stat.ConflictKey[i][j])
				}
			}
		}
	}

	metricStat.FieldMetric = make(map[string]map[string]*metric.CounterStat)
	// fmt.Fprintf(&buf, "--- field equal ---\n")
	for i := common.KeyTypeIndex(0); i < common.EndKeyTypeIndex; i++ {
		metricStat.FieldMetric[i.String()] = make(map[string]*metric.CounterStat)
		if p.stat.ConflictField[i][common.NoneConflict].Total() != 0 {
			metricStat.FieldMetric[i.String()]["equal"] = p.stat.ConflictField[i][common.NoneConflict].Json()
			if p.times == p.compareCount {
				fmt.Fprintf(&buf, "FieldEqualAtLast|%s|%s|%v\n", i, common.NoneConflict,
					p.stat.ConflictField[i][common.NoneConflict])
			} else {
				fmt.Fprintf(&buf, "FieldEqualInProcess|%s|%s|%v\n", i, common.NoneConflict,
					p.stat.ConflictField[i][common.NoneConflict])
			}
		}
	}
	// fmt.Fprintf(&buf, "--- field conflict  ---\n")
	for i := common.KeyTypeIndex(0); i < common.EndKeyTypeIndex; i++ {
		for j := common.ConflictType(0); j < common.NoneConflict; j++ {
			if p.stat.ConflictField[i][j].Total() != 0 {
				metricStat.FieldMetric[i.String()][j.String()] = p.stat.ConflictField[i][j].Json()
				if p.times == p.compareCount {
					fmt.Fprintf(&buf, "FieldConflictAtLast|%s|%s|%v\n", i, j, p.stat.ConflictField[i][j])
					p.totalFieldConflict += p.stat.ConflictField[i][j].Total()
				} else {
					fmt.Fprintf(&buf, "FieldConflictInProcess|%s|%s|%v\n", i, j, p.stat.ConflictField[i][j])
				}
			}
		}
	}

	p.totalConflict = p.totalKeyConflict + p.totalFieldConflict
	if len(opts.MetricFile) > 0 {
		metricsfile, _ := os.OpenFile(opts.MetricFile, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
		defer metricsfile.Close()

		metricstr, _ := json.Marshal(metricStat)
		metricsfile.WriteString(fmt.Sprintf("%s\n", string(metricstr)))

		if p.times == p.compareCount && finished {
			metricStat.AllFinished = true
			metricStat.Process = int64(100)
			metricStat.TotalConflict = p.totalConflict
			metricStat.TotalKeyConflict = p.totalKeyConflict
			metricStat.TotalFieldConflict = p.totalFieldConflict

			metricstr, _ := json.Marshal(metricStat)
			metricsfile.WriteString(fmt.Sprintf("%s\n", string(metricstr)))
		}
	} else {
		logger.Infof("stat:\n%s", string(buf.Bytes()))
	}
}

func (p *FullCheck) IncrScanStat(a int) {
	p.stat.Scan.Inc(a)
}

func (p *FullCheck) Start() {
	if len(opts.MetricFile) > 0 {
		os.Remove(opts.MetricFile)
	}

	var err error

	for i := 1; i <= p.compareCount; i++ {
		// init sqlite db
		os.Remove(p.resultDBFile + "." + strconv.Itoa(i))
		p.db[i], err = sql.Open("sqlite3", p.resultDBFile+"."+strconv.Itoa(i))
		if err != nil {
			panic(logger.Critical(err))
		}
		defer p.db[i].Close()
	}

	// 取keyspace
	sourceClient, err := NewRedisClient(p.sourceHost, 0)
	if err != nil {
		panic(logger.Errorf("create redis client with host[%v] db[%v] error[%v]",
			p.sourceHost, 0, err))
	}

	keyspaceContent, err := sourceClient.Do("info", "Keyspace")
	if err != nil {
		panic(logger.Error(err))
	}
	p.sourceDBNums, err = ParseKeyspace(keyspaceContent.([]byte))
	if err != nil {
		panic(logger.Error(err))
	}
	_, err = sourceClient.Do("iscan", 0, 0, "count", 1)
	if err != nil {
		if strings.Contains(err.Error(), "ERR unknown command") {
			p.sourceIsProxy = false
			p.sourceNodeCount = 1
		} else {
			panic(logger.Error(err))
		}
	} else {
		p.sourceIsProxy = true
		info, err := redis.Bytes(sourceClient.Do("info", "Cluster"))
		if err != nil {
			panic(logger.Error(err))
		}
		result := ParseInfo(info)
		sourceNodeCount, err := strconv.ParseInt(result["nodecount"], 10, 0)
		if err != nil {
			panic(logger.Error(err))
		}
		if sourceNodeCount <= 0 {
			panic(logger.Errorf("sourceNodeCount %d <=0", sourceNodeCount))
		}
		p.sourceNodeCount = int(sourceNodeCount)
	}
	logger.Infof("sourceIsProxy=%v,p.sourceNodeCount=%d", p.sourceIsProxy, p.sourceNodeCount)

	sourceClient.Close()
	for db, keyNum := range p.sourceDBNums {
		logger.Infof("db=%d:keys=%d", db, keyNum)
	}

	for p.times = 1; p.times <= p.compareCount; p.times++ {
		p.CreateDbTable(p.times)
		if p.times != 1 {
			logger.Infof("wait %d seconds before start", p.interval)
			time.Sleep(time.Second * time.Duration(p.interval))
		}
		logger.Infof("start %dth time compare", p.times)

		for db, _ := range p.sourceDBNums {
			p.currentDB = db
			p.stat.Reset()
			// init stat timer
			var tickerStat *time.Ticker = time.NewTicker(time.Second * common.StatRollFrequency)
			ctxStat, cancelStat := context.WithCancel(context.Background()) // 主动cancel
			go func(ctx context.Context) {
				defer tickerStat.Stop()
				for _ = range tickerStat.C {
					select { // 判断是否结束
					case <-ctx.Done():
						return
					default:
					}
					p.stat.Rotate()
					p.PrintStat(false)
				}
			}(ctxStat)

			logger.Infof("start compare db %d", p.currentDB)
			keys := make(chan []*common.Key, 1024)
			conflictKey := make(chan *common.Key, 1024)
			var wg, wg2 sync.WaitGroup
			// start scan, get all keys
			if p.times == 1 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					p.ScanFromSourceRedis(keys)
				}()
			} else {
				wg.Add(1)
				go func() {
					defer wg.Done()
					p.ScanFromDB(keys)
				}()
			}

			// start check
			wg.Add(p.parallel)
			for i := 0; i < p.parallel; i++ {
				go func() {
					defer wg.Done()
					p.VerifyAllKeyInfo(keys, conflictKey)
				}()
			}
			// start write conflictKey
			wg2.Add(1)
			if p.times == 1 {
				go func() {
					defer wg2.Done()
					p.WriteConflictKey(conflictKey)
				}()
			} else if p.times < p.compareCount {
				go func() {
					defer wg2.Done()
					p.WriteConflictKey(conflictKey)
				}()
			} else {
				go func() {
					defer wg2.Done()
					p.WriteConflictKey(conflictKey)
				}()
			}

			wg.Wait()
			close(conflictKey)
			wg2.Wait()
			cancelStat() // stop stat goroutine
			p.PrintStat(true)
		} // for db, keyNum := range dbNums
	} // end for
	logger.Infof("all finish successfully, totally %d keys or fields conflict", p.totalConflict)
}

func (p *FullCheck) GetCurrentResultTable() (key string, field string) {
	if p.times != p.compareCount {
		return fmt.Sprintf("key_%d", p.times), fmt.Sprintf("field_%d", p.times)
	} else {
		return "key", "field"
	}
}

func (p *FullCheck) GetLastResultTable() (key string, field string) {
	return fmt.Sprintf("key_%d", p.times-1), fmt.Sprintf("field_%d", p.times-1)
}

func (p *FullCheck) CreateDbTable(times int) {
	/** create table **/
	conflictKeyTableName, conflictFieldTableName := p.GetCurrentResultTable()

	conflictKeyTableSql := fmt.Sprintf(`
CREATE TABLE %s(
   id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
   key            TEXT NOT NULL,
   type           TEXT NOT NULL,
   conflict_type  TEXT NOT NULL,
   db             INTEGER NOT NULL,
   source_len     INTEGER NOT NULL,
   target_len     INTEGER NOT NULL
);
`, conflictKeyTableName)
	_, err := p.db[times].Exec(conflictKeyTableSql)
	if err != nil {
		panic(logger.Errorf("exec sql %s failed: %s", conflictKeyTableSql, err))
	}
	conflictFieldTableSql := fmt.Sprintf(`
CREATE TABLE %s(
   id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
   field          TEXT NOT NULL,
   conflict_type  TEXT NOT NULL,
   key_id         INTEGER NOT NULL
);
`, conflictFieldTableName)
	_, err = p.db[times].Exec(conflictFieldTableSql)
	if err != nil {
		panic(logger.Errorf("exec sql %s failed: %s", conflictFieldTableSql, err))
	}

	conflictResultSql := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s(
	InstanceA	TEXT NOT NULL,
	InstanceB	TEXT NOT NULL,
	Key			TEXT NOT NULL,
	Schema		TEXT NOT NULL,
	InconsistentType TEXT NOT NULL,
	Extra	    TEXT NOT NULL
	);`, "FINAL_RESULT")
	_, err = p.db[times].Exec(conflictResultSql)
	if err != nil {
		panic(logger.Errorf("exec sql %s failed: %s", conflictResultSql, err))
	}
}

func (p *FullCheck) ScanFromSourceRedis(allKeys chan<- []*common.Key) {
	sourceClient, err := NewRedisClient(p.sourceHost, p.currentDB)
	if err != nil {
		panic(logger.Errorf("create redis client with host[%v] db[%v] error[%v]",
			p.sourceHost, p.currentDB, err))
	}
	defer sourceClient.Close()

	for idx := 0; idx < p.sourceNodeCount; idx++ {
		cursor := 0
		for {
			var reply interface{}
			var err error
			if p.sourceIsProxy == false {
				reply, err = sourceClient.Do("scan", cursor, "count", p.batchCount)
			} else {
				reply, err = sourceClient.Do("iscan", idx, cursor, "count", p.batchCount)
			}

			if err != nil {
				panic(logger.Critical(err))
			}

			replyList, ok := reply.([]interface{})
			if ok == false || len(replyList) != 2 {
				panic(logger.Criticalf("scan %d count %d failed, result: %+v", cursor, p.batchCount, reply))
			}

			bytes, ok := replyList[0].([]byte)
			if ok == false {
				panic(logger.Criticalf("scan %d count %d failed, result: %+v", cursor, p.batchCount, reply))
			}

			cursor, err = strconv.Atoi(string(bytes))
			if err != nil {
				panic(logger.Critical(err))
			}

			keylist, ok := replyList[1].([]interface{})
			if ok == false {
				panic(logger.Criticalf("scan failed, result: %+v", reply))
			}
			keysInfo := make([]*common.Key, 0, len(keylist))
			for _, value := range keylist {
				bytes, ok = value.([]byte)
				if ok == false {
					panic(logger.Criticalf("scan failed, result: %+v", reply))
				}

				// check filter list
				if common.CheckFilter(p.filterTree, bytes) == false {
					continue
				}
				
				keysInfo = append(keysInfo, &common.Key{
					Key:          bytes,
					Tp:           common.EndKeyType,
					ConflictType: common.EndConflict,
				})
			}
			p.IncrScanStat(len(keysInfo))
			allKeys <- keysInfo

			if cursor == 0 {
				break
			}
		} // end for{}
	} // end fo for idx := 0; idx < p.sourceNodeCount; idx++
	close(allKeys)
}

func (p *FullCheck) ScanFromDB(allKeys chan<- []*common.Key) {
	conflictKeyTableName, conflictFieldTableName := p.GetLastResultTable()

	keyQuery := fmt.Sprintf("select id,key,type,conflict_type,source_len,target_len from %s where id>? and db=%d limit %d",
		conflictKeyTableName, p.currentDB, p.batchCount)
	keyStatm, err := p.db[p.times-1].Prepare(keyQuery)
	if err != nil {
		panic(logger.Error(err))
	}
	defer keyStatm.Close()

	fieldQuery := fmt.Sprintf("select field,conflict_type from %s where key_id=?", conflictFieldTableName)
	fieldStatm, err := p.db[p.times-1].Prepare(fieldQuery)
	if err != nil {
		panic(logger.Error(err))
	}
	defer fieldStatm.Close()

	var startId int64 = 0
	for {
		rows, err := keyStatm.Query(startId)
		if err != nil {
			panic(logger.Error(err))
		}
		keyInfo := make([]*common.Key, 0, p.batchCount)
		for rows.Next() {
			var key, keytype, conflictType string
			var id, source_len, target_len int64
			err = rows.Scan(&id, &key, &keytype, &conflictType, &source_len, &target_len)
			if err != nil {
				panic(logger.Error(err))
			}
			oneKeyInfo := &common.Key{
				Key:          []byte(key),
				Tp:           common.NewKeyType(keytype),
				ConflictType: common.NewConflictType(conflictType),
				SourceAttr:   common.Attribute{ItemCount: source_len},
				TargetAttr:   common.Attribute{ItemCount: target_len},
			}
			if oneKeyInfo.Tp == common.EndKeyType {
				panic(logger.Errorf("invalid type from table %s: key=%s type=%s ", conflictKeyTableName, key, keytype))
			}
			if oneKeyInfo.ConflictType == common.EndConflict {
				panic(logger.Errorf("invalid conflict_type from table %s: key=%s conflict_type=%s ", conflictKeyTableName, key, conflictType))
			}

			if oneKeyInfo.Tp != common.StringKeyType {
				oneKeyInfo.Field = make([]common.Field, 0, 10)
				rowsField, err := fieldStatm.Query(id)
				if err != nil {
					panic(logger.Error(err))
				}
				for rowsField.Next() {
					var field, conflictType string
					err = rowsField.Scan(&field, &conflictType)
					if err != nil {
						panic(logger.Error(err))
					}
					oneField := common.Field{
						Field:        []byte(field),
						ConflictType: common.NewConflictType(conflictType),
					}
					if oneField.ConflictType == common.EndConflict {
						panic(logger.Errorf("invalid conflict_type from table %s: field=%s type=%s ", conflictFieldTableName, field, conflictType))
					}
					oneKeyInfo.Field = append(oneKeyInfo.Field, oneField)
				}
				if err := rowsField.Err(); err != nil {
					panic(logger.Error(err))
				}
				rowsField.Close()
			}
			keyInfo = append(keyInfo, oneKeyInfo)
			if startId < id {
				startId = id
			}
		} // rows.Next
		if err := rows.Err(); err != nil {
			panic(logger.Error(err))
		}
		rows.Close()
		// 结束
		if len(keyInfo) == 0 {
			close(allKeys)
			break
		}
		p.IncrScanStat(len(keyInfo))
		allKeys <- keyInfo
	} // for{}
}

func (p *FullCheck) VerifyAllKeyInfo(allKeys <-chan []*common.Key, conflictKey chan<- *common.Key) {
	sourceClient, err := NewRedisClient(p.sourceHost, p.currentDB)
	if err != nil {
		panic(logger.Errorf("create redis client with host[%v] db[%v] error[%v]",
			p.sourceHost, p.currentDB, err))
	}
	defer sourceClient.Close()

	targetClient, err := NewRedisClient(p.targetHost, p.currentDB)
	if err != nil {
		panic(logger.Errorf("create redis client with host[%v] db[%v] error[%v]",
			p.targetHost, p.currentDB, err))
	}
	defer targetClient.Close()

	divisor := int(math.Max(1, float64(opts.Qps / 1000 / opts.Parallel)))
	standardTime := int64(p.batchCount * 3 / divisor)
	for keyInfo := range allKeys {
		begin := time.Now().UnixNano() / 1000 / 1000

		p.verifier.VerifyOneGroupKeyInfo(keyInfo, conflictKey, &sourceClient, &targetClient)

		interval := time.Now().UnixNano()/1000/1000 - begin
		if standardTime-interval > 0 {
			time.Sleep(time.Duration(standardTime-interval) * time.Millisecond)
		}

	} // for oneGroupKeys := range allKeys
}

func (p *FullCheck) FetchTypeAndLen(keyInfo []*common.Key, sourceClient *RedisClient, targetClient *RedisClient) {
	// fetch type
	sourceKeyTypeStr, err := sourceClient.PipeTypeCommand(keyInfo)
	if err != nil {
		panic(logger.Critical(err))
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
			panic(logger.Critical(err))
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
			panic(logger.Critical(err))
		}
		for i, keylen := range targetKeyLen {
			keyInfo[i].TargetAttr.ItemCount = keylen
		}
		wg.Done()
	}()

	wg.Wait()
}

func (p *FullCheck) WriteConflictKey(conflictKey <-chan *common.Key) {
	conflictKeyTableName, conflictFieldTableName := p.GetCurrentResultTable()

	var resultfile *os.File
	if len(opts.ResultFile) > 0 {
		resultfile, _ = os.OpenFile(opts.ResultFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
		defer resultfile.Close()
	}

	tx, _ := p.db[p.times].Begin()
	statInsertKey, err := tx.Prepare(fmt.Sprintf("insert into %s (key, type, conflict_type, db, source_len, target_len) values(?,?,?,?,?,?)", conflictKeyTableName))
	if err != nil {
		panic(logger.Error(err))
	}
	statInsertField, err := tx.Prepare(fmt.Sprintf("insert into %s (field, conflict_type, key_id) values (?,?,?)", conflictFieldTableName))
	if err != nil {
		panic(logger.Error(err))
	}

	count := 0
	for oneKeyInfo := range conflictKey {
		if count%1000 == 0 {
			var err error
			statInsertKey.Close()
			statInsertField.Close()
			e := tx.Commit()
			if e != nil {
				logger.Error(e.Error())
			}

			tx, _ = p.db[p.times].Begin()
			statInsertKey, err = tx.Prepare(fmt.Sprintf("insert into %s (key, type, conflict_type, db, source_len, target_len) values(?,?,?,?,?,?)", conflictKeyTableName))
			if err != nil {
				panic(logger.Error(err))
			}

			statInsertField, err = tx.Prepare(fmt.Sprintf("insert into %s (field, conflict_type, key_id) values (?,?,?)", conflictFieldTableName))
			if err != nil {
				panic(logger.Error(err))
			}
		}
		count += 1

		result, err := statInsertKey.Exec(string(oneKeyInfo.Key), oneKeyInfo.Tp.Name, oneKeyInfo.ConflictType.String(), p.currentDB, oneKeyInfo.SourceAttr.ItemCount, oneKeyInfo.TargetAttr.ItemCount)
		if err != nil {
			panic(logger.Error(err))
		}
		if len(oneKeyInfo.Field) != 0 {
			lastId, _ := result.LastInsertId()
			for i := 0; i < len(oneKeyInfo.Field); i++ {
				_, err = statInsertField.Exec(string(oneKeyInfo.Field[i].Field), oneKeyInfo.Field[i].ConflictType.String(), lastId)
				if err != nil {
					panic(logger.Error(err))
				}

				if p.times == p.compareCount {
					finalstat, err := tx.Prepare(fmt.Sprintf("insert into FINAL_RESULT (InstanceA, InstanceB, Key, Schema, InconsistentType, Extra) VALUES(?, ?, ?, ?, ?, ?)"))
					if err != nil {
						panic(logger.Error(err))
					}
					// defer finalstat.Close()
					_, err = finalstat.Exec("", "", string(oneKeyInfo.Key), strconv.Itoa(int(p.currentDB)),
						oneKeyInfo.Field[i].ConflictType.String(),
						string(oneKeyInfo.Field[i].Field))
					if err != nil {
						panic(logger.Error(err))
					}

					finalstat.Close()

					if len(opts.ResultFile) != 0 {
						resultfile.WriteString(fmt.Sprintf("%d\t%s\t%s\t%s\n", int(p.currentDB), oneKeyInfo.Field[i].ConflictType.String(), string(oneKeyInfo.Key), string(oneKeyInfo.Field[i].Field)))
					}
				}
			}
		} else {
			if p.times == p.compareCount {
				finalstat, err := tx.Prepare(fmt.Sprintf("insert into FINAL_RESULT (InstanceA, InstanceB, Key, Schema, InconsistentType, Extra) VALUES(?, ?, ?, ?, ?, ?)"))
				if err != nil {
					panic(logger.Error(err))
				}
				// defer finalstat.Close()
				_, err = finalstat.Exec("", "", string(oneKeyInfo.Key), strconv.Itoa(int(p.currentDB)), oneKeyInfo.ConflictType.String(), "")
				if err != nil {
					panic(logger.Error(err))
				}
				finalstat.Close()

				if len(opts.ResultFile) != 0 {
					resultfile.WriteString(fmt.Sprintf("%d\t%s\t%s\t%s\n", int(p.currentDB), oneKeyInfo.ConflictType.String(), string(oneKeyInfo.Key), ""))
				}
			}
		}
	}
	statInsertKey.Close()
	statInsertField.Close()
	tx.Commit()
}
