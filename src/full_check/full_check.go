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
)

type CheckType int

const (
	FullValue            = 1
	ValueLengthOutline   = 2
	KeyOutline           = 3
	FullValueWithOutline = 4
)

var (
	BigKeyThreshold int64 = 16384
)

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

type Stat struct {
	scan          AtomicSpeedCounter
	conflictField [EndKeyTypeIndex][EndConflict]AtomicSpeedCounter
	conflictKey   [EndKeyTypeIndex][EndConflict]AtomicSpeedCounter
}

func (p *Stat) Rotate() {
	p.scan.Rotate()
	for keyType := KeyTypeIndex(0); keyType < EndKeyTypeIndex; keyType++ {
		for conType := ConflictType(0); conType < EndConflict; conType++ {
			p.conflictField[keyType][conType].Rotate()
			p.conflictKey[keyType][conType].Rotate()
		}
	}
}

func (p *Stat) Reset() {
	p.scan.Reset()
	for keyType := KeyTypeIndex(0); keyType < EndKeyTypeIndex; keyType++ {
		for conType := ConflictType(0); conType < EndConflict; conType++ {
			p.conflictField[keyType][conType].Reset()
			p.conflictKey[keyType][conType].Reset()
		}
	}
}

type IVerifier interface {
	VerifyOneGroupKeyInfo(keyInfo []*Key, conflictKey chan<- *Key, sourceClient *RedisClient, targetClient *RedisClient)
}

type VerifierBase struct {
	stat         *Stat
	param        *FullCheckParameter
}

func (p *VerifierBase) IncrKeyStat(oneKeyInfo *Key) {
	p.stat.conflictKey[oneKeyInfo.tp.index][oneKeyInfo.conflictType].Inc(1)
}

func (p *VerifierBase) IncrFieldStat(oneKeyInfo *Key, conType ConflictType) {
	p.stat.conflictField[oneKeyInfo.tp.index][conType].Inc(1)
}

func (p *VerifierBase) FetchTypeAndLen(keyInfo []*Key, sourceClient *RedisClient, targetClient *RedisClient) {
	// fetch type
	sourceKeyTypeStr, err := sourceClient.PipeTypeCommand(keyInfo)
	if err != nil {
		panic(logger.Critical(err))
	}
	for i, t := range sourceKeyTypeStr {
		keyInfo[i].tp = NewKeyType(t)
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
			keyInfo[i].sourceAttr.itemcount = keylen
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
			keyInfo[i].targetAttr.itemcount = keylen
		}
		wg.Done()
	}()

	wg.Wait()
}

type ValueOutlineVerifier struct {
	VerifierBase
}

func NewValueOutlineVerifier(stat *Stat, param *FullCheckParameter) *ValueOutlineVerifier {
	return &ValueOutlineVerifier{VerifierBase{stat, param}}
}

func (p *ValueOutlineVerifier) VerifyOneGroupKeyInfo(keyInfo []*Key, conflictKey chan<- *Key, sourceClient *RedisClient, targetClient *RedisClient) {
	p.FetchTypeAndLen(keyInfo, sourceClient, targetClient)

	// compare, filter
	for i := 0; i < len(keyInfo); i++ {
		// 取type时，source redis上key已经被删除，认为是没有不一致
		if keyInfo[i].tp == NoneType {
			keyInfo[i].conflictType = NoneConflict
			p.IncrKeyStat(keyInfo[i])
			continue
		}

		// key lack in target redis
		if keyInfo[i].targetAttr.itemcount == 0 {
			keyInfo[i].conflictType = LackTargetConflict
			p.IncrKeyStat(keyInfo[i])
			conflictKey <- keyInfo[i]
			continue
		}

		// type mismatch, itemcount == -1，表明key在target redis上的type与source不同
		if keyInfo[i].targetAttr.itemcount == -1 {
			keyInfo[i].conflictType = TypeConflict
			p.IncrKeyStat(keyInfo[i])
			conflictKey <- keyInfo[i]
			continue
		}

		// string,  strlen mismatch, 先过滤一遍
		if keyInfo[i].sourceAttr.itemcount != keyInfo[i].targetAttr.itemcount {
			keyInfo[i].conflictType = ValueConflict
			p.IncrKeyStat(keyInfo[i])
			conflictKey <- keyInfo[i]
			continue
		}
	} // end of for i := 0; i < len(keyInfo); i++
}

type KeyOutlineVerifier struct {
	VerifierBase
}

func NewKeyOutlineVerifier(stat *Stat, param *FullCheckParameter) *KeyOutlineVerifier {
	return &KeyOutlineVerifier{VerifierBase{stat, param}}
}

func (p *KeyOutlineVerifier) FetchKeys(keyInfo []*Key, sourceClient *RedisClient, targetClient *RedisClient) {
	// fetch type
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		sourceKeyTypeStr, err := sourceClient.PipeTypeCommand(keyInfo)
		if err != nil {
			panic(logger.Critical(err))
		}
		for i, t := range sourceKeyTypeStr {
			keyInfo[i].tp = NewKeyType(t)
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
			keyInfo[i].targetAttr.itemcount = t
		}
		wg.Done()
	}()

	wg.Wait()
}

func (p *KeyOutlineVerifier) VerifyOneGroupKeyInfo(keyInfo []*Key, conflictKey chan<- *Key, sourceClient *RedisClient, targetClient *RedisClient) {
	p.FetchKeys(keyInfo, sourceClient, targetClient)

	// compare, filter
	for i := 0; i < len(keyInfo); i++ {
		// key lack in target redis
		if keyInfo[i].targetAttr.itemcount == 0 {
			keyInfo[i].conflictType = LackTargetConflict
			p.IncrKeyStat(keyInfo[i])
			conflictKey <- keyInfo[i]
		}
	} // end of for i := 0; i < len(keyInfo); i++
}

type FullValueVerifier struct {
	VerifierBase
	ignoreBigKey bool // only compare value length for big key when this parameter is enabled.
}

func NewFullValueVerifier(stat *Stat, param *FullCheckParameter, ignoreBigKey bool) *FullValueVerifier {
	return &FullValueVerifier{
		VerifierBase: VerifierBase{stat, param},
		ignoreBigKey: ignoreBigKey,
	}
}

func (p *FullValueVerifier) VerifyOneGroupKeyInfo(keyInfo []*Key, conflictKey chan<- *Key, sourceClient *RedisClient, targetClient *RedisClient) {
	// 对于没有类型的Key, 取类型和长度
	noTypeKeyInfo := make([]*Key, 0, len(keyInfo))
	for i := 0; i < len(keyInfo); i++ {
		if keyInfo[i].tp == EndKeyType {
			noTypeKeyInfo = append(noTypeKeyInfo, keyInfo[i])
		}
	}
	p.FetchTypeAndLen(noTypeKeyInfo, sourceClient, targetClient)

	// compare, filter
	fullCheckFetchAllKeyInfo := make([]*Key, 0, len(keyInfo))
	retryNewVerifyKeyInfo := make([]*Key, 0, len(keyInfo))
	for i := 0; i < len(keyInfo); i++ {
		/************ 所有第一次比较的key，之前未比较的 key ***********/
		if keyInfo[i].conflictType == EndConflict { // 第二轮及以后比较的key，conflictType 肯定不是EndConflict
			// 取type时，source redis上key已经被删除，认为是没有不一致
			if keyInfo[i].tp == NoneType {
				keyInfo[i].conflictType = NoneConflict
				p.IncrKeyStat(keyInfo[i])
				continue
			}

			// key lack in target redis
			if keyInfo[i].targetAttr.itemcount == 0 &&
					keyInfo[i].targetAttr.itemcount != keyInfo[i].sourceAttr.itemcount  {
				keyInfo[i].conflictType = LackTargetConflict
				p.IncrKeyStat(keyInfo[i])
				conflictKey <- keyInfo[i]
				continue
			}

			// type mismatch, itemcount == -1，表明key在target redis上的type与source不同
			if keyInfo[i].targetAttr.itemcount == -1 {
				keyInfo[i].conflictType = TypeConflict
				p.IncrKeyStat(keyInfo[i])
				conflictKey <- keyInfo[i]
				continue
			}

			// string,  strlen mismatch, 先过滤一遍
			if keyInfo[i].tp == StringType && keyInfo[i].sourceAttr.itemcount != keyInfo[i].targetAttr.itemcount {
				keyInfo[i].conflictType = ValueConflict
				p.IncrKeyStat(keyInfo[i])
				conflictKey <- keyInfo[i]
				continue
			}

			// 太大的 hash、list、set、zset 特殊单独处理
			if keyInfo[i].tp != StringType &&
					(keyInfo[i].sourceAttr.itemcount > BigKeyThreshold || keyInfo[i].targetAttr.itemcount > BigKeyThreshold) {
				if p.ignoreBigKey {
					// 如果启用忽略大key开关，则进入这个分支
					if keyInfo[i].sourceAttr.itemcount != keyInfo[i].targetAttr.itemcount {
						keyInfo[i].conflictType = ValueConflict
						p.IncrKeyStat(keyInfo[i])
						conflictKey <- keyInfo[i]
					} else {
						keyInfo[i].conflictType = NoneConflict
						p.IncrKeyStat(keyInfo[i])
					}
					continue
				}

				switch keyInfo[i].tp {
				case HashType:
					fallthrough
				case SetType:
					fallthrough
				case ZsetType:
					sourceValue, err := sourceClient.FetchValueUseScan_Hash_Set_SortedSet(keyInfo[i], p.param.batchCount)
					if err != nil {
						panic(logger.Error(err))
					}
					targetValue, err := targetClient.FetchValueUseScan_Hash_Set_SortedSet(keyInfo[i], p.param.batchCount)
					if err != nil {
						panic(logger.Error(err))
					}
					p.Compare_Hash_Set_SortedSet(keyInfo[i], conflictKey, sourceValue, targetValue)
				case ListType:
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
			if keyInfo[i].conflictType == LackSourceConflict ||
					keyInfo[i].conflictType == LackTargetConflict ||
					keyInfo[i].conflictType == TypeConflict {
				keyInfo[i].tp = EndKeyType            // 重新取 type、len
				keyInfo[i].conflictType = EndConflict // 使用 第一轮比较用的方式
				retryNewVerifyKeyInfo = append(retryNewVerifyKeyInfo, keyInfo[i])
				continue
			}

			if keyInfo[i].conflictType == ValueConflict {
				if keyInfo[i].tp != StringType &&
						(keyInfo[i].sourceAttr.itemcount > BigKeyThreshold || keyInfo[i].targetAttr.itemcount > BigKeyThreshold) &&
						p.ignoreBigKey {
					// 如果启用忽略大key开关，则进入这个分支
					if keyInfo[i].sourceAttr.itemcount != keyInfo[i].targetAttr.itemcount {
						keyInfo[i].conflictType = ValueConflict
						p.IncrKeyStat(keyInfo[i])
						conflictKey <- keyInfo[i]
					} else {
						keyInfo[i].conflictType = NoneConflict
						p.IncrKeyStat(keyInfo[i])
					}
					continue
				}

				switch keyInfo[i].tp {
				// string 和 list 每次都要重新比较所有field value。
				// list有lpush、lpop，会导致field value平移，所以需要重新比较所有field value
				case StringType:
					fullCheckFetchAllKeyInfo = append(fullCheckFetchAllKeyInfo, keyInfo[i])
				case ListType:
					if keyInfo[i].sourceAttr.itemcount > BigKeyThreshold || keyInfo[i].targetAttr.itemcount > BigKeyThreshold {
						p.CheckFullBigValue_List(keyInfo[i], conflictKey, sourceClient, targetClient)
					} else {
						fullCheckFetchAllKeyInfo = append(fullCheckFetchAllKeyInfo, keyInfo[i])
					}
					// hash、set、zset, 只比较前一轮有不一致的field
				case HashType:
					p.CheckPartialValueHash(keyInfo[i], conflictKey, sourceClient, targetClient)
				case SetType:
					p.CheckPartialValueSet(keyInfo[i], conflictKey, sourceClient, targetClient)
				case ZsetType:
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

func (p *FullValueVerifier) CheckFullValueFetchAll(keyInfo []*Key, conflictKey chan<- *Key, sourceClient *RedisClient, targetClient *RedisClient) {
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
		switch oneKeyInfo.tp {
		case StringType:
			var sourceValue, targetValue []byte
			if sourceReply[i] != nil {
				sourceValue = sourceReply[i].([]byte)
			}
			if targetReply[i] != nil {
				targetValue = targetReply[i].([]byte)
			}
			p.Compare_String(oneKeyInfo, conflictKey, sourceValue, targetValue)
			p.IncrKeyStat(oneKeyInfo)
		case HashType:
			fallthrough
		case ZsetType:
			sourceValue, targetValue := ValueHelper_Hash_SortedSet(sourceReply[i]), ValueHelper_Hash_SortedSet(targetReply[i])
			p.Compare_Hash_Set_SortedSet(oneKeyInfo, conflictKey, sourceValue, targetValue)
		case ListType:
			sourceValue, targetValue := ValueHelper_List(sourceReply[i]), ValueHelper_List(targetReply[i])
			p.Compare_List(oneKeyInfo, conflictKey, sourceValue, targetValue)
		case SetType:
			sourceValue, targetValue := ValueHelper_Set(sourceReply[i]), ValueHelper_Set(targetReply[i])
			p.Compare_Hash_Set_SortedSet(oneKeyInfo, conflictKey, sourceValue, targetValue)
		}
	}
}

func (p *FullValueVerifier) CheckPartialValueHash(oneKeyInfo *Key, conflictKey chan<- *Key, sourceClient *RedisClient, targetClient *RedisClient) {
	sourceValue, targetValue := make(map[string][]byte), make(map[string][]byte)
	for fieldIndex := 0; fieldIndex < len(oneKeyInfo.field); {
		args := make([]interface{}, 0, p.param.batchCount)
		args = append(args, oneKeyInfo.key)
		for count := 0; count < p.param.batchCount && fieldIndex < len(oneKeyInfo.field); count, fieldIndex = count+1, fieldIndex+1 {
			args = append(args, oneKeyInfo.field[fieldIndex].field)
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
	} // end of for fieldIndex := 0; fieldIndex < len(oneKeyInfo.field)
	p.Compare_Hash_Set_SortedSet(oneKeyInfo, conflictKey, sourceValue, targetValue)
}

func (p *FullValueVerifier) CheckPartialValueSet(oneKeyInfo *Key, conflictKey chan<- *Key, sourceClient *RedisClient, targetClient *RedisClient) {
	sourceValue, targetValue := make(map[string][]byte), make(map[string][]byte)
	for fieldIndex := 0; fieldIndex < len(oneKeyInfo.field); {
		sendField := make([][]byte, 0, p.param.batchCount)
		for count := 0; count < p.param.batchCount && fieldIndex < len(oneKeyInfo.field); count, fieldIndex = count+1, fieldIndex+1 {
			sendField = append(sendField, oneKeyInfo.field[fieldIndex].field)
		}
		tmpSourceValue, err := sourceClient.PipeSismemberCommand(oneKeyInfo.key, sendField)
		if err != nil {
			panic(logger.Error(err))
		}
		tmpTargetValue, err := targetClient.PipeSismemberCommand(oneKeyInfo.key, sendField)
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
	} // for fieldIndex := 0; fieldIndex < len(oneKeyInfo.field);
	p.Compare_Hash_Set_SortedSet(oneKeyInfo, conflictKey, sourceValue, targetValue)
}

func (p *FullValueVerifier) CheckPartialValueSortedSet(oneKeyInfo *Key, conflictKey chan<- *Key, sourceClient *RedisClient, targetClient *RedisClient) {
	sourceValue, targetValue := make(map[string][]byte), make(map[string][]byte)
	for fieldIndex := 0; fieldIndex < len(oneKeyInfo.field); {
		sendField := make([][]byte, 0, p.param.batchCount)
		for count := 0; count < p.param.batchCount && fieldIndex < len(oneKeyInfo.field); count, fieldIndex = count+1, fieldIndex+1 {
			sendField = append(sendField, oneKeyInfo.field[fieldIndex].field)
		}

		tmpSourceValue, err := sourceClient.PipeZscoreCommand(oneKeyInfo.key, sendField)
		if err != nil {
			panic(logger.Error(err))
		}
		tmpTargetValue, err := targetClient.PipeZscoreCommand(oneKeyInfo.key, sendField)
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

func (p *FullValueVerifier) CheckFullBigValue_List(oneKeyInfo *Key, conflictKey chan<- *Key, sourceClient *RedisClient, targetClient *RedisClient) {
	conflictField := make([]Field, 0, oneKeyInfo.sourceAttr.itemcount/100+1)
	oneCmpCount := p.param.batchCount * 10
	if oneCmpCount > 10240 {
		oneCmpCount = 10240
	}

	startIndex := 0
	for {
		sourceReply, err := sourceClient.Do("lrange", oneKeyInfo.key, startIndex, startIndex+oneCmpCount-1)
		if err != nil {
			panic(logger.Critical(err))
		}
		sourceValue := sourceReply.([]interface{})

		targetReply, err := targetClient.Do("lrange", oneKeyInfo.key, startIndex, startIndex+oneCmpCount-1)
		if err != nil {
			panic(logger.Error(err))
		}
		targetValue := targetReply.([]interface{})

		minLen := min(len(sourceValue), len(targetValue))
		for i := 0; i < minLen; i++ {
			if bytes.Equal(sourceValue[i].([]byte), targetValue[i].([]byte)) == false {
				field := Field{
					field:        []byte(strconv.FormatInt(int64(startIndex+i), 10)),
					conflictType: ValueConflict,
				}
				conflictField = append(conflictField, field)
				p.IncrFieldStat(oneKeyInfo, ValueConflict)
			} else {
				p.IncrFieldStat(oneKeyInfo, NoneConflict)
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
		oneKeyInfo.field = conflictField[0:1]
		oneKeyInfo.conflictType = ValueConflict
		conflictKey <- oneKeyInfo
	} else {
		oneKeyInfo.field = nil
		oneKeyInfo.conflictType = NoneConflict
	}
	p.IncrKeyStat(oneKeyInfo)
}

func (p *FullValueVerifier) Compare_String(oneKeyInfo *Key, conflictKey chan<- *Key, sourceValue, targetValue []byte) {
	if len(sourceValue) == 0 {
		if len(targetValue) == 0 {
			oneKeyInfo.conflictType = NoneConflict
		} else {
			oneKeyInfo.conflictType = LackSourceConflict
		}
	} else if len(targetValue) == 0 {
		if len(sourceValue) == 0 {
			oneKeyInfo.conflictType = NoneConflict
		} else {
			oneKeyInfo.conflictType = LackTargetConflict
		}
	} else if bytes.Equal(sourceValue, targetValue) == false {
		oneKeyInfo.conflictType = ValueConflict
	} else {
		oneKeyInfo.conflictType = NoneConflict
	}
	if oneKeyInfo.conflictType != NoneConflict {
		conflictKey <- oneKeyInfo
	}
}

func (p *FullValueVerifier) Compare_Hash_Set_SortedSet(oneKeyInfo *Key, conflictKey chan<- *Key, sourceValue, targetValue map[string][]byte) {
	conflictField := make([]Field, 0, len(sourceValue)/50+1)
	for k, v := range sourceValue {
		vTarget, ok := targetValue[k]
		if ok == false {
			conflictField = append(conflictField, Field{field: []byte(k), conflictType: LackTargetConflict})
			p.IncrFieldStat(oneKeyInfo, LackTargetConflict)
		} else {
			delete(targetValue, k)
			if bytes.Equal(v, vTarget) == false {
				conflictField = append(conflictField, Field{field: []byte(k), conflictType: ValueConflict})
				p.IncrFieldStat(oneKeyInfo, ValueConflict)
			} else {
				p.IncrFieldStat(oneKeyInfo, NoneConflict)
			}
		}
	}

	for k, _ := range targetValue {
		conflictField = append(conflictField, Field{field: []byte(k), conflictType: LackSourceConflict})
		p.IncrFieldStat(oneKeyInfo, LackSourceConflict)
	}

	if len(conflictField) != 0 {
		oneKeyInfo.field = conflictField
		oneKeyInfo.conflictType = ValueConflict
		conflictKey <- oneKeyInfo
	} else {
		oneKeyInfo.conflictType = NoneConflict
	}
	p.IncrKeyStat(oneKeyInfo)
}

func (p *FullValueVerifier) Compare_List(oneKeyInfo *Key, conflictKey chan<- *Key, sourceValue, targetValue [][]byte) {
	minLen := min(len(sourceValue), len(targetValue))

	oneKeyInfo.conflictType = NoneConflict
	for i := 0; i < minLen; i++ {
		if bytes.Equal(sourceValue[i], targetValue[i]) == false {
			// list 只保存第一个不一致的field
			oneKeyInfo.field = make([]Field, 1)
			oneKeyInfo.field[0] = Field{field: []byte(strconv.FormatInt(int64(i), 10)), conflictType: ValueConflict}
			oneKeyInfo.conflictType = ValueConflict
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

	stat            Stat
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

type Metric struct {
	DateTime           string                             `json:"datetime"`
	Timestamp          int64                              `json:"timestamp"`
	CompareTimes       int                                `json:"comparetimes"`
	Id                 string                             `json:"id"`
	JobId              string                             `json:"jobid"`
	TaskId             string                             `json:"taskid"`
	Db                 int32                              `json:"db"`
	DbKeys             int64                              `json:"dbkeys"`
	Process            int64                              `json:"process"`
	OneCompareFinished bool                               `json:"has_finished"`
	AllFinished        bool                               `json:"all_finished"`
	KeyScan            *CounterStat                       `json:"key_scan"`
	TotalConflict      int64                              `json:"total_conflict"`
	TotalKeyConflict   int64                              `json:"total_key_conflict"`
	TotalFieldConflict int64                              `json:"total_field_conflict"`
	KeyMetric          map[string]map[string]*CounterStat `json:"key_stat"`
	FieldMetric        map[string]map[string]*CounterStat `json:"field_stat"`
}

type MetricItem struct {
	Type     string       `json:"type"`
	Conflict string       `json:"conflict"`
	Stat     *CounterStat `json:"stat"`
}

func (p *FullCheck) PrintStat(finished bool) {
	var buf bytes.Buffer

	var metric *Metric
	finishPercent := p.stat.scan.Total() * 100 * int64(p.times) / (p.sourceDBNums[p.currentDB] * int64(p.compareCount))
	if p.times == 1 {
		metric = &Metric{CompareTimes: p.times, Db: p.currentDB, DbKeys: p.sourceDBNums[p.currentDB], Process: finishPercent, OneCompareFinished: finished,
			AllFinished: false,
			Timestamp:   time.Now().Unix(), DateTime: time.Now().Format("2006-01-02T15:04:05Z"), Id: opts.Id, JobId: opts.JobId, TaskId: opts.TaskId}
		fmt.Fprintf(&buf, "times:%d,db:%d,dbkeys:%d,finish:%d%%,finished:%v\n", p.times, p.currentDB, p.sourceDBNums[p.currentDB], finishPercent, finished)
	} else {
		metric = &Metric{CompareTimes: p.times, Db: p.currentDB, Process: finishPercent, OneCompareFinished: finished, AllFinished: false,
			Timestamp: time.Now().Unix(), DateTime: time.Now().Format("2006-01-02T15:04:05Z"), Id: opts.Id, JobId: opts.JobId, TaskId: opts.TaskId}
		fmt.Fprintf(&buf, "times:%d,db:%d,finished:%v\n", p.times, p.currentDB, finished)
	}

	p.totalConflict = int64(0)
	p.totalKeyConflict = int64(0)
	p.totalFieldConflict = int64(0)

	// fmt.Fprintf(&buf, "--- key scan ---\n")
	fmt.Fprintf(&buf, "KeyScan:%v\n", p.stat.scan)
	metric.KeyScan = p.stat.scan.Json()
	metric.KeyMetric = make(map[string]map[string]*CounterStat)

	// fmt.Fprintf(&buf, "--- key equal ---\n")
	for i := KeyTypeIndex(0); i < EndKeyTypeIndex; i++ {
		metric.KeyMetric[i.String()] = make(map[string]*CounterStat)
		if p.stat.conflictKey[i][NoneConflict].Total() != 0 {
			metric.KeyMetric[i.String()]["equal"] = p.stat.conflictKey[i][NoneConflict].Json()
			if p.times == p.compareCount {
				fmt.Fprintf(&buf, "KeyEqualAtLast|%s|%s|%v\n", i, NoneConflict, p.stat.conflictKey[i][NoneConflict])
			} else {
				fmt.Fprintf(&buf, "KeyEqualInProcess|%s|%s|%v\n", i, NoneConflict, p.stat.conflictKey[i][NoneConflict])
			}
		}
	}
	// fmt.Fprintf(&buf, "--- key conflict ---\n")
	for i := KeyTypeIndex(0); i < EndKeyTypeIndex; i++ {
		for j := ConflictType(0); j < NoneConflict; j++ {
			if p.stat.conflictKey[i][j].Total() != 0 {
				metric.KeyMetric[i.String()][j.String()] = p.stat.conflictKey[i][j].Json()
				if p.times == p.compareCount {
					fmt.Fprintf(&buf, "KeyConflictAtLast|%s|%s|%v\n", i, j, p.stat.conflictKey[i][j])
					p.totalKeyConflict += p.stat.conflictKey[i][j].Total()
				} else {
					fmt.Fprintf(&buf, "KeyConflictInProcess|%s|%s|%v\n", i, j, p.stat.conflictKey[i][j])
				}
			}
		}
	}

	metric.FieldMetric = make(map[string]map[string]*CounterStat)
	// fmt.Fprintf(&buf, "--- field equal ---\n")
	for i := KeyTypeIndex(0); i < EndKeyTypeIndex; i++ {
		metric.FieldMetric[i.String()] = make(map[string]*CounterStat)
		if p.stat.conflictField[i][NoneConflict].Total() != 0 {
			metric.FieldMetric[i.String()]["equal"] = p.stat.conflictField[i][NoneConflict].Json()
			if p.times == p.compareCount {
				fmt.Fprintf(&buf, "FieldEqualAtLast|%s|%s|%v\n", i, NoneConflict, p.stat.conflictField[i][NoneConflict])
			} else {
				fmt.Fprintf(&buf, "FieldEqualInProcess|%s|%s|%v\n", i, NoneConflict, p.stat.conflictField[i][NoneConflict])
			}
		}
	}
	// fmt.Fprintf(&buf, "--- field conflict  ---\n")
	for i := KeyTypeIndex(0); i < EndKeyTypeIndex; i++ {
		for j := ConflictType(0); j < NoneConflict; j++ {
			if p.stat.conflictField[i][j].Total() != 0 {
				metric.FieldMetric[i.String()][j.String()] = p.stat.conflictField[i][j].Json()
				if p.times == p.compareCount {
					fmt.Fprintf(&buf, "FieldConflictAtLast|%s|%s|%v\n", i, j, p.stat.conflictField[i][j])
					p.totalFieldConflict += p.stat.conflictField[i][j].Total()
				} else {
					fmt.Fprintf(&buf, "FieldConflictInProcess|%s|%s|%v\n", i, j, p.stat.conflictField[i][j])
				}
			}
		}
	}

	p.totalConflict = p.totalKeyConflict + p.totalFieldConflict
	if len(opts.MetricFile) > 0 {
		metricsfile, _ := os.OpenFile(opts.MetricFile, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
		defer metricsfile.Close()

		metricstr, _ := json.Marshal(metric)
		metricsfile.WriteString(fmt.Sprintf("%s\n", string(metricstr)))

		if p.times == p.compareCount && finished {
			metric.AllFinished = true
			metric.Process = int64(100)
			metric.TotalConflict = p.totalConflict
			metric.TotalKeyConflict = p.totalKeyConflict
			metric.TotalFieldConflict = p.totalFieldConflict

			metricstr, _ := json.Marshal(metric)
			metricsfile.WriteString(fmt.Sprintf("%s\n", string(metricstr)))
		}
	} else {
		logger.Infof("stat:\n%s", string(buf.Bytes()))
	}
}

func (p *FullCheck) IncrScanStat(a int) {
	p.stat.scan.Inc(a)
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
			var tickerStat *time.Ticker = time.NewTicker(time.Second * StatRollFrequency)
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
			keys := make(chan []*Key, 1024)
			conflictKey := make(chan *Key, 1024)
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

func (p *FullCheck) ScanFromSourceRedis(allKeys chan<- []*Key) {
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
			keysInfo := make([]*Key, 0, len(keylist))
			for _, value := range keylist {
				bytes, ok = value.([]byte)
				if ok == false {
					panic(logger.Criticalf("scan failed, result: %+v", reply))
				}

				// check filter list
				if common.CheckFilter(p.filterTree, bytes) == false {
					continue
				}
				
				keysInfo = append(keysInfo, &Key{
					key:          bytes,
					tp:           EndKeyType,
					conflictType: EndConflict,
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

func (p *FullCheck) ScanFromDB(allKeys chan<- []*Key) {
	conflictKeyTableName, conflictFieldTableName := p.GetLastResultTable()

	keyStatm, err := p.db[p.times-1].Prepare(fmt.Sprintf("select id,key,type,conflict_type,source_len,target_len from %s where id>? and db=%d limit %d", conflictKeyTableName, p.currentDB, p.batchCount))
	if err != nil {
		panic(logger.Error(err))
	}
	defer keyStatm.Close()

	fieldStatm, err := p.db[p.times-1].Prepare(fmt.Sprintf("select field,conflict_type from %s where key_id=?", conflictFieldTableName))
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
		keyInfo := make([]*Key, 0, p.batchCount)
		for rows.Next() {
			var key, keytype, conflictType string
			var id, source_len, target_len int64
			err = rows.Scan(&id, &key, &keytype, &conflictType, &source_len, &target_len)
			if err != nil {
				panic(logger.Error(err))
			}
			oneKeyInfo := &Key{
				key:          []byte(key),
				tp:           NewKeyType(keytype),
				conflictType: NewConflictType(conflictType),
				sourceAttr:   Attribute{itemcount: source_len},
				targetAttr:   Attribute{itemcount: target_len},
			}
			if oneKeyInfo.tp == EndKeyType {
				panic(logger.Errorf("invalid type from table %s: key=%s type=%s ", conflictKeyTableName, key, keytype))
			}
			if oneKeyInfo.conflictType == EndConflict {
				panic(logger.Errorf("invalid conflict_type from table %s: key=%s conflict_type=%s ", conflictKeyTableName, key, conflictType))
			}

			if oneKeyInfo.tp != StringType {
				oneKeyInfo.field = make([]Field, 0, 10)
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
					oneField := Field{
						field:        []byte(field),
						conflictType: NewConflictType(conflictType),
					}
					if oneField.conflictType == EndConflict {
						panic(logger.Errorf("invalid conflict_type from table %s: field=%s type=%s ", conflictFieldTableName, field, conflictType))
					}
					oneKeyInfo.field = append(oneKeyInfo.field, oneField)
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

func (p *FullCheck) VerifyAllKeyInfo(allKeys <-chan []*Key, conflictKey chan<- *Key) {
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

func (p *FullCheck) FetchTypeAndLen(keyInfo []*Key, sourceClient *RedisClient, targetClient *RedisClient) {
	// fetch type
	sourceKeyTypeStr, err := sourceClient.PipeTypeCommand(keyInfo)
	if err != nil {
		panic(logger.Critical(err))
	}
	for i, t := range sourceKeyTypeStr {
		keyInfo[i].tp = NewKeyType(t)
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
			keyInfo[i].sourceAttr.itemcount = keylen
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
			keyInfo[i].targetAttr.itemcount = keylen
		}
		wg.Done()
	}()

	wg.Wait()
}

func (p *FullCheck) WriteConflictKey(conflictKey <-chan *Key) {
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

		result, err := statInsertKey.Exec(string(oneKeyInfo.key), oneKeyInfo.tp.name, oneKeyInfo.conflictType.String(), p.currentDB, oneKeyInfo.sourceAttr.itemcount, oneKeyInfo.targetAttr.itemcount)
		if err != nil {
			panic(logger.Error(err))
		}
		if len(oneKeyInfo.field) != 0 {
			lastId, _ := result.LastInsertId()
			for i := 0; i < len(oneKeyInfo.field); i++ {
				_, err = statInsertField.Exec(string(oneKeyInfo.field[i].field), oneKeyInfo.field[i].conflictType.String(), lastId)
				if err != nil {
					panic(logger.Error(err))
				}

				if p.times == p.compareCount {
					finalstat, err := tx.Prepare(fmt.Sprintf("insert into FINAL_RESULT (InstanceA, InstanceB, Key, Schema, InconsistentType, Extra) VALUES(?, ?, ?, ?, ?, ?)"))
					if err != nil {
						panic(logger.Error(err))
					}
					// defer finalstat.Close()
					_, err = finalstat.Exec("", "", string(oneKeyInfo.key), strconv.Itoa(int(p.currentDB)),
						oneKeyInfo.field[i].conflictType.String(),
						string(oneKeyInfo.field[i].field))
					if err != nil {
						panic(logger.Error(err))
					}

					finalstat.Close()

					if len(opts.ResultFile) != 0 {
						resultfile.WriteString(fmt.Sprintf("%d\t%s\t%s\t%s\n", int(p.currentDB), oneKeyInfo.field[i].conflictType.String(), string(oneKeyInfo.key), string(oneKeyInfo.field[i].field)))
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
				_, err = finalstat.Exec("", "", string(oneKeyInfo.key), strconv.Itoa(int(p.currentDB)), oneKeyInfo.conflictType.String(), "")
				if err != nil {
					panic(logger.Error(err))
				}
				finalstat.Close()

				if len(opts.ResultFile) != 0 {
					resultfile.WriteString(fmt.Sprintf("%d\t%s\t%s\t%s\n", int(p.currentDB), oneKeyInfo.conflictType.String(), string(oneKeyInfo.key), ""))
				}
			}
		}
	}
	statInsertKey.Close()
	statInsertField.Close()
	tx.Commit()
}
