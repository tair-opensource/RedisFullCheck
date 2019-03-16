package checker

import (
	"full_check/common"
	"bytes"
	"full_check/metric"
	"full_check/client"
	"strconv"
)

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

func (p *FullValueVerifier) VerifyOneGroupKeyInfo(keyInfo []*common.Key, conflictKey chan<- *common.Key, sourceClient *client.RedisClient, targetClient *client.RedisClient) {
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
					sourceValue, err := sourceClient.FetchValueUseScan_Hash_Set_SortedSet(keyInfo[i], p.Param.BatchCount)
					if err != nil {
						panic(common.Logger.Error(err))
					}
					targetValue, err := targetClient.FetchValueUseScan_Hash_Set_SortedSet(keyInfo[i], p.Param.BatchCount)
					if err != nil {
						panic(common.Logger.Error(err))
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
						(keyInfo[i].SourceAttr.ItemCount > common.BigKeyThreshold ||
							keyInfo[i].TargetAttr.ItemCount > common.BigKeyThreshold) &&
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

func (p *FullValueVerifier) CheckFullValueFetchAll(keyInfo []*common.Key, conflictKey chan<- *common.Key, sourceClient *client.RedisClient, targetClient *client.RedisClient) {
	// fetch value
	sourceReply, err := sourceClient.PipeValueCommand(keyInfo)
	if err != nil {
		panic(common.Logger.Critical(err))
	}

	targetReply, err := targetClient.PipeValueCommand(keyInfo)
	if err != nil {
		panic(common.Logger.Critical(err))
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
			sourceValue, targetValue := common.ValueHelper_Hash_SortedSet(sourceReply[i]), common.ValueHelper_Hash_SortedSet(targetReply[i])
			p.Compare_Hash_Set_SortedSet(oneKeyInfo, conflictKey, sourceValue, targetValue)
		case common.ListKeyType:
			sourceValue, targetValue := common.ValueHelper_List(sourceReply[i]), common.ValueHelper_List(targetReply[i])
			p.Compare_List(oneKeyInfo, conflictKey, sourceValue, targetValue)
		case common.SetKeyType:
			sourceValue, targetValue := common.ValueHelper_Set(sourceReply[i]), common.ValueHelper_Set(targetReply[i])
			p.Compare_Hash_Set_SortedSet(oneKeyInfo, conflictKey, sourceValue, targetValue)
		}
	}
}

func (p *FullValueVerifier) CheckPartialValueHash(oneKeyInfo *common.Key, conflictKey chan<- *common.Key, sourceClient *client.RedisClient, targetClient *client.RedisClient) {
	sourceValue, targetValue := make(map[string][]byte), make(map[string][]byte)
	for fieldIndex := 0; fieldIndex < len(oneKeyInfo.Field); {
		args := make([]interface{}, 0, p.Param.BatchCount)
		args = append(args, oneKeyInfo.Key)
		for count := 0; count < p.Param.BatchCount && fieldIndex < len(oneKeyInfo.Field); count, fieldIndex = count+1, fieldIndex+1 {
			args = append(args, oneKeyInfo.Field[fieldIndex].Field)
		}

		sourceReply, err := sourceClient.Do("hmget", args...)
		if err != nil {
			panic(common.Logger.Error(err))
		}
		targetReply, err := targetClient.Do("hmget", args...)
		if err != nil {
			panic(common.Logger.Error(err))
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

func (p *FullValueVerifier) CheckPartialValueSet(oneKeyInfo *common.Key, conflictKey chan<- *common.Key, sourceClient *client.RedisClient, targetClient *client.RedisClient) {
	sourceValue, targetValue := make(map[string][]byte), make(map[string][]byte)
	for fieldIndex := 0; fieldIndex < len(oneKeyInfo.Field); {
		sendField := make([][]byte, 0, p.Param.BatchCount)
		for count := 0; count < p.Param.BatchCount && fieldIndex < len(oneKeyInfo.Field); count, fieldIndex = count+1, fieldIndex+1 {
			sendField = append(sendField, oneKeyInfo.Field[fieldIndex].Field)
		}
		tmpSourceValue, err := sourceClient.PipeSismemberCommand(oneKeyInfo.Key, sendField)
		if err != nil {
			panic(common.Logger.Error(err))
		}
		tmpTargetValue, err := targetClient.PipeSismemberCommand(oneKeyInfo.Key, sendField)
		if err != nil {
			panic(common.Logger.Error(err))
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

func (p *FullValueVerifier) CheckPartialValueSortedSet(oneKeyInfo *common.Key, conflictKey chan<- *common.Key, sourceClient *client.RedisClient, targetClient *client.RedisClient) {
	sourceValue, targetValue := make(map[string][]byte), make(map[string][]byte)
	for fieldIndex := 0; fieldIndex < len(oneKeyInfo.Field); {
		sendField := make([][]byte, 0, p.Param.BatchCount)
		for count := 0; count < p.Param.BatchCount && fieldIndex < len(oneKeyInfo.Field); count, fieldIndex = count+1, fieldIndex+1 {
			sendField = append(sendField, oneKeyInfo.Field[fieldIndex].Field)
		}

		tmpSourceValue, err := sourceClient.PipeZscoreCommand(oneKeyInfo.Key, sendField)
		if err != nil {
			panic(common.Logger.Error(err))
		}
		tmpTargetValue, err := targetClient.PipeZscoreCommand(oneKeyInfo.Key, sendField)
		if err != nil {
			panic(common.Logger.Error(err))
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

func (p *FullValueVerifier) CheckFullBigValue_List(oneKeyInfo *common.Key, conflictKey chan<- *common.Key, sourceClient *client.RedisClient, targetClient *client.RedisClient) {
	conflictField := make([]common.Field, 0, oneKeyInfo.SourceAttr.ItemCount/100+1)
	oneCmpCount := p.Param.BatchCount * 10
	if oneCmpCount > 10240 {
		oneCmpCount = 10240
	}

	startIndex := 0
	for {
		sourceReply, err := sourceClient.Do("lrange", oneKeyInfo.Key, startIndex, startIndex+oneCmpCount-1)
		if err != nil {
			panic(common.Logger.Critical(err))
		}
		sourceValue := sourceReply.([]interface{})

		targetReply, err := targetClient.Do("lrange", oneKeyInfo.Key, startIndex, startIndex+oneCmpCount-1)
		if err != nil {
			panic(common.Logger.Error(err))
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