package common

func ValueHelper_Hash_SortedSet(reply interface{}) map[string][]byte {
	if reply == nil {
		return nil
	}

	tmpValue := reply.([]interface{})
	if len(tmpValue) == 0 {
		return nil
	}
	value := make(map[string][]byte)
	for i := 0; i < len(tmpValue); i += 2 {
		value[string(tmpValue[i].([]byte))] = tmpValue[i+1].([]byte)
	}
	return value
}

func ValueHelper_Set(reply interface{}) map[string][]byte {
	tmpValue := reply.([]interface{})
	if len(tmpValue) == 0 {
		return nil
	}
	value := make(map[string][]byte)
	for i := 0; i < len(tmpValue); i++ {
		value[string(tmpValue[i].([]byte))] = nil
	}
	return value
}

func ValueHelper_List(reply interface{}) [][]byte {
	tmpValue := reply.([]interface{})
	if len(tmpValue) == 0 {
		return nil
	}
	value := make([][]byte, len(tmpValue))
	for i := 0; i < len(tmpValue); i++ {
		value[i] = tmpValue[i].([]byte)
	}
	return value
}