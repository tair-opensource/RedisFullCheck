package common

import (
	"bytes"
	"fmt"
	"strings"
	"strconv"
)

func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// ParseInfo convert result of info command to map[string]string.
// For example, "opapply_source_count:1\r\nopapply_source_0:server_id=3171317,applied_opid=1\r\n" is converted to map[string]string{"opapply_source_count": "1", "opapply_source_0": "server_id=3171317,applied_opid=1"}.
func ParseInfo(content []byte) map[string]string {
	result := make(map[string]string, 10)
	lines := bytes.Split(content, []byte("\r\n"))
	for i := 0; i < len(lines); i++ {
		items := bytes.SplitN(lines[i], []byte(":"), 2)
		if len(items) != 2 {
			continue
		}
		result[string(items[0])] = string(items[1])
	}
	return result
}

func FilterDBList(dbs string) map[int]struct{} {
	ret := make(map[int]struct{})
	// empty
	if dbs == "-1" {
		return ret
	}

	// empty
	dbList := strings.Split(dbs, Splitter)
	if len(dbList) == 0 {
		return ret
	}

	for _, ele := range dbList {
		val, err := strconv.Atoi(ele)
		if err != nil {
			panic(err)
		}

		ret[val] = struct{}{}
	}
	return ret
}

// ParseDBMap parses the source->target db mapping, e.g. "313:1;413:2" means source db313 is
// compared with target db1 and source db413 with target db2. An empty input or "-1" means no
// mapping, so every source db is compared with the target db of the same number.
func ParseDBMap(dbs string) (map[int]int, error) {
	ret := make(map[int]int)
	if len(dbs) == 0 || dbs == "-1" {
		return ret, nil
	}

	for _, ele := range strings.Split(dbs, Splitter) {
		ele = strings.TrimSpace(ele)
		if len(ele) == 0 {
			continue
		}

		items := strings.Split(ele, ":")
		if len(items) != 2 {
			return nil, fmt.Errorf("invalid targetdbmap item[%v], expect format 'source:target'", ele)
		}

		sourceDB, err := strconv.Atoi(strings.TrimSpace(items[0]))
		if err != nil {
			return nil, fmt.Errorf("invalid source db in targetdbmap item[%v]: %v", ele, err)
		}
		targetDB, err := strconv.Atoi(strings.TrimSpace(items[1]))
		if err != nil {
			return nil, fmt.Errorf("invalid target db in targetdbmap item[%v]: %v", ele, err)
		}
		if sourceDB < 0 || targetDB < 0 {
			return nil, fmt.Errorf("invalid targetdbmap item[%v], db must be non-negative", ele)
		}
		if old, ok := ret[sourceDB]; ok && old != targetDB {
			return nil, fmt.Errorf("conflict targetdbmap item[%v], source db %d is already mapped to %d",
				ele, sourceDB, old)
		}

		ret[sourceDB] = targetDB
	}
	return ret, nil
}