package common

import (
	"bytes"
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