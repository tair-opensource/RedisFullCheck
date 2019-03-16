package common

import (
	"bytes"
	"fmt"
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


func ParseKeyspace(content []byte) (map[int32]int64, error) {
	if bytes.HasPrefix(content, []byte("# Keyspace")) == false {
		return nil, fmt.Errorf("invalid info Keyspace: %s", string(content))
	}

	lines := bytes.Split(content, []byte("\n"))
	reply := make(map[int32]int64)
	for _, line := range lines {
		line = bytes.TrimSpace(line)
		if bytes.HasPrefix(line, []byte("db")) == true {
			// line "db0:keys=18,expires=0,avg_ttl=0"
			items := bytes.Split(line, []byte(":"))
			db, err := strconv.Atoi(string(items[0][2:]))
			if err != nil {
				return nil, err
			}
			nums := bytes.Split(items[1], []byte(","))
			if bytes.HasPrefix(nums[0], []byte("keys=")) == false {
				return nil, fmt.Errorf("invalid info Keyspace: %s", string(content))
			}
			keysNum, err := strconv.ParseInt(string(nums[0][5:]), 10, 0)
			if err != nil {
				return nil, err
			}
			reply[int32(db)] = int64(keysNum)
		} // end true
	} // end for
	return reply, nil
}