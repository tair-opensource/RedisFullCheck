package client

import (
	"strconv"
	"fmt"

	"full_check/common"

	"github.com/garyburd/redigo/redis"
)

/*
 * Get base db info.
 * Return:
 *     map[int32]int64: logical db node map.
 *     []string: physical db nodes.
 */
func (p *RedisClient) FetchBaseInfo() (map[int32]int64, []string, error) {
	// get keyspace
	keyspaceContent, err := p.Do("info", "Keyspace")
	if err != nil {
		return nil, nil, fmt.Errorf("get keyspace failed[%v]", err)
	}

	// parse to map
	logicalDBMap, err := common.ParseKeyspace(keyspaceContent.([]byte))
	if err != nil {
		return nil, nil, fmt.Errorf("parse keyspace failed[%v]", err)
	}

	// set to 1 logical db if source is tencentProxy and map length is null
	if len(logicalDBMap) == 0 && p.redisHost.DBType == common.TypeTencentProxy {
		logicalDBMap[0] = 0
	}

	physicalDBList := make([]string, 0)
	// get db list
	switch p.redisHost.DBType {
	case common.TypeAliyunProxy:
		info, err := redis.Bytes(p.Do("info", "Cluster"))
		if err != nil {
			return nil, nil, fmt.Errorf("get cluster info failed[%v]", err)
		}

		result := common.ParseInfo(info)
		if count, err := strconv.ParseInt(result["nodecount"], 10, 0); err != nil {
			return nil, nil, fmt.Errorf("parse node count failed[%v]", err)
		} else if count <= 0 {
			return nil, nil, fmt.Errorf("source node count[%v] illegal", count)
		} else {
			for id := int64(0); id < count; id++ {
				physicalDBList[id] = fmt.Sprintf("%v", id)
			}
		}
	case common.TypeTencentProxy:
		var err error
		physicalDBList, err = common.GetAllClusterNode(p.conn, "master", "id")
		if err != nil {
			return nil, nil, fmt.Errorf("get tencent cluster node failed[%v]", err)
		}
	case common.TypeDB:
		// do nothing
		physicalDBList = append(physicalDBList, "meaningless")
	default:
		return nil, nil, fmt.Errorf("unknown redis db type[%v]", p.redisHost.DBType)
	}

	return logicalDBMap, physicalDBList, nil
}
