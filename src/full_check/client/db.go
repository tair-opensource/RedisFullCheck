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
func (p *RedisClient) FetchBaseInfo(isCluster bool) (map[int32]int64, []string, error) {
	var logicalDBMap map[int32]int64

	if !isCluster {
		// get keyspace
		keyspaceContent, err := p.Do("info", "Keyspace")
		if err != nil {
			return nil, nil, fmt.Errorf("get keyspace failed[%v]", err)
		}

		// parse to map
		logicalDBMap, err = common.ParseKeyspace(keyspaceContent.([]byte))
		if err != nil {
			return nil, nil, fmt.Errorf("parse keyspace failed[%v]", err)
		}

		// set to 1 logical db if source is tencentProxy and map length is null
		if len(logicalDBMap) == 0 && p.redisHost.DBType == common.TypeTencentProxy {
			logicalDBMap[0] = 0
		}
	} else {
		// is cluster
		logicalDBMap = make(map[int32]int64)
		logicalDBMap[0] = 0
	}

	// remove db that isn't in DBFilterList(white list)
	if len(p.redisHost.DBFilterList) != 0 {
		for key := range logicalDBMap {
			if _, ok := p.redisHost.DBFilterList[int(key)]; !ok {
				delete(logicalDBMap, key)
			}
		}
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
				physicalDBList = append(physicalDBList, fmt.Sprintf("%v", id))
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
	case common.TypeCluster:
		// equal to the source ip list
		physicalDBList = p.redisHost.Addr
	default:
		return nil, nil, fmt.Errorf("unknown redis db type[%v]", p.redisHost.DBType)
	}

	return logicalDBMap, physicalDBList, nil
}
