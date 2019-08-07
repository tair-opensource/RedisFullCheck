package client

import (
	"strings"
	"fmt"

	"full_check/common"
)

const (
	AddressSplitter        = "@"
	AddressClusterSplitter = ";"

	RoleMaster = "master"
	RoleSlave  = "slave"
)

func HandleAddress(address, password, authType string) ([]string, error) {
	if strings.Contains(address, AddressSplitter) {
		arr := strings.Split(address, AddressSplitter)
		if len(arr) != 2 {
			return nil, fmt.Errorf("redis address[%v] length[%v] != 2", address, len(arr))
		}

		if arr[0] != RoleMaster && arr[0] != RoleSlave && arr[0] != "" {
			return nil, fmt.Errorf("unknown role type[%v], should be 'master' or 'slave'", arr[0])
		}

		clusterList := strings.Split(arr[1], AddressClusterSplitter)

		role := arr[0]
		if role == "" {
			role = RoleMaster
		}

		// create client to fetch
		client, err := NewRedisClient(RedisHost{
			Addr:     []string{clusterList[0]},
			Password: password,
			Authtype: authType,
		}, 0)
		if err != nil {
			return nil, fmt.Errorf("fetch cluster info failed[%v]", err)
		}

		if addressList, err := common.GetAllClusterNode(client.conn, role, "address"); err != nil {
			return nil, fmt.Errorf("fetch cluster node failed[%v]", err)
		} else {
			return addressList, nil
		}
	} else {
		clusterList := strings.Split(address, AddressClusterSplitter)
		return clusterList, nil
	}
}