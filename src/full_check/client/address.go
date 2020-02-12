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

		return fetchNodeList(clusterList[0], password, authType, role)
	} else {
		clusterList := strings.Split(address, AddressClusterSplitter)
		if len(clusterList) <= 1 {
			return clusterList, nil
		}

		// fetch master
		masterList, err := fetchNodeList(clusterList[0], password, authType, common.TypeMaster)
		if err != nil {
			return nil, err
		}
		// compare master list equal
		if common.CompareUnorderedList(masterList, clusterList) {
			return clusterList, nil
		}

		slaveList, err := fetchNodeList(clusterList[0], password, authType, common.TypeSlave)
		if err != nil {
			return nil, err
		}
		// compare slave list equal
		if common.CompareUnorderedList(slaveList, clusterList) {
			return clusterList, nil
		}

		return nil, fmt.Errorf("if type isn't cluster, should only used 1 node. if type is cluster, " +
			"input list should be all master or all slave: 'master1;master2;master3...' or " +
			"'slave1;slave2;slave3...'")
	}
}

func fetchNodeList(oneNode, password, authType, role string) ([]string, error) {
	// create client to fetch
	client, err := NewRedisClient(RedisHost{
		Addr:     []string{oneNode},
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
}
