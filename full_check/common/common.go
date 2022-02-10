package common

import (
	"github.com/cihub/seelog"
	"fmt"
)

const (
	MaxRetryCount     = 20 // client attribute
	StatRollFrequency = 2  // client attribute

	TypeChanged int64 = -1 // marks the given key type is change, e.g. from string to list

	// db type
	TypeDB           = 0 // db
	TypeCluster      = 1
	TypeAliyunProxy  = 2 // aliyun proxy
	TypeTencentProxy = 3 // tencent cloud proxy

	TypeMaster = "master"
	TypeSlave  = "slave"
	TypeAll    = "all"

	Splitter = ";"
)

var (
	BigKeyThreshold int64 = 16384
	Logger          seelog.LoggerInterface
)

/*
 * @Vinllen Chen. check filter hit the key.
 * return: true/false. true means pass.
 * Actually, it's better to use trie tree instead of for-loop brute way. The reason I choose this is because
 * input filterList is not long in general, and I'm a lazy guy~.
 */
func CheckFilter(filterTree *Trie, keyBytes []byte) bool {
	if filterTree == nil { // all pass when filter list is empty
		return true
	}
	return filterTree.Search(keyBytes)
}

func HandleLogLevel(logLevel string) (string, error) {
	// seelog library is disgusting
	switch logLevel {
	case "debug":
		return "debug,info,warn,error,critical", nil
	case "":
		fallthrough
	case "info":
		return "info,warn,error,critical", nil
	case "warn":
		return "warn,error,critical", nil
	case "error":
		return "error,critical", nil
	default:
		return "", fmt.Errorf("unknown log level[%v]", logLevel)
	}
}