package main

import (
	"fmt"
	"full_check/common"
	"github.com/jessevdk/go-flags"
	"os"
	"strconv"
	"strings"
	"full_check/configure"
	"full_check/full_check"
	"full_check/checker"
	"full_check/client"
)

var VERSION = "$"

func main() {
	// parse conf.Opts
	args, err := flags.Parse(&conf.Opts)

	if conf.Opts.Version {
		fmt.Println(VERSION)
		os.Exit(0)
	}

	// 若err != nil, 会自动打印错误到 stderr
	if err != nil {
		if flagsErr, ok := err.(*flags.Error); ok && flagsErr.Type == flags.ErrHelp {
			os.Exit(0)
		} else {
			fmt.Fprintf(os.Stderr, "flag err %s\n", flagsErr)
			os.Exit(1)
		}
	}

	if conf.Opts.SourceAddr == "" || conf.Opts.TargetAddr == "" {
		fmt.Fprintf(os.Stderr, "-s, --source or -t, --target not specified\n")
		os.Exit(1)
	}

	if len(args) != 0 {
		fmt.Fprintf(os.Stderr, "unexpected args %+v", args)
		os.Exit(1)
	}

	// init log
	common.Logger, err = common.InitLog(conf.Opts.LogFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "init log failed: ", err)
		os.Exit(1)
	}
	common.Logger.Info("init log success")
	defer common.Logger.Flush()

	compareCount, err := strconv.Atoi(conf.Opts.CompareTimes)
	if err != nil || compareCount < 1 {
		panic(common.Logger.Errorf("invalid option cmpcount %s, expect int >=1", conf.Opts.CompareTimes))
	}
	if conf.Opts.Interval < 0 {
		panic(common.Logger.Errorf("invalid option interval %d, expect int >=0", conf.Opts.Interval))
	}
	batchCount, err := strconv.Atoi(conf.Opts.BatchCount)
	if err != nil || batchCount < 1 || batchCount > 10000 {
		panic(common.Logger.Errorf("invalid option batchcount %s, expect int 1<=batchcount<=10000", conf.Opts.BatchCount))
	}
	parallel := conf.Opts.Parallel
	if err != nil || parallel < 1 || parallel > 100 {
		panic(common.Logger.Errorf("invalid option parallel %d, expect 1<=parallel<=100", conf.Opts.Parallel))
	}
	qps := conf.Opts.Qps
	if err != nil || qps < 1 || qps > 5000000 {
		panic(common.Logger.Errorf("invalid option qps %d, expect 1<=qps<=5000000", conf.Opts.Qps))
	}
	if conf.Opts.SourceAuthType != "auth" && conf.Opts.SourceAuthType != "adminauth" {
		panic(common.Logger.Errorf("invalid sourceauthtype %s, expect auth/adminauth", conf.Opts.SourceAuthType))
	}
	if conf.Opts.TargetAuthType != "auth" && conf.Opts.TargetAuthType != "adminauth" {
		panic(common.Logger.Errorf("invalid targetauthtype %s, expect auth/adminauth", conf.Opts.TargetAuthType))
	}
	if conf.Opts.CompareMode < full_check.FullValue || conf.Opts.CompareMode > full_check.FullValueWithOutline {
		panic(common.Logger.Errorf("invalid compare mode %d", conf.Opts.CompareMode))
	}
	if conf.Opts.BigKeyThreshold < 0 {
		panic(common.Logger.Errorf("invalid big key threshold: %d", conf.Opts.BigKeyThreshold))
	} else if conf.Opts.BigKeyThreshold == 0 {
		common.BigKeyThreshold = 16384
	} else {
		common.BigKeyThreshold = conf.Opts.BigKeyThreshold
	}

	// filter list
	var filterTree *common.Trie
	if len(conf.Opts.FilterList) != 0 {
		filterTree = common.NewTrie()
		filterList := strings.Split(conf.Opts.FilterList, "|")
		for _, filter := range filterList {
			if filter == "" {
				panic(common.Logger.Errorf("invalid input filter list: %v", filterList))
			}
			filterTree.Insert([]byte(filter))
		}
		common.Logger.Infof("filter list enabled: %v", filterList)
	}

	fullCheckParameter := checker.FullCheckParameter{
		SourceHost: client.RedisHost{
			Addr:         strings.Split(conf.Opts.SourceAddr, common.Splitter),
			Password:     conf.Opts.SourcePassword,
			TimeoutMs:    0,
			Role:         "source",
			Authtype:     conf.Opts.SourceAuthType,
			DBType:       conf.Opts.SourceDBType,
			DBFilterList: common.FilterDBList(conf.Opts.SourceDBFilterList),
		},
		TargetHost: client.RedisHost{
			Addr:         strings.Split(conf.Opts.TargetAddr, common.Splitter),
			Password:     conf.Opts.TargetPassword,
			TimeoutMs:    0,
			Role:         "target",
			Authtype:     conf.Opts.TargetAuthType,
			DBType:       conf.Opts.TargetDBType,
			DBFilterList: common.FilterDBList(conf.Opts.TargetDBFilterList),
		},
		ResultDBFile: conf.Opts.ResultDBFile,
		CompareCount: compareCount,
		Interval:     conf.Opts.Interval,
		BatchCount:   batchCount,
		Parallel:     parallel,
		FilterTree:   filterTree,
	}
	fullCheck := full_check.NewFullCheck(fullCheckParameter, full_check.CheckType(conf.Opts.CompareMode))
	fullCheck.Start()
}
