package main

import (
	"fmt"
	"github.com/cihub/seelog"
	"github.com/jessevdk/go-flags"
	"os"
	"strconv"
)

var logger seelog.LoggerInterface

var VERSION = "$"

var opts struct {
	SourceAddr      string `short:"s" long:"source" value-name:"SOURCE"  description:"Set host:port of source redis."`
	SourcePassword  string `short:"p" long:"sourcepassword" value-name:"Password" description:"Set source redis password"`
	SourceAuthType  string `long:"sourceauthtype" value-name:"AUTH-TYPE" default:"auth" description:"useless for opensource redis, valid value:auth/adminauth" `
	TargetAddr      string `short:"t" long:"target" value-name:"TARGET"  description:"Set host:port of target redis."`
	TargetPassword  string `short:"a" long:"targetpassword" value-name:"Password" description:"Set target redis password"`
	TargetAuthType  string `long:"targetauthtype" value-name:"AUTH-TYPE" default:"auth" description:"useless for opensource redis, valid value:auth/adminauth" `
	ResultDBFile    string `short:"d" long:"db" value-name:"Sqlite3-DB-FILE" default:"result.db" description:"sqlite3 db file for store result. If exist, it will be removed and a new file is created."`
	CompareTimes    string `long:"comparetimes" value-name:"COUNT" default:"3" description:"Total compare count, at least 1. In the first round, all keys will be compared. The subsequent rounds of the comparison will be done on the previous results."`
	CompareMode     int    `short:"m" long:"comparemode" default:"2" description:"compare mode, 1: compare full value, 2: only compare value length, 3: only compare keys outline, 4: compare full value, but only compare value length when meets big key"`
	Id              string `long:"id" default:"unknown" description:"used in metric, run id"`
	JobId           string `long:"jobid" default:"unknown" description:"used in metric, job id"`
	TaskId          string `long:"taskid" default:"unknown" description:"used in metric, task id"`
	Qps             int    `short:"q" long:"qps" default:"15000" description:"max qps limit"`
	Interval        int    `long:"interval" value-name:"Second" default:"5" description:"The time interval for each round of comparison(Second)"`
	BatchCount      string `long:"batchcount" value-name:"COUNT" default:"256" description:"the count of key/field per batch compare, valid value [1, 10000]"`
	Parallel        int    `long:"parallel" value-name:"COUNT" default:"5" description:"concurrent goroutine number for comparison, valid value [1, 100]"`
	LogFile         string `long:"log" value-name:"FILE" description:"log file, if not specified, log is put to console"`
	ResultFile      string `long:"result" value-name:"FILE" description:"store all diff result, format is 'db\tdiff-type\tkey\tfield'"`
	MetricFile      string `long:"metric" value-name:"FILE" description:"metrics file"`
	BigKeyThreshold int64  `long:"bigkeythreshold" value-name:"COUNT" default:"16384"`
	Version         bool   `short:"v" long:"version"`
}

func main() {
	// parse opts
	args, err := flags.Parse(&opts)

	if opts.Version {
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

	if opts.SourceAddr == "" || opts.TargetAddr == "" {
		fmt.Fprintf(os.Stderr, "-s, --source or -t, --target not specified\n")
		os.Exit(1)
	}

	if len(args) != 0 {
		fmt.Fprintf(os.Stderr, "unexpected args %+v", args)
		os.Exit(1)
	}

	// init log
	logger, err = initLog(opts.LogFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "init log failed: ", err)
		os.Exit(1)
	}
	logger.Info("init log success")
	defer logger.Flush()

	compareCount, err := strconv.Atoi(opts.CompareTimes)
	if err != nil || compareCount < 1 {
		panic(logger.Errorf("invalid option cmpcount %s, expect int >=1", opts.CompareTimes))
	}
	if opts.Interval < 0 {
		panic(logger.Errorf("invalid option interval %d, expect int >=0", opts.Interval))
	}
	batchCount, err := strconv.Atoi(opts.BatchCount)
	if err != nil || batchCount < 1 || batchCount > 10000 {
		panic(logger.Errorf("invalid option batchcount %s, expect int 1<=batchcount<=10000", opts.BatchCount))
	}
	parallel := opts.Parallel
	if err != nil || parallel < 1 || parallel > 100 {
		panic(logger.Errorf("invalid option parallel %d, expect 1<=parallel<=100", opts.Parallel))
	}
	qps := opts.Qps
	if err != nil || qps < 1 || qps > 5000000 {
		panic(logger.Errorf("invalid option qps %d, expect 1<=qps<=5000000", opts.Qps))
	}
	if opts.SourceAuthType != "auth" && opts.SourceAuthType != "adminauth" {
		panic(logger.Errorf("invalid sourceauthtype %s, expect auth/adminauth", opts.SourceAuthType))
	}
	if opts.TargetAuthType != "auth" && opts.TargetAuthType != "adminauth" {
		panic(logger.Errorf("invalid targetauthtype %s, expect auth/adminauth", opts.TargetAuthType))
	}
	if opts.CompareMode < FullValue || opts.CompareMode > FullValueWithOutline {
		panic(logger.Errorf("invalid compare mode %d", opts.CompareMode))
	}
	if opts.BigKeyThreshold <= 0 {
		panic(logger.Errorf("invalid big key threshold: %d", opts.BigKeyThreshold))
	} else {
		BigKeyThreshold = opts.BigKeyThreshold
	}

	fullCheckParameter := FullCheckParameter{
		sourceHost: RedisHost{
			addr:      opts.SourceAddr,
			password:  opts.SourcePassword,
			timeoutMs: 0,
			role:      "source",
			authtype:  opts.SourceAuthType,
		},
		targetHost: RedisHost{
			addr:      opts.TargetAddr,
			password:  opts.TargetPassword,
			timeoutMs: 0,
			role:      "target",
			authtype:  opts.TargetAuthType,
		},
		resultDBFile: opts.ResultDBFile,
		compareCount: compareCount,
		interval:     opts.Interval,
		batchCount:   batchCount,
		parallel:     parallel,
	}
	fullCheck := NewFullCheck(fullCheckParameter, CheckType(opts.CompareMode))
	fullCheck.Start()
}
