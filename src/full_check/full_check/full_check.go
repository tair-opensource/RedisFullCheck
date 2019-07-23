package full_check

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	_ "path"
	"strconv"
	"sync"
	"time"

	"full_check/common"
	"full_check/metric"
	"full_check/checker"
	"full_check/configure"
	"full_check/client"

	_ "github.com/mattn/go-sqlite3"
)

type CheckType int

const (
	FullValue            = 1
	ValueLengthOutline   = 2
	KeyOutline           = 3
	FullValueWithOutline = 4
)

type FullCheck struct {
	checker.FullCheckParameter

	stat                 metric.Stat
	currentDB            int32
	times                int
	db                   [100]*sql.DB
	sourcePhysicalDBList []string
	sourceLogicalDBMap   map[int32]int64

	totalConflict      int64
	totalKeyConflict   int64
	totalFieldConflict int64

	verifier checker.IVerifier
}

func NewFullCheck(f checker.FullCheckParameter, checktype CheckType) *FullCheck {
	var verifier checker.IVerifier

	fullcheck := &FullCheck{
		FullCheckParameter: f,
	}

	switch checktype {
	case ValueLengthOutline:
		verifier = checker.NewValueOutlineVerifier(&fullcheck.stat, &fullcheck.FullCheckParameter)
	case KeyOutline:
		verifier = checker.NewKeyOutlineVerifier(&fullcheck.stat, &fullcheck.FullCheckParameter)
	case FullValue:
		verifier = checker.NewFullValueVerifier(&fullcheck.stat, &fullcheck.FullCheckParameter, false)
	case FullValueWithOutline:
		verifier = checker.NewFullValueVerifier(&fullcheck.stat, &fullcheck.FullCheckParameter, true)
	default:
		panic(fmt.Sprintf("no such check type : %d", checktype))
	}

	fullcheck.verifier = verifier
	return fullcheck
}

func (p *FullCheck) PrintStat(finished bool) {
	var buf bytes.Buffer

	var metricStat *metric.Metric
	var finishPercent int64
	if p.SourceHost.IsCluster() == false {
		finishPercent = p.stat.Scan.Total() * 100 * int64(p.times) / (p.sourceLogicalDBMap[p.currentDB] * int64(p.CompareCount))
	} else {
		// meaningless for cluster
		finishPercent = -1
	}

	if p.times == 1 {
		metricStat = &metric.Metric{
			CompareTimes:       p.times,
			Db:                 p.currentDB,
			DbKeys:             p.sourceLogicalDBMap[p.currentDB],
			Process:            finishPercent, // meaningless for cluster
			OneCompareFinished: finished,
			AllFinished:        false,
			Timestamp:          time.Now().Unix(),
			DateTime:           time.Now().Format("2006-01-02T15:04:05Z"),
			Id:                 conf.Opts.Id,
			JobId:              conf.Opts.JobId,
			TaskId:             conf.Opts.TaskId}
		fmt.Fprintf(&buf, "times:%d, db:%d, dbkeys:%d, finish:%d%%, finished:%v\n", p.times, p.currentDB,
			p.sourceLogicalDBMap[p.currentDB], finishPercent, finished)
	} else {
		metricStat = &metric.Metric{
			CompareTimes:       p.times,
			Db:                 p.currentDB,
			Process:            finishPercent, // meaningless for cluster
			OneCompareFinished: finished,
			AllFinished:        false,
			Timestamp:          time.Now().Unix(),
			DateTime:           time.Now().Format("2006-01-02T15:04:05Z"),
			Id:                 conf.Opts.Id,
			JobId:              conf.Opts.JobId,
			TaskId:             conf.Opts.TaskId}
		fmt.Fprintf(&buf, "times:%d, db:%d, finished:%v\n", p.times, p.currentDB, finished)
	}

	p.totalConflict = int64(0)
	p.totalKeyConflict = int64(0)
	p.totalFieldConflict = int64(0)

	// fmt.Fprintf(&buf, "--- key scan ---\n")
	fmt.Fprintf(&buf, "KeyScan:%v\n", p.stat.Scan)
	metricStat.KeyScan = p.stat.Scan.Json()
	metricStat.KeyMetric = make(map[string]map[string]*metric.CounterStat)

	// fmt.Fprintf(&buf, "--- key equal ---\n")
	for i := common.KeyTypeIndex(0); i < common.EndKeyTypeIndex; i++ {
		metricStat.KeyMetric[i.String()] = make(map[string]*metric.CounterStat)
		if p.stat.ConflictKey[i][common.NoneConflict].Total() != 0 {
			metricStat.KeyMetric[i.String()]["equal"] = p.stat.ConflictKey[i][common.NoneConflict].Json()
			if p.times == p.CompareCount {
				fmt.Fprintf(&buf, "KeyEqualAtLast|%s|%s|%v\n", i, common.NoneConflict, 
					p.stat.ConflictKey[i][common.NoneConflict])
			} else {
				fmt.Fprintf(&buf, "KeyEqualInProcess|%s|%s|%v\n", i, common.NoneConflict, 
					p.stat.ConflictKey[i][common.NoneConflict])
			}
		}
	}
	// fmt.Fprintf(&buf, "--- key conflict ---\n")
	for i := common.KeyTypeIndex(0); i < common.EndKeyTypeIndex; i++ {
		for j := common.ConflictType(0); j < common.NoneConflict; j++ {
			// fmt.Println(i, j, p.stat.ConflictKey[i][j].Total())
			if p.stat.ConflictKey[i][j].Total() != 0 {
				metricStat.KeyMetric[i.String()][j.String()] = p.stat.ConflictKey[i][j].Json()
				if p.times == p.CompareCount {
					fmt.Fprintf(&buf, "KeyConflictAtLast|%s|%s|%v\n", i, j, p.stat.ConflictKey[i][j])
					p.totalKeyConflict += p.stat.ConflictKey[i][j].Total()
				} else {
					fmt.Fprintf(&buf, "KeyConflictInProcess|%s|%s|%v\n", i, j, p.stat.ConflictKey[i][j])
				}
			}
		}
	}

	metricStat.FieldMetric = make(map[string]map[string]*metric.CounterStat)
	// fmt.Fprintf(&buf, "--- field equal ---\n")
	for i := common.KeyTypeIndex(0); i < common.EndKeyTypeIndex; i++ {
		metricStat.FieldMetric[i.String()] = make(map[string]*metric.CounterStat)
		if p.stat.ConflictField[i][common.NoneConflict].Total() != 0 {
			metricStat.FieldMetric[i.String()]["equal"] = p.stat.ConflictField[i][common.NoneConflict].Json()
			if p.times == p.CompareCount {
				fmt.Fprintf(&buf, "FieldEqualAtLast|%s|%s|%v\n", i, common.NoneConflict,
					p.stat.ConflictField[i][common.NoneConflict])
			} else {
				fmt.Fprintf(&buf, "FieldEqualInProcess|%s|%s|%v\n", i, common.NoneConflict,
					p.stat.ConflictField[i][common.NoneConflict])
			}
		}
	}
	// fmt.Fprintf(&buf, "--- field conflict  ---\n")
	for i := common.KeyTypeIndex(0); i < common.EndKeyTypeIndex; i++ {
		for j := common.ConflictType(0); j < common.NoneConflict; j++ {
			if p.stat.ConflictField[i][j].Total() != 0 {
				metricStat.FieldMetric[i.String()][j.String()] = p.stat.ConflictField[i][j].Json()
				if p.times == p.CompareCount {
					fmt.Fprintf(&buf, "FieldConflictAtLast|%s|%s|%v\n", i, j, p.stat.ConflictField[i][j])
					p.totalFieldConflict += p.stat.ConflictField[i][j].Total()
				} else {
					fmt.Fprintf(&buf, "FieldConflictInProcess|%s|%s|%v\n", i, j, p.stat.ConflictField[i][j])
				}
			}
		}
	}

	p.totalConflict = p.totalKeyConflict + p.totalFieldConflict
	if conf.Opts.MetricPrint {
		metricstr, _ := json.Marshal(metricStat)
		common.Logger.Info(string(metricstr))
		// fmt.Println(string(metricstr))

		if p.times == p.CompareCount && finished {
			metricStat.AllFinished = true
			metricStat.Process = int64(100)
			metricStat.TotalConflict = p.totalConflict
			metricStat.TotalKeyConflict = p.totalKeyConflict
			metricStat.TotalFieldConflict = p.totalFieldConflict

			metricstr, _ := json.Marshal(metricStat)
			common.Logger.Info(string(metricstr))
			// fmt.Println(string(metricstr))
		}
	} else {
		common.Logger.Infof("stat:\n%s", string(buf.Bytes()))
	}
}

func (p *FullCheck) IncrScanStat(a int) {
	p.stat.Scan.Inc(a)
}

func (p *FullCheck) Start() {
	var err error

	for i := 1; i <= p.CompareCount; i++ {
		// init sqlite db
		os.Remove(p.ResultDBFile + "." + strconv.Itoa(i))
		p.db[i], err = sql.Open("sqlite3", p.ResultDBFile+"."+strconv.Itoa(i))
		if err != nil {
			panic(common.Logger.Critical(err))
		}
		defer p.db[i].Close()
	}

	sourceClient, err := client.NewRedisClient(p.SourceHost, 0)
	if err != nil {
		panic(common.Logger.Errorf("create redis client with host[%v] db[%v] error[%v]",
			p.SourceHost, 0, err))
	}

	p.sourceLogicalDBMap, p.sourcePhysicalDBList, err = sourceClient.FetchBaseInfo(conf.Opts.SourceDBType == 1)
	if err != nil {
		panic(common.Logger.Critical(err))
	}

	common.Logger.Infof("sourceDbType=%v, p.sourcePhysicalDBList=%v", p.FullCheckParameter.SourceHost.DBType,
		p.sourcePhysicalDBList)

	sourceClient.Close()
	for db, keyNum := range p.sourceLogicalDBMap {
		if p.SourceHost.IsCluster() == true {
			common.Logger.Infof("db=%d:keys=%d(inaccurate for type cluster)", db, keyNum)
		} else {
			common.Logger.Infof("db=%d:keys=%d", db, keyNum)
		}
	}

	for p.times = 1; p.times <= p.CompareCount; p.times++ {
		p.CreateDbTable(p.times)
		if p.times != 1 {
			common.Logger.Infof("wait %d seconds before start", p.Interval)
			time.Sleep(time.Second * time.Duration(p.Interval))
		}
		common.Logger.Infof("---------------- start %dth time compare", p.times)

		for db := range p.sourceLogicalDBMap {
			p.currentDB = db
			p.stat.Reset()
			// init stat timer
			tickerStat := time.NewTicker(time.Second * common.StatRollFrequency)
			ctxStat, cancelStat := context.WithCancel(context.Background()) // 主动cancel
			go func(ctx context.Context) {
				defer func() {
					tickerStat.Stop()
				}()

				for range tickerStat.C {
					select { // 判断是否结束
					case <-ctx.Done():
						return
					default:
					}
					p.stat.Rotate()
					p.PrintStat(false)
				}
			}(ctxStat)

			common.Logger.Infof("start compare db %d", p.currentDB)
			keys := make(chan []*common.Key, 1024)
			conflictKey := make(chan *common.Key, 1024)
			var wg, wg2 sync.WaitGroup
			// start scan, get all keys
			if p.times == 1 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					p.ScanFromSourceRedis(keys)
				}()
			} else {
				wg.Add(1)
				go func() {
					defer wg.Done()
					p.ScanFromDB(keys)
				}()
			}

			// start check
			wg.Add(p.Parallel)
			for i := 0; i < p.Parallel; i++ {
				go func() {
					defer wg.Done()
					p.VerifyAllKeyInfo(keys, conflictKey)
				}()
			}

			// start write conflictKey
			wg2.Add(1)
			go func() {
				defer wg2.Done()
				p.WriteConflictKey(conflictKey)
			}()

			wg.Wait()
			close(conflictKey)
			wg2.Wait()
			cancelStat() // stop stat goroutine
			p.PrintStat(true)
		} // for db, keyNum := range dbNums
	} // end for

	p.stat.Reset()
	common.Logger.Infof("--------------- finished! ----------------\nall finish successfully, totally %d key(s) and %d field(s) conflict",
		p.stat.TotalConflictKeys, p.stat.TotalConflictFields)
}

func (p *FullCheck) GetCurrentResultTable() (key string, field string) {
	if p.times != p.CompareCount {
		return fmt.Sprintf("key_%d", p.times), fmt.Sprintf("field_%d", p.times)
	} else {
		return "key", "field"
	}
}

func (p *FullCheck) GetLastResultTable() (key string, field string) {
	return fmt.Sprintf("key_%d", p.times-1), fmt.Sprintf("field_%d", p.times-1)
}

func (p *FullCheck) CreateDbTable(times int) {
	/** create table **/
	conflictKeyTableName, conflictFieldTableName := p.GetCurrentResultTable()

	conflictKeyTableSql := fmt.Sprintf(`
CREATE TABLE %s(
   id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
   key            TEXT NOT NULL,
   type           TEXT NOT NULL,
   conflict_type  TEXT NOT NULL,
   db             INTEGER NOT NULL,
   source_len     INTEGER NOT NULL,
   target_len     INTEGER NOT NULL
);
`, conflictKeyTableName)
	_, err := p.db[times].Exec(conflictKeyTableSql)
	if err != nil {
		panic(common.Logger.Errorf("exec sql %s failed: %s", conflictKeyTableSql, err))
	}
	conflictFieldTableSql := fmt.Sprintf(`
CREATE TABLE %s(
   id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
   field          TEXT NOT NULL,
   conflict_type  TEXT NOT NULL,
   key_id         INTEGER NOT NULL
);
`, conflictFieldTableName)
	_, err = p.db[times].Exec(conflictFieldTableSql)
	if err != nil {
		panic(common.Logger.Errorf("exec sql %s failed: %s", conflictFieldTableSql, err))
	}

	conflictResultSql := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s(
	InstanceA	TEXT NOT NULL,
	InstanceB	TEXT NOT NULL,
	Key			TEXT NOT NULL,
	Schema		TEXT NOT NULL,
	InconsistentType TEXT NOT NULL,
	Extra	    TEXT NOT NULL
	);`, "FINAL_RESULT")
	_, err = p.db[times].Exec(conflictResultSql)
	if err != nil {
		panic(common.Logger.Errorf("exec sql %s failed: %s", conflictResultSql, err))
	}
}

func (p *FullCheck) VerifyAllKeyInfo(allKeys <-chan []*common.Key, conflictKey chan<- *common.Key) {
	sourceClient, err := client.NewRedisClient(p.SourceHost, p.currentDB)
	if err != nil {
		panic(common.Logger.Errorf("create redis client with host[%v] db[%v] error[%v]",
			p.SourceHost, p.currentDB, err))
	}
	defer sourceClient.Close()

	targetClient, err := client.NewRedisClient(p.TargetHost, p.currentDB)
	if err != nil {
		panic(common.Logger.Errorf("create redis client with host[%v] db[%v] error[%v]",
			p.TargetHost, p.currentDB, err))
	}
	defer targetClient.Close()

	// limit qps
	qos := common.StartQoS(conf.Opts.Qps)
	for keyInfo := range allKeys {
		<-qos.Bucket
		p.verifier.VerifyOneGroupKeyInfo(keyInfo, conflictKey, &sourceClient, &targetClient)
	} // for oneGroupKeys := range allKeys

	qos.Close()
}

func (p *FullCheck) WriteConflictKey(conflictKey <-chan *common.Key) {
	conflictKeyTableName, conflictFieldTableName := p.GetCurrentResultTable()

	var resultfile *os.File
	if len(conf.Opts.ResultFile) > 0 {
		resultfile, _ = os.OpenFile(conf.Opts.ResultFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
		defer resultfile.Close()
	}

	tx, _ := p.db[p.times].Begin()
	statInsertKey, err := tx.Prepare(fmt.Sprintf("insert into %s (key, type, conflict_type, db, source_len, target_len) values(?,?,?,?,?,?)", conflictKeyTableName))
	if err != nil {
		panic(common.Logger.Error(err))
	}
	statInsertField, err := tx.Prepare(fmt.Sprintf("insert into %s (field, conflict_type, key_id) values (?,?,?)", conflictFieldTableName))
	if err != nil {
		panic(common.Logger.Error(err))
	}

	count := 0
	for oneKeyInfo := range conflictKey {
		if count%1000 == 0 {
			var err error
			statInsertKey.Close()
			statInsertField.Close()
			e := tx.Commit()
			if e != nil {
				common.Logger.Error(e.Error())
			}

			tx, _ = p.db[p.times].Begin()
			statInsertKey, err = tx.Prepare(fmt.Sprintf("insert into %s (key, type, conflict_type, db, source_len, target_len) values(?,?,?,?,?,?)", conflictKeyTableName))
			if err != nil {
				panic(common.Logger.Error(err))
			}

			statInsertField, err = tx.Prepare(fmt.Sprintf("insert into %s (field, conflict_type, key_id) values (?,?,?)", conflictFieldTableName))
			if err != nil {
				panic(common.Logger.Error(err))
			}
		}
		count += 1

		result, err := statInsertKey.Exec(string(oneKeyInfo.Key), oneKeyInfo.Tp.Name, oneKeyInfo.ConflictType.String(), p.currentDB, oneKeyInfo.SourceAttr.ItemCount, oneKeyInfo.TargetAttr.ItemCount)
		if err != nil {
			panic(common.Logger.Error(err))
		}
		if len(oneKeyInfo.Field) != 0 {
			lastId, _ := result.LastInsertId()
			for i := 0; i < len(oneKeyInfo.Field); i++ {
				_, err = statInsertField.Exec(string(oneKeyInfo.Field[i].Field), oneKeyInfo.Field[i].ConflictType.String(), lastId)
				if err != nil {
					panic(common.Logger.Error(err))
				}

				if p.times == p.CompareCount {
					finalstat, err := tx.Prepare(fmt.Sprintf("insert into FINAL_RESULT (InstanceA, InstanceB, Key, Schema, InconsistentType, Extra) VALUES(?, ?, ?, ?, ?, ?)"))
					if err != nil {
						panic(common.Logger.Error(err))
					}
					// defer finalstat.Close()
					_, err = finalstat.Exec("", "", string(oneKeyInfo.Key), strconv.Itoa(int(p.currentDB)),
						oneKeyInfo.Field[i].ConflictType.String(),
						string(oneKeyInfo.Field[i].Field))
					if err != nil {
						panic(common.Logger.Error(err))
					}

					finalstat.Close()

					if len(conf.Opts.ResultFile) != 0 {
						resultfile.WriteString(fmt.Sprintf("%d\t%s\t%s\t%s\n", int(p.currentDB), oneKeyInfo.Field[i].ConflictType.String(), string(oneKeyInfo.Key), string(oneKeyInfo.Field[i].Field)))
					}
				}
			}
		} else {
			if p.times == p.CompareCount {
				finalstat, err := tx.Prepare(fmt.Sprintf("insert into FINAL_RESULT (InstanceA, InstanceB, Key, Schema, InconsistentType, Extra) VALUES(?, ?, ?, ?, ?, ?)"))
				if err != nil {
					panic(common.Logger.Error(err))
				}
				// defer finalstat.Close()
				_, err = finalstat.Exec("", "", string(oneKeyInfo.Key), strconv.Itoa(int(p.currentDB)), oneKeyInfo.ConflictType.String(), "")
				if err != nil {
					panic(common.Logger.Error(err))
				}
				finalstat.Close()

				if len(conf.Opts.ResultFile) != 0 {
					resultfile.WriteString(fmt.Sprintf("%d\t%s\t%s\t%s\n", int(p.currentDB), oneKeyInfo.ConflictType.String(), string(oneKeyInfo.Key), ""))
				}
			}
		}
	}
	statInsertKey.Close()
	statInsertField.Close()
	tx.Commit()
}
