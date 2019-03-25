package conf

var Opts struct {
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
	MetricPrint     bool   `long:"metric" value-name:"BOOL" description:"print metric in log"`
	BigKeyThreshold int64  `long:"bigkeythreshold" value-name:"COUNT" default:"16384"`
	FilterList      string `short:"f" long:"filterlist" value-name:"FILTER" default:"" description:"if the filter list isn't empty, all elements in list will be synced. The input should be split by '|'. The end of the string is followed by a * to indicate a prefix match, otherwise it is a full match. e.g.: 'abc*|efg|m*' matches 'abc', 'abc1', 'efg', 'm', 'mxyz', but 'efgh', 'p' aren't'"`
	Version         bool   `short:"v" long:"version"`
}
