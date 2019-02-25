redis-full-check is used to compare whether two redis have the same data. We also offer a data synchronization tool called [redis-shake](https://github.com/aliyun/redis-shake) to syncing data from one redis to another redis.
Thanks to the Douyu's WSD team for the support. 感谢斗鱼公司的web服务部提供的支持。

* [中文文档](https://yq.aliyun.com/articles/690463)

# redis-full-check
---
redis-full-check is developed and maintained by NoSql Team in Alibaba-Cloud.
redis-full-check performs full data verification by comparing the data of the source database and the destination database. The entire check process consists of multiple comparisons, in every comparison, redis-full-check fetches data from two dabatases and then compared, the inconsistent data is put into sqlite3 db for the next comparison. By this iteratively comparing method, the difference continues to converge. The following figure shows the dataflow. In every comparison which is the yellow box, redis-full-check fetches all keys firstly. After that, it runs comparison and stores the difference result(key and field) into the sqlite3 db which is the position that keys and fields can be fetched in next round instead of the source database.<br>
![dataflow.png](https://github.com/aliyun/redis-full-check/blob/master/resources/dataflow.png)<br>
redis-full-check only checks whether the target database is a subset of the source database. If you want to know whether the data in the source and destination databases are exactly the same, you need to set up a bidirectional link.<br>

# Code branch rules
version rules: a.b.c.

*  a: major version
*  b: minor version. even number means stable version.
*  c: bugfix version

| branch name | rules |
| - | :- |
| master | master branch, do not allowed push code. store the latest stable version. |
| develop | develop branch. all the bellowing branches fork from this. |
| feature-\* | new feature branch. forked from develop branch and then merge back after finish developing, testing, and code review. |
| bugfix-\* | bugfix branch. forked from develop branch and then merge back after finish developing, testing, and code review. |
| improve-\* | improvement branch. forked from develop branch and then merge back after finish developing, testing, and code review.  |

tag rules:
add tag when releasing: "release-v{version}-{date}". for example: "release-v1.0.2-20180628"

# Paramters
```
Usage:
  redis-full-check [OPTIONS]

Application Options:
  -s, --source=SOURCE               Set host:port of source redis.
  -p, --sourcepassword=Password     Set source redis password
      --sourceauthtype=AUTH-TYPE    useless for opensource redis, valid value:auth/adminauth (default: auth)
  -t, --target=TARGET               Set host:port of target redis.
  -a, --targetpassword=Password     Set target redis password
      --targetauthtype=AUTH-TYPE    useless for opensource redis, valid value:auth/adminauth (default: auth)
  -d, --db=Sqlite3-DB-FILE          sqlite3 db file for store result. If exist, it will be removed and a new file is created. (default: result.db)
      --comparetimes=COUNT          Total compare count, at least 1. In the first round, all keys will be compared. The subsequent rounds of the comparison will be done on the previous results. (default: 3)
  -m, --comparemode=                compare mode, 1: compare full value, 2: only compare value length, 3: only compare keys outline (default: 2)
      --id=                         used in metric, run id (default: unknown)
      --jobid=                      used in metric, job id (default: unknown)
      --taskid=                     used in metric, task id (default: unknown)
  -q, --qps=                        max qps limit (default: 15000)
      --interval=Second             The time interval for each round of comparison(Second) (default: 5)
      --batchcount=COUNT            the count of key/field per batch compare, valid value [1, 10000] (default: 256)
      --parallel=COUNT              concurrent goroutine number for comparison, valid value [1, 100] (default: 5)
      --log=FILE                    log file, if not specified, log is put to console
      --result=FILE                 store all diff result, format is 'db	diff-type	key	field'
      --metric=FILE                 metrics file
  -v, --version

Help Options:
  -h, --help                        Show this help message
```

# Usage
---
*  git clone https://github.com/aliyun/redis-full-check.git
*  cd redis-full-check/src/vendor
*  GOPATH=\`pwd\`/../..; govendor sync     #please note: must install govendor first and then pull all dependencies
*  cd ../../ && ./build.sh
*  ./redis-full-check -s $(source_redis_ip_port) -p $(source_password) -t $(target_redis_ip_port) -a $(target_password) # these parameters should be given by users
