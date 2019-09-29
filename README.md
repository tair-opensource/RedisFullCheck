Redis-full-check is used to compare whether two redis have the same data. We also offer a data synchronization tool called [redis-shake](https://github.com/aliyun/redis-shake) to syncing data from one redis to another redis.<br>
Thanks to the Douyu's WSD team for the support. <br>

* [中文文档](https://yq.aliyun.com/articles/690463)
* [下载地址 redis-full-check.tar.gz](https://github.com/alibaba/RedisFullCheck/releases)
* [第一次使用，如何进行配置](https://github.com/alibaba/RedisFullCheck/wiki/%E7%AC%AC%E4%B8%80%E6%AC%A1%E4%BD%BF%E7%94%A8%EF%BC%8C%E5%A6%82%E4%BD%95%E8%BF%9B%E8%A1%8C%E9%85%8D%E7%BD%AE%EF%BC%9F)

# redis-full-check
---
Redis-full-check is developed and maintained by NoSQL Team in Alibaba-Cloud Database department.<br>
Redis-full-check performs full data verification by comparing the data of the source database and the destination database. The entire check process consists of multiple comparisons, in every comparison, redis-full-check fetches data from two dabatases and then compared, the inconsistent data is put into sqlite3 db for the next comparison. By this iteratively comparing method, the difference continues to converge. The following figure shows the dataflow. In every comparison which is the yellow box, redis-full-check fetches all keys firstly. After that, it runs comparison and stores the difference result(key and field) into the sqlite3 db which is the position that keys and fields can be fetched in next round instead of the source database.<br>
![dataflow.png](https://github.com/aliyun/redis-full-check/blob/master/resources/dataflow.png)<br>
Redis-full-check fetches keys from source and then checks these keys exist on the target. So if one key exists on the target but lack on the source, redis-full-check can't find it. If you want to know whether the data in the source and destination databases are exactly the same, you need to set up a bidirectional link: <br>

* source->RedisFullCheck->target
* target->RedisFullCheck->source

# supports
standalone, cluster, proxy(aliyun-cluster, tencent-cluster). Redis version from 2.x to 5.x.

# Code branch rules
Version rules: a.b.c.<br>

*  a: major version
*  b: minor version. even number means stable version.
*  c: bugfix version

| branch name | rules |
| - | :- |
| master | master branch, do not allowed push code. store the latest stable version. develop branch will merge into this branch once new version created. |
| **develop**(main branch) | develop branch. all the bellowing branches fork from this. |
| feature-\* | new feature branch. forked from develop branch and then merge back after finish developing, testing, and code review. |
| bugfix-\* | bugfix branch. forked from develop branch and then merge back after finish developing, testing, and code review. |
| improve-\* | improvement branch. forked from develop branch and then merge back after finish developing, testing, and code review.  |

Tag rules:<br>
Add tag when releasing: "release-v{version}-{date}". for example: "release-v1.0.2-20180628"

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
      --comparetimes=COUNT          Total compare count, at least 1. In the first round, all keys will be compared. The subsequent rounds of the comparison
                                    will be done on the previous results. (default: 3)
  -m, --comparemode=                compare mode, 1: compare full value, 2: only compare value length, 3: only compare keys outline, 4: compare full value,
                                    but only compare value length when meets big key (default: 2)
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
      --bigkeythreshold=COUNT
  -f, --filterlist=FILTER           if the filter list isn't empty, all elements in list will be synced. The input should be split by '|'. The end of the
                                    string is followed by a * to indicate a prefix match, otherwise it is a full match. e.g.: 'abc*|efg|m*' matches 'abc',
                                    'abc1', 'efg', 'm', 'mxyz', but 'efgh', 'p' aren't'
  -v, --version

Help Options:
  -h, --help                        Show this help message
```

# Usage
---
Run `./bin/redis-full-check.darwin64` or `redis-full-check.linux64` which is built in OSX and Linux respectively, however, the binaries aren't always the newest version.<br>
Or you can build redis-full-check yourself according to the following steps:<br>
*  git clone https://github.com/alibaba/RedisFullCheck.git
*  cd RedisFullCheck/src/vendor
*  GOPATH=\`pwd\`/../..; govendor sync     #please note: must install govendor first and then pull all dependencies
*  cd ../../ && ./build.sh
*  ./redis-full-check -s $(source_redis_ip_port) -p $(source_password) -t $(target_redis_ip_port) -a $(target_password) # these parameters should be given by users

Here comes the sqlite3 example to display the conflict result:<br>
```
$ sqlite3 result.db.3  # result.db.x shows the x-round comparison conflict result. len == -1 means inconsistent key type.

sqlite> select * from key;
id          key              type        conflict_type  db          source_len  target_len
----------  ---------------  ----------  -------------  ----------  ----------  ----------
1           keydiff1_string  string      value          1           6           6
2           keydiff_hash     hash        value          0           2           1
3           keydiff_string   string      value          0           6           6
4           key_string_diff  string      value          0           6           6
5           keylack_string   string      lack_target    0           6           0
sqlite>

sqlite> select * from field;
id          field       conflict_type  key_id
----------  ----------  -------------  ----------
1           k1          lack_source    2
2           k2          value          2
3           k3          lack_target    2
```

# Shake series tool
---
We also provide some tools for synchronization in Shake series.<br>

* [MongoShake](https://github.com/aliyun/MongoShake): mongodb data synchronization tool.
* [RedisShake](https://github.com/aliyun/RedisShake): redis data synchronization tool.
* [RedisFullCheck](https://github.com/aliyun/RedisFullCheck): redis data synchronization verification tool.

Plus, we have a DingDing(钉钉) group so that users can join and discuss, please scan the code.
![DingDing](resources/dingding_group.png)<br>
