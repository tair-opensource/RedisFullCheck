# issue1: 支持 source / target 异构 db 映射（新增 `--targetdbmap`）

## 1. 问题背景

`--sourcedbfilterlist` 和 `--targetdbfilterlist` 从名字上看像是一对「按位置配对」的参数，容易让人以为：

```bash
--sourcedbfilterlist=113;213;313;413 \
--targetdbfilterlist=113;213;1;2
```

的含义是 `113↔113`、`213↔213`、`313↔1`、`413↔2`。

实际排查后发现：**不存在任何位置配对逻辑**，并且 **`--targetdbfilterlist` 是一个完全无效的参数**。

## 2. 调查结论（原代码行为）

### 2.1 只有 source 的 db 列表驱动比较

`full_check/full_check.go` 中 `Start()` 只取 source 的 db：

```go
p.sourceLogicalDBMap, p.sourcePhysicalDBList, err = sourceClient.FetchBaseInfo(conf.Opts.SourceDBType == 1)
...
for db := range p.sourceLogicalDBMap {
    p.currentDB = db
```

`FetchBaseInfo()` 在整个仓库中**只被 source 调用**一次（`full_check.go`），target 的 keyspace / 白名单从未被读取。

### 2.2 source 白名单是「取交集」

`client/db.go` 的 `FetchBaseInfo()`：

```go
logicalDBMap, err = common.ParseKeyspace(keyspaceContent.([]byte)) // 来自 INFO keyspace
...
if len(p.redisHost.DBFilterList) != 0 {
    for key := range logicalDBMap {
        if _, ok := p.redisHost.DBFilterList[int(key)]; !ok {
            delete(logicalDBMap, key)
        }
    }
}
```

- `ParseKeyspace()` 只返回 `INFO keyspace` 中出现过的 db，即**有 key 的 db**
- 所以白名单是「取交集」，不是「按列表去扫」：白名单里有 313，但 source 的 db313 为空或不存在时，该 db 完全不会参与比较
- `-1` 转换为空 map，表示不启用过滤

### 2.3 source / target 永远使用同一个 db 号

`full_check.go` 的 `VerifyAllKeyInfo()`：

```go
sourceClient, _ := client.NewRedisClient(p.SourceHost, p.currentDB)
targetClient, _ := client.NewRedisClient(p.TargetHost, p.currentDB) // 同一个 p.currentDB
```

`client.Connect()` 里对非 cluster 类型执行 `select p.db`，因此 target 访问的永远是 source 的 db 号。

### 2.4 原行为总结

| 输入 | 实际比较 |
|---|---|
| `--sourcedbfilterlist=113;213;313;413`<br>`--targetdbfilterlist=113;213;1;2` | source db113 ↔ target db113<br>source db213 ↔ target db213<br>source db313 ↔ target db313<br>source db413 ↔ target db413 |

`--targetdbfilterlist` 的值对结果**没有任何影响**。

### 2.5 附带发现的坑

- 分隔符是 **`;`**（`common/common.go` 的 `Splitter`），不是逗号。`--sourcedbfilterlist=113,213` 会在 `strconv.Atoi` 处 panic
- 在 shell 中必须加引号：`--sourcedbfilterlist='113;213'`，否则 `;` 会被 bash 当命令分隔符截断

## 3. 改动方案

新增一个显式的 db 映射参数，语义确定、不依赖顺序：

```
--targetdbmap="313:1;413:2"
```

- 格式：`source:target`，多项之间用 `;` 分隔
- **未出现在映射中的 source db，仍然与 target 同名 db 比较**，因此不传该参数时行为与改动前完全一致（向后兼容）
- 校验：非数字、负数、缺少 `:`、同一个 source db 映射到不同 target db，都会在启动时报错退出
- 启动时打印实际映射关系，便于核对：

```
db=113:keys=100 -> target db=113
db=313:keys=200 -> target db=1
```

### 3.1 用法示例

```bash
./bin/redis-full-check \
  -s <source_host:port> -p '<source_password>' \
  -t <target_host:port> -a '<target_password>' \
  --sourcedbfilterlist='113;213;313;413' \
  --targetdbmap='313:1;413:2'
```

比较关系：

| 源 db | 目标 db | 说明 |
|---|---|---|
| 113 | 113 | 未在 map 中，默认同号 |
| 213 | 213 | 未在 map 中，默认同号 |
| 313 | 1 | 由 `313:1` 指定 |
| 413 | 2 | 由 `413:2` 指定 |

> 前提：source 上这些 db 在 `INFO keyspace` 中存在且有 key，否则不会被比较。

### 3.2 关于 `--targetdbfilterlist`

本次**没有改变**它的行为（仍是无效参数），只在启动时打一条 warning 提示改用 `--targetdbmap`，避免误用：

```
option targetdbfilterlist[113;213;1;2] is ignored, use --targetdbmap to map source db to target db
```

## 4. 代码改动

| 文件 | 改动 |
|---|---|
| `configure/conf.go` | 新增 `--targetdbmap` 选项 |
| `common/mix.go` | 新增 `ParseDBMap()`，负责解析与校验 |
| `checker/base.go` | `FullCheckParameter` 增加 `TargetDBMap map[int]int` 字段 |
| `main.go` | 解析参数、校验、传入 `FullCheckParameter`、对 `--targetdbfilterlist` 告警 |
| `full_check/full_check/full_check.go` | 新增 `currentTargetDB` 字段与 `TargetDB()` 方法；target client 改用映射后的 db；启动时打印映射关系 |

关键实现（`full_check.go`）：

```go
// TargetDB returns the target db which the given source db should be compared with.
func (p *FullCheck) TargetDB(sourceDB int32) int32 {
    if targetDB, ok := p.TargetDBMap[int(sourceDB)]; ok {
        return int32(targetDB)
    }
    return sourceDB
}
```

```go
targetClient, err := client.NewRedisClient(p.TargetHost, p.currentTargetDB)
```

### 4.1 结果文件说明

sqlite 结果表中的 `db` 列（以及 `FINAL_RESULT.Schema`、`--result` 输出的第一列）记录的仍然是 **source db**，未做改动，保证下游解析逻辑不受影响。多轮比较（`comparetimes > 1`）从上一轮 sqlite 中按 `db = sourceDB` 取冲突 key 的逻辑也保持不变。

## 5. 测试

### 5.1 单元测试（`common/mix_test.go`）

`TestParseDBMap` 覆盖：

- 空串 / `-1` → 空映射
- 单项映射、多项映射
- 空格、多余分隔符的容错
- 非法输入：缺 `:`、多个 `:`、非数字、负数、同一 source 映射到不同 target、误用逗号

`TestFilterDBList` 覆盖原白名单解析（含 `-1` 与逗号导致 panic 的行为固化）。

### 5.2 集成测试（`full_check/full_check_test.go::TestTargetDBMap`）

场景：source db3 与 target db1 存放相同数据（`MapSameKey`、`MapSameHash`），source db3 额外多一个 `MapLackTargetKey`，target db3 为空。

| 运行方式 | 期望结果 | 结果 |
|---|---|---|
| `--sourcedbfilterlist=3 --targetdbmap=3:1` | 两个相同 key 不产生冲突，仅 `MapLackTargetKey` 报 `lack_target` | 通过 |
| `--sourcedbfilterlist=3`（不映射） | 三个 key 全部报 `lack_target` | 通过 |

第二种情况正是改动前「313 实际拿去对 313」的现象，可作为回归用例。

### 5.3 运行方式

```bash
# 依赖：本地 6000(source) / 7000(target) 两个无密码 redis
docker run -d --name rfc-src -p 6000:6379 redis:6.2.24
docker run -d --name rfc-tgt -p 7000:6379 redis:6.2.24

cd src/full_check
go build -o full_check/full_check .          # 集成测试会 exec ./full_check
GOMODCACHE=<repo>/pkg/mod GOFLAGS=-mod=mod \
  go test -ldflags=-linkmode=external -count=1 ./...
```

结果：

```
ok      full_check/common
ok      full_check/full_check   (4 个用例全部通过，含原有 3 个 + 新增 TestTargetDBMap)
```

> `-ldflags=-linkmode=external` 是本机环境所需（go1.18.1 darwin/amd64 + 新版 macOS 下测试二进制会报
> `dyld: missing LC_UUID load command`），与本次改动无关。
>
> `go.sum` 补充了 testify 及其依赖的 `h1:` 校验和：这些包在 `go.mod` 中已有 `require`，但 `go.sum`
> 缺少条目会导致 `go test` 直接报 `missing go.sum entry`（改动前单元测试无法运行）。

## 6. 兼容性

- 不传 `--targetdbmap` 时行为与改动前完全一致
- 结果 sqlite 表结构、`FINAL_RESULT`、`--result` 输出格式均未改动
- `--sourcedbfilterlist` 语义未改动
- `--targetdbfilterlist` 仍保持无效，仅新增 warning 日志

## 7. 未覆盖的场景

- **cluster / proxy 类型下没有做映射验证**：cluster 的 `logicalDBMap` 恒为 `{0:0}`，给 db0 做映射虽有实现但缺乏实际意义，未做专门测试
- **没有实现「两个 filter list 按位置配对」的隐式语义**：位置配对依赖 map 遍历顺序，不可靠，因此选择了显式映射方案
