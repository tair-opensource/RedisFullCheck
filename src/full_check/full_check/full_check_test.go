package full_check

import (
	"database/sql"
	"fmt"
	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"os"
	"os/exec"
	"strconv"
	"testing"
)

type RedisFullCheckTestSuite struct {
	suite.Suite
}

func (suite *RedisFullCheckTestSuite) SetupTest() {
	conn, err := redis.Dial("tcp", "127.0.0.1:6000")
	if err != nil {
		panic(err.Error())
	}

	conn.Do("FLUSHALL")

	// same value
	conn.Do("SET", "SameValue", "val")
	// lack key
	conn.Do("SET", "LackKeyA", "valA")
	// different length
	conn.Do("SET", "DiffLength", "valA")
	// same length & different value
	conn.Do("SET", "SameLength", "valA")
	// type
	conn.Do("SADD", "TypeError", "a", "b")

	// set same
	conn.Do("SADD", "SetSame", "a")
	// set different field
	conn.Do("SADD", "SetDiffField", "a", "b")
	// set different fields
	conn.Do("SADD", "SetDiffFields", "a", "b")

	// hash same
	conn.Do("HSET", "HashSame", "a", "b")
	// hash different field
	conn.Do("HSET", "HashDiffField", "a", "b")
	// hash different fields
	conn.Do("HSET", "HashDiffFields", "a", "b")
	conn.Do("HSET", "HashDiffFields", "b", "c")

	// zset same
	conn.Do("ZADD", "ZsetSame", "1", "a")
	// zset different field
	conn.Do("ZADD", "ZsetDiffField", "2", "a")
	// zset different fields
	conn.Do("ZADD", "ZsetDiffFields", "2", "a")
	conn.Do("ZADD", "ZsetDiffFields", "2", "b")

	// list same
	conn.Do("LPUSH", "ListSame", "a")
	// list different field
	conn.Do("LPUSH", "ListDiffField", "c")
	// list different fields
	conn.Do("LPUSH", "ListDiffFields", "a")

	conn.Close()

	conn, err = redis.Dial("tcp", "127.0.0.1:7000")
	if err != nil {
		panic(err.Error())
	}

	conn.Do("FLUSHALL")

	// same value
	conn.Do("SET", "SameValue", "val")
	// different length
	conn.Do("SET", "DiffLength", "valAA")
	// same length & different value
	conn.Do("SET", "SameLength", "valB")
	// lack key
	conn.Do("SET", "LackKeyB", "valB")
	// type
	conn.Do("HSET", "TypeError", "a", "b")

	// set same
	conn.Do("SADD", "SetSame", "a")
	// set different field
	conn.Do("SADD", "SetDiffField", "c", "b")
	// set different fields
	conn.Do("SADD", "SetDiffFields", "a")

	// hash same
	conn.Do("HSET", "HashSame", "a", "b")
	// hash different field
	conn.Do("HSET", "HashDiffField", "b", "d")
	// hash different fields
	conn.Do("HSET", "HashDiffFields", "a", "b")

	// zset same
	conn.Do("ZADD", "ZsetSame", "1", "a")
	// zset different field
	conn.Do("ZADD", "ZsetDiffField", "2", "b")
	// zset different fields
	conn.Do("ZADD", "ZsetDiffFields", "2", "a")

	// list same
	conn.Do("LPUSH", "ListSame", "a")
	// list different field
	conn.Do("LPUSH", "ListDiffField", "b")
	// list different fields
	conn.Do("LPUSH", "ListDiffFields", "3", "a")

	conn.Close()

}

func (suite *RedisFullCheckTestSuite) TestValueLength() {
	cmd := exec.Command("/bin/bash", "-c", "./full_check -s 127.0.0.1:6000 -p '' -t 127.0.0.1:7000 -a '' --comparetimes=3 --comparemode=2 --interval=1 --log FFFF")
	if err := cmd.Run(); err != nil {
		panic(err)
	}

	db, err := sql.Open("sqlite3", "result.db.3")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	statem, err := db.Prepare(fmt.Sprintf("SELECT * FROM FINAL_RESULT WHERE Key=?"))
	if err != nil {
		panic(err)
	}
	defer statem.Close()

	suite.checkKey(statem, "LackKeyA", "lack_target", "", 1)
	suite.checkKey(statem, "DiffLength", "value", "", 1)
	suite.checkKey(statem, "SameLength", "value", "", 0)
	suite.checkKey(statem, "TypeError", "type", "", 1)

	suite.checkKey(statem, "HashDiffFields", "value", "", 1)
	suite.checkKey(statem, "HashSame", "", "", 0)
	suite.checkKey(statem, "HashDiffField", "", "", 0)

	suite.checkKey(statem, "ZsetDiffFields", "value", "", 1)
	suite.checkKey(statem, "ZsetSame", "", "", 0)
	suite.checkKey(statem, "ZsetDiffField", "", "", 0)

	suite.checkKey(statem, "SetDiffFields", "value", "", 1)
	suite.checkKey(statem, "SetSame", "", "", 0)
	suite.checkKey(statem, "SetDiffField", "", "", 0)

	suite.checkKey(statem, "ListDiffFields", "value", "", 1)
	suite.checkKey(statem, "ListSame", "", "", 0)
	suite.checkKey(statem, "ListDiffField", "", "", 0)
}

func (suite *RedisFullCheckTestSuite) TestFullValueCheck() {
	cmd := exec.Command("/bin/bash", "-c", "./full_check -s 127.0.0.1:6000 -p '' -t 127.0.0.1:7000 -a '' --comparetimes=3 --comparemode=1 --interval=1 --log FFFF")
	if err := cmd.Run(); err != nil {
		panic(err)
	}

	db, err := sql.Open("sqlite3", "result.db.3")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	statem, err := db.Prepare(fmt.Sprintf("SELECT * FROM FINAL_RESULT WHERE Key=?"))
	if err != nil {
		panic(err)
	}
	defer statem.Close()

	suite.checkKey(statem, "LackKeyA", "lack_target", "", 1)
	suite.checkKey(statem, "DiffLength", "value", "", 1)
	suite.checkKey(statem, "SameLength", "value", "", 1)
	suite.checkKey(statem, "TypeError", "type", "", 1)

	suite.checkKey(statem, "HashDiffFields", "lack_target", "b", 1)
	suite.checkKey(statem, "HashSame", "", "", 0)
	suite.checkKey(statem, "HashDiffField", "lack_target", "a", 1)

	// ???
	suite.checkKey(statem, "ZsetDiffFields", "lack_target", "b", 1)
	suite.checkKey(statem, "ZsetSame", "", "", 0)
	suite.checkKey(statem, "ZsetDiffField", "lack_target", "a", 1)

	suite.checkKey(statem, "SetDiffFields", "lack_target", "b", 1)
	suite.checkKey(statem, "SetSame", "", "", 0)
	suite.checkKey(statem, "SetDiffField", "lack_target", "a", 1)

	// ???
	suite.checkKey(statem, "ListDiffFields", "lack_target", "", 0)
	suite.checkKey(statem, "ListSame", "", "", 0)
	suite.checkKey(statem, "ListDiffField", "value", "0", 1)
}

func (suite *RedisFullCheckTestSuite) TestKeyOutline() {
	cmd := exec.Command("/bin/bash", "-c", "./full_check -s 127.0.0.1:6000 -p '' -t 127.0.0.1:7000 -a '' --comparetimes=3 --comparemode=3 --interval=1 --log FFFF")
	if err := cmd.Run(); err != nil {
		panic(err)
	}

	db, err := sql.Open("sqlite3", "result.db.3")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	statem, err := db.Prepare(fmt.Sprintf("SELECT * FROM FINAL_RESULT WHERE Key=?"))
	if err != nil {
		panic(err)
	}
	defer statem.Close()

	suite.checkKey(statem, "LackKeyA", "lack_target", "", 1)
	suite.checkKey(statem, "DiffLength", "value", "", 0)
	suite.checkKey(statem, "SameLength", "value", "", 0)
	suite.checkKey(statem, "TypeError", "type", "", 0)

	suite.checkKey(statem, "HashDiffFields", "value", "", 0)
	suite.checkKey(statem, "HashSame", "", "", 0)
	suite.checkKey(statem, "HashDiffField", "", "", 0)

	suite.checkKey(statem, "ZsetDiffFields", "value", "", 0)
	suite.checkKey(statem, "ZsetSame", "", "", 0)
	suite.checkKey(statem, "ZsetDiffField", "", "", 0)

	suite.checkKey(statem, "SetDiffFields", "value", "", 0)
	suite.checkKey(statem, "SetSame", "", "", 0)
	suite.checkKey(statem, "SetDiffField", "", "", 0)

	suite.checkKey(statem, "ListDiffFields", "value", "", 0)
	suite.checkKey(statem, "ListSame", "", "", 0)
	suite.checkKey(statem, "ListDiffField", "", "", 0)
}

// TestTargetDBMap checks that --targetdbmap compares one source db with another target db.
// The data is identical between source db3 and target db1, so without the map every key
// should be reported as lack_target, and with "--targetdbmap=3:1" only MapLackTargetKey
// should be reported.
func (suite *RedisFullCheckTestSuite) TestTargetDBMap() {
	const (
		sourceDB = 3
		targetDB = 1
	)

	// clean the dbs used by this test
	for _, addr := range []string{"127.0.0.1:6000", "127.0.0.1:7000"} {
		conn, err := redis.Dial("tcp", addr)
		if err != nil {
			panic(err.Error())
		}
		for _, db := range []int{sourceDB, targetDB} {
			conn.Do("SELECT", db)
			conn.Do("FLUSHDB")
		}
		conn.Close()
	}

	// source db3: two identical keys plus one key that doesn't exist on the target
	sourceConn, err := redis.Dial("tcp", "127.0.0.1:6000")
	if err != nil {
		panic(err.Error())
	}
	sourceConn.Do("SELECT", sourceDB)
	sourceConn.Do("SET", "MapSameKey", "val")
	sourceConn.Do("HSET", "MapSameHash", "f1", "v1")
	sourceConn.Do("SET", "MapLackTargetKey", "val")
	sourceConn.Close()

	// target db1: the same two keys
	targetConn, err := redis.Dial("tcp", "127.0.0.1:7000")
	if err != nil {
		panic(err.Error())
	}
	targetConn.Do("SELECT", targetDB)
	targetConn.Do("SET", "MapSameKey", "val")
	targetConn.Do("HSET", "MapSameHash", "f1", "v1")
	targetConn.Close()

	// 1. with db mapping: source db3 is compared with target db1
	cmd := exec.Command("/bin/bash", "-c", fmt.Sprintf(
		"./full_check -s 127.0.0.1:6000 -p '' -t 127.0.0.1:7000 -a '' --sourcedbfilterlist=%d "+
			"--targetdbmap=%d:%d --comparetimes=1 --comparemode=1 --interval=1 --db result_dbmap.db --log dbmap.log",
		sourceDB, sourceDB, targetDB))
	if out, err := cmd.CombinedOutput(); err != nil {
		panic(fmt.Sprintf("run with targetdbmap failed: %v, output: %s", err, string(out)))
	}

	db, err := sql.Open("sqlite3", "result_dbmap.db.1")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	statem, err := db.Prepare(fmt.Sprintf("SELECT * FROM FINAL_RESULT WHERE Key=?"))
	if err != nil {
		panic(err)
	}

	suite.checkKeyInDb(statem, "MapSameKey", "", "", strconv.Itoa(sourceDB), 0)
	suite.checkKeyInDb(statem, "MapSameHash", "", "", strconv.Itoa(sourceDB), 0)
	suite.checkKeyInDb(statem, "MapLackTargetKey", "lack_target", "", strconv.Itoa(sourceDB), 1)

	// 2. without db mapping: source db3 is compared with target db3 which is empty
	cmd = exec.Command("/bin/bash", "-c", fmt.Sprintf(
		"./full_check -s 127.0.0.1:6000 -p '' -t 127.0.0.1:7000 -a '' --sourcedbfilterlist=%d "+
			"--comparetimes=1 --comparemode=1 --interval=1 --db result_nodbmap.db --log dbmap.log",
		sourceDB))
	if out, err := cmd.CombinedOutput(); err != nil {
		panic(fmt.Sprintf("run without targetdbmap failed: %v, output: %s", err, string(out)))
	}

	db2, err := sql.Open("sqlite3", "result_nodbmap.db.1")
	if err != nil {
		panic(err)
	}
	defer db2.Close()

	statem2, err := db2.Prepare(fmt.Sprintf("SELECT * FROM FINAL_RESULT WHERE Key=?"))
	if err != nil {
		panic(err)
	}
	defer statem2.Close()

	suite.checkKeyInDb(statem2, "MapSameKey", "lack_target", "", strconv.Itoa(sourceDB), 1)
	suite.checkKeyInDb(statem2, "MapSameHash", "lack_target", "", strconv.Itoa(sourceDB), 1)
	suite.checkKeyInDb(statem2, "MapLackTargetKey", "lack_target", "", strconv.Itoa(sourceDB), 1)
}

// checkKeyInDb is the same as checkKey but also checks the schema(source db) column.
func (suite *RedisFullCheckTestSuite) checkKeyInDb(statem *sql.Stmt, key string, inconsistentType string, field string, db string, num int) {
	rows, err := statem.Query(key)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var InstanceA, InstanceB, Key, Schema, InconsistentType, Extra string
		if err := rows.Scan(&InstanceA, &InstanceB, &Key, &Schema, &InconsistentType, &Extra); err != nil {
			panic(err)
		}

		assert.Equal(suite.T(), key, Key)
		assert.Equal(suite.T(), db, Schema)
		assert.Equal(suite.T(), inconsistentType, InconsistentType)
		assert.Equal(suite.T(), field, Extra)

		count += 1
	}

	assert.Equal(suite.T(), count, num)
}

func (suite *RedisFullCheckTestSuite) checkKey(statem *sql.Stmt, key string, inconsistentType string, field string, num int) {
	rows, err := statem.Query(key)
	if err != nil {
		panic(err)
	}

	count := 0
	for rows.Next() {
		var InstanceA, InstanceB, Key, Schema, InconsistentType, Extra string
		err := rows.Scan(&InstanceA, &InstanceB, &Key, &Schema, &InconsistentType, &Extra)
		if err != nil {
			panic(err)
		}

		assert.Equal(suite.T(), key, Key)
		assert.Equal(suite.T(), "0", Schema)
		assert.Equal(suite.T(), inconsistentType, InconsistentType)
		assert.Equal(suite.T(), field, Extra)

		count += 1
		break
	}

	assert.Equal(suite.T(), count, num)
}

func TestFullCheck(t *testing.T) {
	suite.Run(t, new(RedisFullCheckTestSuite))
}

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}
