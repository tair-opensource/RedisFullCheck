package client

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
	"full_check/common"
)

type RedisHost struct {
	Addr      string
	Password  string
	TimeoutMs uint64
	Role      string // "source" or "target"
	Authtype  string // "auth" or "adminauth"
}

func (p RedisHost) String() string {
	return fmt.Sprintf("%s redis addr: %s", p.Role, p.Addr)
}

type RedisClient struct {
	redisHost RedisHost
	db        int32
	conn      redis.Conn
}

func (p RedisClient) String() string {
	return p.redisHost.String()
}

func NewRedisClient(redisHost RedisHost, db int32) (RedisClient, error) {
	rc := RedisClient{
		redisHost: redisHost,
		db:        db,
	}

	// send ping command first
	ret, err := rc.Do("ping")
	if err == nil && ret.(string) != "PONG" {
		return RedisClient{}, fmt.Errorf("ping return invaild[%v]", string(ret.([]byte)))
	}
	return rc, err
}

func (p *RedisClient) CheckHandleNetError(err error) bool {
	if err == io.EOF { // 对方断开网络
		if p.conn != nil {
			p.conn.Close()
			p.conn = nil
			// 网络相关错误1秒后重试
			time.Sleep(time.Second)
		}
		return true
	} else if _, ok := err.(net.Error); ok {
		if p.conn != nil {
			p.conn.Close()
			p.conn = nil
			// 网络相关错误1秒后重试
			time.Sleep(time.Second)
		}
		return true
	}
	return false
}

func (p *RedisClient) Connect() error {
	var err error
	if p.conn == nil {
		if p.redisHost.TimeoutMs == 0 {
			p.conn, err = redis.Dial("tcp", p.redisHost.Addr)
		} else {
			p.conn, err = redis.DialTimeout("tcp", p.redisHost.Addr, time.Millisecond*time.Duration(p.redisHost.TimeoutMs),
				time.Millisecond*time.Duration(p.redisHost.TimeoutMs), time.Millisecond*time.Duration(p.redisHost.TimeoutMs))
		}
		if err != nil {
			return err
		}
		if len(p.redisHost.Password) != 0 {
			_, err = p.conn.Do(p.redisHost.Authtype, p.redisHost.Password)
			if err != nil {
				return err
			}
		}
		_, err = p.conn.Do("select", p.db)
		if err != nil {
			return err
		}
	} // p.conn == nil
	return nil
}

func (p *RedisClient) Do(commandName string, args ...interface{}) (interface{}, error) {
	var err error
	var result interface{}
	tryCount := 0
begin:
	for {
		if tryCount > common.MaxRetryCount {
			return nil, err
		}
		tryCount++

		if p.conn == nil {
			err = p.Connect()
			if err != nil {
				if p.CheckHandleNetError(err) {
					break begin
				}
				return nil, err
			}
		}

		result, err = p.conn.Do(commandName, args...)
		if err != nil {
			if p.CheckHandleNetError(err) {
				break begin
			}
			return nil, err
		}
		break
	} // end for {}
	return result, err
}

func (p *RedisClient) Close() {
	if p.conn != nil {
		p.conn.Close()
		p.conn = nil
	}
}

func (p *RedisClient) PipeTypeCommand(keyInfo []*common.Key) ([]string, error) {
	var err error
	result := make([]string, len(keyInfo))
	tryCount := 0
begin:
	for {
		if tryCount > common.MaxRetryCount {
			return nil, err
		}
		tryCount++

		if p.conn == nil {
			err = p.Connect()
			if err != nil {
				if p.CheckHandleNetError(err) {
					break begin
				}
				return nil, err
			}
		}

		for _, key := range keyInfo {
			err = p.conn.Send("type", key.Key)
			if err != nil {
				if p.CheckHandleNetError(err) {
					break begin
				}
				return nil, err
			}
		}
		err = p.conn.Flush()
		if err != nil {
			if p.CheckHandleNetError(err) {
				break begin
			}
			return nil, err
		}

		for i := 0; i < len(keyInfo); i++ {
			reply, err := p.conn.Receive()
			if err != nil {
				if p.CheckHandleNetError(err) {
					break begin
				}
				return nil, err
			}
			result[i] = reply.(string)
		}
		break
	} // end for {}
	return result, nil
}

func (p *RedisClient) PipeExistsCommand(keyInfo []*common.Key) ([]int64, error) {
	var err error
	result := make([]int64, len(keyInfo))
	tryCount := 0
begin:
	for {
		if tryCount > common.MaxRetryCount {
			return nil, err
		}
		tryCount++

		if p.conn == nil {
			err = p.Connect()
			if err != nil {
				if p.CheckHandleNetError(err) {
					break begin
				}
				return nil, err
			}
		}

		for _, key := range keyInfo {
			err = p.conn.Send("exists", key.Key)
			if err != nil {
				if p.CheckHandleNetError(err) {
					break begin
				}
				return nil, err
			}
		}
		err = p.conn.Flush()
		if err != nil {
			if p.CheckHandleNetError(err) {
				break begin
			}
			return nil, err
		}

		for i := 0; i < len(keyInfo); i++ {
			reply, err := p.conn.Receive()
			if err != nil {
				if p.CheckHandleNetError(err) {
					break begin
				}
				return nil, err
			}
			result[i] = reply.(int64)
		}
		break
	} // end for {}
	return result, nil
}

func (p *RedisClient) PipeLenCommand(keys []*common.Key) ([]int64, error) {
	var err error
	result := make([]int64, len(keys))
	tryCount := 0
begin:
	for {
		if tryCount > common.MaxRetryCount {
			return nil, err
		}
		tryCount++

		if p.conn == nil {
			err = p.Connect()
			if err != nil {
				if p.CheckHandleNetError(err) {
					break begin
				}
				return nil, err
			}
		}

		for _, key := range keys {
			err = p.conn.Send(key.Tp.FetchLenCommand, key.Key)
			if err != nil {
				if p.CheckHandleNetError(err) {
					break begin
				}
				return nil, err
			}
		}
		err = p.conn.Flush()
		if err != nil {
			if p.CheckHandleNetError(err) {
				break begin
			}
			return nil, err
		}

		for i := 0; i < len(keys); i++ {
			reply, err := p.conn.Receive()
			if err != nil {
				if p.CheckHandleNetError(err) {
					break begin
				}
				if strings.HasPrefix(err.Error(), "WRONGTYPE") {
					result[i] = -1
				}
			} else {
				result[i] = reply.(int64)
			}
		}
		break
	} // end for {}
	return result, nil
}

func (p *RedisClient) PipeValueCommand(fetchValueKeyInfo []*common.Key) ([]interface{}, error) {
	var err error
	result := make([]interface{}, len(fetchValueKeyInfo))
	tryCount := 0
begin:
	for {
		if tryCount > common.MaxRetryCount {
			return nil, err
		}
		tryCount++

		if p.conn == nil {
			err = p.Connect()
			if err != nil {
				if p.CheckHandleNetError(err) {
					break begin
				}
				return nil, err
			}
		}

		for _, item := range fetchValueKeyInfo {
			switch item.Tp {
			case common.StringKeyType:
				err = p.conn.Send("get", item.Key)
			case common.HashKeyType:
				err = p.conn.Send("hgetall", item.Key)
			case common.ListKeyType:
				err = p.conn.Send("lrange", item.Key, 0, -1)
			case common.SetKeyType:
				err = p.conn.Send("smembers", item.Key)
			case common.ZsetKeyType:
				err = p.conn.Send("zrange", item.Key, 0, -1, "WITHSCORES")
			default:
				err = p.conn.Send("get", item.Key)
			}

			if err != nil {
				if p.CheckHandleNetError(err) {
					break begin
				}
				return nil, err
			}
		}
		err = p.conn.Flush()
		if err != nil {
			if p.CheckHandleNetError(err) {
				break begin
			}
			return nil, err
		}

		for i := 0; i < len(fetchValueKeyInfo); i++ {
			reply, err := p.conn.Receive()
			if err != nil {
				if p.CheckHandleNetError(err) {
					break begin
				}
				return nil, err
			}
			result[i] = reply
		}
		break
	} // end for {}
	return result, nil
}

func (p *RedisClient) PipeSismemberCommand(key []byte, field [][]byte) ([]interface{}, error) {
	var err error
	result := make([]interface{}, len(field))
	tryCount := 0
begin:
	for {
		if tryCount > common.MaxRetryCount {
			return nil, err
		}
		tryCount++

		if p.conn == nil {
			err = p.Connect()
			if err != nil {
				if p.CheckHandleNetError(err) {
					break begin
				}
				return nil, err
			}
		}

		for _, item := range field {
			err = p.conn.Send("SISMEMBER", key, item)
			if err != nil {
				if p.CheckHandleNetError(err) {
					break begin
				}
				return nil, err
			}
		}
		err = p.conn.Flush()
		if err != nil {
			if p.CheckHandleNetError(err) {
				break begin
			}
			return nil, err
		}

		for i := 0; i < len(field); i++ {
			reply, err := p.conn.Receive()
			if err != nil {
				if p.CheckHandleNetError(err) {
					break begin
				}
				return nil, err
			}
			result[i] = reply
		}
		break
	} // end for {}
	return result, nil
}

func (p *RedisClient) PipeZscoreCommand(key []byte, field [][]byte) ([]interface{}, error) {
	var err error
	result := make([]interface{}, len(field))
	tryCount := 0
begin:
	for {
		if tryCount > common.MaxRetryCount {
			return nil, err
		}
		tryCount++

		if p.conn == nil {
			err = p.Connect()
			if err != nil {
				if p.CheckHandleNetError(err) {
					break begin
				}
				return nil, err
			}
		}

		for _, item := range field {
			err = p.conn.Send("ZSCORE", key, item)
			if err != nil {
				if p.CheckHandleNetError(err) {
					break begin
				}
				return nil, err
			}
		}
		err = p.conn.Flush()
		if err != nil {
			if p.CheckHandleNetError(err) {
				break begin
			}
			return nil, err
		}

		for i := 0; i < len(field); i++ {
			reply, err := p.conn.Receive()
			if err != nil {
				if p.CheckHandleNetError(err) {
					break begin
				}
				return nil, err
			}
			result[i] = reply
		}
		break
	} // end for {}
	return result, nil
}

func (p *RedisClient) FetchValueUseScan_Hash_Set_SortedSet(oneKeyInfo *common.Key, onceScanCount int) (map[string][]byte, error) {
	var scanCmd string
	switch oneKeyInfo.Tp {
	case common.HashKeyType:
		scanCmd = "hscan"
	case common.SetKeyType:
		scanCmd = "sscan"
	case common.ZsetKeyType:
		scanCmd = "zscan"
	default:
		return nil, fmt.Errorf("key type %s is not hash/set/zset", oneKeyInfo.Tp)
	}
	cursor := 0
	value := make(map[string][]byte)
	for {
		reply, err := p.Do(scanCmd, oneKeyInfo.Key, cursor, "count", onceScanCount)
		if err != nil {
			return nil, err
		}

		replyList, ok := reply.([]interface{})
		if ok == false || len(replyList) != 2 {
			return nil, fmt.Errorf("%s %s %d count %d failed, result: %+v", scanCmd, string(oneKeyInfo.Key),
				cursor, onceScanCount, reply)
		}

		cursorBytes, ok := replyList[0].([]byte)
		if ok == false {
			return nil, fmt.Errorf("%s %s %d count %d failed, result: %+v", scanCmd, string(oneKeyInfo.Key),
				cursor, onceScanCount, reply)
		}

		cursor, err = strconv.Atoi(string(cursorBytes))
		if err != nil {
			return nil, err
		}

		keylist, ok := replyList[1].([]interface{})
		if ok == false {
			panic(common.Logger.Criticalf("%s %s failed, result: %+v", scanCmd, string(oneKeyInfo.Key), reply))
		}
		switch oneKeyInfo.Tp {
		case common.HashKeyType:
			fallthrough
		case common.ZsetKeyType:
			for i := 0; i < len(keylist); i += 2 {
				value[string(keylist[i].([]byte))] = keylist[i+1].([]byte)
			}
		case common.SetKeyType:
			for i := 0; i < len(keylist); i++ {
				value[string(keylist[i].([]byte))] = nil
			}
		default:
			return nil, fmt.Errorf("key type %s is not hash/set/zset", oneKeyInfo.Tp)
		}

		if cursor == 0 {
			break
		}
	} // end for{}
	return value, nil
}