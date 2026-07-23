package client

import (
	"crypto/tls"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
	redigoCluster "github.com/najoast/redis-go-cluster"
)

const redisClusterSlotCount = 16384

type TLSClusterOptions struct {
	StartNodes []string
	Password   string
	AuthType   string
	Timeout    time.Duration
	TLSConfig  *tls.Config
}

type tlsClusterCommand struct {
	name string
	args []interface{}
}

type tlsClusterReply struct {
	value interface{}
	err   error
}

// TLSClusterConn implements redigo.Conn while routing every cluster node
// connection through TLS. It is intentionally used only for TLS clusters;
// the existing cluster client remains unchanged for plaintext connections.
type TLSClusterConn struct {
	options TLSClusterOptions

	topologyMu sync.RWMutex
	slots      [redisClusterSlotCount]string
	pools      map[string]*redis.Pool
	nodes      []string

	pipelineMu sync.Mutex
	pending    []tlsClusterCommand
	replies    []tlsClusterReply

	closeMu sync.RWMutex
	closed  bool
}

func NewTLSClusterConn(options TLSClusterOptions) (redis.Conn, error) {
	if len(options.StartNodes) == 0 {
		return nil, fmt.Errorf("TLS cluster requires at least one start node")
	}
	if options.TLSConfig == nil {
		return nil, fmt.Errorf("TLS cluster requires a TLS configuration")
	}
	if options.AuthType == "" {
		options.AuthType = "auth"
	}

	cluster := &TLSClusterConn{
		options: options,
		pools:   make(map[string]*redis.Pool),
	}

	errors := make([]string, 0, len(options.StartNodes))
	for _, address := range options.StartNodes {
		if err := cluster.refresh(address); err == nil {
			return cluster, nil
		} else {
			errors = append(errors, fmt.Sprintf("%s: %v", address, err))
		}
	}
	cluster.Close()
	return nil, fmt.Errorf("initialize TLS cluster from %v failed: %s", options.StartNodes, strings.Join(errors, "; "))
}

func (c *TLSClusterConn) dial(address string) (redis.Conn, error) {
	options := []redis.DialOption{
		redis.DialUseTLS(true),
		redis.DialTLSConfig(c.options.TLSConfig),
	}
	if c.options.Timeout > 0 {
		options = append(options,
			redis.DialConnectTimeout(c.options.Timeout),
			redis.DialReadTimeout(c.options.Timeout),
			redis.DialWriteTimeout(c.options.Timeout),
		)
	}

	conn, err := redis.Dial("tcp", address, options...)
	if err != nil {
		return nil, err
	}
	if c.options.Password == "" {
		return conn, nil
	}

	authArgs := make([]interface{}, 0, 2)
	for _, arg := range strings.SplitN(c.options.Password, ":", 2) {
		authArgs = append(authArgs, arg)
	}
	if _, err := conn.Do(c.options.AuthType, authArgs...); err != nil {
		conn.Close()
		return nil, err
	}
	return conn, nil
}

func (c *TLSClusterConn) pool(address string) (*redis.Pool, error) {
	c.closeMu.RLock()
	closed := c.closed
	c.closeMu.RUnlock()
	if closed {
		return nil, fmt.Errorf("TLS cluster connection is closed")
	}

	c.topologyMu.Lock()
	defer c.topologyMu.Unlock()
	if pool, ok := c.pools[address]; ok {
		return pool, nil
	}

	pool := &redis.Pool{
		MaxIdle:     16,
		IdleTimeout: 60 * time.Second,
		Dial: func() (redis.Conn, error) {
			return c.dial(address)
		},
	}
	c.pools[address] = pool
	return pool, nil
}

func (c *TLSClusterConn) refresh(seedAddress string) error {
	pool, err := c.pool(seedAddress)
	if err != nil {
		return err
	}
	conn := pool.Get()
	defer conn.Close()
	if err := conn.Err(); err != nil {
		return err
	}

	reply, err := redis.Values(conn.Do("CLUSTER", "SLOTS"))
	if err != nil {
		return fmt.Errorf("CLUSTER SLOTS: %w", err)
	}

	var slots [redisClusterSlotCount]string
	covered := 0
	nodeSet := make(map[string]struct{})
	seedHost, _, _ := net.SplitHostPort(seedAddress)
	for _, rawSlot := range reply {
		values, err := redis.Values(rawSlot, nil)
		if err != nil || len(values) < 3 {
			return fmt.Errorf("invalid CLUSTER SLOTS entry %v", rawSlot)
		}
		start, err := redis.Int(values[0], nil)
		if err != nil {
			return fmt.Errorf("invalid cluster slot start: %w", err)
		}
		end, err := redis.Int(values[1], nil)
		if err != nil || start < 0 || end < start || end >= redisClusterSlotCount {
			return fmt.Errorf("invalid cluster slot range %d-%d", start, end)
		}

		primary, err := redis.Values(values[2], nil)
		if err != nil || len(primary) < 2 {
			return fmt.Errorf("invalid cluster primary entry %v", values[2])
		}
		host, err := redis.String(primary[0], nil)
		if err != nil {
			return fmt.Errorf("invalid cluster primary host: %w", err)
		}
		if host == "" {
			host = seedHost
		}
		port, err := redis.Int(primary[1], nil)
		if err != nil {
			return fmt.Errorf("invalid cluster primary port: %w", err)
		}
		address := net.JoinHostPort(strings.Trim(host, "[]"), strconv.Itoa(port))

		for slot := start; slot <= end; slot++ {
			if slots[slot] == "" {
				covered++
			}
			slots[slot] = address
		}
		nodeSet[address] = struct{}{}
	}
	if covered != redisClusterSlotCount {
		return fmt.Errorf("CLUSTER SLOTS covers %d of %d slots", covered, redisClusterSlotCount)
	}

	nodes := make([]string, 0, len(nodeSet))
	for address := range nodeSet {
		nodes = append(nodes, address)
		if _, err := c.pool(address); err != nil {
			return err
		}
	}
	sort.Strings(nodes)

	c.topologyMu.Lock()
	c.slots = slots
	c.nodes = nodes
	c.topologyMu.Unlock()
	return nil
}

func (c *TLSClusterConn) route(commandName string, args []interface{}) (string, error) {
	command := strings.ToUpper(commandName)
	if command == "PING" || command == "CLUSTER" || command == "INFO" {
		c.topologyMu.RLock()
		defer c.topologyMu.RUnlock()
		if len(c.nodes) == 0 {
			return "", fmt.Errorf("TLS cluster has no available nodes")
		}
		return c.nodes[0], nil
	}
	if len(args) == 0 {
		return "", fmt.Errorf("cluster command %s requires a key", commandName)
	}

	slot, err := redigoCluster.GetSlot(args[0])
	if err != nil {
		return "", err
	}
	c.topologyMu.RLock()
	address := c.slots[slot]
	c.topologyMu.RUnlock()
	if address == "" {
		return "", fmt.Errorf("no cluster node for slot %d", slot)
	}
	return address, nil
}

func (c *TLSClusterConn) executeAt(address, commandName string, args ...interface{}) (interface{}, error) {
	pool, err := c.pool(address)
	if err != nil {
		return nil, err
	}
	conn := pool.Get()
	defer conn.Close()
	if err := conn.Err(); err != nil {
		return nil, err
	}
	return conn.Do(commandName, args...)
}

func parseClusterRedirect(err error) (kind, address string, ok bool) {
	if err == nil {
		return "", "", false
	}
	fields := strings.Fields(err.Error())
	if len(fields) != 3 || (fields[0] != "MOVED" && fields[0] != "ASK") {
		return "", "", false
	}
	return fields[0], fields[2], true
}

func (c *TLSClusterConn) execute(commandName string, args ...interface{}) (interface{}, error) {
	command := strings.ToUpper(commandName)
	if command == "AUTH" || command == "ADMINAUTH" || command == "SELECT" {
		return "OK", nil
	}

	address, err := c.route(commandName, args)
	if err != nil {
		return nil, err
	}
	for redirects := 0; redirects < 3; redirects++ {
		reply, err := c.executeAt(address, commandName, args...)
		kind, redirectAddress, redirected := parseClusterRedirect(err)
		if !redirected {
			return reply, err
		}

		address = redirectAddress
		if kind == "ASK" {
			pool, poolErr := c.pool(address)
			if poolErr != nil {
				return nil, poolErr
			}
			conn := pool.Get()
			if conn.Err() != nil {
				err = conn.Err()
				conn.Close()
				return nil, err
			}
			if _, err = conn.Do("ASKING"); err == nil {
				reply, err = conn.Do(commandName, args...)
			}
			conn.Close()
			return reply, err
		}
		_ = c.refresh(address)
	}
	return nil, fmt.Errorf("too many cluster redirects for %s", commandName)
}

func (c *TLSClusterConn) Do(commandName string, args ...interface{}) (interface{}, error) {
	return c.execute(commandName, args...)
}

func (c *TLSClusterConn) Send(commandName string, args ...interface{}) error {
	c.pipelineMu.Lock()
	defer c.pipelineMu.Unlock()
	c.pending = append(c.pending, tlsClusterCommand{name: commandName, args: args})
	return nil
}

func (c *TLSClusterConn) Flush() error {
	c.pipelineMu.Lock()
	commands := c.pending
	c.pending = nil
	c.replies = nil
	c.pipelineMu.Unlock()

	replies := make([]tlsClusterReply, len(commands))
	groups := make(map[string][]int)
	for index, command := range commands {
		address, err := c.route(command.name, command.args)
		if err != nil {
			replies[index].err = err
			continue
		}
		groups[address] = append(groups[address], index)
	}

	for address, indexes := range groups {
		pool, err := c.pool(address)
		if err != nil {
			return err
		}
		conn := pool.Get()
		if err := conn.Err(); err != nil {
			conn.Close()
			return err
		}
		for _, index := range indexes {
			command := commands[index]
			if err := conn.Send(command.name, command.args...); err != nil {
				conn.Close()
				return err
			}
		}
		if err := conn.Flush(); err != nil {
			conn.Close()
			return err
		}
		for _, index := range indexes {
			reply, err := conn.Receive()
			if _, _, redirected := parseClusterRedirect(err); redirected {
				command := commands[index]
				reply, err = c.execute(command.name, command.args...)
			}
			replies[index] = tlsClusterReply{value: reply, err: err}
		}
		conn.Close()
	}

	c.pipelineMu.Lock()
	c.replies = replies
	c.pipelineMu.Unlock()
	return nil
}

func (c *TLSClusterConn) Receive() (interface{}, error) {
	c.pipelineMu.Lock()
	defer c.pipelineMu.Unlock()
	if len(c.replies) == 0 {
		return nil, fmt.Errorf("no pending TLS cluster replies")
	}
	reply := c.replies[0]
	c.replies = c.replies[1:]
	return reply.value, reply.err
}

func (c *TLSClusterConn) Err() error {
	c.closeMu.RLock()
	defer c.closeMu.RUnlock()
	if c.closed {
		return fmt.Errorf("TLS cluster connection is closed")
	}
	return nil
}

func (c *TLSClusterConn) Close() error {
	c.closeMu.Lock()
	if c.closed {
		c.closeMu.Unlock()
		return nil
	}
	c.closed = true
	c.closeMu.Unlock()

	c.topologyMu.Lock()
	defer c.topologyMu.Unlock()
	for _, pool := range c.pools {
		pool.Close()
	}
	return nil
}
