package client

import (
	"bufio"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	redigoCluster "github.com/najoast/redis-go-cluster"
)

type testTLSCluster struct {
	servers []*testTLSServer
	config  *tls.Config
}

type testTLSServer struct {
	id       string
	listener net.Listener
	cluster  *testTLSCluster

	mu       sync.Mutex
	commands []string
	wg       sync.WaitGroup
}

func newTestTLSCertificate(t *testing.T) (tls.Certificate, []byte, *x509.CertPool) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "localhost"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:              []string{"localhost"},
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certificatePEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	certificate, err := tls.X509KeyPair(certificatePEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(certificatePEM)
	return certificate, certificatePEM, roots
}

func newTestTLSCluster(t *testing.T) *testTLSCluster {
	t.Helper()
	certificate, _, roots := newTestTLSCertificate(t)
	cluster := &testTLSCluster{
		config: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots},
	}
	for _, id := range []string{"node-a", "node-b"} {
		listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
			MinVersion:   tls.VersionTLS12,
			Certificates: []tls.Certificate{certificate},
		})
		if err != nil {
			t.Fatal(err)
		}
		server := &testTLSServer{id: id, listener: listener, cluster: cluster}
		cluster.servers = append(cluster.servers, server)
	}
	for _, server := range cluster.servers {
		server.wg.Add(1)
		go server.serve()
	}
	return cluster
}

func (c *testTLSCluster) Close() {
	for _, server := range c.servers {
		server.listener.Close()
	}
	for _, server := range c.servers {
		server.wg.Wait()
	}
}

func (s *testTLSServer) serve() {
	defer s.wg.Done()
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer conn.Close()
			s.serveConn(conn)
		}()
	}
}

func (s *testTLSServer) serveConn(conn net.Conn) {
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	for {
		command, err := readRESPCommand(reader)
		if err != nil {
			return
		}
		s.mu.Lock()
		s.commands = append(s.commands, strings.Join(command, " "))
		s.mu.Unlock()

		switch strings.ToUpper(command[0]) {
		case "PING":
			writeSimpleString(writer, "PONG")
		case "AUTH", "ADMINAUTH", "SELECT", "ASKING":
			writeSimpleString(writer, "OK")
		case "CLUSTER":
			if len(command) > 1 && strings.EqualFold(command[1], "SLOTS") {
				s.writeClusterSlots(writer)
			} else if len(command) > 1 && strings.EqualFold(command[1], "NODES") {
				s.writeClusterNodes(writer)
			} else {
				writeError(writer, "ERR unsupported CLUSTER command")
			}
		case "GET":
			writeBulkString(writer, s.id+":"+command[1])
		case "TYPE":
			writeSimpleString(writer, "string")
		default:
			writeError(writer, "ERR unsupported command")
		}
		if err := writer.Flush(); err != nil {
			return
		}
	}
}

func (s *testTLSServer) writeClusterSlots(writer *bufio.Writer) {
	fmt.Fprint(writer, "*2\r\n")
	for index, bounds := range [][2]int{{0, 8191}, {8192, 16383}} {
		host, portText, _ := net.SplitHostPort(s.cluster.servers[index].listener.Addr().String())
		port, _ := strconv.Atoi(portText)
		fmt.Fprintf(writer, "*3\r\n:%d\r\n:%d\r\n*3\r\n", bounds[0], bounds[1])
		writeBulkString(writer, host)
		fmt.Fprintf(writer, ":%d\r\n", port)
		writeBulkString(writer, s.cluster.servers[index].id)
	}
}

func (s *testTLSServer) writeClusterNodes(writer *bufio.Writer) {
	var lines strings.Builder
	for index, server := range s.cluster.servers {
		start, end := 0, 8191
		if index == 1 {
			start, end = 8192, 16383
		}
		fmt.Fprintf(&lines, "%s %s@1 master - 0 0 %d connected %d-%d\n",
			server.id, server.listener.Addr().String(), index+1, start, end)
	}
	writeBulkString(writer, lines.String())
}

func (s *testTLSServer) sawCommand(command string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, actual := range s.commands {
		if actual == command {
			return true
		}
	}
	return false
}

func readRESPCommand(reader *bufio.Reader) ([]string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if len(line) < 4 || line[0] != '*' {
		return nil, fmt.Errorf("invalid RESP array %q", line)
	}
	count, err := strconv.Atoi(strings.TrimSpace(line[1:]))
	if err != nil {
		return nil, err
	}
	command := make([]string, count)
	for index := range command {
		line, err = reader.ReadString('\n')
		if err != nil || len(line) < 4 || line[0] != '$' {
			return nil, fmt.Errorf("invalid RESP bulk length %q: %v", line, err)
		}
		length, err := strconv.Atoi(strings.TrimSpace(line[1:]))
		if err != nil {
			return nil, err
		}
		value := make([]byte, length+2)
		if _, err = io.ReadFull(reader, value); err != nil {
			return nil, err
		}
		command[index] = string(value[:length])
	}
	return command, nil
}

func writeSimpleString(writer *bufio.Writer, value string) {
	fmt.Fprintf(writer, "+%s\r\n", value)
}

func writeBulkString(writer *bufio.Writer, value string) {
	fmt.Fprintf(writer, "$%d\r\n%s\r\n", len(value), value)
}

func writeError(writer *bufio.Writer, value string) {
	fmt.Fprintf(writer, "-%s\r\n", value)
}

func keyInSlotRange(t *testing.T, start, end int) []byte {
	t.Helper()
	for index := 0; index < 100000; index++ {
		key := []byte(fmt.Sprintf("key-%d", index))
		slot, err := redigoCluster.GetSlot(key)
		if err != nil {
			t.Fatal(err)
		}
		if int(slot) >= start && int(slot) <= end {
			return key
		}
	}
	t.Fatal("could not find key in slot range")
	return nil
}

func TestRedisClientUsesTLS(t *testing.T) {
	cluster := newTestTLSCluster(t)
	defer cluster.Close()

	client, err := NewRedisClient(RedisHost{
		Addr:      []string{cluster.servers[0].listener.Addr().String()},
		Authtype:  "auth",
		TLSConfig: cluster.config,
	}, 0)
	if err != nil {
		t.Fatal(err)
	}
	client.Close()
	if !cluster.servers[0].sawCommand("ping") {
		t.Fatal("TLS server did not receive PING")
	}
}

func TestTLSClusterRoutesAndPipelines(t *testing.T) {
	cluster := newTestTLSCluster(t)
	defer cluster.Close()

	conn, err := NewTLSClusterConn(TLSClusterOptions{
		StartNodes: []string{cluster.servers[0].listener.Addr().String()},
		AuthType:   "auth",
		Timeout:    5 * time.Second,
		TLSConfig:  cluster.config,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	firstKey := keyInSlotRange(t, 0, 8191)
	secondKey := keyInSlotRange(t, 8192, 16383)
	firstReply, err := conn.Do("GET", firstKey)
	if err != nil {
		t.Fatal(err)
	}
	secondReply, err := conn.Do("GET", secondKey)
	if err != nil {
		t.Fatal(err)
	}
	if string(firstReply.([]byte)) != "node-a:"+string(firstKey) {
		t.Fatalf("unexpected first node reply %q", firstReply)
	}
	if string(secondReply.([]byte)) != "node-b:"+string(secondKey) {
		t.Fatalf("unexpected second node reply %q", secondReply)
	}

	if err := conn.Send("TYPE", firstKey); err != nil {
		t.Fatal(err)
	}
	if err := conn.Send("TYPE", secondKey); err != nil {
		t.Fatal(err)
	}
	if err := conn.Flush(); err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 2; index++ {
		reply, err := conn.Receive()
		if err != nil || reply != "string" {
			t.Fatalf("pipeline reply %d = %v, %v", index, reply, err)
		}
	}

	if !cluster.servers[0].sawCommand("GET " + string(firstKey)) {
		t.Fatalf("first key %q was not routed to node-a", firstKey)
	}
	if !cluster.servers[1].sawCommand("GET " + string(secondKey)) {
		t.Fatalf("second key %q was not routed to node-b", secondKey)
	}
}
