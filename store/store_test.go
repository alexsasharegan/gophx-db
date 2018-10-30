package store

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"testing"
)

func TestCmdSplit(t *testing.T) {
	tt := []struct {
		data  []byte
		cmd   CommandType
		key   []byte
		value []byte
	}{
		{nil, EMPTY, nil, nil},
		{[]byte(""), EMPTY, nil, nil},
		{[]byte("GET"), GET, nil, nil},
		{[]byte("get foo"), ERR, []byte("foo"), nil},
		{[]byte("DEL foo"), DEL, []byte("foo"), nil},
		{[]byte("GET foo"), GET, []byte("foo"), nil},
		{[]byte("SET foo bar"), SET, []byte("foo"), []byte("bar")},
		{[]byte("SET foo bar with spaces"), SET, []byte("foo"), []byte("bar with spaces")},

		{[]byte("BEGIN"), BEGIN, nil, nil},
		{[]byte("begin"), ERR, nil, nil},
		{[]byte("COMMIT"), COMMIT, nil, nil},
		{[]byte("commit"), ERR, nil, nil},
		{[]byte("QUIT"), QUIT, nil, nil},
		{[]byte("quit"), ERR, nil, nil},

		{[]byte("any command here"), ERR, []byte("command"), []byte("here")},
		{[]byte(" any command here"), EMPTY, []byte("any"), []byte("command here")},
		{[]byte("  any command here"), EMPTY, nil, []byte("any command here")},
		{[]byte("   any command here"), EMPTY, nil, []byte(" any command here")},
	}

	var (
		cmd CommandType

		key, value []byte
	)

	for _, tc := range tt {
		cmd, key, value = splitCmds(tc.data)
		if cmd != tc.cmd {
			t.Errorf("Expected %v, received %v\n", tc.cmd, cmd)
		}
		if !bytes.Equal(key, tc.key) {
			t.Errorf("Expected '%s', received '%s'\n", tc.key, key)
		}
		if !bytes.Equal(value, tc.value) {
			t.Errorf("Expected '%s', received '%s'\n", tc.value, value)
		}
	}
}

func TestClient(t *testing.T) {
	client, rx, done := setupClient(t)
	defer done()

	client.Write([]byte("GET foo\r\n"))
	assertEQ(t, <-rx, nil)
	client.Write([]byte("SET foo a value with spaces\r\n"))
	assertEQ(t, <-rx, []byte("OK"))
	client.Write([]byte("GET foo\r\n"))
	assertEQ(t, <-rx, []byte("a value with spaces"))
	client.Write([]byte("DEL foo\r\n"))
	assertEQ(t, <-rx, []byte("OK"))
	client.Write([]byte("GET foo\r\n"))
	assertEQ(t, <-rx, nil)

	for i := 0; i < 1000; i++ {
		client.Write([]byte(fmt.Sprintf("SET k-%d %d\r\n", i, i)))
		assertEQ(t, <-rx, []byte("OK"))
		client.Write([]byte(fmt.Sprintf("GET k-%d\r\n", i)))
		assertEQ(t, <-rx, []byte(strconv.Itoa(i)))
	}

	client.Write([]byte("BEGIN\r\n"))
	client.Write([]byte("GET test\r\n"))
	client.Write([]byte("SET test 1\r\n"))
	client.Write([]byte("GET test\r\n"))
	client.Write([]byte("DEL test\r\n"))
	client.Write([]byte("GET test\r\n"))
	client.Write([]byte("COMMIT\r\n"))
	// Test that QUIT still flushes our transaction
	client.Write([]byte("QUIT\r\n"))
	assertEQ(t, <-rx, nil)
	assertEQ(t, <-rx, []byte("OK"))
	assertEQ(t, <-rx, []byte("1"))
	assertEQ(t, <-rx, []byte("OK"))
	assertEQ(t, <-rx, nil)
}

func setupClient(t *testing.T) (net.Conn, chan []byte, func()) {
	var srv net.Conn
	ctx, cancel := context.WithCancel(context.Background())
	tx := NewTransactionQueue()
	rx := make(chan []byte, 16)
	client, srv := net.Pipe()

	go scanConn(client, rx)
	go RunDB(ctx, tx)
	go ServeClient(ctx, srv, tx)

	return client, rx, func() {
		cancel()
		client.Close()
		srv.Close()
	}
}

func TestParsing(t *testing.T) {
	client, rx, done := setupParse()
	defer done()

	// Error states --------------------------------------------------------------
	client.Write([]byte("\r\n"))
	assertEQ(t, readConn(t, client), append([]byte(ErrEmpty.Error()), '\r', '\n'))

	client.Write([]byte("COMMIT\r\n"))
	assertEQ(t, readConn(t, client), append([]byte(ErrCmd.Error()), '\r', '\n'))

	client.Write([]byte("BEGIN\r\n"))
	client.Write([]byte("COMMIT\r\n"))
	assertEQ(t, readConn(t, client), append([]byte(ErrEmpty.Error()), '\r', '\n'))

	client.Write([]byte("notacommand\r\n"))
	assertEQ(t, readConn(t, client), append([]byte(ErrCmd.Error()), '\r', '\n'))

	client.Write([]byte("DEL\r\n"))
	assertEQ(t, readConn(t, client), append([]byte(ErrArgs.Error()), '\r', '\n'))

	client.Write([]byte("GET\r\n"))
	assertEQ(t, readConn(t, client), append([]byte(ErrArgs.Error()), '\r', '\n'))

	client.Write([]byte("SET\r\n"))
	assertEQ(t, readConn(t, client), append([]byte(ErrArgs.Error()), '\r', '\n'))

	// Valid commands ------------------------------------------------------------
	client.Write([]byte("GET foo\r\n"))
	trans := <-rx
	assertCmdLen(t, trans, 1)
	assertCmd(t, trans.Commands[0], GET, []byte("foo"), nil)

	client.Write([]byte("SET foo value with spaces\r\n"))
	trans = <-rx
	assertCmdLen(t, trans, 1)
	assertCmd(t, trans.Commands[0], SET, []byte("foo"), []byte("value with spaces"))

	client.Write([]byte("BEGIN\r\n"))
	client.Write([]byte("SET foo value with spaces\r\n"))
	client.Write([]byte("GET foo\r\n"))
	client.Write([]byte("DEL foo\r\n"))
	client.Write([]byte("COMMIT\r\n"))
	trans = <-rx
	assertCmdLen(t, trans, 3)
	assertCmd(t, trans.Commands[0], SET, []byte("foo"), []byte("value with spaces"))
	assertCmd(t, trans.Commands[1], GET, []byte("foo"), nil)
	assertCmd(t, trans.Commands[2], DEL, []byte("foo"), nil)

	// Edge cases ----------------------------------------------------------------
	client.Write([]byte("GET foo and some other junk\r\n"))
	trans = <-rx
	assertCmdLen(t, trans, 1)
	assertCmd(t, trans.Commands[0], GET, []byte("foo"), nil)
}

func BenchmarkParseGet(b *testing.B) {
	client, rx, done := setupParse()
	defer done()

	message := []byte("GET foo\r\n")

	for n := 0; n < b.N; n++ {
		client.Write(message)
		<-rx
	}
}

func BenchmarkParseSet(b *testing.B) {
	client, rx, done := setupParse()
	defer done()

	message := []byte("SET foo bar with spaces\r\n")

	for n := 0; n < b.N; n++ {
		client.Write(message)
		<-rx
	}
}

func BenchmarkParseDel(b *testing.B) {
	client, rx, done := setupParse()
	defer done()

	message := []byte("DEL foo\r\n")

	for n := 0; n < b.N; n++ {
		client.Write(message)
		<-rx
	}
}

func setupParse() (net.Conn, chan Transaction, func()) {
	ctx, cancel := context.WithCancel(context.Background())
	tx := NewTransactionQueue()
	client, server := net.Pipe()

	go ServeClient(ctx, server, tx)

	return client, tx, func() {
		cancel()
		client.Close()
		server.Close()
	}
}

func BenchmarkDBGet(b *testing.B) {
	tx, done := setupDB()
	defer done()

	transaction := Transaction{
		Commands: []Command{
			Command{Type: GET, Key: []byte("abcdefghijklmnopqrstuvwxyz")},
		},
		Rx: make(chan []Command),
	}

	for n := 0; n < b.N; n++ {
		tx <- transaction
		<-transaction.Rx
	}
}

func BenchmarkDBDel(b *testing.B) {
	tx, done := setupDB()
	defer done()

	transaction := Transaction{
		Commands: []Command{
			Command{Type: DEL, Key: []byte("abcdefghijklmnopqrstuvwxyz")},
		},
		Rx: make(chan []Command),
	}

	for n := 0; n < b.N; n++ {
		tx <- transaction
		<-transaction.Rx
	}
}

func BenchmarkDBSet(b *testing.B) {
	tx, done := setupDB()
	defer done()

	transaction := Transaction{
		Commands: []Command{
			Command{Type: SET, Key: []byte("abcdefghijklmnopqrstuvwxyz"), Value: []byte("0123456789")},
		},
		Rx: make(chan []Command),
	}

	for n := 0; n < b.N; n++ {
		tx <- transaction
		<-transaction.Rx
	}
}

func BenchmarkDBMulti(b *testing.B) {
	tx, done := setupDB()
	defer done()

	transaction := Transaction{
		Commands: []Command{
			Command{Type: GET, Key: []byte("foo")},
			Command{Type: SET, Key: []byte("foo"), Value: []byte("bar")},
			Command{Type: SET, Key: []byte("a"), Value: []byte("1")},
			Command{Type: SET, Key: []byte("a"), Value: []byte("2")},
			Command{Type: SET, Key: []byte("b"), Value: []byte("2")},
			Command{Type: DEL, Key: []byte("foo")},
			Command{Type: GET, Key: []byte("foo")},
			Command{Type: GET, Key: []byte("a")},
			Command{Type: GET, Key: []byte("b")},
		},
		Rx: make(chan []Command),
	}

	for n := 0; n < b.N; n++ {
		tx <- transaction
		<-transaction.Rx
	}
}

func setupDB() (chan Transaction, func()) {
	ctx, cancel := context.WithCancel(context.Background())
	tx := NewTransactionQueue()
	go RunDB(ctx, tx)

	return tx, func() {
		cancel()
	}
}

func scanConn(rx net.Conn, c chan []byte) {
	scanner := bufio.NewScanner(rx)
	scanner.Split(ScanCRLF)

	for scanner.Scan() {
		c <- scanner.Bytes()
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, fmt.Errorf("Error during scan: %v", err))
	}
}

// byte slice for use with readConn
var buf = make([]byte, 4096)

func readConn(t *testing.T, conn net.Conn) []byte {
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatal(err)
	}

	return buf[:n]
}

func assertEQ(t *testing.T, actual, expected []byte) {
	if !bytes.Equal(actual, expected) {
		t.Errorf("Expected '%s', received: '%s'", expected, actual)
	}
}

func assertCmd(t *testing.T, c Command, ctype CommandType, key, value []byte) {
	if c.Type != ctype {
		t.Errorf("Expected %v type, received %v\n", ctype, c.Type)
	}
	if !bytes.Equal(c.Key, key) {
		t.Errorf("Expected '%s' key, received '%s'\n", key, c.Key)
	}
	if !bytes.Equal(c.Value, value) {
		t.Errorf("Expected '%s' value, received '%s'\n", value, c.Value)
	}
}

func assertCmdLen(t *testing.T, trans Transaction, length int) {
	if len(trans.Commands) != length {
		t.Fatalf(
			"Expected %d commands to be parsed, received %d\n",
			length, len(trans.Commands),
		)
	}
}
