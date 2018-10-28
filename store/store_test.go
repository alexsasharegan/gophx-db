package store

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"testing"
)

func TestCmdSplit(t *testing.T) {
	tt := []struct {
		data     string
		expected []string
	}{
		{"", []string{"", "", ""}},
		{"GET", []string{"GET", "", ""}},
		{"GET foo", []string{"GET", "foo", ""}},
		{"SET foo bar", []string{"SET", "foo", "bar"}},
		{"SET foo bar with spaces", []string{"SET", "foo", "bar with spaces"}},
		{"any command here", []string{"any", "command", "here"}},
		{" any command here", []string{"", "any", "command here"}},
		{"  any command here", []string{"", "", "any command here"}},
		{"   any command here", []string{"", "", " any command here"}},
	}

	sample := make([][]byte, 3)
	for _, tc := range tt {
		sample[0], sample[1], sample[2] = splitCmds([]byte(tc.data))
		for i, expected := range tc.expected {
			if expected != string(sample[i]) {
				t.Errorf("Expected '%s', received '%s'\n", expected, sample[i])
			}
		}
	}
}

func TestClient(t *testing.T) {
	var (
		conn net.Conn
		wg   sync.WaitGroup
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	trans := NewTransactionQueue()
	results := make(chan []byte, 16)

	srv, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Error(err)
	}
	defer srv.Close()

	wg.Add(1)
	go func() {
		conn, err = srv.Accept()
		wg.Done()
		if err != nil {
			t.Error(err)
		}
	}()

	client, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Error(err)
	}
	defer client.Close()

	wg.Wait()
	defer conn.Close()
	go scanConn(client, results)
	go RunDB(ctx, trans)
	go ServeClient(ctx, conn, trans)

	client.Write([]byte("GET foo\r\n"))
	assertEQ(t, <-results, nil)
	client.Write([]byte("SET foo a value with spaces\r\n"))
	assertEQ(t, <-results, []byte("OK"))
	client.Write([]byte("GET foo\r\n"))
	assertEQ(t, <-results, []byte("a value with spaces"))
	client.Write([]byte("DEL foo\r\n"))
	assertEQ(t, <-results, []byte("OK"))
	client.Write([]byte("GET foo\r\n"))
	assertEQ(t, <-results, nil)

	client.Write([]byte("BEGIN\r\n"))
	client.Write([]byte("GET test\r\n"))
	client.Write([]byte("SET test 1\r\n"))
	client.Write([]byte("GET test\r\n"))
	client.Write([]byte("DEL test\r\n"))
	client.Write([]byte("GET test\r\n"))
	client.Write([]byte("COMMIT\r\n"))
	assertEQ(t, <-results, nil)
	assertEQ(t, <-results, []byte("OK"))
	assertEQ(t, <-results, []byte("1"))
	assertEQ(t, <-results, []byte("OK"))
	assertEQ(t, <-results, nil)

	for i := 0; i < 1000; i++ {
		client.Write([]byte(fmt.Sprintf("SET k-%d %d\r\n", i, i)))
		assertEQ(t, <-results, []byte("OK"))
		client.Write([]byte(fmt.Sprintf("GET k-%d\r\n", i)))
		assertEQ(t, <-results, []byte(strconv.Itoa(i)))
	}

	client.Write([]byte("QUIT\r\n"))
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
	assertEQ(t, readConn(t, client), append([]byte(ErrUnknown.Error()), '\r', '\n'))

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
		close(tx)
		client.Close()
		server.Close()
	}
}

func BenchmarkDBGet(b *testing.B) {
	tx, done := setupDB()
	defer done()

	var wg sync.WaitGroup
	transaction := Transaction{
		Commands: []Command{
			Command{Type: GET, Key: []byte("foo")},
		},
		Done: func(cmds []Command) {
			wg.Done()
		},
	}

	for n := 0; n < b.N; n++ {
		wg.Add(1)
		tx <- transaction
		wg.Wait()
	}
}

func BenchmarkDBDel(b *testing.B) {
	tx, done := setupDB()
	defer done()

	var wg sync.WaitGroup
	transaction := Transaction{
		Commands: []Command{
			Command{Type: DEL, Key: []byte("foo")},
		},
		Done: func(cmds []Command) {
			wg.Done()
		},
	}

	for n := 0; n < b.N; n++ {
		wg.Add(1)
		tx <- transaction
		wg.Wait()
	}
}

func BenchmarkDBSet(b *testing.B) {
	tx, done := setupDB()
	defer done()

	var wg sync.WaitGroup
	transaction := Transaction{
		Commands: []Command{
			Command{Type: SET, Key: []byte("foo"), Value: []byte("bar")},
		},
		Done: func(cmds []Command) {
			wg.Done()
		},
	}

	for n := 0; n < b.N; n++ {
		wg.Add(1)
		tx <- transaction
		wg.Wait()
	}
}

func BenchmarkDBMulti(b *testing.B) {
	tx, done := setupDB()
	defer done()

	var wg sync.WaitGroup
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
		Done: func(cmds []Command) {
			wg.Done()
		},
	}

	for n := 0; n < b.N; n++ {
		// WaitGroup here to ensure the result is fully processed
		wg.Add(1)
		tx <- transaction
		wg.Wait()
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
