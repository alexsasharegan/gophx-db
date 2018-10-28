package store

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os"
	"sync"
	"testing"
)

func TestCmdSplit(t *testing.T) {
	type testCase struct {
		data     string
		expected []string
	}

	tcs := []testCase{
		testCase{
			data:     "",
			expected: []string{"", "", ""},
		},
		testCase{
			data:     "GET",
			expected: []string{"GET", "", ""},
		},
		testCase{
			data:     "GET foo",
			expected: []string{"GET", "foo", ""},
		},
		testCase{
			data:     "SET foo bar",
			expected: []string{"SET", "foo", "bar"},
		},
		testCase{
			data:     "SET foo bar with spaces",
			expected: []string{"SET", "foo", "bar with spaces"},
		},
	}

	sample := make([]string, 3)
	for _, tc := range tcs {
		sample[0], sample[1], sample[2] = splitCmds(tc.data)

		for i, expected := range tc.expected {
			if expected != sample[i] {
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
	trans := NewTransChan()
	results := make(chan string, 16)

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
	assertEQ(t, <-results, "")
	client.Write([]byte("SET foo a value with spaces\r\n"))
	assertEQ(t, <-results, "OK")
	client.Write([]byte("GET foo\r\n"))
	assertEQ(t, <-results, "a value with spaces")
	client.Write([]byte("DEL foo\r\n"))
	assertEQ(t, <-results, "OK")
	client.Write([]byte("GET foo\r\n"))
	assertEQ(t, <-results, "")

	client.Write([]byte("BEGIN\r\n"))
	client.Write([]byte("GET test\r\n"))
	client.Write([]byte("SET test 1\r\n"))
	client.Write([]byte("GET test\r\n"))
	client.Write([]byte("DEL test\r\n"))
	client.Write([]byte("GET test\r\n"))
	client.Write([]byte("COMMIT\r\n"))
	assertEQ(t, <-results, "")
	assertEQ(t, <-results, "OK")
	assertEQ(t, <-results, "1")
	assertEQ(t, <-results, "OK")
	assertEQ(t, <-results, "")

	client.Write([]byte("QUIT\r\n"))
}

func TestParsing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	transc := NewTransChan()
	defer close(transc)
	txA, txB := net.Pipe()
	defer txA.Close()
	defer txB.Close()

	go ServeClient(ctx, txB, transc)

	txA.Write([]byte("GET foo\r\n"))
	trans := <-transc
	assertCmdLen(t, trans, 1)
	assertCmd(t, trans.Commands[0], GET, "foo", "")

	txA.Write([]byte("SET foo value with spaces\r\n"))
	trans = <-transc
	assertCmdLen(t, trans, 1)
	assertCmd(t, trans.Commands[0], SET, "foo", "value with spaces")

	// Error states
	txA.Write([]byte("\r\n"))
	b := make([]byte, 256)
	n, err := txA.Read(b)
	if err != nil {
		t.Fatal(err)
	}
	assertEQ(t, string(b[:n]), ErrEmpty.Error()+"\r\n")

	txA.Write([]byte("COMMIT\r\n"))
	n, err = txA.Read(b)
	if err != nil {
		t.Fatal(err)
	}
	assertEQ(t, string(b[:n]), ErrCmd.Error()+"\r\n")

	txA.Write([]byte("BEGIN\r\n"))
	txA.Write([]byte("COMMIT\r\n"))
	n, err = txA.Read(b)
	if err != nil {
		t.Fatal(err)
	}
	assertEQ(t, string(b[:n]), ErrEmpty.Error()+"\r\n")

	txA.Write([]byte("notacommand\r\n"))
	n, err = txA.Read(b)
	if err != nil {
		t.Fatal(err)
	}
	assertEQ(t, string(b[:n]), ErrUnknown.Error()+"\r\n")

	txA.Write([]byte("DEL\r\n"))
	n, err = txA.Read(b)
	if err != nil {
		t.Fatal(err)
	}
	assertEQ(t, string(b[:n]), ErrArgs.Error()+"\r\n")

	txA.Write([]byte("GET\r\n"))
	n, err = txA.Read(b)
	if err != nil {
		t.Fatal(err)
	}
	assertEQ(t, string(b[:n]), ErrArgs.Error()+"\r\n")

	txA.Write([]byte("SET\r\n"))
	n, err = txA.Read(b)
	if err != nil {
		t.Fatal(err)
	}
	assertEQ(t, string(b[:n]), ErrArgs.Error()+"\r\n")
	// End error states

	txA.Write([]byte("BEGIN\r\n"))
	txA.Write([]byte("SET foo value with spaces\r\n"))
	txA.Write([]byte("GET foo\r\n"))
	txA.Write([]byte("DEL foo\r\n"))
	txA.Write([]byte("COMMIT\r\n"))
	trans = <-transc
	assertCmdLen(t, trans, 3)
	assertCmd(t, trans.Commands[2], DEL, "foo", "")
}

func assertCmd(t *testing.T, c Command, ctype CommandType, key, value string) {
	if c.Type != ctype {
		t.Errorf("Expected %v type, received %v\n", ctype, c.Type)
	}
	if c.Key != key {
		t.Errorf("Expected '%s' key, received '%s'\n", key, c.Key)
	}
	if c.Value != value {
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

func BenchmarkParsing(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	transc := NewTransChan()
	defer close(transc)
	txA, txB := net.Pipe()
	defer txA.Close()
	defer txB.Close()

	go ServeClient(ctx, txB, transc)

	messages := [][]byte{
		[]byte("GET foo\r\n"),
		[]byte("SET foo bar with spaces\r\n"),
		[]byte("DEL foo\r\n"),
	}

	for n := 0; n < b.N; n++ {
		txA.Write(messages[0])
		<-transc
		txA.Write(messages[1])
		<-transc
		txA.Write(messages[2])
		<-transc
	}
}

func BenchmarkDB(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	trans := NewTransChan()
	go RunDB(ctx, trans)
	var wg sync.WaitGroup
	var t Transaction

	done := func(cmds []Command) {
		wg.Done()
	}
	t = Transaction{
		Commands: []Command{
			Command{Type: GET, Key: "foo"},
			Command{Type: SET, Key: "foo", Value: "bar"},
			Command{Type: SET, Key: "a", Value: "1"},
			Command{Type: SET, Key: "a", Value: "2"},
			Command{Type: SET, Key: "b", Value: "2"},
			Command{Type: DEL, Key: "foo"},
			Command{Type: GET, Key: "foo"},
			Command{Type: GET, Key: "a"},
			Command{Type: GET, Key: "b"},
		},
		Done: done,
	}

	for n := 0; n < b.N; n++ {
		// WaitGroup here to ensure the result is fully processed
		wg.Add(1)
		trans <- t
		wg.Wait()
	}

	cancel()
}

func scanConn(rx net.Conn, c chan string) {
	scanner := bufio.NewScanner(rx)
	scanner.Split(ScanCRLF)

	for scanner.Scan() {
		c <- scanner.Text()
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, fmt.Errorf("Error during scan: %v", err))
	}
}

func assertEQ(t *testing.T, actual, expected string) {
	if actual != expected {
		t.Errorf("Expected '%s', received: '%s'", expected, actual)
	}
}
