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

	for n := 0; n < b.N; n++ {
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
