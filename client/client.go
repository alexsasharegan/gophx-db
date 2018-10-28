package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alexsasharegan/gophx-db/store"
)

var crlf = []byte{'\r', '\n'}

type testClient struct {
	conn  net.Conn
	rchan chan string
}

func main() {
	numCPU := runtime.NumCPU()
	clients := make([]*testClient, numCPU)

	var wg sync.WaitGroup
	wg.Add(numCPU)
	var totalMessages int64

	for i := 0; i < numCPU; i++ {
		clients[i] = &testClient{
			conn:  connectDB(),
			rchan: make(chan string),
		}
	}

	for i, tc := range clients {
		go func(i int, tc *testClient) {
			defer tc.conn.Close()
			defer close(tc.rchan)
			defer wg.Done()

			go scanLines(tc.conn, tc.rchan)
			var count int64

			// 10 messages 10,000 times.
			// Messages are sent and awaited like a client would do to a DB.
			// Transactions are flushed, then each result is awaited.
			for j := 0; j < 10000; j++ {
				tc.conn.Write([]byte(fmt.Sprintf("GET k%d\r\n", i)))
				<-tc.rchan
				count++

				tc.conn.Write([]byte(fmt.Sprintf("SET k%d golang-phoenix\r\n", i)))
				<-tc.rchan
				count++

				tc.conn.Write([]byte("BEGIN\r\n"))
				tc.conn.Write([]byte(fmt.Sprintf("SET %d-%d %d-%d\r\n", i, j, i, j)))
				tc.conn.Write([]byte(fmt.Sprintf("GET %d-%d\r\n", i, j)))
				tc.conn.Write([]byte(fmt.Sprintf("DEL %d-%d\r\n", i, j)))
				tc.conn.Write([]byte("COMMIT\r\n"))
				<-tc.rchan
				count++
				<-tc.rchan
				count++
				<-tc.rchan
				count++

				tc.conn.Write([]byte(fmt.Sprintf("GET k%d\r\n", i)))
				<-tc.rchan
				count++

				tc.conn.Write([]byte(fmt.Sprintf("DEL k%d\r\n", i)))
				<-tc.rchan
				count++

				tc.conn.Write([]byte("BEGIN\r\n"))
				tc.conn.Write([]byte("GET shared-key\r\n"))
				tc.conn.Write([]byte(fmt.Sprintf("SET shared-key %d-%d\r\n", i, j)))
				tc.conn.Write([]byte("GET shared-key\r\n"))
				tc.conn.Write([]byte("COMMIT\r\n"))
				<-tc.rchan
				count++
				<-tc.rchan
				count++
				<-tc.rchan
				count++
			}

			tc.conn.Write([]byte("QUIT\r\n"))
			atomic.AddInt64(&totalMessages, count)
		}(i+1, tc)
	}

	start := time.Now()
	wg.Wait()
	totalTime := time.Now().Sub(start)

	fmt.Println(
		fmt.Sprintf(
			"%d clients took %s to send %d messages each (%d total)",
			numCPU, totalTime, totalMessages/int64(numCPU), totalMessages,
		),
	)
}

func scanLines(conn net.Conn, linec chan<- string) {
	scanner := bufio.NewScanner(conn)
	scanner.Split(store.ScanCRLF)

	for scanner.Scan() {
		linec <- scanner.Text()
	}
}

func connectDB() net.Conn {
	conn, err := net.Dial("tcp", ":8888")
	if err != nil {
		log.Fatal(err)
	}

	return conn
}
