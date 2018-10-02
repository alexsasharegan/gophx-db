package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

/*------------------------------------------------------------------------------
Requirements
============
* Listen for TCP connections on :8888
* Handle Commands (terminated by CRLF):
  - GET
  - SET
  - DEL
  - QUIT
* Support Transactions (terminated by CRLF):
  - BEGIN
  - COMMIT
------------------------------------------------------------------------------*/

// Command codes
const (
	GET    = "GET"
	SET    = "SET"
	DEL    = "DEL"
	QUIT   = "QUIT"
	BEGIN  = "BEGIN"
	COMMIT = "COMMIT"
	OK     = "OK"
)

var (
	// CRLF Carriage Return Line Feed
	CRLF = []byte{'\r', '\n'}
)

// Error types for parsing commands
var (
	ErrEmpty   = errors.New("ErrEmpty Empty command")
	ErrArgs    = errors.New("ErrArgs Command received with incorrect number of arguments")
	ErrUnknown = errors.New("ErrUnknown Unknown command string received")
	ErrConTerm = errors.New("ErrConTerm Server is terminating the connection")
)

// CommandSet is a slice of commands to be executed together.
type CommandSet [][]string

// DoneFunc receives the results from a command set.
type DoneFunc func(results []string)

// Transaction contains the CommandSet and a DoneFunc needed for running commands.
type Transaction struct {
	Commands CommandSet
	Done     DoneFunc
}

// StartSession listening for commands.
func StartSession(ctx context.Context, conn net.Conn, trans chan<- Transaction) {
	var (
		t      Transaction
		wg     sync.WaitGroup
		argLen int
	)

	closed := false
	bufferingTransaction := false
	scanner := bufio.NewScanner(conn)
	scanner.Split(scanCRLF)

	send := func(s string) (int, error) {
		return conn.Write(append([]byte(s), CRLF...))
	}

	fail := func(err error) {
		bufferingTransaction = false
		send(err.Error())
	}

	respond := func(results []string) {
		for _, r := range results {
			send(r)
		}
		wg.Done()
	}

	tknc := make(chan string)

	go func(sendc chan<- string) {
		for scanner.Scan() {
			sendc <- scanner.Text()
		}

		if err := scanner.Err(); err != nil && ctx.Err() != context.Canceled && !closed {
			log.Println("Scan err:")
			fmt.Println(err)
		}
	}(tknc)

L:
	for {
		select {
		case <-ctx.Done():
			break L

		case s := <-tknc:
			args := strings.Split(s, " ")
			argLen = len(args)
			if argLen == 0 || (argLen == 1 && args[0] == "") {
				fail(ErrEmpty)
				continue
			}

			if !bufferingTransaction {
				t = Transaction{
					Commands: CommandSet{},
					Done:     respond,
				}
			}

			switch args[0] {
			case BEGIN:
				bufferingTransaction = true

			case COMMIT:
				bufferingTransaction = false

			case DEL, GET:
				if argLen < 2 {
					fail(ErrArgs)
					continue
				}

				t.Commands = append(t.Commands, args[0:2])

			case SET:
				if argLen < 3 {
					fail(ErrArgs)
					continue
				}

				t.Commands = append(
					t.Commands,
					[]string{
						args[0],
						args[1],
						strings.Join(args[2:], " "),
					},
				)

			case QUIT:
				wg.Wait()
				closed = true
				conn.Close()
				return

			default:
				fail(ErrUnknown)
				continue
			}

			if !bufferingTransaction {
				wg.Add(1)
				trans <- t
			}
		}
	}

	closed = true
	conn.Close()
}

// Adapted from bufio/scan.go
func scanCRLF(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	if i := bytes.Index(data, CRLF); i >= 0 {
		// We have a full newline-terminated line.
		return i + 2, data[0:i], nil
	}
	// If we're at EOF, we have a final, non-terminated line. Return it.
	if atEOF {
		return len(data), data, nil
	}
	// Request more data.
	return 0, nil, nil
}

// KVStore is a key-value store
type KVStore struct {
	cache map[string]string
}

// NewStore returns an initialized key value store.
func NewStore() *KVStore {
	return &KVStore{
		cache: make(map[string]string),
	}
}

// Get returns the string at the given key, or an empty string.
func (s *KVStore) Get(k string) string {
	return s.cache[k]
}

// Set writes the value at the given key.
func (s *KVStore) Set(k, v string) error {
	s.cache[k] = v
	return nil
}

// Del deletes the given key from the store.
func (s *KVStore) Del(k string) error {
	delete(s.cache, k)
	return nil
}

func main() {
	log.Println("PID:", os.Getpid())
	log.Println("Starting server...")
	srv, err := net.Listen("tcp", ":8888")
	if err != nil {
		log.Fatalln(fmt.Sprintf("Failed to listen on port 8888: %v", err))
	}

	var closing uint32
	store := NewStore()

	sig := make(chan os.Signal)
	conns := make(chan net.Conn)
	trans := make(chan Transaction, 1024)
	ctx, stop := context.WithCancel(context.Background())

	cleanup := func() {
		log.Println("\nShutting down server.")

		atomic.AddUint32(&closing, 1)
		// trigger connections close
		stop()
		// give it a sec...
		time.Sleep(time.Second)
		srv.Close()
	}

	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		for {
			conn, err := srv.Accept()
			if err != nil {
				if atomic.LoadUint32(&closing) != 0 {
					return
				}

				log.Printf("Error accepting connection: %v (%T)\n", err, err)
				continue
			}

			conns <- conn
		}
	}()

	go func(ctx context.Context, c <-chan Transaction) {
		for {
			select {
			case <-ctx.Done():
				return

			case t := <-c:
				results := make([]string, len(t.Commands))

				for i, command := range t.Commands {
					switch command[0] {
					case GET:
						results[i] = store.Get(command[1])

					case SET:
						err := store.Set(command[1], command[2])
						if err != nil {
							results[i] = err.Error()
						} else {
							results[i] = OK
						}

					case DEL:
						err := store.Del(command[1])
						if err != nil {
							results[i] = err.Error()
						} else {
							results[i] = OK
						}
					}
				}

				t.Done(results)
			}
		}
	}(ctx, trans)

	log.Println("Listening: http://127.0.0.1:8888")

	for {
		select {
		case conn := <-conns:
			go StartSession(ctx, conn, trans)
		case <-sig:
			cleanup()
			os.Exit(0)
		}
	}
}
