package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
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
	var t Transaction

	bufferingTransaction := false
	scanner := bufio.NewScanner(conn)
	scanner.Split(scanCRLF)

	send := func(s string) (int, error) {
		return conn.Write(append([]byte(s), CRLF...))
	}

	respond := func(results []string) {
		for _, r := range results {
			send(r)
		}
	}

	expectMinArgs := func(argList []string, min int) bool {
		if len(argList) < min {
			send(fmt.Sprintf("ErrArgLength The command issued requires at least %d arguments.", min))
			return true
		}

		return false
	}

	for scanner.Scan() {
		args := strings.Split(scanner.Text(), " ")
		if len(args) == 0 {
			send("ErrNoArgs Empty command.")
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
			if expectMinArgs(args, 2) {
				continue
			}
			t.Commands = append(t.Commands, args[0:2])

		case SET:
			if expectMinArgs(args, 3) {
				continue
			}
			t.Commands = append(t.Commands, args[0:3])

		case QUIT:
			conn.Close()
			return

		default:
			send("ErrBadCommand Unknown command string received.")
			continue
		}

		if !bufferingTransaction {
			trans <- t
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("Session scanner received an error: %v\n", err)
	}

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
	store map[string]string
}

// NewStore returns an initialized key value store.
func NewStore() *KVStore {
	return &KVStore{
		store: make(map[string]string),
	}
}

// Get returns the string at the given key, or an empty string.
func (s *KVStore) Get(k string) string {
	return s.store[k]
}

// Set writes the value at the given key.
func (s *KVStore) Set(k, v string) error {
	s.store[k] = v
	return nil
}

// Del deletes the given key from the store.
func (s *KVStore) Del(k string) error {
	delete(s.store, k)
	return nil
}

func main() {
	srv, err := net.Listen("tcp", ":8888")
	if err != nil {
		log.Fatalln(fmt.Sprintf("Failed to listen on port 8888: %v", err))
	}
	defer srv.Close()

	store := NewStore()

	sig := make(chan os.Signal)
	conns := make(chan net.Conn)
	trans := make(chan Transaction, 1024)
	ctx, stop := context.WithCancel(context.Background())

	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		for {
			conn, err := srv.Accept()
			if err != nil {
				fmt.Fprintln(
					os.Stderr,
					fmt.Sprintf("Error accepting connection: %v", err),
				)
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

	for {
		select {
		case conn := <-conns:
			go StartSession(ctx, conn, trans)
		case <-sig:
			fmt.Println("\nShutting down server.")
			stop()
			os.Exit(0)
		}
	}
}
