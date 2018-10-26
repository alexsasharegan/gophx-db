package store

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
)

// Command codes
const (
	GET = 1
	SET = 2
	DEL = 3
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
	ErrCmd     = errors.New("ErrCmd Invalid command received")
	ErrConTerm = errors.New("ErrConTerm Server is terminating the connection")
)

// Command is a uniform container for commands.
type Command struct {
	Type   uint
	Key    string
	Value  string
	Result string
}

// DoneFunc is a callback for the command set with results applied.
type DoneFunc func(commands []Command)

// Transaction contains the CommandSet and a DoneFunc needed for running commands.
type Transaction struct {
	Commands []Command
	Done     DoneFunc
}

// KeyValue is a key-value store
type KeyValue struct {
	cache map[string]string
}

// Get returns the string at the given key, or an empty string.
func (s *KeyValue) Get(k string) string {
	return s.cache[k]
}

// Set writes the value at the given key.
func (s *KeyValue) Set(k, v string) error {
	s.cache[k] = v
	return nil
}

// Del deletes the given key from the store.
func (s *KeyValue) Del(k string) error {
	delete(s.cache, k)
	return nil
}

// NewTransChan returns a buffered channel of Transaction
func NewTransChan() chan Transaction {
	return make(chan Transaction, 1024)
}

// ServeClient listening for commands.
func ServeClient(ctx context.Context, conn net.Conn, trans chan<- Transaction) {
	var (
		t      Transaction
		wg     sync.WaitGroup
		argLen int

		closed, buffering bool
	)

	scanner := bufio.NewScanner(conn)
	scanner.Split(scanCRLF)

	send := func(s string) (int, error) {
		fmt.Println("Sending '" + s + "' to client")
		return conn.Write(append([]byte(s), '\r', '\n'))
	}
	fail := func(err error) {
		buffering = false
		send(err.Error())
	}
	respond := func(commands []Command) {
		for _, cmd := range commands {
			send(cmd.Result)
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

Loop:
	for {
		select {
		case <-ctx.Done():
			break Loop
		case s := <-tknc:
			args := strings.Split(s, " ")
			argLen = len(args)
			if argLen == 0 || (argLen == 1 && args[0] == "") {
				fail(ErrEmpty)
				continue
			}
			if !buffering {
				t = Transaction{
					Commands: []Command{},
					Done:     respond,
				}
			}
			switch args[0] {
			default:
				fail(ErrUnknown)
				continue
			case "BEGIN":
				buffering = true
			case "COMMIT":
				if !buffering {
					fail(ErrCmd)
					continue
				}
				if len(t.Commands) == 0 {
					fail(ErrEmpty)
					continue
				}
				buffering = false
			case "DEL":
				if argLen < 2 {
					fail(ErrArgs)
					continue
				}
				t.Commands = append(t.Commands, Command{
					Type: DEL,
					Key:  args[1],
				})
			case "GET":
				if argLen < 2 {
					fail(ErrArgs)
					continue
				}
				t.Commands = append(t.Commands, Command{
					Type: GET,
					Key:  args[1],
				})
			case "SET":
				if argLen < 3 {
					fail(ErrArgs)
					continue
				}
				t.Commands = append(t.Commands, Command{
					Type:  SET,
					Key:   args[1],
					Value: strings.Join(args[2:], " "),
				})
			case "QUIT":
				wg.Wait()
				closed = true
				conn.Close()
				return
			}
			if !buffering {
				wg.Add(1)
				trans <- t
			}
		}
	}

	closed = true
	conn.Close()
}

// RunDB listens for transactions or a done signal from context.
func RunDB(ctx context.Context, trans <-chan Transaction) {
	store := &KeyValue{make(map[string]string)}
	for {
		select {
		case <-ctx.Done():
			return
		case t := <-trans:
			for i, cmd := range t.Commands {
				switch cmd.Type {
				case GET:
					cmd.Result = store.Get(cmd.Key)
				case SET:
					store.Set(cmd.Key, cmd.Value)
					cmd.Result = "OK"
				case DEL:
					store.Del(cmd.Key)
					cmd.Result = "OK"
				}
				// Value semantics require us to reassign this to mutate the slice.
				t.Commands[i] = cmd
			}
			t.Done(t.Commands)
		}
	}
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
