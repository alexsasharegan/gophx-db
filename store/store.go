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

// CommandType is an enum of allowed commands.
type CommandType uint

// Command codes
const (
	_ CommandType = iota
	GET
	SET
	DEL
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
	// Type is an integer representing GET, SET, or DEL
	Type CommandType
	// Key is the key for the DB operation
	Key string
	// Value is the value to set going IN, and the result going OUT
	Value string
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
	return make(chan Transaction, 512)
}

// ServeClient listening for commands.
func ServeClient(ctx context.Context, conn net.Conn, trans chan<- Transaction) {
	var (
		t  Transaction
		wg sync.WaitGroup

		cmd, key, val     string
		closed, buffering bool
	)

	send := func(s string) (int, error) {
		return conn.Write(append([]byte(s), '\r', '\n'))
	}
	fail := func(err error) {
		buffering = false
		send(err.Error())
	}
	respond := func(commands []Command) {
		for _, c := range commands {
			send(c.Value)
		}
		wg.Done()
	}

	tknc := make(chan string)
	scanner := bufio.NewScanner(conn)
	scanner.Split(ScanCRLF)
	go func() {
		for scanner.Scan() {
			tknc <- scanner.Text()
		}

		if err := scanner.Err(); err != nil && ctx.Err() != context.Canceled && !closed {
			log.Println("Scan err:")
			fmt.Println(err)
		}
	}()

Loop:
	for {
		select {
		case <-ctx.Done():
			break Loop
		case s := <-tknc:
			cmd, key, val = splitCmds(s)
			if cmd == "" {
				fail(ErrEmpty)
				continue
			}
			if !buffering {
				t = Transaction{
					Commands: []Command{},
					Done:     respond,
				}
			}

			switch cmd {
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
				if key == "" {
					fail(ErrArgs)
					continue
				}
				t.Commands = append(t.Commands, Command{
					Type: DEL,
					Key:  key,
				})
			case "GET":
				if key == "" {
					fail(ErrArgs)
					continue
				}
				t.Commands = append(t.Commands, Command{
					Type: GET,
					Key:  key,
				})
			case "SET":
				if key == "" {
					fail(ErrArgs)
					continue
				}
				t.Commands = append(t.Commands, Command{
					Type:  SET,
					Key:   key,
					Value: val,
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
					cmd.Value = store.Get(cmd.Key)
				case SET:
					store.Set(cmd.Key, cmd.Value)
					cmd.Value = "OK"
				case DEL:
					store.Del(cmd.Key)
					cmd.Value = "OK"
				}
				// Value semantics require us to reassign this to mutate the slice.
				t.Commands[i] = cmd
			}
			t.Done(t.Commands)
		}
	}
}

// ScanCRLF is adapted from bufio/scan.go
func ScanCRLF(data []byte, atEOF bool) (advance int, token []byte, err error) {
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

// Adapted from the strings package function 'genSplit'.
func splitCmds(s string) (cmd, key, val string) {
	m := strings.IndexByte(s, ' ')
	if m < 0 {
		return s, key, val
	}
	cmd = s[:m]
	s = s[m+1:]

	m = strings.IndexByte(s, ' ')
	if m < 0 {
		return cmd, s, val
	}

	return cmd, s[:m], s[m+1:]
}
