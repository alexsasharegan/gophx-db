package store

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash"
	"hash/fnv"
	"log"
	"net"
	"sync"
)

// Command codes
const (
	// Nothing received
	EMPTY CommandType = iota

	// DB operations
	GET
	SET
	DEL

	// Parsing directives
	BEGIN
	COMMIT
	QUIT

	// All other conditions
	ERR
)

var (
	// CRLF Carriage Return Line Feed
	CRLF = []byte{'\r', '\n'}
)

// Error types for parsing commands
var (
	ErrEmpty = errors.New("ErrEmpty Empty command")
	ErrArgs  = errors.New("ErrArgs Command received with incorrect number of arguments")
	ErrCmd   = errors.New("ErrCmd Invalid command received")
)

// CommandType is an enum of allowed commands.
type CommandType uint

func (ct CommandType) parse(b []byte) CommandType {
	switch length := len(b); {
	case length == 0:
		return EMPTY
	case length == 5:
		if bytes.Equal(b, []byte("BEGIN")) {
			return BEGIN
		}
		return ERR
	case length == 6:
		if bytes.Equal(b, []byte("COMMIT")) {
			return COMMIT
		}
		return ERR
	case length == 4:
		if bytes.Equal(b, []byte("QUIT")) {
			return QUIT
		}
		return ERR

	case length == 3:
		switch {
		case bytes.Equal(b, []byte("GET")):
			return GET
		case bytes.Equal(b, []byte("DEL")):
			return DEL
		case bytes.Equal(b, []byte("SET")):
			return SET
		}
	}

	return ERR
}

// Command is a uniform container for commands.
type Command struct {
	// Type is an integer representing GET, SET, or DEL
	Type CommandType
	// Key is the key for the DB operation
	Key []byte
	// Value is the value to set going IN, and the result going OUT
	Value []byte
}

// Transaction contains the CommandSet and a Result channel
// needed for running commands and receiving their results.
type Transaction struct {
	Commands []Command
	// Result is the channel by which the DB responds to commands
	Result chan []Command
}

// KeyValue is a key-value store
type KeyValue struct {
	cache map[uint64][]byte
	h     hash.Hash64
}

func (kv *KeyValue) hash(b []byte) (uint64, error) {
	kv.h.Reset()
	_, err := kv.h.Write(b)
	if err != nil {
		return 0, err
	}

	return kv.h.Sum64(), nil
}

// Get returns the string at the given key, or an empty string.
func (kv *KeyValue) Get(k []byte) []byte {
	key, err := kv.hash(k)
	if err != nil {
		return nil
	}

	return kv.cache[key]
}

// Set writes the value at the given key.
func (kv *KeyValue) Set(k []byte, v []byte) error {
	key, err := kv.hash(k)
	if err != nil {
		return err
	}

	kv.cache[key] = v
	return nil
}

// Del deletes the given key from the store.
func (kv *KeyValue) Del(k []byte) error {
	key, err := kv.hash(k)
	if err != nil {
		return err
	}

	delete(kv.cache, key)
	return nil
}

// NewTransactionQueue returns a buffered channel of Transaction
func NewTransactionQueue() chan Transaction {
	return make(chan Transaction, 512)
}

// ServeClient listens for commands and responds with results.
func ServeClient(ctx context.Context, conn net.Conn, trans chan<- Transaction) {
	var (
		t   Transaction
		wg  sync.WaitGroup
		cmd CommandType

		key, val          []byte
		closed, buffering bool

		// Start with a small, but decently sized slice to minimize resize allocs.
		cmds = make([]Command, 0, 8)
		// Use a buffered channel to ensure the DB does not block on channel send.
		results = make(chan []Command, 4)
	)

	send := func(b []byte) (int, error) {
		return conn.Write(append(b, '\r', '\n'))
	}
	fail := func(err error) {
		buffering = false
		send([]byte(err.Error()))
	}

	tknc := make(chan []byte)
	scanner := bufio.NewScanner(conn)
	scanner.Split(ScanCRLF)
	go func() {
		for scanner.Scan() {
			tknc <- scanner.Bytes()
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
		case commands := <-results:
			for _, c := range commands {
				if c.Value == nil && c.Type != GET {
					send([]byte("OK"))
				} else {
					send(c.Value)
				}
			}
			wg.Done()
		case b := <-tknc:
			cmd, key, val = splitCmds(b)
			if cmd == EMPTY {
				fail(ErrEmpty)
				continue
			}
			if cmd == ERR {
				fail(ErrCmd)
				continue
			}
			if !buffering {
				t = Transaction{
					Commands: cmds[:0], // resuse the slice by zeroing it
					Result:   results,
				}
			}

			switch cmd {
			case BEGIN:
				buffering = true
			case COMMIT:
				if !buffering {
					fail(ErrCmd)
					continue
				}
				if len(t.Commands) == 0 {
					fail(ErrEmpty)
					continue
				}
				buffering = false
			case DEL:
				if bytes.Equal(key, nil) {
					fail(ErrArgs)
					continue
				}
				t.Commands = append(t.Commands, Command{
					Type: DEL,
					Key:  key,
				})
			case GET:
				if bytes.Equal(key, nil) {
					fail(ErrArgs)
					continue
				}
				t.Commands = append(t.Commands, Command{
					Type: GET,
					Key:  key,
				})
			case SET:
				if bytes.Equal(key, nil) {
					fail(ErrArgs)
					continue
				}
				t.Commands = append(t.Commands, Command{
					Type:  SET,
					Key:   key,
					Value: val,
				})
			case QUIT:
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
	store := &KeyValue{
		cache: make(map[uint64][]byte),
		h:     fnv.New64a(),
	}

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
					if err := store.Set(cmd.Key, cmd.Value); err != nil {
						cmd.Value = []byte(err.Error())
					} else {
						cmd.Value = nil
					}
				case DEL:
					if err := store.Del(cmd.Key); err != nil {
						cmd.Value = []byte(err.Error())
					} else {
						cmd.Value = nil
					}
				}
				t.Commands[i] = cmd
			}
			t.Result <- t.Commands
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
func splitCmds(b []byte) (cmd CommandType, key, val []byte) {
	if len(b) == 0 {
		return
	}

	m := bytes.IndexByte(b, ' ')
	if m < 0 {
		return cmd.parse(b), key, val
	}
	cmd = cmd.parse(b[:m])
	b = b[m+1:]

	m = bytes.IndexByte(b, ' ')
	if m < 0 {
		return cmd, b, val
	}

	return cmd, b[:m], b[m+1:]
}
