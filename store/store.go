package store

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"log"
	"net"
	"runtime"
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
	CRLF    = []byte{'\r', '\n'}
	bufSize = runtime.NumCPU()
)

// Error types for parsing commands
var (
	ErrEmpty = errors.New("ErrEmpty Empty command")
	ErrArgs  = errors.New("ErrArgs Command received with incorrect number of arguments")
	ErrCmd   = errors.New("ErrCmd Invalid command received")
	ErrTerm  = errors.New("ErrTerm Connection terminate by server")
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
	case length == 6:
		if bytes.Equal(b, []byte("COMMIT")) {
			return COMMIT
		}
	case length == 4:
		if bytes.Equal(b, []byte("QUIT")) {
			return QUIT
		}
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
	// Rx is the channel by which the DB responds to commands
	Rx chan []Command
}

// KeyValue is a key-value store
type KeyValue struct {
	cache map[string][]byte
}

// NewKeyValue initializes a KeyValue store.
func NewKeyValue() *KeyValue {
	return &KeyValue{
		cache: make(map[string][]byte),
	}
}

// Get returns the string at the given key, or an empty string.
func (kv *KeyValue) Get(k []byte) []byte {
	return kv.cache[string(k)]
}

// Set writes the value at the given key.
func (kv *KeyValue) Set(k []byte, v []byte) error {
	// The key slice escapes to the heap,
	// but benchmarks show negligible perf penalty.
	kv.cache[string(k)] = v
	return nil
}

// Del deletes the given key from the store.
func (kv *KeyValue) Del(k []byte) error {
	delete(kv.cache, string(k))
	return nil
}

// NewTransactionQueue returns a buffered channel of Transaction
func NewTransactionQueue() chan Transaction {
	return make(chan Transaction, bufSize)
}

// ServeClient listens for commands and responds with results.
func ServeClient(ctx context.Context, conn net.Conn, tx chan<- Transaction) {
	var (
		// Our reusable var for buffering commands.
		t Transaction
		// The command parsed from a token emit.
		cmd CommandType
		// The count of transactions awaiting results.
		tcount int
		// Parsed key/value slices.
		key, val []byte
		// A flag for ignoring scan errors
		closed bool
		// A flag for knowing when to append commands vs. flush a transaction.
		buffering bool
		// Start with a little room in the slice to minimize resize allocs.
		commands = make([]Command, 0, 8)
		// Use a buffered channel to ensure the DB does not block on channel send.
		rx = make(chan []Command, bufSize)
		// Channel of tokens our scanner will emit from the connection.
		tokenx = make(chan []byte)
	)

	defer conn.Close()

	// We handle failures in many places,
	// so use a closure to ensure we set our buffering flag in response.
	replyErr := func(err error) {
		buffering = false
		conn.Write([]byte(err.Error() + "\r\n"))
	}

	scanner := bufio.NewScanner(conn)
	scanner.Split(ScanCRLF)
	go func() {
		for scanner.Scan() {
			// When we QUIT, we set the chan=nil to block.
			if tokenx == nil {
				return
			}
			tokenx <- scanner.Bytes()
		}

		// Ignore errors when we've signaled the conn closed.
		if err := scanner.Err(); err != nil && !closed {
			log.Printf("Scan err:\n%v\n", err)
		}
	}()

	for {
		select {
		// Handle graceful shutdown
		case <-ctx.Done():
			replyErr(ErrTerm)
			closed = true
			return

		// Transaction results
		case commands := <-rx:
			for _, c := range commands {
				if c.Value == nil && c.Type != GET {
					conn.Write([]byte("OK\r\n"))
				} else {
					conn.Write(append(c.Value, '\r', '\n'))
				}
			}
			tcount--
			if closed {
				return
			}

		// Client emits a token
		case b := <-tokenx:
			cmd, key, val = splitCmds(b)
			if cmd == EMPTY {
				replyErr(ErrEmpty)
				continue
			}
			if cmd == ERR {
				replyErr(ErrCmd)
				continue
			}
			if !buffering {
				t = Transaction{
					// Zero out the slice for reuse
					Commands: commands[:0],
					Rx:       rx,
				}
			}

			switch cmd {
			case BEGIN:
				buffering = true
			case COMMIT:
				if !buffering {
					replyErr(ErrCmd)
					continue
				}
				if len(t.Commands) == 0 {
					replyErr(ErrEmpty)
					continue
				}
				buffering = false
			case DEL:
				if bytes.Equal(key, nil) {
					replyErr(ErrArgs)
					continue
				}
				t.Commands = append(t.Commands, Command{
					Type: DEL,
					Key:  key,
				})
			case GET:
				if bytes.Equal(key, nil) {
					replyErr(ErrArgs)
					continue
				}
				t.Commands = append(t.Commands, Command{
					Type: GET,
					Key:  key,
				})
			case SET:
				if bytes.Equal(key, nil) {
					replyErr(ErrArgs)
					continue
				}
				t.Commands = append(t.Commands, Command{
					Type:  SET,
					Key:   key,
					Value: val,
				})
			case QUIT:
				// Make this channel block so we stop receiving further commands.
				tokenx = nil
				closed = true
				// If we aren't waiting on other results, exit.
				if tcount == 0 {
					return
				}
			}
			// Perform a flush
			if !buffering {
				tcount++
				tx <- t
			}
		}
	}
}

// RunDB listens for transactions or a done signal from context.
func RunDB(ctx context.Context, trans <-chan Transaction) {
	store := NewKeyValue()

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
						// This escapes to the heap,
						// but we only care about the success path.
						cmd.Value = []byte(err.Error())
					} else {
						cmd.Value = nil
					}
				case DEL:
					if err := store.Del(cmd.Key); err != nil {
						// This escapes to the heap,
						// but we only care about the success path.
						cmd.Value = []byte(err.Error())
					} else {
						cmd.Value = nil
					}
				}
				t.Commands[i] = cmd
			}
			t.Rx <- t.Commands
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
