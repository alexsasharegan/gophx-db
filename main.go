package main

import (
	"bytes"
	"fmt"
	"log"
	"sync"

	"github.com/tidwall/evio"
)

type database struct {
	data map[string][]byte
	mu   sync.RWMutex
}

func (db *database) get(k string) []byte {
	return db.data[k]
}

func (db *database) set(k string, v []byte) []byte {
	db.data[k] = v
	return []byte("OK")
}

func (db *database) del(k string) []byte {
	delete(db.data, k)
	return []byte("OK")
}

func (db *database) process(c command) []byte {
	var b []byte
	switch {
	case bytes.Equal(c[0], []byte("GET")):
		db.mu.RLock()
		b = db.get(string(c[1]))
		db.mu.RUnlock()
	case bytes.Equal(c[0], []byte("DEL")):
		db.mu.Lock()
		b = db.del(string(c[1]))
		db.mu.Unlock()
	case bytes.Equal(c[0], []byte("SET")):
		db.mu.Lock()
		b = db.set(string(c[1]), c[2])
		db.mu.Unlock()
	}
	return b
}

type command [3][]byte

type client struct {
	inTransaction bool
	transaction   []command
}

var (
	db     = &database{data: make(map[string][]byte)}
	m      = make(map[evio.Conn]*client)
	cmd    command
	events evio.Events
	buf    bytes.Buffer
)

func main() {
	events.Opened = openConnection
	events.Data = onData

	err := evio.Serve(events, "tcp://0.0.0.0:8888")
	if err != nil {
		log.Fatalln(fmt.Sprintf("Failed to listen on port 8888: %v", err))
	}
}

func openConnection(conn evio.Conn) (b []byte, opts evio.Options, action evio.Action) {
	m[conn] = new(client)
	return
}

func onData(conn evio.Conn, in []byte) ([]byte, evio.Action) {
	var action evio.Action
	client := m[conn]
	lines := bytes.Split(in, []byte{'\r', '\n'})
	buf.Reset()

	for i := 0; i < len(lines); i++ {
		if len(lines[i]) == 0 {
			continue
		}

		split := bytes.SplitN(lines[i], []byte{' '}, 3)
		for j := 0; j < len(split); j++ {
			cmd[j] = split[j]
		}

		switch {
		case bytes.Equal(cmd[0], []byte("QUIT")):
			delete(m, conn)
			action = evio.Close

		case bytes.Equal(cmd[0], []byte("BEGIN")):
			client.inTransaction = true

		case bytes.Equal(cmd[0], []byte("COMMIT")):
			for _, c := range client.transaction {
				buf.Write(db.process(c))
				buf.Write([]byte{'\r', '\n'})
			}
			client.inTransaction = false
			client.transaction = client.transaction[:0]

		case client.inTransaction:
			client.transaction = append(client.transaction, cmd)

		default:
			buf.Write(db.process(cmd))
			buf.Write([]byte{'\r', '\n'})
		}
	}

	return buf.Bytes(), action
}
