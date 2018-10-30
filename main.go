package main

import (
	"bytes"
	"fmt"
	"log"

	"github.com/tidwall/evio"
)

// declaring these globally avoids alloc 🤷
var (
	ok       = []byte("OK")
	errReply = []byte("Err Unknown command")
)

type database struct {
	data map[string][]byte
}

func (db *database) process(c command) []byte {
	switch {
	case bytes.Equal(c[0], []byte("GET")):
		return db.data[string(c[1])]
	case bytes.Equal(c[0], []byte("DEL")):
		delete(db.data, string(c[1]))
		return ok
	case bytes.Equal(c[0], []byte("SET")):
		db.data[string(c[1])] = c[2]
		return ok
	}

	return errReply
}

type command [3][]byte

type clientContext struct {
	inTransaction bool
	transaction   []command
}

var (
	db  = new(database)
	buf = new(bytes.Buffer)
	cmd command
)

func main() {
	db.data = make(map[string][]byte)
	err := evio.Serve(evio.Events{
		Opened: handleOpen,
		Data:   handleData,
	}, "tcp://0.0.0.0:8888")
	if err != nil {
		log.Fatalln(fmt.Sprintf("Serve failed on port 8888: %v", err))
	}
}

func handleOpen(conn evio.Conn) (b []byte, opts evio.Options, action evio.Action) {
	conn.SetContext(new(clientContext))
	return
}

func handleData(conn evio.Conn, in []byte) ([]byte, evio.Action) {
	var action evio.Action
	client := conn.Context().(*clientContext)
	lines := bytes.Split(in, []byte{'\r', '\n'})
	buf.Reset()

ProcessLines:
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
			action = evio.Close
			break ProcessLines

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
