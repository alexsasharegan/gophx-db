package main

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
)

type command [3]string
type transaction struct {
	commands []command
}

type database struct {
	data map[string]string
	mu   sync.RWMutex
}

func (db *database) get(k string) string {
	return db.data[k]
}

func (db *database) set(k, v string) string {
	db.data[k] = v
	return "OK"
}

func (db *database) del(k string) string {
	delete(db.data, k)
	return "OK"
}

func (db *database) process(c command) string {
	var s string
	switch c[0] {
	case "GET":
		db.mu.RLock()
		s = db.get(c[1])
		db.mu.RUnlock()
	case "DEL":
		db.mu.Lock()
		s = db.del(c[1])
		db.mu.Unlock()
	case "SET":
		db.mu.Lock()
		s = db.set(c[1], c[2])
		db.mu.Unlock()
	}
	return s
}

func main() {
	srv, err := net.Listen("tcp", ":8888")
	if err != nil {
		log.Fatalln(fmt.Sprintf("Failed to listen on port 8888: %v", err))
	}

	db := &database{data: make(map[string]string)}

	for {
		conn, err := srv.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v (%T)\n", err, err)
			return
		}
		go serveClient(conn, db)
	}
}

func serveClient(conn net.Conn, db *database) {
	defer conn.Close()
	tokenx := make(chan command)
	go scan(conn, tokenx)

	var inTransaction bool
	transaction := new(transaction)
	for {
		c := <-tokenx

		if c[0] == "QUIT" {
			return
		}

		if c[0] == "BEGIN" {
			transaction.commands = transaction.commands[:0]
			inTransaction = true
			continue
		}

		if c[0] == "COMMIT" {
			inTransaction = false
			for _, c := range transaction.commands {
				conn.Write([]byte(db.process(c) + "\r\n"))
			}
			continue
		}

		if inTransaction {
			transaction.commands = append(transaction.commands, c)
		} else {
			conn.Write([]byte(db.process(c) + "\r\n"))
		}
	}
}

func scan(conn net.Conn, tokenx chan<- command) {
	scanner := bufio.NewScanner(conn)
	scanner.Split(ScanCRLF)

	for scanner.Scan() {
		var c command
		for i, s := range strings.SplitN(scanner.Text(), " ", 3) {
			c[i] = s
		}
		tokenx <- c
	}
}

// ScanCRLF is adapted from bufio/scan.go
func ScanCRLF(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	if i := bytes.Index(data, []byte{'\r', '\n'}); i >= 0 {
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
