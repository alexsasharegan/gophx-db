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
		go func() {
			defer conn.Close()
			tokenx := make(chan command)
			go func() {
				scanner := bufio.NewScanner(conn)
				scanner.Split(ScanCRLF)

				for scanner.Scan() {
					var c command
					for i, s := range strings.SplitN(scanner.Text(), " ", 3) {
						c[i] = s
					}
					tokenx <- c
				}
			}()

			var buffering bool
			var transaction []command
			for {
				if !buffering {
					transaction = transaction[:0]
				}

				c := <-tokenx

				if c[0] == "BEGIN" {
					buffering = true
					continue
				}

				if c[0] == "COMMIT" {
					buffering = false
				} else {
					transaction = append(transaction, c)
				}

				if !buffering {
					for _, c := range transaction {
						switch c[0] {
						case "GET":
							db.mu.RLock()
							conn.Write([]byte(db.get(c[1]) + "\r\n"))
							db.mu.RUnlock()
						case "SET":
							db.mu.Lock()
							conn.Write([]byte(db.set(c[1], c[2]) + "\r\n"))
							db.mu.Unlock()
						case "DEL":
							db.mu.Lock()
							conn.Write([]byte(db.del(c[1]) + "\r\n"))
							db.mu.Unlock()
						}
					}
				}
			}
		}()
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
