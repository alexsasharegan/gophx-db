package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"
)

var crlf = []byte{'\r', '\n'}

func main() {
	var wg sync.WaitGroup
	start := time.Now()

	for i := 0; i < 10000; i++ {
		wg.Add(1)
		go func(count int) {
			conn, err := net.Dial("tcp", ":8888")
			if err != nil {
				log.Fatalln(
					fmt.Sprintf("Failed to dial server on port 8888: %v", err),
				)
			}

			var wgInner sync.WaitGroup
			ctx, stop := context.WithCancel(context.Background())
			sendx, recvx := make(chan []byte), make(chan []byte)

			go func() {
				var j int
				var isTrans, isQuit bool
				for {
					select {
					case <-ctx.Done():
						return
					case b := <-sendx:
						isTrans = bytes.Contains(b, []byte("BEGIN")) || bytes.Contains(b, []byte("COMMIT"))
						isQuit = bytes.Contains(b, []byte("QUIT"))

						if !isTrans && !isQuit {
							wgInner.Add(1)
						}

						j = bytes.IndexByte(b, '\r')
						if j == -1 {
							j = len(b)
						}

						conn.Write(b)
					}
				}
			}()

			go func() {
				scanner := bufio.NewScanner(conn)
				scanner.Split(scanCRLF)

				for scanner.Scan() {
					b := scanner.Bytes()
					recvx <- b
				}

				if err := scanner.Err(); err != nil {
					fmt.Fprintln(os.Stderr, fmt.Errorf("Error during scan: %v", err))
				}

				stop()
			}()

			go func() {
				for {
					select {
					case <-ctx.Done():
						return
					case <-recvx:
						wgInner.Done()
					}
				}
			}()

			sendx <- []byte("GET foo\r\n")
			sendx <- []byte("BEGIN\r\n")
			sendx <- []byte("GET foo\r\n")
			sendx <- []byte("SET foo bar\r\n")
			sendx <- []byte("GET foo\r\n")
			sendx <- []byte("DEL foo\r\n")
			sendx <- []byte("COMMIT\r\n")
			sendx <- []byte("SET foo bar baz\r\n")
			sendx <- []byte(fmt.Sprintf("SET lorem%d Lorem ipsum dolor, sit amet consectetur adipisicing elit. Saepe possimus tempora culpa accusamus aliquid ut dolorum reiciendis ducimus doloremque quasi est ipsam, similique cupiditate nam corrupti incidunt rerum reprehenderit beatae!Lorem ipsum dolor, sit amet consectetur adipisicing elit. Saepe possimus tempora culpa accusamus aliquid ut dolorum reiciendis ducimus doloremque quasi est ipsam, similique cupiditate nam corrupti incidunt rerum reprehenderit beatae!Lorem ipsum dolor, sit amet consectetur adipisicing elit. Saepe possimus tempora culpa accusamus aliquid ut dolorum reiciendis ducimus doloremque quasi est ipsam, similique cupiditate nam corrupti incidunt rerum reprehenderit beatae!\r\n", count))
			sendx <- []byte("QUIT\r\n")
			wgInner.Wait()
			wg.Done()
		}(i)
		wg.Wait()
	}

	total := time.Now().Sub(start)
	fmt.Println(
		fmt.Sprintf("Took %s", total),
	)
}

// Adapted from bufio/scan.go
func scanCRLF(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	if i := bytes.Index(data, crlf); i >= 0 {
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
