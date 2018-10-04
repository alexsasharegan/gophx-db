package main

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

func main() {
	fmt.Println("Connecting to database on port 8888...")

	conn, err := net.Dial("tcp", ":8888")
	if err != nil {
		log.Fatalln(err)
	}

	fmt.Println("✅ Connected")

	sig := make(chan os.Signal)
	userx := make(chan string)
	dbsendx := make(chan string)
	dbrecvx := make(chan string)

	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)

	// User input stream
	go func() {
		input := bufio.NewScanner(os.Stdin)

		for input.Scan() {
			userx <- input.Text()
		}
	}()

	// DB send stream
	go func() {
		for {
			cmd := <-dbsendx

			_, err := conn.Write(append([]byte(cmd), '\r', '\n'))
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
			}
		}
	}()

	// DB reply stream
	go func() {
		replies := bufio.NewScanner(conn)
		replies.Split(scanCRLF)

		for replies.Scan() {
			dbrecvx <- replies.Text()
		}

		if err := replies.Err(); err != nil {
			fmt.Fprintln(os.Stderr, fmt.Errorf("Error during scan: %v", err))
		}
	}()

	// REPL
	inTrans := false
	count := 0
	fmt.Print("> ")
	for {
		select {
		case txt := <-userx:
			if txt == "exit" || txt == "QUIT" {
				dbsendx <- "QUIT"
				time.Sleep(100 * time.Millisecond)
				conn.Close()
				os.Exit(0)
			}

			if strings.Contains(txt, "BEGIN") {
				inTrans = true
				fmt.Print("> ")
			} else if inTrans {
				if strings.Contains(txt, "COMMIT") {
					inTrans = false
				} else {
					fmt.Print("> ")
					count++
				}
			}

			dbsendx <- txt

		case reply := <-dbrecvx:
			fmt.Println(reply)
			if count > 0 {
				count--
			}
			if count == 0 {
				fmt.Print("> ")
			}

		case <-sig:
			conn.Close()
			os.Exit(1)
		}
	}
}

// Adapted from bufio/scan.go
func scanCRLF(data []byte, atEOF bool) (advance int, token []byte, err error) {
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
