package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/alexsasharegan/gophx-db/store"
)

func main() {
	// Handy to kill the process when you let a go routine run wild...
	// log.Println("PID:", os.Getpid())

	srv, err := net.Listen("tcp", ":8888")
	if err != nil {
		log.Fatalln(fmt.Sprintf("Failed to listen on port 8888: %v", err))
	}

	var closing uint32

	sig := make(chan os.Signal)
	conns := make(chan net.Conn)
	trans := store.NewTransChan()
	ctx, cancel := context.WithCancel(context.Background())

	cleanup := func() {
		log.Println("\nShutting down server.")

		atomic.AddUint32(&closing, 1)
		// trigger connections close
		cancel()
		// give it a sec...
		time.Sleep(time.Second)
		srv.Close()
	}

	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		for {
			conn, err := srv.Accept()
			if err != nil {
				if atomic.LoadUint32(&closing) != 0 {
					return
				}

				log.Printf("Error accepting connection: %v (%T)\n", err, err)
				continue
			}

			conns <- conn
		}
	}()

	go store.RunDB(ctx, trans)

	log.Println("Listening: http://127.0.0.1:8888")

	for {
		select {
		case conn := <-conns:
			go store.ClientSession(ctx, conn, trans)

		case <-sig:
			cleanup()
			// Call exit in case connections are still lingering,
			// or the signal.Notify routine doesn't want to quit.
			os.Exit(0)
		}
	}
}
