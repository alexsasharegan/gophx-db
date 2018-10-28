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
	srv, err := net.Listen("tcp", ":8888")
	if err != nil {
		log.Fatalln(fmt.Sprintf("Failed to listen on port 8888: %v", err))
	}

	sigc := make(chan os.Signal)
	connc := make(chan net.Conn)
	tx := store.NewTransChan()
	ctx, cancel := context.WithCancel(context.Background())

	var closing uint32
	cleanup := func() {
		fmt.Println()
		log.Println("Shutting down server.")

		atomic.AddUint32(&closing, 1)
		// trigger connections close
		cancel()
		// give connections a sec...
		time.Sleep(time.Second)
		err := srv.Close()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
		}
	}

	signal.Notify(sigc, syscall.SIGTERM, syscall.SIGINT)

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
			connc <- conn
		}
	}()

	go store.RunDB(ctx, tx)

	log.Println("Listening: http://127.0.0.1:8888")

	for {
		select {
		case conn := <-connc:
			go store.ServeClient(ctx, conn, tx)
		case <-sigc:
			cleanup()
			// Call exit in case connections are still lingering,
			// or the signal.Notify routine doesn't exit.
			os.Exit(0)
		}
	}
}
