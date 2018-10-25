package store

import (
	"context"
	"sync"
	"testing"
)

func BenchmarkGet(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	trans := NewTransChan()
	go RunDB(ctx, trans)
	var wg sync.WaitGroup
	var t Transaction

	done := func(cmds []Command) {
		wg.Done()
	}

	for n := 0; n < b.N; n++ {
		t = Transaction{
			Commands: []Command{
				Command{Type: GET, Key: "foo"},
				Command{Type: SET, Key: "foo", Value: "bar"},
				Command{Type: DEL, Key: "foo"},
			},
			Done: done,
		}
		wg.Add(1)
		trans <- t
		wg.Wait()
	}

	cancel()
}
