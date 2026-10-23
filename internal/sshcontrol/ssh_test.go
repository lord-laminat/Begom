package sshcontrol

import (
	"sync"
	"testing"
)

func TestConcurrentTerminalOutputBound(t *testing.T) {
	var b limitedBuffer
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			for range 100 {
				b.Write(make([]byte, 1024))
			}
		})
	}
	wg.Wait()
	if len(b.String()) != 512<<10 {
		t.Fatal("output bound violated")
	}
}
