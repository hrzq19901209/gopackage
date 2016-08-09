package main

import (
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

var wg sync.WaitGroup
var counter int64

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}
func main() {
	wg.Add(2)
	go incrementer("foo")
	go incrementer("bar")
	wg.Wait()
	fmt.Println("counter:", counter)
}

func incrementer(s string) {
	for i := 0; i < 10; i++ {
		time.Sleep(time.Duration(rand.Intn(3)) * time.Millisecond)
		atomic.AddInt64(&counter, 1)
		fmt.Println(s, counter)
	}
	wg.Done()
}
