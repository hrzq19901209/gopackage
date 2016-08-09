package main

import (
	"fmt"
	_ "runtime"
	"sync"
	"time"
)

var wg sync.WaitGroup

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func foo() {
	for i := 0; i < 50; i++ {
		fmt.Println("foo:", i)
		time.Sleep(3 * time.Second)
	}
	wg.Done()
}
func bar() {
	for i := 0; i < 50; i++ {
		fmt.Println("bar:", i)
		time.Sleep(3 * time.Second)
	}
	wg.Done()
}

func main() {
	wg.Add(2)
	go foo()
	go bar()
	wg.Wait()
}
