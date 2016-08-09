package main

import (
	"fmt"
	"sync"
)

func main() {
	var wg sync.WaitGroup
	wg.Add(2)
	c := make(chan int)
	go func() {
		for i := 1; i < 10; i++ {
			c <- i
		}
		wg.Done()
	}()

	go func() {
		for i := 1; i < 10; i++ {
			c <- i
		}
		wg.Done()
	}()

	go func() {
		wg.Wait()
		close(c)
	}()

	for n := range c {
		fmt.Println(n)
	}
}
