package main

import (
	"fmt"
)

func producer(s string) chan int {
	out := make(chan int)
	go func() {
		for i := 0; i < 10; i++ {
			out <- i
			fmt.Println(s, i)
		}
		close(out)
	}()
	return out
}

func caulator(in chan int) chan int {
	out := make(chan int)
	go func() {
		var sum int
		for n := range in {
			sum += n
		}
		out <- sum
		close(out)
	}()
	return out
}

func main() {
	c1 := producer("Foo")
	c2 := producer("Bar")
	c3 := caulator(c1)
	c4 := caulator(c2)

	fmt.Println("fint counter:", <-c3+<-c4)
}
