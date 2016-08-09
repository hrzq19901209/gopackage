package main

import (
	"fmt"
)

func main() {
	input1 := incrementer("1")
	input2 := incrementer("2")
	out := merge(input1, input2)
	count := 0
	for n := range out {
		count += n
	}
	fmt.Println(count)
}

func incrementer(s string) chan int {
	out := make(chan int)
	go func() {
		for i := 1; i <= 20; i++ {
			out <- i
		}
		close(out)
	}()
	return out
}

func merge(cs ...chan int) chan int {
	out := make(chan int)
	done := make(chan bool)
	for _, c := range cs {
		go func(in chan int) {
			for n := range in {
				out <- n
			}
			done <- true
		}(c)
	}
	go func() {
		for _ = range cs {
			<-done
		}
		close(out)
	}()
	return out
}
