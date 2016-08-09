package main

import (
	"fmt"
	"runtime"
)

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func main() {
	c := gen(1, 2, 3, 7, 91, 32, 434, 211, 323, 2, 44, 12, 21)
	out := sq(c)
	for v := range out {
		fmt.Println(v)
	}
}

func gen(nums ...int) chan int {
	out := make(chan int)
	go func() {
		for _, v := range nums {
			out <- v
		}
		close(out)
	}()
	return out
}

func sq(in chan int) chan int {
	out := make(chan int)
	go func() {
		for v := range in {
			out <- v * v
		}
		close(out)
	}()
	return out
}
