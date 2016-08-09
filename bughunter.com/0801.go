package main

import (
	"fmt"
)

func main() {
	done := make(chan bool)
	strs := []string{"a", "b", "c"}
	for _, v := range strs {
		go func(u string) {
			fmt.Println(u)
			done <- true
		}(v)
	}

	for _ = range strs {
		<-done
	}
}
