package main

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Println("Usage: go run progress.go $seconds")
		return
	}
	fmt.Println(os.Args)
	interval, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Println("parse seconds error", err.Error())
		return
	}
	for i := 0; i <= 100; i += 20 {
		s := fmt.Sprintf("##########%d\n", i)
		os.Stdout.WriteString(s)
		time.Sleep(time.Second * time.Duration(interval))
	}
}
