package main

import (
	"fmt"
	"os"
)

func main() {
	file, err := os.Create("wat.txt")
	if err != nil {
		panic(err)
	}
	defer file.Close()
	file.WriteString("I love you!")
	n, err := file.WriteAt([]byte("you"), 5)
	if err != nil {
		panic(err)
	}
	fmt.Println(n)
}
