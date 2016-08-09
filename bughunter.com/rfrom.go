package main

import (
	"bufio"
	"os"
)

func main() {
	file, err := os.Open("wat.txt")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	writer := bufio.NewWriter(os.Stdout)
	writer.ReadFrom(file)
	writer.Flush()
}
