package main

import (
	"fmt"
	"io"
	"strings"
)

func ReadFrom(reader io.Reader, num int) ([]byte, error) {
	buf := make([]byte, num)
	n, err := reader.Read(buf)
	if n > 0 {
		return buf[:n], nil
	}
	return buf, err
}

func main() {
	data, err := ReadFrom(strings.NewReader("hello world"), 12)
	if err != nil {
		panic(err)
	}
	s := string(data)
	fmt.Println(s)
}
