package main

import (
	"fmt"
	"strings"
)

func main() {
	reader := strings.NewReader("I love you!baby!")
	buf := make([]byte, 10)
	n, err := reader.ReadAt(buf, 2)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s, %d\n", string(buf), n)

}
