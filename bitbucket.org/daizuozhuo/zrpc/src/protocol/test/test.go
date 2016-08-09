package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Errorf("Usage: go run test.go input test/")
	}
	data, err := ioutil.ReadFile(os.Args[1])
	if err != nil {
		fmt.Errorf("read %s error %v", os.Args[1], err)
	}
	outFile := filepath.Join(os.Args[2], "result")
	ioutil.WriteFile(outFile, data, 0666)
}
