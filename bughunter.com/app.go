package main

import (
	"fmt"
)

type Hello interface {
	Hello(msg string)
}

type People struct {
	Name string
}

func (p *People) Hello(msg string) {
	fmt.Println(msg, p.Name)
}

type Army struct {
	People
}

func main() {
	p := People{
		Name: "xiaoming",
	}

	a := Army{
		People: p,
	}
	a.Hello("nihao")
}
