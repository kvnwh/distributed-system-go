package main

import (
	"fmt"

	"github.com/kvnwh/distributed-system-go/distributed/coordinator"
)

func main() {
	ql := coordinator.NewQueueListener()
	go ql.ListenForNewSource()

	var a string
	fmt.Scanln(&a)
}
