package main

import (
	"fmt"

	"github.com/kvnwh/distributed-system-go/distributed/coordinator"
)

var dc *coordinator.DataBaseConsumer

func main() {
	ea := coordinator.NewEventAggregator()

	dc = coordinator.NewDataBaseConsumer(ea)
	ql := coordinator.NewQueueListener(ea)

	go ql.ListenForNewSource()

	var a string
	fmt.Scanln(&a)
}
