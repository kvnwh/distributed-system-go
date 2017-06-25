package qutils

import (
	"fmt"
	"log"

	"github.com/streadway/amqp"
)

func GetChannel(url string) (*amqp.Connection, *amqp.Channel) {
	conn, err := amqp.Dial(url)
	failOnError(err, "Failed to establish connection to message broke")
	ch, err := conn.Channel()
	failOnError(err, "Failed to get channel for connection")
	return conn, ch
}

func GetQueue(name string, ch *amqp.Channel) *amqp.Queue {
	q, err := ch.QueueDeclare(
		name,
		false, // durable
		false, // autodelete
		false, // exclusive
		false, // nowait
		nil,   // args
	)
	failOnError(err, "Failed to declare queue")
	return &q
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatal("%s : %s", msg, err)
		// app will kill itself
		panic(fmt.Sprint("%s: %s", msg, err))
	}
}
