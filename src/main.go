package main

import (
	"fmt"

	"log"

	"github.com/streadway/amqp"
)

func main() {
	server()
}

func server() {
	conn, ch, q := getQueue()
	defer conn.Close()
	defer ch.Close()

	msg := amqp.Publishing{
		ContentType: "text/plain",
		Body:        []byte("Hello From Kevin"),
	}

	ch.Publish("", q.Name, false, false, msg)
}

func getQueue() (*amqp.Connection, *amqp.Channel, *amqp.Queue) {
	conn, err := amqp.Dial("amqp:guest@localhost:15672")
	failOnError(err, "Failed to connect to rabbitmq")
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	q, err := ch.QueueDeclare("hello", false, false, false, false, nil)
	failOnError(err, "failed to declare a queue")
	return conn, ch, &q
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatal("%s: %s", msg, err)
		// app will kill itself
		panic(fmt.Sprint("%s: %s", msg, err))
	}
}
