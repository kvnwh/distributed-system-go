package main

import (
	"fmt"

	"log"

	"github.com/streadway/amqp"

	"time"
)

func main() {
	go client()
	go server()

	// keep go routine alive
	var a string
	fmt.Scanln(&a)
}

func client() {
	conn, ch, q := getQueue()
	// close at the end
	defer conn.Close()
	// close after other functions are done
	defer ch.Close()

	msgs, err := ch.Consume(q.Name, // queue
		"",    // consumer,
		true,  // autoAck
		false, //exclusive
		false, //nolocal,
		false, // nowait
		nil)   // args amqp.Table
	failOnError(err, "failed to register a consumer")

	for msg := range msgs {
		log.Printf("Receive message with body: %s", msg.Body)
	}
}

func server() {
	conn, ch, q := getQueue()
	defer conn.Close()
	defer ch.Close()

	for {
		msg := amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte("Hello From Kevin at " + time.Now().Format("03:04:05PM")),
		}
		ch.Publish("", q.Name, false, false, msg)
		// sleep for .5 seconds
		time.Sleep(500 * time.Millisecond)
	}
}

func getQueue() (*amqp.Connection, *amqp.Channel, *amqp.Queue) {
	conn, err := amqp.Dial("amqp:guest@localhost:5672")
	failOnError(err, "Failed to connect to rabbitmq")
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	q, err := ch.QueueDeclare("hello", false, false, false, false, nil)
	failOnError(err, "failed to declare a queue")
	return conn, ch, &q
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatal("%s : %s", msg, err)
		// app will kill itself
		panic(fmt.Sprint("%s: %s", msg, err))
	}
}
