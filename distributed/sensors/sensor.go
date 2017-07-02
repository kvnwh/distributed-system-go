package main

import (
	"bytes"
	"encoding/gob"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"time"

	"github.com/kvnwh/distributed-system-go/distributed/dto"
	"github.com/kvnwh/distributed-system-go/distributed/qutils"
	"github.com/streadway/amqp"
)

var url = "amqp://guest:guest@localhost:5672"

var name = flag.String("name", "sensor", "name of the sensor")
var freq = flag.Uint("freq", 5, "updat frequency in the cycles/sec")
var max = flag.Float64("max", 5., "max value for generated readings")
var min = flag.Float64("min", 1., "min value for generated readings")
var stepSize = flag.Float64("step", 0.1, "max allowable change per measurement")

var r = rand.New(rand.NewSource(time.Now().UnixNano()))

var value = r.Float64()*(*max-*min) + *min
var nom = (*max-*min)/2 + *min

func main() {
	flag.Parse()

	conn, ch := qutils.GetChannel(url)
	defer conn.Close()
	defer ch.Close()

	// create a new queue
	dataQueue := qutils.GetQueue(*name, ch, false)

	publishQueueName(ch)

	discoveryQueue := qutils.GetQueue("", ch, false)

	ch.QueueBind(discoveryQueue.Name, "", qutils.SensorDiscoveryExchage, false, nil)

	go listenForDiscoverRequests(discoveryQueue.Name, ch)

	dur, _ := time.ParseDuration(strconv.Itoa(1000/int(*freq)) + "ms")

	// this is a chan
	signal := time.Tick(dur)

	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)

	for range signal {
		calcValue()
		reading := dto.SensorMessage{
			Name:      *name,
			Value:     value,
			Timestamp: time.Now(),
		}
		buf.Reset()
		enc = gob.NewEncoder(buf)
		enc.Encode(reading)

		// message to be published
		msg := amqp.Publishing{
			Body: buf.Bytes(),
		}

		// publish sensor message to data queue
		ch.Publish("", dataQueue.Name, false, false, msg)
		log.Printf("reading sent. Value: %v\n", value)
	}
}

func calcValue() {
	var maxStep, minStep float64

	if value < nom {
		maxStep = *stepSize
		minStep = -1 * *stepSize * (value - *min) / (nom - *min)
	} else {
		maxStep = *stepSize * (*max - value) / (*max - nom)
		minStep = -1 * *stepSize
	}
	value = r.Float64()*(maxStep-minStep) + minStep
}

func publishQueueName(ch *amqp.Channel) {
	// publish the queue name to fanout exchange
	msg := amqp.Publishing{Body: []byte(*name)}
	ch.Publish(
		"amq.fanout",
		"",
		false,
		false,
		msg,
	)
	fmt.Printf("published queue %s to fanout exchange \n", *name)
}

func listenForDiscoverRequests(name string, ch *amqp.Channel) {
	msgs, _ := ch.Consume(name, "", true, false, false, false, nil)

	for range msgs {
		publishQueueName(ch)
	}
}
