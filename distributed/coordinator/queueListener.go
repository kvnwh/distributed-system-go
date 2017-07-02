package coordinator

import (
	"bytes"
	"encoding/gob"
	"fmt"

	"github.com/kvnwh/distributed-system-go/distributed/dto"
	"github.com/kvnwh/distributed-system-go/distributed/qutils"
	"github.com/streadway/amqp"
)

const url = "amqp://guest:guest@localhost:5672"

type QueueListener struct {
	conn      *amqp.Connection
	ch        *amqp.Channel
	sources   map[string]<-chan amqp.Delivery
	listeners *EventAggregator
}

func NewQueueListener() *QueueListener {
	ql := QueueListener{
		sources:   make(map[string]<-chan amqp.Delivery),
		listeners: NewEventAggregator(),
	}
	ql.conn, ql.ch = qutils.GetChannel(url)
	return &ql
}

func (ql *QueueListener) ListenForNewSource() {
	q := qutils.GetQueue("", ql.ch)
	// bind the queue to fanout exchange instead of default exchange
	ql.ch.QueueBind(q.Name, "", "amq.fanout", false, nil)
	fmt.Printf("coordinator %s binded to fanout exchange \n", q.Name)
	// msgs channel
	msgs, _ := ql.ch.Consume(q.Name, "", true, false, false, false, nil)
	fmt.Println("coordinator listening for incoming queue names...")

	// broadcast this coordinator to discover exchange
	ql.DiscoverSensors()

	for msg := range msgs {
		queueName := string(msg.Body)
		fmt.Printf("detected a queue name: %s \n", queueName)
		//  the queue name is in the msg body
		sourceChan, _ := ql.ch.Consume(queueName, "", true, false, false, false, nil)
		fmt.Printf("start consuming incoming messages on queue %s \n", queueName)
		if ql.sources[queueName] == nil {
			ql.sources[queueName] = sourceChan

			go ql.AddListener(sourceChan)
		}
	}
}

func (ql *QueueListener) AddListener(msgs <-chan amqp.Delivery) {
	for msg := range msgs {
		r := bytes.NewReader(msg.Body)
		d := gob.NewDecoder(r)
		sd := new(dto.SensorMessage)
		d.Decode(sd)
		fmt.Printf("Received message: %v \n", sd)

		eventData := EventData{
			Name:      sd.Name,
			Value:     sd.Value,
			TimeStamp: sd.Timestamp,
		}

		ql.listeners.PublishEvent("Message received_"+msg.RoutingKey, eventData)
	}
}

// this is added because the if coordiantor is started after sensor started, there is no way to
// let the coordinator to pick up the data from sensor
func (ql *QueueListener) DiscoverSensors() {
	fmt.Printf("creating %s exchange... \n", qutils.SensorDiscoveryExchage)
	ql.ch.ExchangeDeclare(qutils.SensorDiscoveryExchage, "fanout", false, false, false, false, nil)

	ql.ch.Publish(qutils.SensorDiscoveryExchage, "", false, false, amqp.Publishing{})
}
