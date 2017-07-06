package coordinator

import (
	"bytes"
	"time"

	"encoding/gob"

	"github.com/kvnwh/distributed-system-go/distributed/dto"
	"github.com/kvnwh/distributed-system-go/distributed/qutils"
	"github.com/streadway/amqp"
)

const maxRate = 5 * time.Second

type DataBaseConsumer struct {
	er      EventRaiser
	conn    *amqp.Connection
	ch      *amqp.Channel
	queue   *amqp.Queue
	sources []string
}

func NewDataBaseConsumer(er EventRaiser) *DataBaseConsumer {
	dc := DataBaseConsumer{
		er: er,
	}

	dc.conn, dc.ch = qutils.GetChannel(url)
	dc.queue = qutils.GetQueue(qutils.PersistReadingQueue, dc.ch, false)

	dc.er.AddListener(qutils.DataSourceDiscovered, func(eventData interface{}) {
		dc.SubscribeToDataEvent(eventData.(string))
	})
	return &dc
}

func (dc *DataBaseConsumer) SubscribeToDataEvent(eventName string) {
	for _, v := range dc.sources {
		if v == eventName {
			return
		}
	}

	dc.er.AddListener("MessageReceived_"+eventName, func() func(interface{}) {
		prevTime := time.Unix(0, 0)

		buf := new(bytes.Buffer)

		return func(eventData interface{}) {
			ed := eventData.(EventData)
			if time.Since(prevTime) > maxRate {
				prevTime = time.Now()

				sm := dto.SensorMessage{
					Name:      ed.Name,
					Value:     ed.Value,
					Timestamp: ed.TimeStamp,
				}

				buf.Reset()

				enc := gob.NewEncoder(buf)
				enc.Encode(sm)

				msg := amqp.Publishing{
					Body: buf.Bytes(),
				}

				dc.ch.Publish("", qutils.PersistReadingQueue, false, false, msg)
			}
		}
	}())
}
