package coordinator

import (
	"time"
)

type EventRaiser interface {
	AddListener(eventName string, f func(interface{}))
}

type EventAggregator struct {
	listeners map[string][]func(interface{}) // list of event handlers
}

type EventData struct {
	Name      string
	Value     float64
	TimeStamp time.Time
}

func NewEventAggregator() *EventAggregator {
	ea := EventAggregator{
		listeners: make(map[string][]func(interface{})),
	}
	return &ea
}

// add listener to specific event
func (ea *EventAggregator) AddListener(name string, f func(interface{})) {
	ea.listeners[name] = append(ea.listeners[name], f)
}

func (ea *EventAggregator) PublishEvent(name string, eventData interface{}) {
	if ea.listeners[name] != nil {
		for _, listener := range ea.listeners[name] {
			listener(eventData) // use the copy of the eventdata, since each handler handles the copy of the event
		}
	}
}
