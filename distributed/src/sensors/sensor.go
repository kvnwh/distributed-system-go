package main

import (
	"flag"
	"math/rand"
	"strconv"
	"time"

	"../src/dto"
)

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

	dur, _ := time.ParseDuration(strconv.Itoa(1000/int(*freq)) + "ms")

	signal := time.Tick(dur)

	for range signal {
		calcValue()
		reading := dto.
			log.Printf("rading send. value: %v\n", value)
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
