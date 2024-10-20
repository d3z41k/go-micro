package main

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"listener/event"
	"log"
	"math"
	"os"
	"time"
)

func main() {
	// Try to connect RabbitMQ
	rabbitConn, err := connect()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer rabbitConn.Close()

	// Start listening for messages
	fmt.Println("Listening and consuming rabbitMQ Messages...")

	// Create consumer
	consumer, err := event.NewConsumer(rabbitConn)
	if err != nil {
		panic(err)
	}

	// Watch the queue and consume events
	err = consumer.Listen([]string{"logs.INFO", "logs.WARNING", "logs.ERROR"})
	if err != nil {
		log.Println(err)
	}
}

func connect() (*amqp.Connection, error) {
	var counts int64
	var backOff = 1 * time.Second
	var connection *amqp.Connection

	// Don't continue until rabbit is ready
	for {
		c, err := amqp.Dial("amqp://guest:guest@rabbitmq")
		if err != nil {
			fmt.Println("RabbitMQ not yet ready... ")
			counts++
		} else {
			log.Println("Connected to RabbitMQ")
			connection = c
			break
		}

		if counts > 5 {
			fmt.Println("RabbitMQ not ready after 5 attempts... ")
			return nil, err
		}

		backOff = time.Duration(math.Pow(float64(counts), 2)) * time.Second
		time.Sleep(backOff)
		continue
	}

	return connection, nil
}
