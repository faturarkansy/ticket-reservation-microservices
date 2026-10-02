package main

import (
	"log"

	"notification-service/event"
)

func main() {
	amqpURL := "amqp://guest:guest@127.0.0.1:5672/"
	queueName := "booking_created_queue"

	consumer, err := event.NewEventConsumer(amqpURL, queueName)
	if err != nil {
		log.Fatalf("Failed to initialize RabbitMQ Consumer: %v", err)
	}
	defer consumer.Close()

	if err := consumer.StartConsuming(); err != nil {
		log.Fatalf("Error running consumer: %v", err)
	}
}