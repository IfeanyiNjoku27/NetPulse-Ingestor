package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
)

// type event struct to map json keys in python script to go struct fields
type Event struct {
	Device       string `json:"device"`
	URL          string `json:"url"`
	Status       string `json:"status"`
	Statuscode   int    `json:"status_code"`
	Latencyms    *int   `json:"latency_ms"` // pointer to int to allow for null values on failure
	ErrorMessage string `json:"error"`
}

func main() {
	// create a new kafka reader with the appropriate configuration
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "netpulse-events",
		GroupID: "netpulse-processor-group", // tracks place in queue
	})

	// clean up reader connection when main function exits
	defer reader.Close()

	fmt.Println("Go Processor is running and listening for events...")

	// loop to continuously read messages from the kafka topic
	for {
		// blocks until a new message is available
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Error reading message: %v", err)
			continue // skip to next iteration to keep processor running
		}

		// Parse the JSON
		// The raw JSON bytes are stored in msg.Value
		// create an empty Event struct, and use json.Unmarshal() to fill it.
		// Then, print the parsed Device name and Status to the console to prove it worked!
		var event Event
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Printf("Error parsing JSON: %v", err)
			continue
		}

		fmt.Printf("Device: %s | Status: %s\n", event.Device, event.Status)
		fmt.Printf("Received message at offset %d: %s\n", msg.Offset, string(msg.Value))
	}
}
