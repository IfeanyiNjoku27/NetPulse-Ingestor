package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

// runs infinitely in the background, filtering events and dispaching items to the appropriate handler
func StartKafkaConsumer(db *sql.DB, deviceStates map[string]string, broadcastChan chan Event) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "netpulse-events",
		GroupID: "netpulse-processor-group",
	})
	defer reader.Close()

	fmt.Println("Kafka Consumer is running and listening for events...")

	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Error reading message from Kafka: %v", err)
			continue
		}
		fmt.Printf("Raw Ping Received: %s\n", string(msg.Value))

		var event Event
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Printf("Error unmarshalling message: %v", err)
			continue
		}

		prevStatus := deviceStates[event.Device]
		if prevStatus == "" {
			prevStatus = "UNKNOWN"
		}

		// fill struct with the transition context for the db and ui
		event.PreviousStatus = prevStatus
		event.Time = time.Now()

		// send event to the web server via the broadcast channel. always broadcast to ui
		broadcastChan <- event


		if prevStatus != event.Status {
			fmt.Printf("State Transition for %s: %s -> %s\n", event.Device, prevStatus, event.Status)

			// update in memory state
			deviceStates[event.Device] = event.Status

			// log to database
			if err := LogStateTransition(db, event, prevStatus); err != nil {
				log.Printf("Error logging event to database: %v", err)
			}
		}

	}
}
