package main

import (
	"fmt"
	"log"
	"net/http"
)

func main() {
	// initialize database and run warmup queries
	db, deviceStates := ConnectToDatabase()
	defer db.Close()

	// initialize SSE broker
	broker := NewBroker()

	// start the broker in a separate goroutine
	go broker.Start()

	// start the Kafka consumer in a separate goroutine
	go StartKafkaConsumer(db, deviceStates, broker.BroadcastChan)

	// set up HTTP server and routes
	http.HandleFunc("/api/history", handleHistory(db))
	http.HandleFunc("/api/events", handleStream(broker))

	// start web server
	port := ":8081"
	fmt.Println("=================================================")
	fmt.Printf("🚀 NetPulse API running on http://localhost%s\n", port)
	fmt.Println("=================================================")

	if err := http.ListenAndServe(port, nil); err != nil {
		log.Fatalf("Error starting server: %v", err)
	}
}
