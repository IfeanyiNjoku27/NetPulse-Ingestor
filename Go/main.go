package main

import (
	"fmt"
	"log"
	"net/http"
)

// enableCORS is a middleware wrapper that forces headers on every request
func enableCORS(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// 1. Force CORS headers on EVERYTHING
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		// 2. Intercept and instantly approve Firefox/Chrome Preflight checks
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		// 3. Pass the request down to your actual handler
		next(w, r)
	}
}

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
	http.HandleFunc("/api/stream", handleStream(broker))
	http.HandleFunc("/api/devices", handleDevices(db))

	// start web server
	port := ":8081"
	fmt.Println("=================================================")
	fmt.Printf("🚀 NetPulse API running on http://localhost%s\n", port)
	fmt.Println("=================================================")

	if err := http.ListenAndServe(port, nil); err != nil {
		log.Fatalf("Error starting server: %v", err)
	}
}
