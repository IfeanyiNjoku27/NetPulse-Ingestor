package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
)

// Broker manages connected clients and broadcasts events
type Broker struct {
	clients       map[chan Event]bool // map of all connected clients
	BroadcastChan chan Event          // Receives events from kafka consumer
	register      chan chan Event     // Register new clients
	unregister    chan chan Event     // Unregister clients
}

// NewBroker initializes a new Broker instance
func NewBroker() *Broker {
	return &Broker{
		clients:       make(map[chan Event]bool),
		BroadcastChan: make(chan Event),
		register:      make(chan chan Event),
		unregister:    make(chan chan Event),
	}
}

// Start runs in the background and safely manages client connections and broadcasts events to them
func (b *Broker) Start() {
	for {
		select {
		case client := <-b.register:
			b.clients[client] = true
			fmt.Printf("Client Registered. Total active tabs: %d\n", len(b.clients))
		case client := <-b.unregister:
			if _, ok := b.clients[client]; ok {
				delete(b.clients, client)
				close(client)
				fmt.Printf("Client Unregistered. Total active tabs: %d\n", len(b.clients))
			}
		case event := <-b.BroadcastChan:
			// new transition event received from kafka consumer, broadcast to all connected clients
			for client := range b.clients {
				select {
				case client <- event:
				default:
					// if the client channel is blocked, we can assume the client is not responsive and remove it
					delete(b.clients, client)
					close(client)
					fmt.Printf("Client Unresponsive. Total active tabs: %d\n", len(b.clients))
				}
			}
		}
	}
}

// ---- HTTP HANDLERS ----

// handleDevices serves the REST API for fetching the list of unique devices and their most recent status
func handleDevices(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		//  Set the CORS headers first
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		//  Handle the Firefox Preflight Check
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		// Set SSE specific headers
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")

		// Force the HTTP handshake to complete instantly
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}

		// fetch unique devices and their most recent status from the database
		devices, err := FetchActiveDevices(db)
		if err != nil {
			http.Error(w, fmt.Sprintf("Error fetching devices: %v", err), http.StatusInternalServerError)
			return
		}

		// send the devices as JSON response
		if err := json.NewEncoder(w).Encode(devices); err != nil {
			http.Error(w, fmt.Sprintf("Error encoding devices to JSON: %v", err), http.StatusInternalServerError)
			return
		}
	}
}

// handleHistory serves the REST API for the initial react dashboard load
func handleHistory(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		//  Set the CORS headers first
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		//  Handle the Firefox Preflight Check
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		// Set SSE specific headers
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")

		// Force the HTTP handshake to complete instantly
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}

		// fetch last 800 state transitions from the database
		history, err := FetchStateHistory(db, 800)
		if err != nil {
			http.Error(w, fmt.Sprintf("Error fetching history: %v", err), http.StatusInternalServerError)
			return
		}

		// send the history as JSON response
		if err := json.NewEncoder(w).Encode(history); err != nil {
			http.Error(w, fmt.Sprintf("Error encoding history to JSON: %v", err), http.StatusInternalServerError)
			return
		}
	}
}

// handleStream serves the SSE endpoint for real-time updates to the React dashboard
func handleStream(broker *Broker) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		fmt.Println("React UI is knocking on the SSE stream door")

		//  Set the CORS headers first
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		//  Handle the Firefox Preflight Check
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		// Set SSE specific headers
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")

		if f, ok := w.(http.Flusher); ok {
			f.Flush() // flush the headers to the client
		} else {
			http.Error(w, "Streaming unsupported!", http.StatusInternalServerError)
			return
		}

		// create a new channel for this specific browser
		clientChan := make(chan Event)
		broker.register <- clientChan

		// listen to browser closing the connection
		ctx := r.Context()
		go func() {
			<-ctx.Done()
			broker.unregister <- clientChan
		}()

		// infinite loop that waits for data from the broker and flushes it to the browser
		for event := range clientChan {
			eventJSon, _ := json.Marshal(event)
			fmt.Fprintf(w, "data: %s\n\n", eventJSon)

			// force flush to the browser
			// because the default behavior of http.ResponseWriter is to buffer data until the buffer is full or the handler returns
			// so to send data immediately to the browser for real-time updates this is needed
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		}
	}
}
