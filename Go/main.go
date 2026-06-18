package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"

	_ "github.com/lib/pq"
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

// database connection function to connect to postgres database using connection string
func connectToDatabase() (*sql.DB, map[string]string) {
	// connect to postgres database using connection string
	connStr := "postgres://netpulse:password@localhost:5433/netpulse_db?sslmode=disable"

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("Error connecting to PostgreSQL: %v", err)
	}

	// verify connection to database
	if err := db.Ping(); err != nil {
		db.Close() // ensure database connection is closed when function exits
		log.Fatalf("Database is unreachable: %v", err)
	}
	fmt.Println("Successfully connected to PostgreSQL database!")

	// initialize in memory state map
	deviceStates := make(map[string]string)

	// run distinct on query to get list of unique devices and their most recent status, and populate in-memory state map with results
	rows, err := db.Query(`
		SELECT DISTINCT ON (device_name) device_name, current_status
		FROM network_state_transitions
		ORDER BY device_name, transition_time DESC;
	`)
	if err != nil {
		log.Printf("Error querying database for initial device states: %v", err)
	}
	defer rows.Close() // ensure rows are closed when function exits

	// Iterate through the results one at a time 
	for rows.Next() {
		var deviceName string
		var currentStatus string

		// scan copies the columns from the current row into the provided variables
		if err := rows.Scan(&deviceName, &currentStatus); err != nil {
			log.Printf("Error scanning row for device states: %v", err)
		}

		// populate in-memory state map with results from database query
		deviceStates[deviceName] = currentStatus
		fmt.Printf("Loaded device state from database: %s -> %s\n", deviceName, currentStatus)
	}

	return db, deviceStates
}

func main() {
	// connect to database and get device state map
	db, deviceStates := connectToDatabase()
	defer db.Close() // ensure database connection is closed when main function exits

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
		// Clear and readable console telemetry card
		fmt.Println("\n================ TELEMETRY EVENT ================")
		fmt.Printf(" 🏷️  Device:    %-25s\n", event.Device)
		fmt.Printf(" 🌐 URL:       %-25s\n", event.URL)
		fmt.Printf(" 📊 Status:    %-10s (Code: %v)\n", event.Status, event.Statuscode)
		if event.Latencyms != nil {
			fmt.Printf(" ⏱️  Latency:   %d ms\n", *event.Latencyms)
		} else {
			fmt.Printf(" ⏱️  Latency:   N/A\n")
		}
		if event.ErrorMessage != "None" && event.ErrorMessage != "" {
			fmt.Printf(" ❌ Error:     %s\n", event.ErrorMessage)
		}
		fmt.Println("=================================================")

		// check previous state of device from in-memory map, and only print if state has changed
		prevStatus := deviceStates[event.Device]

		if prevStatus == "" {
			prevStatus = "UNKNOWN"                    // if device is new, set previous state to UNKNOWN
			deviceStates[event.Device] = event.Status // add new device to state map
			fmt.Printf("New device detected: %s | Status: %s\n", event.Device, event.Status)
		} else if prevStatus == event.Status {
			// sliding window gate: status hasnt changed. skip processing to avoid flooding console with redundant messages
			continue
		}

		// if not the first time seeing this device, and status has changed, update state map and print change
		fmt.Printf("State transition detected for device %s: %s -> %s\n", event.Device, prevStatus, event.Status)
		deviceStates[event.Device] = event.Status // update state map with new status

		// SQL Query to insert the transition into PostgreSQL
		insertQuery := `INSERT INTO network_state_transitions 
			(device_name, target_url, previous_status, current_status, status_code, latency_ms, error_message, transition_time) 
			VALUES ($1, $2, $3, $4, $5, $6, $7, Now())`

		// Execute the query using DB context
		_, err = db.Exec(
			insertQuery,
			event.Device,
			event.URL,
			prevStatus,
			event.Status,
			event.Statuscode,
			event.Latencyms,
			event.ErrorMessage,
		)
		if err != nil {
			log.Printf("Error inserting transition into database: %v", err)
		} else {
			fmt.Printf("Successfully logged transition for device %s to database.\n", event.Device)
		}

	}
}
