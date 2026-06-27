package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/lib/pq"
)

// type event struct to map json keys in python script to go struct fields
type Event struct {
	Device         string    `json:"device"`
	URL            string    `json:"url"`
	Status         string    `json:"status"`
	PreviousStatus string    `json:"previous_status"` // added to track previous status for database logging
	Statuscode     int       `json:"status_code"`
	Latencyms      *int      `json:"latency_ms"` // pointer to int to allow for null values on failure
	ErrorMessage   string    `json:"error"`
	Time           time.Time `json:"time"` // handled internally for db logging, not included in kafka message
}

// database connection function to connect to postgres database using connection string
func ConnectToDatabase() (*sql.DB, map[string]string) {
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

// LogStateTransition writes an explicit network state change to the dabatase
func LogStateTransition(db *sql.DB, event Event, prevStatus string) error {
	query := `
		INSERT INTO network_state_transitions
		(device_name, target_url, previous_status, current_status, status_code, latency_ms, error_message, transition_time)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`
	_, err := db.Exec(query,
		event.Device,
		event.URL,
		prevStatus,
		event.Status,
		event.Statuscode,
		event.Latencyms,
		event.ErrorMessage,
		time.Now(),
	)
	return err
}

// FetchStateHistory retrieves the most recent state transitions from the database, limited by the specified number of records
func FetchStateHistory(db *sql.DB, limit int) ([]Event, error) {
	query := `
		SELECT device_name, target_url, previous_status, current_status, status_code, latency_ms, error_message, transition_time
		FROM network_state_transitions
		ORDER BY transition_time DESC
		LIMIT $1
	`
	rows, err := db.Query(query, limit)
	if err != nil {
		return nil, fmt.Errorf("error querying state history: %v", err)
	}
	defer rows.Close()

	var history []Event
	for rows.Next() {
		var ev Event
		err := rows.Scan(
			&ev.Device,
			&ev.URL,
			&ev.PreviousStatus,
			&ev.Status,
			&ev.Statuscode,
			&ev.Latencyms,
			&ev.ErrorMessage,
			&ev.Time,
		)
		if err != nil {
			return nil, fmt.Errorf("error scanning state history row: %v", err)
		}
		history = append(history, ev)
	}
	return history, nil
}
