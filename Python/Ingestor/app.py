# imports 
import json
import os
import time
import requests
from datetime import datetime
from confluent_kafka import Producer

# ---- Kafka Callback Function ----
def delivery_report(err, msg):
    """ Triggered by poll() or flush(). Reports the success or failure of a message delivery. """
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered successfully: {msg.topic()} [Partition: {msg.partition()}] at offset {msg.offset()}")


# --- Helper functions -----
def load_targets_from_json(file_path):
    """ Load target URLs from a JSON file. """

    try:
        with open(file_path, 'r') as file:
            data = json.load(file)
            # extract the list of urls from the "targets" key in the json file
            targets = data.get("targets", [])
            print(f"File loaded successfully with {len(targets)} targets. \n")
            return targets
    except FileNotFoundError:
        print("targets.json file not found. Please create the file with a list of target URLs.")
        return []
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        return []
    
def ping_targets(name, url):
    """ Ping a target URL and return the status, status code, latency, and error message (if any). """
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # ---- Default values ----
    status = "DOWN" # default state is down until we get a successful response
    status_code = "N/A" 
    latency_seconds = None # 
    latency_ms = None 
    error_message = "None"
        
    try:
        # send a get request to API endpoint with a timeout of 5 seconds and store the response
        response = requests.get(url, timeout=5)
        status_code = response.status_code
        latency_seconds = round(response.elapsed.total_seconds(), 3)  # round to 3 decimal places
        latency_ms = int(latency_seconds * 1000)  # convert to milliseconds

        # check if the request was successful
        if status_code == 200:
            status = "UP"

        else:
            status = "DOWN"
            error_message = f"HTTP Error Status: {status_code}"

    except requests.exceptions.Timeout:
        status = "TIMEOUT"
        status_code = "TIMEOUT"
        latency_ms = 5000 # set latency to 5000 ms for timeouts
        error_message = "Request timed out"

    except requests.exceptions.RequestException as e:
        status = "ERROR"
        status_code = "ERROR"
        error_message = str(e)

    return {
        "timestamp": timestamp,
        "device": name,
        "url": url,
        "status": status,
        "status_code": status_code,
        "latency_ms": latency_ms,
        "error": error_message
    }

# --- Main Function ---
def main():
    # Paths and load targets
    script_dir = os.path.dirname(os.path.abspath(__file__))
    json_file_path = os.path.join(script_dir, 'targets.json')
    targets = load_targets_from_json(json_file_path)

    if not targets:
        print("No targets to monitor. Please add target URLs to the targets.json file.")
        return

    # Initialize kafka producer
    # Configuration mapping back to docker setup for kafka producer
    producer_config = { 'bootstrap.servers': 'localhost:9092' }
    producer = Producer(producer_config)
    topic_name = "network-events"

    print("Starting the monitoring loop. Press Ctrl+C to stop...")

    # Monitoring Loop
    try:
        while True:
            for target in targets:
                # execute ping
                event = ping_targets(target.get("name"), target.get("url"))

                # serialize event payload to json string
                event_json = json.dumps(event).encode('utf-8')

                # send to kafka asynchronously with delivery report callback
                producer.produce(
                    topic=topic_name,
                    value=event_json,
                    callback=delivery_report
                )

                # trigger any outstanding callbacks from previous send
                producer.poll(0)

                print("-" * 50)  # separator for readability
                time.sleep(10)  # wait for 10 seconds before pinging the next target (testing)
    except KeyboardInterrupt:
        print("Monitoring stopped by user.")
    finally:
        producer.flush()  # ensure all messages are sent before exiting
        print("Producer flushed and exiting.")


# --- Entry Point ---
if __name__ == "__main__":
    main()