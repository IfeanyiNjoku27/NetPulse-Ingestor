# imports 
import json
import os
import time
import requests
from datetime import datetime


# use os to find the path to the json file and load it into memory
script_dir = os.path.dirname(os.path.abspath(__file__))
json_file_path = os.path.join(script_dir, 'targets.json')

# when script starts it should open json file that has list of urls
# and load the list into memory
try:
    with open(json_file_path, 'r') as file:
        data = json.load(file)
        # extract the list of urls from the "targets" key in the json file
        targets = data.get("targets", [])
    print(f"File loaded successfully with {len(targets)} targets. \n")

    # loop through lists of urls and send get request to each url every 2 minutes
    # for now only 1 url in the list to test 
    while True:
        # if file was loaded but was empty, exit immediately
        if not targets:
            print("No targets found in the JSON file. Ending script.")
            break

        for target in targets:
            # get url from url and name from dictionary item
            url = target.get("url").strip() # remove leading/trailing whitespace
            name = target.get("name", "Unnamed Target")
            #time stamp 
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            if not url: 
                print(f"Target '{name}' does not have a valid URL. Skipping.")
                continue

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

            # final dictionary for kafka event (kafka event not yet implemented)
            # will changee "print(event)" to "kafka_producer.send(event)" once kafka producer is implemented
            event = {
                "timestamp": timestamp,
                "device": name,
                "url": url,
                "status": status,
                "status_code": status_code,
                "latency_ms": latency_ms,
                "error": error_message
            }
            print(f"Streaming Event to Queue: {event}\n" + "-"*50)
            
        # wait for 10 seconds before the next round of requests (as test)
        time.sleep(120) # change to 120 seconds once testing is done

except FileNotFoundError:
    print("targets.json file not found. Please create the file with a list of target URLs.")
