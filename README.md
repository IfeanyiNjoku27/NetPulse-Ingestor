# What is NetPulse? 

## NetPulse is low-level network health monitoring tool. 

## The Project architecture 

---

> Project Architecture
>
> - **The Ingestor(Python / Low-Level Networking)**: A simple Python script that acts as a mock "network probe." It pings a list of IP addresses (or URLs) and checks their latency and HTTP status codes.
> - **The Stream (Kafka)**: The Python script streams this network packet/ping data as JSON messages into a local Kafka topic.
> - **The Processor (Go)**: A tiny Go service that consumes the data from the Kafka topic, logs it, and flags any "device" that has a latency higher than 200ms.
> - **The Dashboard (TypeScript / GraphQL)**: A quick frontend that queries your database using GraphQL/Apollo to display the real-time up/down status of your "network."