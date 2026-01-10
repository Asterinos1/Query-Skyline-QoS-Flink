from kafka import KafkaProducer
from sys import argv
from faker import Faker
import time
import json

"""
Query Trigger Publisher.

This script acts as the Control Signal Generator for the Distributed Skyline Flink Job.
It publishes a "Query Trigger" message to a Kafka topic, which instructs the Flink
workers to calculate and emit their local skylines.

Usage:
    python send_query_trigger.py <topic_name> <algorithm> <sleep_interval>

    - topic_name: The Kafka topic to listen to (Default: "queries")
    - algorithm: The partitioning strategy identifier ("mr-dim", "mr-grid", "mr-angle")
    - sleep_interval: Wait time after sending (Default: 60s)

Note:
    This script sends a simplified payload (Algorithm ID). Because it does not
    append a 'Record Count' barrier (e.g., "1,50000"), the Flink job will interpret
    this as having a requirement of 0 records, effectively triggering an IMMEDIATE
    execution of the skyline query on whatever data is currently in the buffers.
"""

def send_query_trigger():
    """
    Constructs and sends a single Query Trigger message to Kafka.

    Logic:
    1. Parses command line arguments for configuration.
    2. Maps the human-readable algorithm name (e.g., 'mr-dim') to the internal Integer ID
       expected by the legacy components or logging systems.
    3. Serializes the ID into JSON and pushes it to the Kafka broker.
    4. Flushes the stream to ensure network delivery.

    The script sends the trigger once and then waits for the specified interval
    before exiting (useful if wrapping this function in a loop or cron job).
    """

    # --- Configuration Setup ---
    # Defaulting to localhost for development environment
    kafka_nodes = "localhost:9092"
    
    # Parse CLI Arguments with defaults
    topic_name = argv[1] if len(argv) > 1 else "queries"
    algo_str = argv[2] if len(argv) > 2 else "mr-dim"
    trigger_interval = int(argv[3]) if len(argv) > 3 else 60  # in seconds

    # --- Algorithm Mapping ---
    # Maps the string representation used in CLI to the Integer ID
    # that might be used for identifying query types in the backend.
    # 1: mr-dim (Dimensional Partitioning)
    # 2: mr-grid (Grid Partitioning)
    # 3: mr-angle (Angular Partitioning)
    algo_map = {
        "mr-dim": 1,
        "mr-grid": 2,
        "mr-angle": 3
    }

    # Default to 1 (mr-dim) if unknown string provided
    skyline_algorithm = algo_map.get(algo_str.lower(), 1)

    # --- Kafka Producer Initialization ---
    # Configured to serialize values as JSON bytes (UTF-8)
    prod = KafkaProducer(
        bootstrap_servers=kafka_nodes,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    )
    
    print(f"Starting query trigger stream for {skyline_algorithm} ({algo_str}) every {trigger_interval} seconds...")

    try:
        # --- Send Trigger ---
        # Sending the Algorithm ID as the value. 
        # Note: In the Flink consumer, this acts as the "Query Payload".
        # Since it lacks a comma (e.g., just "1"), the Flink Barrier Check 
        # will default 'requiredCount' to 0, causing immediate execution.
        prod.send(topic_name, value=skyline_algorithm)
        
        # Force the buffer to send immediately (Blocking call)
        prod.flush()
        print(f"[{time.strftime('%H:%M:%S')}] Trigger sent: {skyline_algorithm}")

        # Keep process alive for the duration of the interval (or for monitoring)
        time.sleep(trigger_interval)
        
    except KeyboardInterrupt:
        print("Stopping query trigger.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Resource Cleanup
        prod.flush()
        prod.close()

def main():
    """
    Entry point for the script.
    """
    send_query_trigger()


if __name__ == '__main__':
    main()
