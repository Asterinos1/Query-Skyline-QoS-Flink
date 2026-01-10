import json
import csv
import sys
import os
from kafka import KafkaConsumer

"""
Skyline Metrics Collector.

This script acts as the final sink for the Flink Skyline Experiment. It listens to the 
designated output Kafka topic where the Flink job publishes its results (in JSON format). 
It parses these messages and persists them into a structured CSV file for easier analysis 
and plotting later.

The script is designed to be idempotent regarding the output file; if the file does not 
exist, it creates it with the appropriate headers. If the file already exists, it appends 
new records to it, allowing for long-running experiments where multiple queries are 
triggered over time.
"""

# Configuration Constants
TOPIC = "output-skyline"
BOOTSTRAP_SERVERS = ["localhost:9092"]

"""
Main Collection Routine.

This function establishes a connection to the Kafka Broker and continuously polls for 
new messages on the output topic. Upon receiving a message, it deserializes the JSON 
payload, extracts specific performance metrics (like latency, processing time, and 
optimality), and writes a standardized row to the specified CSV file.

The function handles file initialization by checking for the existence of the output 
file. If the file is missing, it writes the column headers first. It also includes 
safety checks for optional fields (like raw skyline points) to prevent crashes if 
the upstream Flink job omits them to save bandwidth.
"""
def collect_metrics(output_filename):
    # Check if file exists to decide if we need to write headers
    file_exists = os.path.isfile(output_filename)
    
    # Connect to Kafka Consumer
    # We use 'latest' offset reset to ensure we only capture results generated 
    # after this collector has started, avoiding processing stale data from previous runs.
    print(f"--- Listening on topic '{TOPIC}' ---")
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset='latest', 
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    # Open CSV Output File
    # The file is opened in append mode ('a') to preserve existing data.
    # newline='' is used to prevent blank lines between rows on Windows platforms.
    with open(output_filename, mode='a', newline='') as file:
        writer = csv.writer(file)
        
        # Define Schema columns
        headers = [
            "QueryID", 
            "Records", 
            "SkylineSize", 
            "Optimality", 
            "IngestTime(ms)", 
            "LocalTime(ms)", 
            "GlobalTime(ms)", 
            "TotalTime(ms)", 
            "Latency(ms)",
            "SkylinePoints" # Stores the raw JSON array of points [[x,y],...]
        ]
        
        # Initialize Headers
        # Only write the header row if we are creating a fresh file.
        if not file_exists:
            writer.writerow(headers)
            print(f"Created '{output_filename}' with headers.")
        else:
            print(f"Appending to existing '{output_filename}'.")

        print("Waiting for Flink results... (Press Ctrl+C to stop)")

        try:
            # Main Event Loop
            for message in consumer:
                data = message.value
                
                # Extract Metrics from JSON Payload
                # Using .get() ensures the script remains robust even if certain keys 
                # are missing from the input stream.
                q_id = data.get("query_id", "N/A")
                records = data.get("record_count", 0)
                size = data.get("skyline_size", 0)
                optimality = data.get("optimality", 0.0)
                
                # Timing Metrics Extraction
                t_ingest = data.get("ingestion_time_ms", 0)
                t_local = data.get("local_processing_time_ms", 0)
                t_global = data.get("global_processing_time_ms", 0)
                t_total = data.get("total_processing_time_ms", 0)
                t_latency = data.get("query_latency_ms", 0)
                
                # Raw Points Extraction
                # This field can be very large. We default to an empty list if the 
                # Flink job disabled point serialization to improve performance.
                # We re-serialize it to a string so it fits into a single CSV cell.
                raw_points_obj = data.get("skyline_points", [])
                raw_points = json.dumps(raw_points_obj)

                # Console Feedback
                # Provides real-time feedback to the user that a query has finished.
                print(f"[Query {q_id}] Records: {records} | Size: {size} | TotalTime: {t_total}ms")

                # Persist to CSV
                writer.writerow([
                    q_id, records, size, optimality, 
                    t_ingest, t_local, t_global, t_total, t_latency, 
                    raw_points
                ])
                
                # Flush buffer to ensure data is written to disk immediately
                # This prevents data loss if the script is terminated abruptly.
                file.flush()

        except KeyboardInterrupt:
            print("\nStopping collector...")
        finally:
            consumer.close()
            print("Collector closed.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python metrics_collector.py <filename.csv>")
        sys.exit(1)
    
    filename = sys.argv[1]
    collect_metrics(filename)
