import json
import csv
import sys
import os
from kafka import KafkaConsumer

# --- Configuration ---
TOPIC = "output-skyline"
BOOTSTRAP_SERVERS = ["localhost:9092"]

def collect_metrics(output_filename):
    # Check if file exists to decide if we need to write headers
    file_exists = os.path.isfile(output_filename)
    
    # 1. Connect to Kafka
    print(f"--- Listening on topic '{TOPIC}' ---")
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset='latest',  # Start reading only new messages
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    # 2. Open CSV for writing
    with open(output_filename, mode='a', newline='') as file:
        writer = csv.writer(file)
        
        # Define Columns
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
            "SkylinePoints" # <-- This will contain the raw [[x,y],...] JSON
        ]
        
        # Write header only if file didn't exist
        if not file_exists:
            writer.writerow(headers)
            print(f"Created '{output_filename}' with headers.")
        else:
            print(f"Appending to existing '{output_filename}'.")

        print("Waiting for Flink results... (Press Ctrl+C to stop)")

        try:
            for message in consumer:
                data = message.value
                
                # 3. Extract Fields safely
                q_id = data.get("query_id", "N/A")
                records = data.get("record_count", 0)
                size = data.get("skyline_size", 0)
                optimality = data.get("optimality", 0.0)
                
                # Timing Metrics
                t_ingest = data.get("ingestion_time_ms", 0)
                t_local = data.get("local_processing_time_ms", 0)
                t_global = data.get("global_processing_time_ms", 0)
                t_total = data.get("total_processing_time_ms", 0)
                t_latency = data.get("query_latency_ms", 0)
                
                # Raw Points (Can be large, so we keep it as a JSON string string)
                # --- SAFE EXTRACTION FOR POINTS ---
                # If key is missing (because I commented it out), default to empty string "[]"
                raw_points_obj = data.get("skyline_points", [])
                raw_points = json.dumps(raw_points_obj)

                # 4. Print Summary to Console (Cleaner than printing the whole JSON)
                print(f"[Query {q_id}] Records: {records} | Size: {size} | TotalTime: {t_total}ms")

                # 5. Write to CSV
                writer.writerow([
                    q_id, records, size, optimality, 
                    t_ingest, t_local, t_global, t_total, t_latency, 
                    raw_points
                ])
                file.flush() # Ensure data is saved immediately

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
