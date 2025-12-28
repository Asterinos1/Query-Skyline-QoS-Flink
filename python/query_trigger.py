from kafka import KafkaProducer
from sys import argv
from faker import Faker
import time
import json

def send_query_trigger():
    kafka_nodes = "localhost:9092"
    topic_name = argv[1] if len(argv) > 1 else "query-trigger"
    algo_str = argv[2] if len(argv) > 2 else "mr-dim"
    trigger_interval = int(argv[3]) if len(argv) > 3 else 60  # in seconds

    algo_map = {
        "mr-dim": 1,
        "mr-grid": 2,
        "mr-angle": 3
    }

    skyline_algorithm = algo_map.get(algo_str.lower(), 1)

    prod = KafkaProducer(
        bootstrap_servers=kafka_nodes,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    )
    print(f"Starting query trigger stream for {skyline_algorithm} every {trigger_interval} seconds...")

    try:
        prod.send(topic_name, value=skyline_algorithm)
        prod.flush()
        print(f"[{time.strftime('%H:%M:%S')}] Trigger sent: {skyline_algorithm}")

        time.sleep(trigger_interval)
    except KeyboardInterrupt:
        print("Stopping query trigger.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        prod.flush()
        prod.close()

def main():
    send_query_trigger()


if __name__ == '__main__':
    main()