from kafka import KafkaProducer
from faker import Faker
from enum import Enum
from sys import argv
import random
import time
import json

class GenMethod(Enum):
    UNIFORM = "uniform"
    CORRELATED = "correlated"
    ANTI_CORRELATED = "anti_correlated"

    @classmethod
    def from_str(cls, label):
        return cls(label.lower())

def generate_uniform_data(faker, dimensions, d_min=0, d_max=100):
    return [faker.random_int(min=d_min, max=d_max) for _ in range(dimensions)]

def generate_correlated_data(faker, dimensions, d_min=0, d_max=100):
    base = faker.random_int(min=d_min + 20, max=d_max - 20)
    offset = int((d_max - d_min) * 0.1) # 10% of the domain
    # Dimensions move together within a small range of the base
    return [base + faker.random_int(min=-offset, max=offset) for _ in range(dimensions)]

def generate_anti_correlated_data(faker, dimensions, d_min=0, d_max=100):
    # One dimension is very low, others are pushed high
    pivot = faker.random_int(min=0, max=dimensions - 1)
    low_bound = d_min + int((d_max - d_min) * 0.2)
    high_bound = d_min + int((d_max - d_min) * 0.8)
    return [faker.random_int(min=d_min, max=low_bound) if i == pivot 
            else faker.random_int(min=high_bound, max=d_max) 
            for i in range(dimensions)]

def generate_data():
    faker = Faker()
    kafka_nodes = "localhost:9092"

    # Get arguments for generation method and dimensions
    topic_name = argv[1] if len(argv) > 1 else "skyline-data"
    method_str = argv[2] if len(argv) > 2 else "uniform"
    dimensions = int(argv[3]) if len(argv) > 3 else 2
    d_min = int(argv[4]) if len(argv) > 4 else 0
    d_max = int(argv[5]) if len(argv) > 5 else 100

    generation_method = GenMethod.from_str(method_str)

    # Use JSON serializer so Flink can parse it easily
    prod = KafkaProducer(
        bootstrap_servers=kafka_nodes,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        # For High Throughput?(TEST)
        #batch_size=65536, 
        #linger_ms=20
    )
    print(f"Starting {generation_method.value} stream with {dimensions} dimensions...")
    


    try:
        point_id = 0
        while True:
            # Select the math based on Enum
            if generation_method == GenMethod.UNIFORM:
                data_points = generate_uniform_data(faker, dimensions, d_min, d_max)
            elif generation_method == GenMethod.CORRELATED:
                data_points = generate_correlated_data(faker, dimensions, d_min, d_max)
            elif generation_method == GenMethod.ANTI_CORRELATED:
                data_points = generate_anti_correlated_data(faker, dimensions, d_min, d_max)
            # Create the structured message
            '''
            message = {
                "id": point_id,
                "v": values,
                "ts": time.time()
            }
            '''

            #prod.send(topic_name, value=message)
            prod.send(topic_name, value=data_points)

            if point_id % 100000 == 0:
                print(f"Sent {point_id} records...")
            
            point_id += 1

    except KeyboardInterrupt:
        print("Stopping data generation.")
    finally:
        prod.flush()
        prod.close()
    

def main():
    generate_data()

if __name__ == '__main__':
    main()
