from kafka import KafkaProducer
import json
import time
from pathlib import Path
import sys
from kafka.errors import NoBrokersAvailable

def connect_kafka():
    retries = 5
    for i in range(retries):
        try:
            print(f"Attempting to connect to Kafka (attempt {i+1}/{retries})...")
            producer = KafkaProducer(
                bootstrap_servers=['kafka:9092'],
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                acks='all',
                retries=3,
                retry_backoff_ms=1000
            )
            print("Successfully connected to Kafka!")
            return producer
        except NoBrokersAvailable:
            if i < retries - 1:
                print(f"Kafka not yet available, waiting 10 seconds...")
                time.sleep(10)
            else:
                print("Failed to connect to Kafka after multiple retries")
                raise
        except Exception as e:
            print(f"Unexpected error: {e}")
            raise

def read_json_files():
    data_path = Path('/opt/bitnami/spark/data/activity-data')
    for json_file in data_path.glob('part-*.json'):
        with open(json_file, 'r') as f:
            for line in f:
                try:
                    record = json.loads(line.strip())
                    yield record
                except json.JSONDecodeError:
                    continue

def main():
    print("Starting Kafka Producer...")
    time.sleep(30)  # Initial wait for Kafka and Zookeeper to be ready
    
    try:
        producer = connect_kafka()
        
        # Read and send each record
        for record in read_json_files():
            try:
                # Send the record to the 'activity-data' topic
                future = producer.send('activity-data', value=record)
                # Wait for the send to complete
                record_metadata = future.get(timeout=10)
                print(f"Sent record for user {record.get('User', 'unknown')} to partition {record_metadata.partition}")
                time.sleep(0.1)  # Small delay to not overwhelm the system
            except Exception as e:
                print(f"Error sending record: {e}")
        
        # Flush and close the producer
        producer.flush()
        producer.close()
        print("Producer finished sending data")
    
    except Exception as e:
        print(f"Producer failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
