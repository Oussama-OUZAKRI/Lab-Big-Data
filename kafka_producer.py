from kafka import KafkaProducer
import json
import time
from pathlib import Path

# Initialize Kafka producer with retries
producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8'),
    retries=5,
    retry_backoff_ms=1000,
    max_block_ms=60000
)

def read_json_files():
    data_path = Path('/app/data/activity-data')
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
    time.sleep(10)  # Wait for Kafka to be ready
    
    # Read and send each record
    for record in read_json_files():
        try:
            # Send the record to the 'activity-data' topic
            producer.send('activity-data', value=record)
            print(f"Sent record for user {record.get('User', 'unknown')}")
            time.sleep(0.1)  # Small delay to not overwhelm the system
        except Exception as e:
            print(f"Error sending record: {e}")
    
    # Flush and close the producer
    producer.flush()
    producer.close()

if __name__ == "__main__":
    main()
