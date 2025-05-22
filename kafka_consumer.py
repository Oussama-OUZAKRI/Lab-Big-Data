import json
import time
import sys
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

def connect_kafka():
    retries = 5
    for i in range(retries):
        try:
            print(f"Attempting to connect to Kafka (attempt {i+1}/{retries})...")
            consumer = KafkaConsumer(
                'activity-data',
                bootstrap_servers=['kafka:9092'],
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='activity_consumer_group',
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                consumer_timeout_ms=60000,
                session_timeout_ms=60000,
                request_timeout_ms=65000
            )
            print("Successfully connected to Kafka!")
            return consumer
        except NoBrokersAvailable:
            if i < retries - 1:
                print("Kafka not yet available, waiting 10 seconds...")
                time.sleep(10)
            else:
                print("Failed to connect to Kafka after multiple retries")
                raise
        except Exception as e:
            print(f"Unexpected error: {e}")
            raise

def main():
    print("Starting Kafka Consumer...")
    time.sleep(30)  # Initial wait for Kafka to be ready
    
    try:
        consumer = connect_kafka()
        
        print("Starting to consume messages...")
        try:
            for message in consumer:
                record = message.value
                print(f"""
Topic: {message.topic}
Partition: {message.partition}
Offset: {message.offset}
User: {record.get('User')}
Activity: {record.get('gt')}
Device: {record.get('Device')}
                """)
        except KeyboardInterrupt:
            print("Stopping consumer...")
        finally:
            consumer.close()
            print("Consumer closed")
    
    except Exception as e:
        print(f"Consumer failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()