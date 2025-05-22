from kafka import KafkaConsumer
import json
import time

def main():
    print("Waiting for Kafka to be ready...")
    time.sleep(20)  # Attendre que Kafka soit prêt

    print("Initializing Kafka consumer...")
    # Initialize Kafka consumer with retries
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

    print("Starting to consume messages...")
    try:
        for message in consumer:
            record = message.value
            print(f"""
            Topic: {message.topic}
            User: {record.get('User')}
            Activity: {record.get('gt')}
            Device: {record.get('Device')}
            """)
    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
