from kafka import KafkaConsumer
import json

def main():
    # Initialize Kafka consumer
    consumer = KafkaConsumer(
        'activity-data',
        bootstrap_servers=['kafka:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='activity_consumer_group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print("Starting Kafka Consumer...")
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
