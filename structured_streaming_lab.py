from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col
from time import sleep
import os
import signal
import sys
from datetime import datetime
from typing import List, Optional
from pyspark.sql.streaming import StreamingQuery

# Get data path from environment variable or use default
DATA_PATH = os.getenv('SPARK_DATA_PATH', '/opt/bitnami/spark/data/activity-data')
CHECKPOINT_DIR = os.getenv('SPARK_CHECKPOINT_DIR', '/opt/bitnami/spark/checkpoints')

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    print("\nShutdown signal received. Stopping all streams...")
    stop_all_streams()
    sys.exit(0)

def stop_all_streams():
    """Stop all active streams gracefully"""
    if not hasattr(spark, 'streams'):
        return
    
    active_streams = spark.streams.active
    for stream in active_streams:
        try:
            if stream.isActive:
                stream.stop()
                print(f"Successfully stopped stream: {stream.name}")
        except Exception as e:
            print(f"Error stopping stream {stream.name}: {e}")
    print(f"{len(active_streams)} streams were stopped.")

def wait_for_query(query: StreamingQuery, timeout: int = 10):
    """Wait for a streaming query to process some data"""
    if query is None:
        return False
    try:
        query.awaitTermination(timeout)
        return not query.isActive or query.lastProgress is not None
    except Exception as e:
        print(f"Error waiting for query: {e}")
        return False

def verify_kafka_topics():
    """Verify that required Kafka topics exist"""
    from kafka.admin import KafkaAdminClient
    admin_client = KafkaAdminClient(bootstrap_servers=['kafka:9092'])
    try:
        topics = admin_client.list_topics()
        print(f"Available Kafka topics: {topics}")
        return "activity-data" in topics
    except Exception as e:
        print(f"Error verifying Kafka topics: {e}")
        return False

def create_streaming_df(schema):
    """Create a new streaming DataFrame with the given schema"""
    return spark.readStream.schema(schema) \
        .option("maxFilesPerTrigger", 1) \
        .json(DATA_PATH)

# Register signal handlers
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Structured Streaming Lab") \
    .config("spark.sql.shuffle.partitions", 5) \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Create checkpoints directory
checkpoint_dir = CHECKPOINT_DIR
if not os.path.exists(checkpoint_dir):
    os.makedirs(checkpoint_dir)

# 1. Read static version of the dataset
print("1. Reading static dataset...")
static = spark.read.json(DATA_PATH)
dataSchema = static.schema

# 2. Create streaming version
print("2. Creating streaming version...")
streaming = create_streaming_df(dataSchema)

# 3. Group and count by activity
print("3. Grouping by activity...")
activityCounts = streaming.groupBy("gt").count()

# 4. Write to memory sink
print("4. Writing to memory sink...")
activityQuery = activityCounts.writeStream \
    .queryName("activity_counts") \
    .format("memory") \
    .outputMode("complete") \
    .option("checkpointLocation", f"{checkpoint_dir}/activity_counts") \
    .start()

# 5. Show active streams and wait for data
print("5. Active streams:")
print(spark.streams.active)
if not wait_for_query(activityQuery):
    print("Warning: Activity query may not have processed any data yet")

# 6. Query the results
print("6. Querying results...")
for x in range(5):
    print(f"\nQuery {x + 1} at {datetime.now()}")
    spark.sql("SELECT * FROM activity_counts").show()
    sleep(2)  # Interval between queries

# 7. Transformations example
print("7. Performing transformations...")
streaming = create_streaming_df(dataSchema)  # Recreate streaming DataFrame
simpleTransform = streaming.withColumn("stairs", expr("gt like '%stairs%'")) \
    .where("stairs") \
    .where("gt is not null") \
    .select("gt", "model", "arrival_time", "creation_time") \
    .writeStream \
    .queryName("simple_transform") \
    .format("memory") \
    .outputMode("append") \
    .option("checkpointLocation", f"{checkpoint_dir}/simple_transform") \
    .start()

sleep(5)  # Wait for data to arrive

# 8. Cube aggregation
print("8. Performing cube aggregation...")
streaming = create_streaming_df(dataSchema)  # Recreate streaming DataFrame
deviceModelStats = streaming.cube("gt", "model").avg() \
    .drop("avg(Arrival_time)") \
    .drop("avg(Creation_Time)") \
    .drop("avg(Index)") \
    .writeStream \
    .queryName("device_model_stats") \
    .format("memory") \
    .outputMode("complete") \
    .option("checkpointLocation", f"{checkpoint_dir}/device_model_stats") \
    .start()

# 9. Query device stats after waiting for data
print("9. Querying device statistics:")
sleep(5)  # Wait for data to arrive
spark.sql("SELECT * FROM device_model_stats").show()

# 10. Join with historical data
print("10. Joining with historical data...")
streaming = create_streaming_df(dataSchema)  # Recreate streaming DataFrame
historicalAgg = static.groupBy("gt", "model").avg()
joinedStats = streaming.drop("Arrival_Time", "Creation_Time", "Index") \
    .cube("gt", "model").avg() \
    .join(historicalAgg, ["gt", "model"]) \
    .writeStream \
    .queryName("joined_stats") \
    .format("memory") \
    .outputMode("complete") \
    .option("checkpointLocation", f"{checkpoint_dir}/joined_stats") \
    .start()

sleep(5)  # Wait for join results

# 11. Kafka Integration
print("11. Verifying and using Kafka...")

# Verify Kafka topics before proceeding
if verify_kafka_topics():
    print("Setting up Kafka streams...")
    
    try:
        # Read from activity-data topic
        kafka_df = spark.readStream.format("kafka")\
            .option("kafka.bootstrap.servers", "kafka:9092")\
            .option("subscribe", "activity-data")\
            .load()
        
        # Parse JSON from Kafka messages
        kafka_query = kafka_df.selectExpr("CAST(value AS STRING) as json")\
            .writeStream\
            .queryName("kafka_messages")\
            .format("memory")\
            .outputMode("append")\
            .option("checkpointLocation", f"{checkpoint_dir}/kafka")\
            .start()
        
        print("Waiting for Kafka messages...")
        sleep(10)  # Give more time for messages to arrive
        
        # Query and show Kafka messages
        print("Querying Kafka messages:")
        spark.sql("SELECT * FROM kafka_messages").show(truncate=False)
        
    except Exception as e:
        print(f"Error processing Kafka stream: {e}")
else:
    print("Skipping Kafka operations - required topics not found")

print("\n=== Final Section: Testing Triggers ===")

# Process time trigger example
print("\n=== Testing Additional Sources and Sinks ===")

# Final streaming query with different trigger types
streaming = create_streaming_df(dataSchema)  # Create fresh streaming DataFrame

# Process time trigger (default)
processTimeQuery = streaming.groupBy("gt").count()\
    .writeStream\
    .queryName("process_time_trigger")\
    .format("memory")\
    .outputMode("complete")\
    .option("checkpointLocation", f"{checkpoint_dir}/process_time")\
    .start()

sleep(5)  # Wait for some data

# Once trigger
onceQuery = streaming.groupBy("gt").count()\
    .writeStream\
    .queryName("once_trigger")\
    .format("console")\
    .outputMode("complete")\
    .trigger(once=True)\
    .option("checkpointLocation", f"{checkpoint_dir}/once")\
    .start()

# Final cleanup
print("\nWaiting for all streams to complete processing...")
spark.streams.awaitAnyTermination(timeout=30)

# Stop all streams gracefully
stop_all_streams()

print("\nLab completed successfully!")
print(f"- Checkpoints directory: {CHECKPOINT_DIR}")
print(f"- Data directory: {DATA_PATH}")
print("You can check the checkpoints directory for stream state information.")

if __name__ == "__main__":
    try:
        # Main code is above
        pass
    except KeyboardInterrupt:
        print("\nReceived keyboard interrupt. Shutting down...")
        stop_all_streams()
    except Exception as e:
        print(f"\nError in main execution: {e}")
        stop_all_streams()
        sys.exit(1)
    finally:
        print("\nExiting application...")
