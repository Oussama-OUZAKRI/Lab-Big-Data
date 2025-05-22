from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from time import sleep
import os

# Initialize Spark Session
spark = SparkSession.builder \
  .appName("Structured Streaming Lab") \
  .config("spark.sql.shuffle.partitions", 5) \
  .master("spark://spark-master:7077") \
  .getOrCreate()

# Création du dossier checkpoints
checkpoint_dir = "/opt/bitnami/spark/checkpoints"
if not os.path.exists(checkpoint_dir):
  os.makedirs(checkpoint_dir)

# 1. Read static version of the dataset
print("1. Reading static dataset...")
static = spark.read.json("/opt/bitnami/spark/data/activity-data")
dataSchema = static.schema

# 2. Create streaming version
print("2. Creating streaming version...")
streaming = spark.readStream.schema(dataSchema) \
  .option("maxFilesPerTrigger", 1) \
  .json("/opt/bitnami/spark/data/activity-data")

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

# 5. Show active streams
print("5. Active streams:")
print(spark.streams.active)

# 6. Query the results
print("6. Querying results every second for 5 seconds...")
time.sleep(5)
for x in range(5):
  spark.sql("SELECT * FROM activity_counts").show()
  sleep(1)

# 7. Transformations example
print("7. Performing transformations...")
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

# 8. Cube aggregation
print("8. Performing cube aggregation...")
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

# 9. Query device stats
print("9. Querying device statistics:")
time.sleep(5)
spark.sql("SELECT * FROM device_model_stats").show()

# 10. Join with historical data
print("10. Joining with historical data...")
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

# 11. Reading from Kafka examples
print("11. Kafka source examples...")

# Subscribe to 1 topic
df1 = spark.readStream.format("kafka")\
  .option("kafka.bootstrap.servers", "kafka:9092")\
  .option("subscribe", "activity-data")\
  .load()

# Subscribe to multiple topics
df2 = spark.readStream.format("kafka")\
  .option("kafka.bootstrap.servers", "kafka:9092")\
  .option("subscribe", "topic1,topic2")\
  .load()

# Subscribe to a pattern
df3 = spark.readStream.format("kafka")\
  .option("kafka.bootstrap.servers", "kafka:9092")\
  .option("subscribePattern", "topic.*")\
  .load()

# 12. Writing to Kafka examples
print("12. Kafka sink examples...")
# Example with topic column
df1.selectExpr("topic", "CAST(key AS STRING)", "CAST(value AS STRING)")\
  .writeStream\
  .format("kafka")\
  .option("kafka.bootstrap.servers", "kafka:9092")\
  .option("checkpointLocation", "./checkpoints")\
  .start()

# Example with topic option
df1.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")\
  .writeStream\
  .format("kafka")\
  .option("kafka.bootstrap.servers", "kafka:9092")\
  .option("checkpointLocation", "./checkpoints")\
  .option("topic", "topic1")\
  .start()

print("\n=== Testing Sources and Sinks ===")
# 13. Socket Source example
print("13. Socket source example...")
socketDF = spark.readStream.format("socket")\
  .option("host", "localhost")\
  .option("port", 9999)\
  .load()

# Console sink example
print("Console sink example...")
activityCounts.writeStream\
  .format("console")\
  .outputMode("complete")\
  .start()

# Memory sink example (already implemented in previous examples)

print("\n=== Trigger Examples ===")
# Processing time trigger (already implemented)

# Once trigger example
print("Once trigger example...")
activityCounts.writeStream\
  .trigger(once=True)\
  .format("console")\
  .outputMode("complete")\
  .start()

# Stop all streams when done
print("Stopping all streams...")
spark.streams.awaitAnyTermination(timeout=10)
for stream in spark.streams.active:
  stream.stop()

print("Lab completed!")
