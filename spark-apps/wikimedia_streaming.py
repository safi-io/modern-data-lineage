from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession
    .builder
    .appName("wikimedia_streaming")
    .getOrCreate()
)

df = (
    spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "wikimedia-recent-changes")
    .option("startingOffsets", "latest")
    .load()
)

result = df.select(
    F.col("key").cast("string").alias("key"),
    F.col("value").cast("string").alias("value"),
    F.col("topic"),
    F.col("partition"),
    F.col("offset"),
    F.col("timestamp")
)

query = (
    result.writeStream
    .format("iceberg")
    .outputMode("append")
    .option(
        "checkpointLocation",
        "s3a://soc/checkpoints/wikimedia/"
    )
    .toTable("wikimedia.raw.recent_changes")
)

query.awaitTermination()

# Console Sink

# query = result.writeStream \
#     .format("console") \
#     .outputMode("append") \
#     .option("truncate", False) \
#     .start()

# Parquet Sink (In Container)

# query = result.writeStream \
#     .format("parquet") \
#     .option("path", "/data/output") \
#     .option("checkpointLocation", "/data/checkpoint") \
#     .outputMode("append") \
#     .start()

# Command to run this script
# /opt/spark/bin/spark-submit \
#     --master "spark://spark-master:7077" \
#     --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.2 \
#     /opt/spark-apps/wikimedia_streaming.py