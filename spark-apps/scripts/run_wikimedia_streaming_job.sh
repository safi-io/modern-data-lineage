#!/bin/bash

set -e
set -o pipefail

echo "======================"
echo "Spark Streaming Job Launcher"
echo "Timestamp: $(date)"
echo "======================"

# Mock Secrets
SPARK_URL=spark://spark-master:7077
MINIO_BUCKET_URL=s3a://soc/
MINIO_ENDPOINT=http://minio:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin

# Validate variables
echo "Validating variables..."

REQUIRED_VARS=(
  SPARK_URL
  MINIO_BUCKET_URL
  MINIO_ENDPOINT
  MINIO_ACCESS_KEY
  MINIO_SECRET_KEY
)

for VAR in "${REQUIRED_VARS[@]}"; do
  if [ -z "${!VAR}" ]; then
    echo "ERROR: $VAR is not set"
    exit 1
  else
    echo "$VAR is set"
  fi
done

echo "Submitting Spark Streaming Job..."

/opt/spark/bin/spark-submit \
    --master "$SPARK_URL" \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.2,org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.wikimedia=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.wikimedia.type=hadoop \
    --conf spark.sql.catalog.wikimedia.warehouse="$MINIO_BUCKET_URL" \
    --conf spark.hadoop.fs.s3a.endpoint="$MINIO_ENDPOINT" \
    --conf spark.hadoop.fs.s3a.access.key="$MINIO_ACCESS_KEY" \
    --conf spark.hadoop.fs.s3a.secret.key="$MINIO_SECRET_KEY" \
    --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
    --conf spark.hadoop.fs.s3a.threads.keepalivetime=60000 \
    --conf spark.hadoop.fs.s3a.connection.timeout=60000 \
    --conf spark.hadoop.fs.s3a.connection.establish.timeout=60000 \
    --conf spark.hadoop.fs.s3a.multipart.purge.age=86400000 \
    --conf spark.hadoop.fs.s3a.retry.interval=500 \
    --conf spark.hadoop.fs.s3a.retry.throttle.interval=500 \
    "/opt/spark-apps/wikimedia_streaming.py"