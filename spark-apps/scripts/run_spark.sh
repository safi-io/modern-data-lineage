#!/bin/bash

set -e
set -o pipefail

echo "======================"
echo "Spark Job Launcher"
echo "Timestamp: $(date)"
echo "======================"

# Input validation
if [ -z "$1" ]; then
  echo "ERROR: No script provided"
  echo "Usage: $0 <spark_python_script> [job_id]"
  exit 1
fi

SCRIPT="$1"
JOB_ID="$2"

echo "Script: $SCRIPT"
echo "Job ID: $JOB_ID"

# Validate environment variables
echo "Validating environment variables..."

REQUIRED_VARS=(
  SPARK_URL
  NESSIE_ENDPOINT
  NESSIE_BRANCH
  MINIO_BUCKET_URL
  MINIO_ENDPOINT
  MINIO_ACCESS_KEY
  MINIO_SECRET_KEY
  POSTGRES_URL
  POSTGRES_USER
  POSTGRES_PASSWORD
)

for VAR in "${REQUIRED_VARS[@]}"; do
  if [ -z "${!VAR}" ]; then
    echo "ERROR: $VAR is not set"
    exit 1
  else
    echo "$VAR is set"
  fi
done

echo "Submitting Spark Job..."

START_TIME=$(date +%s)

/opt/spark/bin/spark-submit \
    --master "$SPARK_URL" \
    --packages org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.7.3 \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.nessie=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.nessie.catalog-impl=org.apache.iceberg.nessie.NessieCatalog \
    --conf spark.sql.catalog.nessie.uri="$NESSIE_ENDPOINT" \
    --conf spark.sql.catalog.nessie.ref="$NESSIE_BRANCH" \
    --conf spark.sql.catalog.nessie.warehouse="$MINIO_BUCKET_URL" \
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
    --conf spark.hadoop.fs.s3a.assumed.role.session.duration=3600000 \
    --conf spark.hadoop.fs.s3a.delegation.token.duration=3600000 \
    --conf spark.app.postgres.url="$POSTGRES_URL" \
    --conf spark.app.postgres.user="$POSTGRES_USER" \
    --conf spark.app.postgres.password="$POSTGRES_PASSWORD" \
    "/opt/spark-apps/$SCRIPT" \
    --job_id "$JOB_ID" \
    2>&1

EXIT_CODE=$?
END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

echo "Job Summary"
echo "Exit Code: $EXIT_CODE"
echo "Duration: ${DURATION}s"

if [ $EXIT_CODE -ne 0 ]; then
  echo "Spark job FAILED"
  exit $EXIT_CODE
else
  echo "Spark job SUCCEEDED"
fi