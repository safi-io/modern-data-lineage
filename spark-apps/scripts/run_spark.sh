#!/bin/bash

set -e
set -o pipefail

LOG_FILE="/tmp/spark_job_$(date +%Y%m%d_%H%M%S).log"

echo "======================" | tee -a "$LOG_FILE"
echo "Spark Job Launcher" | tee -a "$LOG_FILE"
echo "Timestamp: $(date)" | tee -a "$LOG_FILE"
echo "======================" | tee -a "$LOG_FILE"

# Input validation
if [ -z "$1" ]; then
  echo "ERROR: No script provided" | tee -a "$LOG_FILE"
  echo "Usage: $0 <spark_python_script> [job_id]" | tee -a "$LOG_FILE"
  exit 1
fi

SCRIPT="$1"
JOB_ID="$2"

echo "Script: $SCRIPT" | tee -a "$LOG_FILE"
echo "Job ID: $JOB_ID" | tee -a "$LOG_FILE"

# Validate environment variables
echo "Validating environment variables..." | tee -a "$LOG_FILE"

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
    echo "ERROR: $VAR is not set" | tee -a "$LOG_FILE"
    exit 1
  else
    echo "$VAR is set" | tee -a "$LOG_FILE"
  fi
done

echo "Submitting Spark Job..." | tee -a "$LOG_FILE"

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
    2>&1 | tee -a "$LOG_FILE"

EXIT_CODE=${PIPESTATUS[0]}
END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

echo "Job Summary" | tee -a "$LOG_FILE"
echo "Exit Code: $EXIT_CODE" | tee -a "$LOG_FILE"
echo "Duration: ${DURATION}s" | tee -a "$LOG_FILE"
echo "Log File: $LOG_FILE" | tee -a "$LOG_FILE"

if [ $EXIT_CODE -ne 0 ]; then
  echo "Spark job FAILED" | tee -a "$LOG_FILE"
  exit $EXIT_CODE
else
  echo "Spark job SUCCEEDED" | tee -a "$LOG_FILE"
fi