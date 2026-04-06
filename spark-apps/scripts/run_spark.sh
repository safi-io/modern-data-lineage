#!/bin/bash

if [ -z "$1" ]; then
  echo "Usage: $0 <spark_python_script>"
  exit 1
fi

SCRIPT="$1"

 /opt/spark/bin/spark-submit \
    --master $SPARK_URL \
    --packages org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.nessie=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.nessie.catalog-impl=org.apache.iceberg.nessie.NessieCatalog \
    --conf spark.sql.catalog.nessie.uri=$NESSIE_ENDPOINT \
    --conf spark.sql.catalog.nessie.ref=$NESSIE_BRANCH \
    --conf spark.sql.catalog.nessie.warehouse=$MINIO_BUCKET_URL \
    --conf spark.hadoop.fs.s3a.endpoint=$MINIO_ENDPOINT \
    --conf spark.hadoop.fs.s3a.access.key=$MINIO_ACCESS_KEY \
    --conf spark.hadoop.fs.s3a.secret.key=$MINIO_SECRET_KEY \
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
    "/opt/spark-apps/$SCRIPT"