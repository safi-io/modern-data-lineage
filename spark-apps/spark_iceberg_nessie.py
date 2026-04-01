
# This code sets up a Spark session to read Parquet files from MinIO and write them to an Iceberg table in Nessie.
# It includes the necessary configurations for Spark, Iceberg, and MinIO, and demonstrates how to create a namespace, read data, create an Iceberg table,
# and verify the contents of the table.

from pyspark.sql import SparkSession
from dagster_pipes import PipesContext

# Initialize Pipes
pipes = PipesContext.get()

spark = SparkSession.builder \
    .appName("CSV_to_Iceberg_Nessie") \
    .master("spark://spark:7077") \
    .config("spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1,"
            "org.apache.hadoop:hadoop-aws:3.4.2,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.700") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
    .config("spark.sql.catalog.nessie.uri", "http://nessie:19120/api/v1") \
    .config("spark.sql.catalog.nessie.ref", "main") \
    .config("spark.sql.catalog.nessie.warehouse", "s3a://soc/") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# Create namespace in Nessie
spark.sql("CREATE NAMESPACE IF NOT EXISTS raw")

# Read existing Parquet files from MinIO
df = spark.read.parquet("s3a://soc/logs/logs_Data/")

# Create Iceberg table in Nessie
df.writeTo("raw.stream_ldap").createOrReplace()

# Verify table

pipes.log.info(spark.sql("SELECT count(*) FROM raw.stream_ldap").show())

pipes.report_asset_materialization(metadata={"status": "Iceberg table 'stream_ldap' created successfully in Nessie!"})