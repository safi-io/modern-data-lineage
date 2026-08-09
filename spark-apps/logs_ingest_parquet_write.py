from pyspark.sql import SparkSession
from datetime import datetime
import argparse

# Logging Imports
from logging_config_spark import JobAdapter, base_logger

spark = SparkSession.builder.getOrCreate()

parser = argparse.ArgumentParser()
parser.add_argument("--job_id", required=True)
args = parser.parse_args()

job_id = args.job_id
logger = JobAdapter(base_logger, {"job_id": job_id})

logger.info("Spark job started")

jdbc_url = spark.conf.get("spark.app.postgres.url")
db_user = spark.conf.get("spark.app.postgres.user")
db_password = spark.conf.get("spark.app.postgres.password")

logger.info(f"Connecting to Postgres: {jdbc_url}")

query = f"""
    SELECT
        file_name
    FROM
        file_status
    WHERE
        job_id = '{job_id}'
    AND
        status = 'inprogress'
"""

logger.info("Executing JDBC query")

df = (
    spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", f"({query}) AS files")
    .option("user", db_user)
    .option("password", db_password)
    .option("driver", "org.postgresql.Driver")
    .load()
)

count = df.limit(1).count()
logger.info(f"Records fetched: {count}")

if count == 0:
    logger.warning("No files to process")
    spark.stop()
    exit(0)

files = [row.file_name for row in df.collect()]
logger.info(f"Files to process: {files}")

s3_paths = [f"s3a://soc/{file}" for file in files]
logger.info(f"S3 paths: {s3_paths}")

spark.conf.set("spark.sql.csv.maxCharsPerColumn", "1000000")

logger.info("Reading CSV files from S3")
df = (
    spark.read
    .option("header", "true")
    .option("quote", "\"")    
    .option("escape", "\"")
    .option("multiLine", "true")
    .csv(s3_paths)
)

logger.info(f"CSV rows read: {df.count()}")

logger.info("Ensuring namespace exists")
spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.raw")

logger.info("Writing to Iceberg table")

if spark.catalog.tableExists("nessie.raw.stream_ldap"):
    logger.info("Table Exists")

    # Incoming data as a temp view
    df.createOrReplaceTempView("incoming_data")

    spark.sql("""
        MERGE INTO
            nessie.raw.stream_ldap AS target
        USING
            incoming_data AS source
        ON
            target._serial = source._serial

        WHEN MATCHED THEN
            UPDATE SET *

        WHEN NOT MATCHED THEN
            INSERT *
    """)
else:
    # First time, Table needs to be created and then simply insert the logs.
    logger.info("Creating Table for Raw Data")
    df.writeTo("nessie.raw.stream_ldap").createOrReplace()

archive_path = f"s3a://soc/data/archived/{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}"
logger.info(f"Archiving to: {archive_path}")

df.write.parquet(archive_path)

logger.info("nessie.raw.stream_ldap current count:")

spark.sql("SELECT count(*) FROM nessie.raw.stream_ldap").show()

logger.info("Job completed successfully")

spark.stop()