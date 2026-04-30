from pyspark.sql import SparkSession
from datetime import datetime
import argparse
from urllib.parse import urlparse


spark = SparkSession.builder.getOrCreate()

# Parse job_id
parser = argparse.ArgumentParser()
parser.add_argument("--job_id", required=True)
args = parser.parse_args()

job_id = args.job_id

# Read DB config from Spark conf
jdbc_url = spark.conf.get("spark.app.postgres.url")
db_user = spark.conf.get("spark.app.postgres.user")
db_password = spark.conf.get("spark.app.postgres.password")

# Build query safely
query = f"""
(SELECT file_name
 FROM file_status
 WHERE job_id = '{job_id}'
   AND status = 'inprogress')
"""

# Read from Postgres
df = (
    spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", query)
    .option("user", db_user)
    .option("password", db_password)
    .option("driver", "org.postgresql.Driver")
    .load()
)

if df.rdd.isEmpty():
    print(f"No files to process for job_id={job_id}")
    spark.stop()
    exit(0)

files = [row.file_name for row in df.collect()]
s3_paths = [f"s3a://soc/{file}" for file in files]

if not s3_paths:
    print("No files to process")
    spark.stop()
    exit(0)

df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(s3_paths)
)

# Iceberg write
spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.raw")

df.writeTo("nessie.raw.stream_ldap").append()

# Archive path
ARCHIVED_PARQUET_PATH = f"s3a://soc/data/archived/{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}"

df.write.parquet(ARCHIVED_PARQUET_PATH)

# Verification
spark.sql("SELECT count(*) FROM nessie.raw.stream_ldap").show()