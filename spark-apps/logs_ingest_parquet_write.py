from pyspark.sql import SparkSession
from datetime import datetime
spark = SparkSession.builder.getOrCreate()

NEW_CSV_PATH = "s3a://soc/data/*.csv"
ARCHIVED_PARQUET_PATH = f"s3a://soc/data/archived/{datetime.now()}"

# Reading all new CSV Logs Files
df = spark.read.option("header", "true").option("inferSchema", "true").csv(NEW_CSV_PATH)

spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.raw")
df.writeTo("nessie.raw.stream_ldap").createOrReplace()

# Saving back the logs in the parquet format
df.write.parquet(ARCHIVED_PARQUET_PATH)

# Verification
spark.sql("SELECT count(*) FROM nessie.raw.stream_ldap").show()
