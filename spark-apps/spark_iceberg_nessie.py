from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.raw")

df = spark.read.parquet("s3a://soc/logs/logs_data/")

df.writeTo("nessie.raw.stream_ldap").createOrReplace()

spark.sql("SELECT count(*) FROM nessie.raw.stream_ldap").show()