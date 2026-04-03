from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.table("nessie.raw.stream_ldap")

df.printSchema()
