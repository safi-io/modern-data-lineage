from dagster import asset, Definitions
import pandas as pd
import dlt
import subprocess

@asset
def logs_data():
    df = pd.read_csv("../data/logs.csv")
    pipeline = dlt.pipeline(
        pipeline_name="from_csv",
        destination="filesystem",
        dataset_name="logs",
    )
    pipeline.run(df, table_name="logs_data")
    return "loaded"


@asset(deps=[logs_data])
def spark_processed_data(context):

    command = [
        "docker", "exec", "spark",
        "bash",
        "/opt/spark-apps/run_spark.sh",
        "spark_iceberg_nessie.py"
    ]
    
    result = subprocess.run(command, capture_output=True, text=True)

    context.log.info(result.stdout)

    if result.returncode != 0:
        raise Exception(f"Spark job failed: {result.stderr}")

    return "spark job completed"

@asset
def schema_viewer(context):
    
    command = [
        "docker", "exec", "spark",
        "bash",
        "/opt/spark-apps/run_spark.sh",
        "schema_viewer.py"
    ]

    result = subprocess.run(command, capture_output=True, text=True)

    context.log.info(result.stdout)

    if result.returncode != 0:
        raise Exception(f"Spark schema view job failed: {result.stderr}")

    return "spark schema view job completed"
   

defs = Definitions(
    assets=[logs_data, spark_processed_data, schema_viewer],
)
