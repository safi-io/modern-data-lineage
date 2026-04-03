from dagster import asset, Definitions
import pandas as pd
import dlt
import subprocess


@asset
def csv_to_minio():
    df = pd.read_csv("../data/logs.csv")
    pipeline = dlt.pipeline(
        pipeline_name="from_csv",
        destination="filesystem",
        dataset_name="logs",
    )
    pipeline.run(df, table_name="logs_data")
    return "CSV loaded into minio"


@asset(deps=[csv_to_minio])
def logs_ingest_iceberg_nessie(context):

    command = [
        "docker",
        "exec",
        "spark",
        "bash",
        "/opt/spark-apps/scripts/run_spark.sh",
        "logs_ingest_iceberg_nessie.py",
    ]

    result = subprocess.run(command, capture_output=True, text=True)

    context.log.info(result.stdout)

    if result.returncode != 0:
        raise Exception(f"Logs ingest iceberg-nessie job failed: {result.stderr}")

    return "Logs ingest iceberg-nessie job completed"


@asset
def raw_schema_viewer(context):

    command = [
        "docker",
        "exec",
        "spark",
        "bash",
        "/opt/spark-apps/scripts/run_spark.sh",
        "raw_schema_viewer.py",
    ]

    result = subprocess.run(command, capture_output=True, text=True)

    context.log.info(result.stdout)

    if result.returncode != 0:
        raise Exception(f"Raw schema view job failed: {result.stderr}")

    return "Raw schema view job completed"


defs = Definitions(
    assets=[csv_to_minio, logs_ingest_iceberg_nessie, raw_schema_viewer],
)
