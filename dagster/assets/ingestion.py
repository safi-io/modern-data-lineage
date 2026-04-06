from dagster import asset

from resources import MinioResource

from run_spark_script import run_spark

import subprocess


@asset()
def logs_ingest_parquet_write(context):

    command = run_spark("logs_ingest_parquet_write.py")
    
    context.log.info(command)
    
    result = subprocess.run(command, capture_output=True, text=True)

    if result.returncode != 0:
        raise Exception(f"Logs ingest parquet-write job failed: {result.stderr}")

    return "Logs ingest parquet-write job completed"


@asset(deps=["logs_ingest_parquet_write"])
def delete_csv_after_ingestion(context, minio: MinioResource):

    client = minio.get_client()

    bucket_name = "soc"
    folder_prefix = "data/"

    objects = client.list_objects(bucket_name, prefix=folder_prefix, recursive=False)

    for obj in objects:
        if obj.object_name.endswith(".csv"):
            context.log.info(f"Deleting {obj.object_name}")
            client.remove_object(bucket_name, obj.object_name)
