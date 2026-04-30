from dagster import asset

from resources import MinioResource

from run_spark_script import run_spark

import subprocess

@asset(required_resource_keys={"postgres_app"})
def logs_ingest_parquet_write(context):

    job_id = context.op_config["job_id"]

    command = run_spark("logs_ingest_parquet_write.py", job_id)

    context.log.info(command)

    result = subprocess.run(command, capture_output=True, text=True)

    context.log.info("STDOUT:\n" + result.stdout)
    context.log.error("STDERR:\n" + result.stderr)

    postgres = context.resources.postgres_app

    conn = postgres.get_connection()

    try:
        cur = conn.cursor()

        if result.returncode == 0:
            cur.execute(
                """
                UPDATE file_status
                SET status = 'success',
                    finished_at = NOW(),
                    updated_at = NOW()
                WHERE job_id = %s
                AND status = 'inprogress'
            """,
                (job_id,),
            )

            context.log.info(f"Job {job_id} marked SUCCESS")

        else:
            cur.execute(
                """
                UPDATE file_status
                SET status = 'failed',
                    finished_at = NOW(),
                    updated_at = NOW()
                WHERE job_id = %s
                AND status = 'inprogress'
            """,
                (job_id,),
            )

            context.log.error(f"Job {job_id} marked FAILED")

        conn.commit()
        cur.close()

    finally:
        conn.close()

    if result.returncode != 0:
        raise Exception("Spark job failed:\n" + result.stdout + "\n" + result.stderr)

    return "Logs ingest parquet-write job completed"


# @asset(deps=["logs_ingest_parquet_write"])
# def delete_csv_after_ingestion(context, minio: MinioResource):

#     client = minio.get_client()

#     bucket_name = "soc"
#     folder_prefix = "data/"

#     objects = client.list_objects(bucket_name, prefix=folder_prefix, recursive=False)

#     for obj in objects:
#         if obj.object_name.endswith(".csv"):
#             context.log.info(f"Deleting {obj.object_name}")
#             client.remove_object(bucket_name, obj.object_name)
