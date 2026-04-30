import dagster as dg
from dagster import sensor

from jobs import ingestion_job, psql_new_files
from resources import MinioResource, PostgresResource

import uuid


def check_for_csv(minio: MinioResource) -> bool | list[str]:

    client = minio.get_client()

    bucket_name = "soc"
    folder_prefix = "data/"

    objects = list(
        client.list_objects(bucket_name, prefix=folder_prefix, recursive=False)
    )

    csv_files = [obj.object_name for obj in objects if obj.object_name.endswith(".csv")]

    return csv_files if csv_files else False


@sensor(
    job=psql_new_files,
    minimum_interval_seconds=60,
    default_status=dg.DefaultSensorStatus.STOPPED,
)
def s3_csv_check_sensor(context, minio: MinioResource):

    sensor_response = check_for_csv(minio)

    if not sensor_response:
        yield dg.SkipReason("No CSV files found")
        return

    yield dg.RunRequest(
        run_config={"ops": {"process_files": {"config": {"files": sensor_response}}}},
    )


@sensor(
    job=ingestion_job,
    minimum_interval_seconds=50,
    default_status=dg.DefaultSensorStatus.STOPPED,
)
def ingestion_sensor(postgres_app: PostgresResource):

    conn = postgres_app.get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            SELECT 1 FROM file_status
            WHERE status = 'new'
            LIMIT 1;
        """)

        row = cursor.fetchone()

        if not row:
            yield dg.SkipReason("No new CSV files found")
            return

        job_id = str(uuid.uuid4())

        cursor.execute(
            """
            UPDATE file_status
            SET status = 'inprogress',
                job_id = %s,
                started_at = NOW(),
                updated_at = NOW()
            WHERE status = 'new';
            """,
            (job_id,),
        )

        conn.commit()

        yield dg.RunRequest(
            run_key=f"job-{job_id}",
            run_config={
                "ops": {"logs_ingest_parquet_write": {"config": {"job_id": job_id}}}
            },
        )

    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
