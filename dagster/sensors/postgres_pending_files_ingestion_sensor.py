import dagster as dg
from dagster import sensor

from jobs import bronze_logs_ingestion_job
from resources import PostgresResource

import uuid

from .utils.postgres_utils import has_new_files, mark_files_inprogress


@sensor(
    job=bronze_logs_ingestion_job,
    minimum_interval_seconds=50,
    default_status=dg.DefaultSensorStatus.STOPPED,
)
def postgres_pending_files_ingestion_sensor(postgres_app: PostgresResource):

    conn = postgres_app.get_connection()
    cursor = conn.cursor()

    try:
        if not has_new_files(cursor):
            # It means there is no CSV file with a status 'new'
            yield dg.SkipReason("No new CSV files found")
            return

        job_id = str(uuid.uuid4())

        mark_files_inprogress(cursor, job_id)

        conn.commit()

        yield dg.RunRequest(
            run_key=f"job-{job_id}",
            run_config={
                "ops": {"raw__stream_ldap": {"config": {"job_id": job_id}}}
            },
        )

    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
