from dagster import asset

from .utils.run_spark_script import run_spark

import subprocess


@asset(required_resource_keys={"postgres_app"}, key=["raw", "stream_ldap"])
def logs_ingest_parquet_write(context):

    job_id = context.op_config["job_id"]

    command = run_spark("logs_ingest_parquet_write.py", job_id)

    context.log.info(f"Running: {command}")

    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,   
        text=True,
        bufsize=1                 
    )

    spark_logs = []

    for line in process.stdout:
        cleaned = line.rstrip()
        context.log.info(cleaned)
        spark_logs.append(cleaned)

    process.wait()

    full_logs = "\n".join(spark_logs)

    postgres = context.resources.postgres_app
    conn = postgres.get_connection()

    try:
        cur = conn.cursor()

        if process.returncode == 0:
            cur.execute("""
                UPDATE file_status
                SET status = 'success',
                    finished_at = NOW(),
                    updated_at = NOW()
                WHERE job_id = %s
                AND status = 'inprogress'
            """, (job_id,))
            context.log.info(f"Job {job_id} marked SUCCESS")

        else:
            cur.execute("""
                UPDATE file_status
                SET status = 'failed',
                    finished_at = NOW(),
                    updated_at = NOW()
                WHERE job_id = %s
                AND status = 'inprogress'
            """, (job_id,))
            context.log.error(f"Job {job_id} marked FAILED")

        conn.commit()
        cur.close()

    finally:
        conn.close()

    if process.returncode != 0:
        raise Exception("Spark job failed")

    return "Spark job completed"