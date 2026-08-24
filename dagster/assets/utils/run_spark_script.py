import os


def run_spark(script_name: str, job_id: str | None):

    postgres_url = f"jdbc:postgresql://{os.environ.get('DAGSTER_PG_HOST')}:{os.environ.get('DAGSTER_PG_PORT')}/{os.environ.get('DAGSTER_CUSTOM_PG_DB')}"
    
    env_vars = {
        "SPARK_URL": os.environ.get("SPARK_URL"),
        "NESSIE_ENDPOINT": os.environ.get("NESSIE_ENDPOINT"),
        "NESSIE_BRANCH": os.environ.get("NESSIE_BRANCH"),
        "MINIO_BUCKET_URL": os.environ.get("MINIO_BUCKET_URL"),
        "MINIO_ENDPOINT": os.environ.get("MINIO_ENDPOINT"),
        "MINIO_ACCESS_KEY": os.environ.get("MINIO_ACCESS_KEY"),
        "MINIO_SECRET_KEY": os.environ.get("MINIO_SECRET_KEY"),
        "POSTGRES_URL": postgres_url,
        "POSTGRES_USER": os.environ.get("DAGSTER_PG_USER"),
        "POSTGRES_PASSWORD": os.environ.get("DAGSTER_PG_PASSWORD"),
    }

    command = ["docker", "exec"]

    for key, value in env_vars.items():
        command.extend(["-e", f"{key}={value}"])

    command.extend(
        ["spark-master", "bash", f"/opt/spark-apps/scripts/run_spark.sh", script_name]
    )

    if job_id:
        command.extend([job_id])

    return command
