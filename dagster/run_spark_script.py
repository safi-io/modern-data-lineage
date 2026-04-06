import os


def run_spark(script_name: str):
    env_vars = {
        "SPARK_URL": os.environ.get("SPARK_URL"),
        "NESSIE_ENDPOINT": os.environ.get("NESSIE_ENDPOINT"),
        "NESSIE_BRANCH": os.environ.get("NESSIE_BRANCH"),
        "MINIO_BUCKET_URL": os.environ.get("MINIO_BUCKET_URL"),
        "MINIO_ENDPOINT": os.environ.get("MINIO_ENDPOINT"),
        "MINIO_ACCESS_KEY": os.environ.get("MINIO_ACCESS_KEY"),
        "MINIO_SECRET_KEY": os.environ.get("MINIO_SECRET_KEY"),
    }

    command = ["docker", "exec"]

    for key, value in env_vars.items():
        command.extend(["-e", f"{key}={value}"])

    command.extend(
        ["spark", "bash", f"/opt/spark-apps/scripts/run_spark.sh", script_name]
    )

    return command
