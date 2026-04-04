from dagster import asset
import subprocess


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
