from dagster import asset
import subprocess
from .utils.run_spark_script import run_spark

from resources import PostgresResource


@asset(group_name="ops")
def raw_schema_viewer(context):

    command = run_spark("raw_schema_viewer.py")

    result = subprocess.run(command, capture_output=True, text=True)

    context.log.info(result.stdout)

    if result.returncode != 0:
        raise Exception(f"Raw schema view job failed: {result.stderr}")

    return "Raw schema view job completed"


@asset(group_name="ops")
def check_postgres_connection(context, postgres_app: PostgresResource):
    conn = postgres_app.get_connection()
    with conn:
        cursor = conn.cursor()
        cursor.execute("SELECT 1;")
        result = cursor.fetchone()
        context.log.info(f"Postgres connection successful: {result}")
        return result
