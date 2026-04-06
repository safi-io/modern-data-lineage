from dagster import asset
import subprocess

from run_spark_script import run_spark

@asset
def raw_schema_viewer(context):
    
    command = run_spark("raw_schema_viewer.py")
    
    result = subprocess.run(command, capture_output=True, text=True)

    context.log.info(result.stdout)

    if result.returncode != 0:
        raise Exception(f"Raw schema view job failed: {result.stderr}")

    return "Raw schema view job completed"
