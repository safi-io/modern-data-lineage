import subprocess

from dagster import op

@op
def run_meltano_pg_ch(context):

    process = subprocess.Popen(
        ["meltano", "run", "pg-ch"],
        cwd="../meltano",
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True
    )

    for line in process.stdout:
        context.log.info(line.rstrip())

    process.wait()

    if process.returncode != 0:
        raise Exception("Meltano failed")