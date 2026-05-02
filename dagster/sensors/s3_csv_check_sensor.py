import dagster as dg
from dagster import sensor

from jobs import psql_new_files
from resources import MinioResource

from .utils.s3_utils import list_csv_files_from_s3


@sensor(
    job=psql_new_files,
    minimum_interval_seconds=60,
    default_status=dg.DefaultSensorStatus.STOPPED,
)
def s3_csv_check_sensor(context, minio: MinioResource):

    sensor_response = list_csv_files_from_s3(minio)

    if not sensor_response:
        # No new CSV files are found from S3, Skipping the Trigger
        yield dg.SkipReason("No CSV files found")
        return

    yield dg.RunRequest(
        run_config={"ops": {"process_files": {"config": {"files": sensor_response}}}},
    )
