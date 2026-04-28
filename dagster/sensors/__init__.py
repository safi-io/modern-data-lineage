import dagster as dg
from dagster import sensor

from jobs import ingestion_job, psql_new_files
from resources import MinioResource


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
    job=ingestion_job,
    minimum_interval_seconds=180,
    default_status=dg.DefaultSensorStatus.STOPPED,
)
def ingestion_sensor(minio: MinioResource):

    # Convert the functionality from checking minio and then processing CSV's
    # Just check PSQL and then process CSV's according to the file status.

    sensor_response = check_for_csv(minio)

    if not sensor_response:
        yield dg.SkipReason("No new CSV files found")
        return

    yield dg.RunRequest(run_key="-".join(sorted(sensor_response)))


@sensor(
    job=psql_new_files,
    minimum_interval_seconds=60,
    default_status=dg.DefaultSensorStatus.STOPPED,
)
def s3_csv_check_sensor(context, minio: MinioResource):

    sensor_response = check_for_csv(minio)

    if not sensor_response:
        yield dg.SkipReason("No new CSV files found")
        return

    yield dg.RunRequest(
        run_key="-".join(sorted(sensor_response)),
        run_config={"ops": {"process_files": {"config": {"files": sensor_response}}}},
    )
