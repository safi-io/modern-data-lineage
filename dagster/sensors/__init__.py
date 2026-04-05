import dagster as dg
from dagster import RunRequest, sensor

from jobs import ingestion_job


def check_for_csv() -> bool | list[str]:

    from minio import Minio

    client = Minio(
        "minio:9000", access_key="minioadmin", secret_key="minioadmin", secure=False
    )

    bucket_name = "soc"
    folder_prefix = "data/"

    objects = list(
        client.list_objects(bucket_name, prefix=folder_prefix, recursive=False)
    )

    csv_files = [obj.object_name for obj in objects if obj.object_name.endswith(".csv")]

    return csv_files if csv_files else False


@sensor(
    job=ingestion_job,
    minimum_interval_seconds=3600,
    default_status=dg.DefaultSensorStatus.STOPPED,
)
def ingestion_sensor():

    sensor_response = check_for_csv()

    if not sensor_response:
        yield dg.SkipReason("No new CSV files found")
        return

    yield dg.RunRequest(run_key="-".join(sorted(sensor_response)))
