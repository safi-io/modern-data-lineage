from resources import MinioResource

BUCKET_NAME = "soc"
FOLDER_PREFIX = "data/"


def list_csv_files_from_s3(minio: MinioResource) -> list[str]:

    client = minio.get_client()

    objects = list(
        client.list_objects(BUCKET_NAME, prefix=FOLDER_PREFIX, recursive=False)
    )

    return [obj.object_name for obj in objects if obj.object_name.endswith(".csv")]
