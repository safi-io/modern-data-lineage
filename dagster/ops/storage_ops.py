from dagster import op
from resources import MinioResource


@op(required_resource_keys={"minio"})
def delete_csv_after_ingestion(context):
    minio: MinioResource = context.resources.minio

    client = minio.get_client()

    bucket_name = "soc"
    folder_prefix = "data/"

    objects = client.list_objects(bucket_name, prefix=folder_prefix, recursive=False)

    for obj in objects:
        if obj.object_name.endswith(".csv"):
            context.log.info(f"Deleting {obj.object_name}")
            client.remove_object(bucket_name, obj.object_name)
