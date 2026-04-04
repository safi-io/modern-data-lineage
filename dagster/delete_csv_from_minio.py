from minio import Minio


def delete_csv():

    client = Minio(
        "minio:9000", access_key="minioadmin", secret_key="minioadmin", secure=False
    )

    bucket_name = "soc"
    folder_prefix = "data/"

    objects = client.list_objects(bucket_name, prefix=folder_prefix, recursive=False)

    for obj in objects:
        if obj.object_name.endswith(".csv"):
            print(f"Deleting {obj.object_name}")
            client.remove_object(bucket_name, obj.object_name)
