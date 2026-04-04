from minio import Minio

client = Minio(
    "localhost:9000", access_key="minioadmin", secret_key="minioadmin", secure=False
)

client.fput_object(
    bucket_name="soc",
    object_name="data/logs.csv",
    file_path="data/logs.csv",
)
print("Done, Uploading the CSV.")