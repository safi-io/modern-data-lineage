from minio import Minio

client = Minio(
    "localhost:8999", access_key="minioadmin", secret_key="minioadmin", secure=False
)

BUCKET_NAME = "soc"

if not client.bucket_exists(BUCKET_NAME):
    client.make_bucket(BUCKET_NAME)
    print(f"Bucket '{BUCKET_NAME}' created successfully.")
else:
    print(f"Bucket '{BUCKET_NAME}' already exists.")

client.fput_object(
    bucket_name=BUCKET_NAME,
    object_name="data/logs.csv",
    file_path="data/logs.csv",
)
print("Done, Uploading the CSV.")
